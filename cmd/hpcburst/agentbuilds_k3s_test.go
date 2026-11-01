package main

// up and status check the agent builds against the PlatformProfiles of a real API server (#158).
// The profile has an x86_64 partition and an aarch64 one, as on Athena and Helios: a profile of the
// host's own cpuArch only would pass whatever the builds for the other were. The digests the test
// expects are read from internal/launcher/frp-pins.txt and computed from bytes the test chose, not
// asked of the launcher.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

const exampleProfile = `apiVersion: hpcburst.groundnuty.github.io/v1alpha1
kind: PlatformProfile
metadata:
  name: example-batch
spec:
  providerKind: batch
  partitions:
  - name: example-gpu
    gpuModel: GH200
    cpuArch: aarch64
  - name: example-cpu
    gpuModel: A100
    cpuArch: x86_64
`

// pinnedFrpc reads the frpc digests of the pinned frp release out of frp-pins.txt by cpuArch
// (columns: version, architecture, tarball, tarball sha, frps sha, frpc sha).
func pinnedFrpc(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "internal", "launcher", "frp-pins.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		c := strings.Fields(sc.Text())
		if len(c) == 6 && !strings.HasPrefix(c[0], "#") && c[0] == "0.71.1" {
			out[map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[c[1]]] = c[5]
		}
	}
	if out["x86_64"] == "" || out["aarch64"] == "" {
		t.Fatalf("frp-pins.txt pins %v", out)
	}
	return out
}

type buildsStatus struct {
	Health  string   `json:"health"`
	Reasons []string `json:"reasons"`

	Supervisor *struct {
		Running bool `json:"running"`
	} `json:"supervisor"`
	AgentBuilds *struct {
		Checked  bool `json:"checked"`
		Profiles int  `json:"profiles"`
		Problems []struct {
			CPUArch string `json:"cpu_arch"`
			What    string `json:"what"`
			Fix     string `json:"fix"`
		} `json:"problems"`
	} `json:"agent_builds"`
}

func statusBuilds(t *testing.T, h *host, extra ...string) (buildsStatus, int) {
	t.Helper()
	r := cli(t, h.args(append([]string{"status", "--json", "--no-gc-probe"}, extra...)...)...)
	var s buildsStatus
	parseJSON(t, r.out, &s)
	return s, r.code
}

func TestUpAndStatusCheckTheAgentBuilds(t *testing.T) {
	const base = 29220
	h := newHost(t)
	ctl, _ := fakeController(t)
	_, realFrpc := testenv.FrpBinaries(t)
	pins := pinnedFrpc(t)

	manifests := shortTmp(t, false)
	crd, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "hpcburst.groundnuty.github.io_platformprofiles.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"10-crd.yaml": string(crd), "20-profile.yaml": exampleProfile} {
		if err := os.WriteFile(filepath.Join(manifests, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// ---- up with the profile and no builds: the host runs, up says what is missing and exits 4 ----
	r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", ctl, "--manifests", manifests)...)...)
	if r.code != exitBuilds {
		t.Fatalf("up exited %d, want %d (the host runs, the builds are missing)", r.code, exitBuilds)
	}
	var up struct {
		PID         int `json:"pid"`
		AgentBuilds struct {
			Problems []struct{ CPUArch string } `json:"problems"`
		} `json:"agent_builds"`
	}
	parseJSON(t, r.out, &up)
	if len(up.AgentBuilds.Problems) != 4 {
		t.Errorf("up reports %d problems, want the agent build and the frpc of both cpuArchs", len(up.AgentBuilds.Problems))
	}
	buildsDir := filepath.Join(h.dir, "agent-builds")
	for _, want := range []string{
		"ERROR:", "cpuArch aarch64", "cpuArch x86_64", "hb-agent-aarch64 is missing from " + buildsDir,
		"hack/build-agents.sh " + buildsDir + " aarch64", "hack/fetch-frp.sh --agent-builds " + buildsDir,
		"left running", "hpcburst status --state-dir " + h.dir,
	} {
		if !strings.Contains(r.err, want) {
			t.Errorf("up's stderr lacks %q:\n%s", want, r.err)
		}
	}
	// The stack was left running: k3s is the recorded process, and the supervisor runs.
	if procs := recordedProcs(t, h.dir); len(procs) != 2 || !running(procs[0].PID) || !running(procs[1].PID) || procs[0].PID != up.PID {
		t.Fatalf("the host is not left running: %+v", procs)
	}

	// ---- status: unhealthy for the same reasons; --no-api says it did not look ----
	s, code := statusBuilds(t, h)
	if code != exitFail || s.Health != "unhealthy" || s.AgentBuilds == nil || !s.AgentBuilds.Checked || s.AgentBuilds.Profiles != 1 ||
		len(s.AgentBuilds.Problems) != 4 || s.Supervisor == nil || !s.Supervisor.Running {
		t.Fatalf("status exited %d: %+v", code, s)
	}
	if text := cli(t, h.args("status", "--no-gc-probe")...); !strings.Contains(text.out, "aarch64: hb-agent-aarch64 MISSING, frpc-aarch64 MISSING") {
		t.Errorf("the text status does not list the builds per cpuArch:\n%s", text.out)
	}
	if s, code := statusBuilds(t, h, "--no-api"); code != exitOK || s.AgentBuilds == nil || s.AgentBuilds.Checked {
		t.Errorf("status --no-api exited %d with agent_builds %+v, want 0 and not checked", code, s.AgentBuilds)
	}

	// ---- agent builds placed, the aarch64 frpc is not the pinned one, the x86_64 frpc is ----
	if err := os.WriteFile(filepath.Join(buildsDir, "hb-agent-x86_64"), []byte("an example agent build\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(buildsDir, "hb-agent-aarch64"), []byte("another example agent build\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	pinnedX86, err := os.ReadFile(realFrpc)
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(pinnedX86); hex.EncodeToString(got[:]) != pins["x86_64"] {
		t.Fatalf("the frpc at %s is not the pinned x86_64 one", realFrpc)
	}
	other := []byte("an frpc that is not pinned\n")
	if err := os.WriteFile(filepath.Join(buildsDir, "frpc-x86_64"), pinnedX86, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(buildsDir, "frpc-aarch64"), other, 0o755); err != nil {
		t.Fatal(err)
	}
	found := sha256.Sum256(other)
	s, code = statusBuilds(t, h)
	if code != exitFail || s.AgentBuilds == nil || len(s.AgentBuilds.Problems) != 1 {
		t.Fatalf("status exited %d with %+v, want the one mismatched frpc", code, s.AgentBuilds)
	}
	if p := s.AgentBuilds.Problems[0]; p.CPUArch != "aarch64" || !strings.Contains(p.What, hex.EncodeToString(found[:])) || !strings.Contains(p.What, pins["aarch64"]) ||
		p.Fix != "hack/fetch-frp.sh --agent-builds "+buildsDir {
		t.Errorf("the problem is %+v, want the aarch64 frpc with its found and pinned digests", p)
	}

	// ---- the profiles are the source: without the aarch64 partition the host is healthy ----
	c := h.clients()
	ctx, cancel := testCtx()
	defer cancel()
	patch := `{"spec":{"partitions":[{"name":"example-cpu","gpuModel":"A100","cpuArch":"x86_64"}]}}`
	if _, err := c.dyn.Resource(v1alpha1.GroupVersion.WithResource("platformprofiles")).Patch(ctx, "example-batch", types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	s, code = statusBuilds(t, h)
	if code != exitOK || s.Health != "healthy" || s.AgentBuilds == nil || len(s.AgentBuilds.Problems) != 0 {
		t.Fatalf("status exited %d (%s %v) with %+v after the aarch64 partition was removed", code, s.Health, s.Reasons, s.AgentBuilds)
	}
}
