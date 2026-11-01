package main

// A host whose PlatformProfiles have partitions needs the agent builds for their cpuArchs, or up exits 4
// and status is unhealthy (#158). A test that applies a profile with a partition and then expects a healthy
// host calls placeAgentBuilds, which puts builds into the host's agent-builds directory that pass the real
// check, the same one an operator's host meets: nothing here skips or relaxes it.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// minimalELF is the 64-byte header of a little-endian ELF64 executable for the machine number, and then
// tag: a file the check opens as an ELF file of that machine, which no loader would run. The machine
// numbers are written out by the callers (62 is EM_X86_64, 183 EM_AARCH64), not taken from debug/elf,
// which the check reads them with.
func minimalELF(machine uint16, tag string) []byte {
	h := make([]byte, 64)
	copy(h, "\x7fELF")
	h[4], h[5], h[6] = 2, 1, 1                     // 64-bit, little-endian, version 1
	binary.LittleEndian.PutUint16(h[16:], 2)       // ET_EXEC
	binary.LittleEndian.PutUint16(h[18:], machine) // e_machine
	binary.LittleEndian.PutUint32(h[20:], 1)       // e_version
	binary.LittleEndian.PutUint16(h[52:], 64)      // e_ehsize
	binary.LittleEndian.PutUint16(h[54:], 56)      // e_phentsize
	binary.LittleEndian.PutUint16(h[58:], 64)      // e_shentsize
	return append(h, tag...)
}

// placeAgentBuilds puts hb-agent-x86_64 and frpc-x86_64 into h's agent-builds directory. The agent build
// is a minimal ELF file for EM_X86_64. The frpc is the pinned one from HPCBURST_FRP_DIR, the binary the tests
// that start frp already use, and it is checked here against frp-pins.txt, so that a different frpc in
// that directory is reported as what it is and not as a status that is unhealthy for no visible reason.
// Only x86_64 is offered: the pinned aarch64 frpc cannot be had offline, and a test must not fetch, so a
// test that needs a healthy host uses a partition of x86_64.
func placeAgentBuilds(t *testing.T, h *host) {
	t.Helper()
	_, frpc := testenv.FrpBinaries(t)
	b, err := os.ReadFile(frpc)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != pinnedFrpc(t)["x86_64"] {
		t.Fatalf("the frpc at %s is not the x86_64 one that frp-pins.txt pins, so no host can pass the agent-builds check with it", frpc)
	}
	dir := filepath.Join(h.dir, "agent-builds")
	for name, body := range map[string][]byte{
		"hb-agent-x86_64": minimalELF(62, " an example agent build for x86_64\n"),
		"frpc-x86_64":     b,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// The builds placeAgentBuilds puts down pass the real check, not a weaker one: launcher.CheckAgentBuilds, with
// frp-pins.txt's real pins, against a PlatformProfile list that an API server would return. For a profile
// with an x86_64 partition there is no problem. The control is a profile that adds an aarch64 partition: the
// same directory is then refused, for the builds the helper does not place, so the check was not relaxed.
func TestPlacedAgentBuildsPassTheRealCheck(t *testing.T) {
	h := &host{t: t, dir: t.TempDir()}
	dir := filepath.Join(h.dir, "agent-builds")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	placeAgentBuilds(t, h)
	const gv = "hpcburst.groundnuty.github.io/v1alpha1"
	serve := func(partitions string) *rest.Config {
		body := `{"apiVersion":"` + gv + `","kind":"PlatformProfileList","metadata":{"resourceVersion":"1"},"items":[{"apiVersion":"` + gv +
			`","kind":"PlatformProfile","metadata":{"name":"example-platform"},"spec":{"providerKind":"batch","partitions":[` + partitions + `]}}]}`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/apis/"+gv+"/platformprofiles" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		return &rest.Config{Host: srv.URL}
	}
	x86 := `{"name":"example-gpu","gpuModel":"A100","cpuArch":"x86_64"}`
	rep, err := launcher.CheckAgentBuilds(context.Background(), serve(x86), dir)
	if err != nil || !rep.Checked || len(rep.Problems) != 0 || len(rep.Archs) != 1 || rep.Archs[0].CPUArch != "x86_64" || !rep.Archs[0].Agent.OK || !rep.Archs[0].Frpc.OK {
		t.Fatalf("an x86_64 profile with the placed builds: %v, %+v", err, rep)
	}
	rep, err = launcher.CheckAgentBuilds(context.Background(), serve(x86+`,{"name":"example-arm","gpuModel":"GH200","cpuArch":"aarch64"}`), dir)
	if err != nil || len(rep.Problems) != 2 {
		t.Fatalf("with an aarch64 partition added: %v, %+v; want the aarch64 agent build and frpc refused", err, rep)
	}
	for _, p := range rep.Problems {
		if p.CPUArch != "aarch64" {
			t.Errorf("a problem for %s: %+v", p.CPUArch, p)
		}
	}
}
