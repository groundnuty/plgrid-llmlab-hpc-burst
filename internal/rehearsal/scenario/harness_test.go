package scenario_test

// The one-host rehearsal of issue #146: the real hpcburst up (k3s, the
// controller, frps and the front, supervised) manages a batch Provider whose
// cluster is the fake Slurm in run mode, reached over real ssh and sshd. The
// real hb-agent starts inside the fake job, with the engine double as its
// engine. Everything is asserted from the API, events, the front's usage
// files and /metrics, never from log lines.
//
// It runs only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh), so the ordinary
// go test ./... skips it; with that set, a missing piece fails the test.

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

//livecheck:none the package's TestMain: it runs the tests, it is not one
func TestMain(m *testing.M) {
	enginedouble.Dispatch() // the fake apptainer and engine roles
	rehearsal.Dispatch()    // the login node's forced command
	start := time.Now()
	code := m.Run()
	if os.Getenv("HPCBURST_REHEARSAL") == "1" {
		// Issue #146, criterion 4: the rehearsal should take under 15 minutes.
		// It is a measured target, not a bound: hack/rehearsal.sh sets
		// -test.timeout well above it, so that a slow run still cleans up.
		took := time.Since(start).Round(time.Second)
		fmt.Printf("the rehearsal's tests took %s (issue #146's target: under %s)\n", took, rehearsalTarget)
		if took > rehearsalTarget {
			fmt.Printf("FINDING: the rehearsal took %s, above issue #146's target of %s\n", took, rehearsalTarget)
		}
	}
	os.Exit(code)
}

// rehearsalTarget is issue #146's time for the whole rehearsal.
const rehearsalTarget = 15 * time.Minute

// stackPorts are one rehearsal's fixed listeners: hpcburst up's launcher
// block, base..base+12, and the front's client and dial-home listeners.
// Everything else is kernel-chosen. Each test that starts a stack names its
// issue's block, so tests of different issues never share a port.
type stackPorts struct {
	base             int    // hpcburst up's --port-base
	client, dialHome string // the front's client and dial-home listeners
}

// ports146: issue #146's block is 29500-29539. hpcburst up takes launcher
// base 29520 (29520-29532); the front's client and dial-home listeners take
// 29500 and 29501.
var ports146 = stackPorts{base: 29520, client: "127.0.0.1:29500", dialHome: "127.0.0.1:29501"}

// url is http://127.0.0.1:<base+off><path>, one of up's listeners.
func (p stackPorts) url(off int, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.base+off, path)
}

// all lists the stack's listening ports: the front's client and dial-home
// listeners and up's block from base (13 ports).
func (p stackPorts) all() []int {
	var out []int
	for _, a := range []string{p.client, p.dialHome} {
		if i := strings.LastIndexByte(a, ':'); i >= 0 {
			if n, err := strconv.Atoi(a[i+1:]); err == nil {
				out = append(out, n)
			}
		}
	}
	for n := p.base; n <= p.base+12; n++ {
		out = append(out, n)
	}
	return out
}

// binaries are what the rehearsal runs: the four of this repo, built by
// hack/rehearsal.sh into $HPCBURST_REHEARSAL_BIN, k3s from $HPCBURST_BIN_DIR,
// and the pinned frps and frpc from $HPCBURST_FRP_DIR.
type binaries struct {
	hpcburst, controller, front, agent string
	k3sDir, frps, frpc                 string
	repo                               string // the repository root, for config/crd
}

func requireRehearsal(t *testing.T) binaries {
	t.Helper()
	if os.Getenv("HPCBURST_REHEARSAL") != "1" {
		t.Skip("the one-host rehearsal runs the real stack, and only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh)")
	}
	dir := os.Getenv("HPCBURST_REHEARSAL_BIN")
	b := binaries{
		hpcburst: filepath.Join(dir, "hpcburst"), controller: filepath.Join(dir, "hpcburst-controller"),
		front: filepath.Join(dir, "hpcburst-front"), agent: filepath.Join(dir, "hb-agent"),
		k3sDir: os.Getenv("HPCBURST_BIN_DIR"),
	}
	var missing []string
	for _, p := range []string{b.hpcburst, b.controller, b.front, b.agent, filepath.Join(b.k3sDir, "k3s-1.36.5", "k3s")} {
		if fi, err := os.Stat(p); err != nil || fi.Mode()&0o111 == 0 {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("HPCBURST_REHEARSAL=1 but these binaries are missing (HPCBURST_REHEARSAL_BIN, HPCBURST_BIN_DIR): %v", missing)
	}
	b.frps, b.frpc = testenv.FrpBinaries(t)
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	b.repo = repo
	return b
}

// cluster is the fake Slurm in run mode and its login node.
type cluster struct {
	plat    *fakeslurm.Platform
	login   *rehearsal.LoginNode
	root    string // RunMode.Root: the cluster's shared file system
	home    string // the cluster user's HOME
	scratch string
	fakes   string // the engine double's fake apptainer
	// cmds records every command the controller ran on the cluster, with
	// the slot holders just before it (cmdlog_test.go).
	cmds *cmdLog
	// gate holds engine starts while armed; nil unless the shape asks for
	// it (enginegate_test.go).
	gate *engineGate
}

// clusterUser is the fake cluster's own user, never this host's.
const clusterUser = "example-user"

// clusterShape is the fake cluster's partitions and QOSes; the zero value is
// #146's: one GPU partition, no QOS.
type clusterShape struct {
	partitions []fakeslurm.Partition
	qos        []fakeslurm.QOS
	// engineGate puts the engine gate (enginegate_test.go) in front of the
	// jobs' apptainer; it holds nothing until armed.
	engineGate bool
}

func (s clusterShape) partitionsOrDefault() []fakeslurm.Partition {
	if len(s.partitions) > 0 {
		return s.partitions
	}
	return []fakeslurm.Partition{{Name: stack.Partition, GPUOnly: true}}
}

func startCluster(t *testing.T, dir string, jobEnv map[string]string, shape clusterShape) *cluster {
	t.Helper()
	c := &cluster{root: filepath.Join(dir, "c"), fakes: filepath.Join(dir, "fk")}
	c.home, c.scratch = filepath.Join(c.root, "home", clusterUser), filepath.Join(c.root, "scratch", clusterUser)
	for _, d := range []string{c.home, c.scratch, c.fakes} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The login node's directory holds the relay socket, which runs commands
	// on the fake cluster: mode 700.
	if err := os.Mkdir(filepath.Join(dir, "ln"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := enginedouble.Install(c.fakes); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"HPCBURST_APPTAINER": filepath.Join(c.fakes, "apptainer"),
		// No GPU on this host: the agent's GPU probe only warns without one.
		"HPCBURST_NVIDIA_SMI": filepath.Join(c.fakes, "no-nvidia-smi"),
	}
	if shape.engineGate {
		var wrapper string
		c.gate, wrapper = installEngineGate(t, dir, c.fakes)
		env["HPCBURST_APPTAINER"] = wrapper
	}
	for k, v := range jobEnv {
		env[k] = v
	}
	c.plat = fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: shape.partitionsOrDefault(), QOS: shape.qos,
		// KillWait 20 s: longer than the agent's own shutdown (the profile's
		// 5 s stopGrace, its 10 s margin and a 5 s last report), shorter than
		// Slurm's 30 s default, which several scancels would spend from the
		// 15 min budget. Slots stay unlimited, so no ended job's slot goes to
		// a phantom job.
		Run: &fakeslurm.RunMode{Root: c.root, JobDir: filepath.Join(dir, "j"), Env: env, KillWait: 20 * time.Second},
	})
	t.Cleanup(func() {
		if err := c.plat.Close(); err != nil {
			t.Errorf("the fake cluster's Close: %v", err)
		}
	})
	c.cmds = &cmdLog{plat: c.plat}
	login, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(dir, "ln"), Platform: c.plat, Around: c.cmds.around,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": c.home, "SCRATCH": c.scratch}},
	})
	if err != nil {
		t.Fatalf("the login node: %v", err)
	}
	t.Cleanup(func() {
		if err := login.Close(); err != nil {
			t.Errorf("the login node's Close: %v", err)
		}
	})
	c.login = login
	return c
}

// host is one hpcburst up on this machine.
type host struct {
	t         *testing.T
	b         binaries
	ports     stackPorts
	state     string
	env       []string
	usageDir  string
	clientKey string
	downed    bool // hpcburst down has run
}

// dialHome is how jobs reach this host: direct, to the front's dial-home
// listener, or ssh, through the gateway's sshd with the restricted key.
type dialHome struct {
	mode    string // "direct" or "ssh"
	addr    string // --front-addr: the listener (direct) or the gateway (ssh)
	keyPath string // --front-key, ssh only: recorded, never read
}

// startHost runs hpcburst up in the given dial-home mode, with the front and
// its client and dial-home listeners, and registers hpcburst down.
func startHost(t *testing.T, b binaries, dir string, ports stackPorts, dh dialHome, env []string) *host {
	t.Helper()
	h := &host{t: t, b: b, ports: ports, state: filepath.Join(dir, "st"), env: env, usageDir: filepath.Join(dir, "usage"), clientKey: strings.Repeat("c1", 32)}
	keys := filepath.Join(dir, "client-keys.json")
	if err := os.WriteFile(keys, []byte(`{"keys":[{"id":"rehearsal","key":"`+h.clientKey+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "front.json")
	// Scaled probe and pause times (step d): a 2 s probe, and a 10 s pause
	// after HBEP-5's three Instance-emitted 5xx in a row.
	// In ssh mode the dial-home listener takes its default, 127.0.0.1:base+7,
	// which the gateway's key permits.
	listen := ""
	if dh.mode == "direct" {
		listen = `"dialHomeListen":"` + ports.dialHome + `",`
	}
	front := `{"clientListen":"` + ports.client + `","clientKeysFile":"` + keys + `","usageDir":"` + h.usageDir + `",` +
		listen + `"probeInterval":"2s","pauseAfterFailures":3,"pauseDuration":"10s"}`
	if err := os.WriteFile(cfg, []byte(front), 0o600); err != nil {
		t.Fatal(err)
	}
	before := stack.RootK3sListening(t)
	if err := stack.RootK3sBefore(before, os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("the root k3s's listeners before the rehearsal: %q", before)
	// Registered before down's clean-up, so it runs after it.
	t.Cleanup(func() {
		if err := stack.RootK3sAfter(before, stack.RootK3sListening(t)); err != nil {
			t.Error(err)
		}
	})
	args := []string{"up", "--state-dir", h.state, "--bin-dir", b.k3sDir, "--port-base", strconv.Itoa(ports.base), "--nice", "10",
		"--manifests", filepath.Join(b.repo, "config", "crd"), "--controller-bin", b.controller,
		"--front-config", cfg, "--front-bin", b.front, "--frps-bin", b.frps, "--front-mode", dh.mode, "--front-addr", dh.addr}
	if dh.keyPath != "" {
		args = append(args, "--front-key", dh.keyPath)
	}
	t.Cleanup(h.down)
	if out, err := stack.Run(t, stack.Within(t, stack.UpTimeout), env, b.hpcburst, args...); err != nil {
		t.Fatalf("hpcburst up: %v\n%s", err, out)
	}
	stack.WaitFor(t, stack.WaitDeadline, "the controller's /readyz", func() bool {
		return stack.HTTPStatus(ports.url(6, "/readyz")) == http.StatusOK
	})
	return h
}

// down runs hpcburst down with its default grace. It is also the clean-up, so
// its bound is its own, not waitCap.
func (h *host) down() {
	h.t.Helper()
	if _, err := os.Stat(h.state); err != nil || h.downed {
		return
	}
	h.downed = true
	out, err := stack.Run(h.t, stack.DownTimeout, h.env, h.b.hpcburst, "down", "--state-dir", h.state)
	if err != nil {
		h.t.Errorf("hpcburst down: %v\n%s", err, out)
	}
	h.t.Logf("memory peak of the run's cgroup (the whole stack): %s", stack.MemoryPeak())
}

// placeAgentBuild puts the hb-agent build and the pinned frpc where the
// controller reads them, as an operator does (HBEP-4:
// <state>/agent-builds/hb-agent-<cpuArch> and frpc-<cpuArch>); the controller
// stages both on the cluster.
func (h *host) placeAgentBuild(arch string) {
	h.t.Helper()
	for src, name := range map[string]string{h.b.agent: agentapi.AgentBuildName(arch), h.b.frpc: agentapi.FrpcBuildName(arch)} {
		b, err := os.ReadFile(src)
		if err != nil {
			h.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(h.state, "agent-builds", name), b, 0o700); err != nil {
			h.t.Fatal(err)
		}
	}
}
