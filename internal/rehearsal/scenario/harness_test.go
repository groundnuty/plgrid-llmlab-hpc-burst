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
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

func TestMain(m *testing.M) {
	enginedouble.Dispatch() // the fake apptainer and engine roles
	rehearsal.Dispatch()    // the login node's forced command
	os.Exit(m.Run())
}

// Ports: issue #146's block is 29500-29539. hpcburst up takes launcher base
// 29520 (29520-29532); the front's client and dial-home listeners take 29500
// and 29501. Everything else is kernel-chosen.
const (
	portBase     = 29520
	clientAddr   = "127.0.0.1:29500"
	dialHomeAddr = "127.0.0.1:29501"
)

// waitDeadline bounds every wait on state; the stack's own start is slower.
const waitDeadline = 3 * time.Minute

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
		t.Skip("the one-host rehearsal runs only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh)")
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

// shortDir is a scratch directory with a short path: k3s's sockets bound the
// state directory to 80 bytes and an engine's unix socket in a job's TMPDIR to
// 108. It is removed at the end of the test.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hr")
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 48 {
		t.Fatalf("scratch directory %s is longer than 48 bytes; set TMPDIR to a shorter path", d)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// cpuArch is this host's architecture as a profile names it.
func cpuArch(t *testing.T) string {
	switch runtime.GOARCH {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	}
	t.Fatalf("no profile CPU architecture for GOARCH %s", runtime.GOARCH)
	return ""
}

// waitFor polls cond until it holds, failing at the deadline.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// httpStatus is the status of a GET, 0 when it fails.
func httpStatus(url string) int {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// rootK3sListens reports whether something still listens on the root k3s's
// ports 6443 and 10250; the rehearsal must never touch them.
func rootK3sListens() bool {
	for _, p := range []string{"6443", "10250"} {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+p, 2*time.Second)
		if err != nil {
			return false
		}
		c.Close()
	}
	return true
}

// run runs one of the stack's commands and returns its output.
func run(t *testing.T, env []string, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Env = env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// cleanEnv is the environment for hpcburst up: this process's, without
// anything that would point the controller at a fake ssh. up passes its whole
// environment on to the controller (internal/launcher/supervise.go), whose ssh
// client is found on PATH; the engine double's fakes are reached only through
// the fake Slurm's job environment.
func cleanEnv(extra ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HPCBURST_SSH=") || strings.HasPrefix(kv, "HPCBURST_APPTAINER=") || strings.HasPrefix(kv, "HB_TEST_ROLE=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, extra...)
}

// memoryPeak reads the peak memory of this process's cgroup, which also holds
// everything up started: the run's memory cap applies to the whole stack.
func memoryPeak() string {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	line := strings.TrimSpace(string(b))
	i := strings.LastIndex(line, "::")
	if i < 0 {
		return "unknown (no cgroup v2 line)"
	}
	peak, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", line[i+2:], "memory.peak"))
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(peak)), 10, 64)
	return fmt.Sprintf("%d MiB", n>>20)
}

// processExe is the executable of a live process, "" when it is gone.
func processExe(pid int) string {
	exe, _ := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
	return exe
}

// childrenOf lists the live direct children of pid.
func childrenOf(pid int) []int {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", pid, pid))
	if err != nil {
		return nil
	}
	var out []int
	for _, f := range strings.Fields(string(b)) {
		if n, err := strconv.Atoi(f); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// alive reports whether pid names a live process (not a zombie).
func alive(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	f := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
	return len(f) > 0 && f[0] != "Z"
}

// cluster is the fake Slurm in run mode and its login node.
type cluster struct {
	plat    *fakeslurm.Platform
	login   *rehearsal.LoginNode
	root    string // RunMode.Root: the cluster's shared file system
	home    string // the cluster user's HOME
	scratch string
	fakes   string // the engine double's fake apptainer
}

// clusterUser is the fake cluster's own user, never this host's.
const clusterUser = "example-user"

func startCluster(t *testing.T, dir string, jobEnv map[string]string) *cluster {
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
	for k, v := range jobEnv {
		env[k] = v
	}
	c.plat = fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: partition, GPUOnly: true}},
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
	login, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(dir, "ln"), Platform: c.plat,
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
	state     string
	env       []string
	usageDir  string
	clientKey string
}

// startHost runs hpcburst up in the given dial-home mode, with the front and
// its client and dial-home listeners, and registers hpcburst down.
func startHost(t *testing.T, b binaries, dir, mode string, env []string) *host {
	t.Helper()
	h := &host{t: t, b: b, state: filepath.Join(dir, "st"), env: env, usageDir: filepath.Join(dir, "usage"), clientKey: strings.Repeat("c1", 32)}
	keys := filepath.Join(dir, "client-keys.json")
	if err := os.WriteFile(keys, []byte(`{"keys":[{"id":"rehearsal","key":"`+h.clientKey+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "front.json")
	// Scaled probe and pause times (step d): a 2 s probe, and a 10 s pause
	// after HBEP-5's three Instance-emitted 5xx in a row.
	front := `{"clientListen":"` + clientAddr + `","clientKeysFile":"` + keys + `","usageDir":"` + h.usageDir +
		`","dialHomeListen":"` + dialHomeAddr + `","probeInterval":"2s","pauseAfterFailures":3,"pauseDuration":"10s"}`
	if err := os.WriteFile(cfg, []byte(front), 0o600); err != nil {
		t.Fatal(err)
	}
	if !rootK3sListens() {
		t.Fatal("the root k3s does not listen on 6443 and 10250 before the rehearsal; refusing to start")
	}
	args := []string{"up", "--state-dir", h.state, "--bin-dir", b.k3sDir, "--port-base", strconv.Itoa(portBase), "--nice", "10",
		"--manifests", filepath.Join(b.repo, "config", "crd"), "--controller-bin", b.controller,
		"--front-config", cfg, "--front-bin", b.front, "--frps-bin", b.frps, "--front-mode", mode}
	if mode == "direct" {
		args = append(args, "--front-addr", dialHomeAddr)
	}
	t.Cleanup(h.down)
	if out, err := run(t, env, b.hpcburst, args...); err != nil {
		t.Fatalf("hpcburst up: %v\n%s", err, out)
	}
	waitFor(t, waitDeadline, "the controller's /readyz", func() bool {
		return httpStatus(fmt.Sprintf("http://127.0.0.1:%d/readyz", portBase+6)) == http.StatusOK
	})
	return h
}

// down runs hpcburst down with its default grace, then checks that the root
// k3s still listens.
func (h *host) down() {
	h.t.Helper()
	if _, err := os.Stat(h.state); err != nil {
		return
	}
	out, err := run(h.t, h.env, h.b.hpcburst, "down", "--state-dir", h.state)
	if err != nil {
		h.t.Errorf("hpcburst down: %v\n%s", err, out)
	}
	if !rootK3sListens() {
		h.t.Errorf("the root k3s no longer listens on 6443 and 10250 after the rehearsal")
	}
	h.t.Logf("memory peak of the run's cgroup (the whole stack): %s", memoryPeak())
}

// placeAgentBuild puts the hb-agent build where the controller reads it, as
// an operator does (HBEP-4: <state>/agent-builds/hb-agent-<cpuArch>).
func (h *host) placeAgentBuild(arch string) {
	h.t.Helper()
	src, err := os.ReadFile(h.b.agent)
	if err != nil {
		h.t.Fatal(err)
	}
	dst := filepath.Join(h.state, "agent-builds", "hb-agent-"+arch)
	if err := os.WriteFile(dst, src, 0o700); err != nil {
		h.t.Fatal(err)
	}
}
