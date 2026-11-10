package stack

// The host side: bounded waits and commands, the root k3s guard, process
// helpers, and one hpcburst up. The ports and the front's tuning are the
// caller's.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// T is what the helpers need of a test: testing.TB and the test binary's
// deadline, which *testing.T has.
type T interface {
	testing.TB
	Deadline() (time.Time, bool)
}

// WaitDeadline bounds every wait on state; the stack's own start is slower.
const WaitDeadline = 3 * time.Minute

// CleanupMargin is how long before the test binary's deadline (-test.timeout)
// every wait, request and command gives up. The deadline's panic skips
// t.Cleanup, which is what stops the stack, so a run that is too slow must end
// in t.Fatal early enough for the clean-up to run: hpcburst down within its
// 40 s grace, the fake cluster's KILL of every job, and the ssh servers.
const CleanupMargin = 2 * time.Minute

// WaitCap is when the test's waits and bounded calls give up: CleanupMargin
// before the test's deadline, or never when it has none.
func WaitCap(t T) (time.Time, bool) {
	d, ok := t.Deadline()
	if !ok {
		return time.Time{}, false
	}
	return d.Add(-CleanupMargin), true
}

// BoundedDeadline is now plus d, or WaitCap when that comes first; cut says
// the cap shortened it.
func BoundedDeadline(t T, d time.Duration) (deadline time.Time, cut bool) {
	deadline = time.Now().Add(d)
	if c, ok := WaitCap(t); ok && c.Before(deadline) {
		return c, true
	}
	return deadline, false
}

// APICallTimeout bounds one call to the API server.
const APICallTimeout = 30 * time.Second

// CallCtx bounds one call to the API server by APICallTimeout and WaitCap.
func CallCtx(t T) (context.Context, context.CancelFunc) {
	d, _ := BoundedDeadline(t, APICallTimeout)
	return context.WithDeadline(context.Background(), d)
}

// WaitFor polls cond until it holds, failing at the deadline, or at WaitCap
// when that comes first.
func WaitFor(t T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline, cut := BoundedDeadline(t, d)
	for !cond() {
		if time.Now().After(deadline) {
			if cut {
				t.Fatalf("gave up waiting for %s %s before the test's deadline, so that its clean-up runs", what, CleanupMargin)
			}
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// WaitState is WaitFor whose failure names the state cond saw last.
func WaitState(t T, d time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline, cut := BoundedDeadline(t, d)
	for {
		ok, state := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			if cut {
				t.Fatalf("gave up waiting for %s %s before the test's deadline, so that its clean-up runs; last: %s", what, CleanupMargin, state)
			}
			t.Fatalf("timed out after %s waiting for %s; last: %s", d, what, state)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// HTTPStatus is the status of a GET, 0 when it fails.
func HTTPStatus(url string) int {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// RootK3sPorts are a system k3s's ports (IMPL-COMMON), which the stack never
// touches; a CI runner has no system k3s at all.
var RootK3sPorts = []int{6443, 6444, 10250}

// Listening lists which of ports have a TCP listener on this host, from
// ss(8), in the order given.
func Listening(t T, ports []int) []int {
	t.Helper()
	have, err := listening(ports)
	if err != nil {
		t.Fatal(err)
	}
	return have
}

// listening is Listening returning its failure, for a caller that must not stop the test (Left).
func listening(ports []int) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ss", "-ltnH").Output()
	if err != nil {
		return nil, fmt.Errorf("ss -ltnH: %v", err)
	}
	var have []int
	for _, p := range ports {
		for _, l := range strings.Split(string(out), "\n") {
			if f := strings.Fields(l); len(f) >= 4 && strings.HasSuffix(f[3], ":"+strconv.Itoa(p)) {
				have = append(have, p)
				break
			}
		}
	}
	return have, nil
}

// RootK3sListening is which of the system k3s's ports listen, "6443,6444,10250"
// on a host that runs one, "" on a CI runner.
func RootK3sListening(t T) string {
	t.Helper()
	var s []string
	for _, p := range Listening(t, RootK3sPorts) {
		s = append(s, strconv.Itoa(p))
	}
	return strings.Join(s, ",")
}

// RootK3sBefore is the guard before the run: with HPCBURST_TEST_ROOT_K3S=1 a
// system k3s must listen on all its ports; without it none need to.
func RootK3sBefore(before string, required bool) error {
	if required && before != "6443,6444,10250" {
		return fmt.Errorf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", before)
	}
	return nil
}

// RootK3sAfter is the check after the run: the system k3s's listeners are the
// ones there were before it, whichever those were.
func RootK3sAfter(before, after string) error {
	if after != before {
		return fmt.Errorf("the root k3s's listeners changed across the run: %q -> %q", before, after)
	}
	return nil
}

// Bounds of the stack's commands. hpcburst up starts k3s and waits for it; down
// stops everything within its 40 s grace and then k3s; status only reads.
const (
	UpTimeout     = 5 * time.Minute
	DownTimeout   = 100 * time.Second
	StatusTimeout = 30 * time.Second
)

// Run runs one of the stack's commands within timeout and returns its output.
// The command is killed at its deadline, and its output is read for at most
// 10 s after that, in case a process it started kept the pipe open.
func Run(t T, timeout time.Duration, env []string, name string, args ...string) (string, error) {
	t.Helper()
	out, over, err := runBounded(timeout, env, name, args...)
	if over {
		err = errors.Join(err, pastBound(name, timeout))
	}
	return out, err
}

// runBounded is Run without its word on the deadline: the command's output and
// error as they are, and whether the deadline passed.
func runBounded(timeout time.Duration, env []string, name string, args ...string) (out string, over bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.WaitDelay = 10 * time.Second
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err = cmd.Run()
	return buf.String(), ctx.Err() != nil, err
}

// pastBound is what a command that passed its deadline adds to its error.
func pastBound(name string, timeout time.Duration) error {
	return fmt.Errorf("%s did not end within %s", filepath.Base(name), timeout)
}

// Within is d, or less when WaitCap comes first: a command the test body runs
// ends before the clean-up's margin.
func Within(t T, d time.Duration) time.Duration {
	dl, _ := BoundedDeadline(t, d)
	return max(time.Until(dl), time.Second)
}

// CleanEnv is the environment for hpcburst up: this process's, without
// anything that would point the controller at a fake ssh. up passes its whole
// environment on to the controller (internal/launcher/supervise.go), whose ssh
// client is found on PATH; the engine double's fakes are reached only through
// the fake Slurm's job environment.
func CleanEnv(extra ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HPCBURST_SSH=") || strings.HasPrefix(kv, "HPCBURST_APPTAINER=") || strings.HasPrefix(kv, "HB_TEST_ROLE=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, extra...)
}

// MemoryPeak reads the peak memory of this process's cgroup, which also holds
// everything up started: the run's memory cap applies to the whole stack.
func MemoryPeak() string {
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

// ProcessExe is the executable of a live process, "" when it is gone.
func ProcessExe(pid int) string {
	exe, _ := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
	return exe
}

// ChildrenOf lists the live direct children of pid. The kernel lists a
// child under the thread that forked it, and a Go program forks from any of
// its threads, so every thread's list is read.
func ChildrenOf(pid int) []int {
	tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if err != nil {
		return nil
	}
	var out []int
	for _, task := range tasks {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", pid, task.Name()))
		if err != nil {
			continue // the thread ended since the listing
		}
		for _, f := range strings.Fields(string(b)) {
			if n, err := strconv.Atoi(f); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

// Alive reports whether pid names a live process (not a zombie).
func Alive(pid int) bool {
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

// Binaries are what the stack runs: the four of this repo, k3s from a
// directory with k3s-1.36.5/, the pinned frps and frpc, and the repository
// root, for config/crd.
type Binaries struct {
	HPCBurst, Controller, Front, Agent string
	K3sDir, Frps, Frpc                 string
	Repo                               string
}

// builds are the binaries a run needs whatever testenv.FrpBinaries finds.
func (b Binaries) builds() []string {
	return []string{b.HPCBurst, b.Controller, b.Front, b.Agent, filepath.Join(b.K3sDir, "k3s-1.36.5", "k3s")}
}

// Missing lists the binaries that are not executable files. The alpha rows call
// it once testenv.FrpBinaries has found frps and frpc.
func (b Binaries) Missing() []string {
	var missing []string
	for _, p := range append(b.builds(), b.Frps, b.Frpc) {
		if fi, err := os.Stat(p); err != nil || fi.Mode()&0o111 == 0 || fi.IsDir() {
			missing = append(missing, p)
		}
	}
	return missing
}

// MissingBuilds is the rehearsal's check, made before testenv.FrpBinaries, which
// may skip the test: a missing build must not turn into a skip. It lists the
// builds without an executable bit, so a directory in a binary's place passes
// here, and fails when it is run: without frp, that run still skips.
func (b Binaries) MissingBuilds() []string {
	var missing []string
	for _, p := range b.builds() {
		if fi, err := os.Stat(p); err != nil || fi.Mode()&0o111 == 0 {
			missing = append(missing, p)
		}
	}
	return missing
}

// Ports are one stack's loopback ports: hpcburst up's block (base..base+12),
// the front's client listener and, in direct dial-home mode, its dial-home
// listener (in ssh and host-dialed modes it takes its default, 127.0.0.1:base+7).
type Ports struct {
	Base           int
	ClientAddr     string
	DialHomeListen string
}

// URL is http://127.0.0.1:<base+off><path>, one of up's listeners.
func (p Ports) URL(off int, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.Base+off, path)
}

// All lists the stack's listening ports: the front's client and dial-home
// listeners, those that are set, and up's block from Base (13 ports).
func (p Ports) All() []int {
	var out []int
	for _, a := range []string{p.ClientAddr, p.DialHomeListen} {
		if i := strings.LastIndexByte(a, ':'); i >= 0 {
			if n, err := strconv.Atoi(a[i+1:]); err == nil {
				out = append(out, n)
			}
		}
	}
	for n := p.Base; n <= p.Base+12; n++ {
		out = append(out, n)
	}
	return out
}

// DialHome is how jobs and this host reach each other: direct, to the front's
// dial-home listener; ssh, through the gateway's sshd with the restricted key;
// or host-dialed, where this host dials the jobs through the login node and
// there is no address and no key (HBEP-6, Dial-home modes; HBEP-11, open
// question 8, amended 9 Nov 2026).
type DialHome struct {
	Mode    string // "direct", "ssh" or "host-dialed"
	Addr    string // --front-addr: the listener as the nodes see it (direct) or the gateway (ssh); empty for host-dialed, which has none because the host dials the jobs
	KeyPath string // --front-key, ssh only: recorded, never read
}

// upFlags are hpcburst up's flags for this dial-home mode: --front-mode always, --front-addr only when
// there is an address (host-dialed mode has none, because the host dials the jobs), and --front-key only
// for ssh.
func (dh DialHome) upFlags() []string {
	flags := []string{"--front-mode", dh.Mode}
	if dh.Addr != "" {
		flags = append(flags, "--front-addr", dh.Addr)
	}
	if dh.KeyPath != "" {
		flags = append(flags, "--front-key", dh.KeyPath)
	}
	return flags
}

// frontConfigMember is the front's dialHomeListen member (a JSON fragment ending in a comma) in direct
// mode, and nothing in ssh and host-dialed modes, where the listener takes its default, 127.0.0.1:base+7.
func (dh DialHome) frontConfigMember(ports Ports) string {
	if dh.Mode == "direct" {
		return `"dialHomeListen":"` + ports.DialHomeListen + `",`
	}
	return ""
}

// Host is one hpcburst up on this machine.
type Host struct {
	T         T
	B         Binaries
	State     string
	Env       []string
	UsageDir  string
	ClientKey string
	Ports     Ports
	// DownBound bounds hpcburst down; zero is DownTimeout. A test of a down that
	// never ends sets it.
	DownBound     time.Duration
	downed        bool // hpcburst down has run
	nameDownBound bool // NameDownBound
}

// HostOption is a behaviour of StartHost's Host where a caller differs from the
// default, the alpha rows'. It takes effect before StartHost registers the
// clean-up, so it also holds for a stack whose start fails.
type HostOption func(*Host)

// NameDownBound makes a hpcburst down past its bound say so in its error, as Run
// does for the other commands: the rehearsal's choice. Without it the error is
// the command's own, and a down that ended well just as the bound passed is no
// error.
func NameDownBound(h *Host) { h.nameDownBound = true }

// StartHost runs hpcburst up in the given dial-home mode, with the front and
// its client and dial-home listeners, and registers hpcburst down. frontTuning
// is extra members of the front's configuration, a JSON fragment ending in a
// comma ("" for the defaults): the rehearsal's scaled probe and pause times.
func StartHost(t T, b Binaries, dir string, dh DialHome, env []string, ports Ports, frontTuning string, opts ...HostOption) *Host {
	t.Helper()
	h := &Host{T: t, B: b, State: filepath.Join(dir, "st"), Env: env, UsageDir: filepath.Join(dir, "usage"), ClientKey: strings.Repeat("c1", 32), Ports: ports}
	for _, o := range opts {
		o(h)
	}
	keys := filepath.Join(dir, "client-keys.json")
	if err := os.WriteFile(keys, []byte(`{"keys":[{"id":"rehearsal","key":"`+h.ClientKey+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "front.json")
	listen := dh.frontConfigMember(ports)
	front := `{` + listen + frontTuning + `"clientListen":"` + ports.ClientAddr + `","clientKeysFile":"` + keys + `","usageDir":"` + h.UsageDir + `"}`
	if err := os.WriteFile(cfg, []byte(front), 0o600); err != nil {
		t.Fatal(err)
	}
	before := RootK3sListening(t)
	if err := RootK3sBefore(before, os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("the root k3s's listeners before the stack: %q", before)
	// Registered before down's clean-up, so it runs after it.
	t.Cleanup(func() {
		if err := RootK3sAfter(before, RootK3sListening(t)); err != nil {
			t.Error(err)
		}
	})
	args := []string{"up", "--state-dir", h.State, "--bin-dir", b.K3sDir, "--port-base", strconv.Itoa(ports.Base), "--nice", "10",
		"--manifests", filepath.Join(b.Repo, "config", "crd"), "--controller-bin", b.Controller,
		"--front-config", cfg, "--front-bin", b.Front, "--frps-bin", b.Frps}
	args = append(args, dh.upFlags()...)
	t.Cleanup(h.Down)
	if out, err := Run(t, Within(t, UpTimeout), env, b.HPCBurst, args...); err != nil {
		t.Fatalf("hpcburst up: %v\n%s", err, out)
	}
	WaitFor(t, WaitDeadline, "the controller's /readyz", func() bool {
		return HTTPStatus(ports.URL(6, "/readyz")) == http.StatusOK
	})
	return h
}

// Down runs hpcburst down with its default grace. It is also the clean-up, so
// its bound is its own, not WaitCap. It reports a failure as an error, never
// stops the test.
func (h *Host) Down() {
	h.T.Helper()
	if err := h.DownErr(); err != nil {
		h.T.Errorf("%v", err)
	}
}

// DownErr is Down returning its failure. It runs at most once.
func (h *Host) DownErr() error {
	if _, err := os.Stat(h.State); err != nil || h.downed {
		return nil
	}
	h.downed = true
	bound := h.DownBound
	if bound == 0 {
		bound = DownTimeout
	}
	out, over, err := runBounded(bound, h.Env, h.B.HPCBurst, "down", "--state-dir", h.State)
	if over && h.nameDownBound {
		err = errors.Join(err, pastBound(h.B.HPCBurst, bound))
	}
	h.T.Logf("memory peak of the run's cgroup (the whole stack): %s", MemoryPeak())
	if err != nil {
		return fmt.Errorf("hpcburst down: %v\n%s", err, out)
	}
	return nil
}

// Downed reports whether hpcburst down has run: the API no longer serves.
func (h *Host) Downed() bool { return h.downed }

// PlaceAgentBuild puts the hb-agent build and the pinned frpc where the
// controller reads them, as an operator does (HBEP-4:
// <state>/agent-builds/hb-agent-<cpuArch> and frpc-<cpuArch>); the controller
// stages both on the cluster.
func (h *Host) PlaceAgentBuild(arch string) {
	h.T.Helper()
	for src, name := range map[string]string{h.B.Agent: agentapi.AgentBuildName(arch), h.B.Frpc: agentapi.FrpcBuildName(arch)} {
		b, err := os.ReadFile(src)
		if err != nil {
			h.T.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(h.State, "agent-builds", name), b, 0o700); err != nil {
			h.T.Fatal(err)
		}
	}
}

// StatusPID is the PID that hpcburst status reports for a supervised entry,
// 0 when it reports none running.
func (h *Host) StatusPID(name string) int {
	h.T.Helper()
	out, err := Run(h.T, Within(h.T, StatusTimeout), h.Env, h.B.HPCBurst, "status", "--state-dir", h.State, "--json", "--no-api")
	var doc any
	if jerr := json.Unmarshal([]byte(out), &doc); jerr != nil {
		h.T.Logf("hpcburst status --json (%v): %v: %.300s", err, jerr, out)
		return 0
	}
	pid := 0
	var walk func(any)
	walk = func(o any) {
		switch v := o.(type) {
		case map[string]any:
			if v["name"] == name && v["running"] == true {
				if p, ok := v["pid"].(float64); ok {
					pid = int(p)
				}
			}
			for _, x := range v {
				walk(x)
			}
		case []any:
			for _, x := range v {
				walk(x)
			}
		}
	}
	walk(doc)
	return pid
}

// Host is the agentapi view of this stack's host: its state directory and
// port block, from which frps's web address and password file follow.
func (h *Host) AgentHost() agentapi.Host {
	return agentapi.Host{StateDir: h.State, PortBase: h.Ports.Base}
}
