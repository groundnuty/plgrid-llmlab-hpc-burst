package main

// Real controller processes for the Lease test (failover_test.go): the test
// binary, run again with the environment variable childEnvRunner set, which
// TestMain turns into controllerChild before any test runs. controllerChild is
// main(): the same parse, the same logger, the same signal handling, the same
// run, the same exit codes. Only the Slurm platform differs: its Runner is a
// remoteRunner (slurmwire_test.go) that reaches the fake Slurm in the test
// process, because that fake keeps its state in one process's memory.

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// childEnvRunner makes the test binary a controller process; its value is the
// address of the remoteRunner's server.
const childEnvRunner = "HPCBURST_TEST_CONTROLLER_RUNNER"

// childErrPrefix starts the line that controllerChild, like main, prints when run
// returns an error.
const childErrPrefix = "hpcburst-controller:"

// controllerChild is main(), with the fake Slurm's remote Runner as the batch
// platform's transport. It returns the exit code main would exit with.
func controllerChild(args []string, runnerAddr string) int {
	f, err := parse(args, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, childErrPrefix, err)
		return 2
	}
	ctrl.SetLogger(funcr.New(func(prefix, args string) {
		fmt.Fprintln(os.Stderr, prefix, args)
	}, funcr.Options{LogTimestamp: true}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	runner := &remoteRunner{addr: runnerAddr}
	// The state directory holds prepareState's example frpc builds, so the
	// child pins their digests, as startBinary does for an in-process run.
	// The ports were picked by the parent, which holds them until this process
	// has started: they arrive as inherited descriptors (inheritListeners).
	listen, err := inheritedListen(os.Getenv(childEnvListeners))
	if err != nil {
		fmt.Fprintln(os.Stderr, childErrPrefix, err)
		return 1
	}
	d := deps{providerScheme: providerScheme, newPlatform: remotePlatforms(runner), frpcDigests: exampleFrpcDigests, listen: listen}
	if err := run(ctx, f, d); err != nil {
		fmt.Fprintln(os.Stderr, childErrPrefix, err)
		return 1
	}
	return 0
}

// remotePlatforms is the batch platform factory over a Runner, with the poll
// intervals of recordingPlatforms.
func remotePlatforms(r slurm.Runner) providerctl.PlatformFactory {
	return func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
		return slurm.NewBatch(r, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond}), io.NopCloser(nil), nil
	}
}

// childArgs is f as the command line main parses: the arguments of a copy that
// hpcburst up would start: its two identities' kubeconfigs, as the launcher passes them.
func childArgs(f flags) []string {
	args := []string{"--state-dir", f.host.StateDir, "--port-base", strconv.Itoa(f.host.PortBase), "--health-addr", f.healthAddr,
		"--kubeconfig", f.kubeconfig, "--provider-kubeconfig", f.providerKubeconfig}
	if f.frontMode != "" {
		args = append(args, "--front-mode", f.frontMode)
		if f.frontAddr != "" { // host-dialed mode has none
			args = append(args, "--front-addr", f.frontAddr)
		}
	}
	if !f.leaderElect {
		args = append(args, "--leader-elect=false")
	}
	return args
}

// child is one controller process, its stderr in a file of its own.
type child struct {
	name       string
	f          flags
	cmd        *exec.Cmd
	stderrPath string
	// exited is closed once the process has been waited for; waitErr and
	// exitedAt (when the wait returned) are set before that.
	exited   chan struct{}
	waitErr  error
	exitedAt time.Time
}

// startChild starts a controller process on f's ports and waits until it is
// serving or has exited, as startBinary does for an in-process run: a child that
// exits having found the metrics or the health port taken (its stderr says
// "address already in use") is started again with fresh ports, written to *f, at
// most maxBindRetries times, each logged. A caller must check the final *f, not
// the ports it first picked. A child that neither exits nor serves within 90 s
// fails the test. avoid is the flags of the other copies running.
func startChild(t *testing.T, name string, f *flags, runnerAddr string, avoid ...flags) *child {
	t.Helper()
	for attempt := 0; ; attempt++ {
		releaseAtStart(*f)
		c := launchChild(t, name, *f, runnerAddr)
		served, exited := c.awaitServing(90 * time.Second)
		if !served && !exited {
			t.Fatalf("child %s neither exited nor served within 90 s: /healthz on %s or /metrics on %s never answered 200; the end of its stderr:\n%s",
				name, f.healthAddr, f.host.ControllerMetricsAddr(), c.tail(30))
		}
		if !exited || !addrInUseText(c.log()) || attempt == maxBindRetries {
			return c
		}
		t.Logf("child %s's start %d found a port of %s or %s taken; picking fresh ports and starting again (retry %d of %d)",
			name, attempt+1, f.healthAddr, f.host.ControllerMetricsAddr(), attempt+1, maxBindRetries)
		*f = repickPorts(t, *f, avoid...)
	}
}

// awaitServing waits until the process has exited (exited), or its health probes
// and metrics both answer 200 (served), or timeout passes (neither).
func (c *child) awaitServing(timeout time.Duration) (served, exited bool) {
	deadline := time.Now().Add(timeout)
	answers := func(url string) bool {
		resp, err := probeHTTP.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	for time.Now().Before(deadline) {
		if !c.running() {
			return false, true
		}
		if answers("http://"+c.f.healthAddr+"/healthz") && answers("http://"+c.f.host.ControllerMetricsAddr()+"/metrics") {
			return true, false
		}
		select {
		case <-c.exited:
			return false, true
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false, false
}

// launchChild starts a controller process with f's arguments against the fake
// Slurm server at runnerAddr. Its cleanup kills and reaps it if the test has
// not, and a failed test logs the end of its stderr.
func launchChild(t *testing.T, name string, f flags, runnerAddr string) *child {
	t.Helper()
	releaseAtStart(f)
	args := childArgs(f)
	if back, err := parse(args, io.Discard); err != nil || back != f {
		t.Fatalf("the child's arguments %v parse to %+v (%v), not the flags %+v the test means", args, back, err, f)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "child-"+name+".stderr")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close() // the child holds its own descriptor
	c := &child{name: name, f: f, stderrPath: logPath, exited: make(chan struct{})}
	c.cmd = exec.Command(exe, args...)
	// A minimal environment: the child needs the fake's address and little else,
	// and must not inherit a signing socket or any credential.
	files, listenerEnv, closeParent := inheritListeners(t, f)
	defer closeParent() // the child holds its own descriptors once it has started
	c.cmd.ExtraFiles = files
	c.cmd.Env = []string{childEnvRunner + "=" + runnerAddr, childEnvListeners + "=" + listenerEnv, "PATH=" + os.Getenv("PATH"), "TMPDIR=" + os.TempDir(), "HOME=" + os.TempDir()}
	c.cmd.Stdout, c.cmd.Stderr = logFile, logFile
	// Pdeathsig: a -test.timeout panic skips t.Cleanup, and then the kernel
	// ends the child.
	c.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("start child %s: %v", name, err)
	}
	c.watch()
	t.Cleanup(func() {
		c.kill(t)
		if t.Failed() {
			t.Logf("child %s: the end of its stderr:\n%s", name, c.tail(60))
		}
	})
	return c
}

// running reports whether the process has not exited yet.
func (c *child) running() bool {
	select {
	case <-c.exited:
		return false
	default:
		return true
	}
}

// watch reaps the started process in the background. waitErr and exitedAt are
// set before exited closes, so a reader that has seen exited closed may read
// them.
func (c *child) watch() {
	go func() {
		c.waitErr = c.cmd.Wait()
		c.exitedAt = time.Now()
		close(c.exited)
	}()
}

// procState is the state letter of the process in /proc ("R", "S", "Z" for a zombie, which has exited and
// waits to be reaped), 0 when there is no such process. The caller holds a PID that is not reaped yet.
func procState(pid int) byte {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	// pid (comm) state ...: the state follows the last ')'.
	rest := strings.TrimSpace(string(b[bytes.LastIndexByte(b, ')')+1:]))
	if rest == "" {
		return 0
	}
	return rest[0]
}

// alive reports whether the process runs at this moment: it is not reaped (exited is open and os.Process,
// which holds the process's own handle, still finds it) and it has not exited and waits to be reaped.
func (c *child) alive() bool {
	if !c.running() {
		return false
	}
	if err := c.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return procState(c.cmd.Process.Pid) != 'Z'
}

// sigkill sends SIGKILL to a child that has not been reaped yet, and says whether the signal was delivered
// to a live process. It was not when the child was already reaped, or had exited and waited to be reaped (a
// signal to such a zombie succeeds and ends nothing), or when Signal found the process finished
// (os.ErrProcessDone): os.Process can know that the process is gone before the goroutine of watch has closed
// exited, and the signal then has nothing left to do, which is not an error. Any other Signal error is
// returned. A process that ends by itself between the check for a zombie and the signal is the one case it
// cannot tell apart.
func (c *child) sigkill() (delivered bool, err error) {
	if !c.running() {
		return false, nil
	}
	zombie := procState(c.cmd.Process.Pid) == 'Z'
	switch err := c.cmd.Process.Signal(syscall.SIGKILL); {
	case err == nil:
		return !zombie, nil
	case errors.Is(err, os.ErrProcessDone):
		return false, nil
	default:
		return false, fmt.Errorf("SIGKILL to child %s: %w", c.name, err)
	}
}

// reaped waits up to wait for the process to be reaped and returns its wait
// status; ok is false when it was not reaped in time.
func (c *child) reaped(wait time.Duration) (ws syscall.WaitStatus, ok bool) {
	select {
	case <-c.exited:
	case <-time.After(wait):
		return 0, false
	}
	ws, _ = c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	return ws, true
}

// kill sends SIGKILL, waits for the process to be reaped and returns its wait
// status. A process that is already reaped is not signalled again, and one that
// finished while it was being signalled is waited for, not reported.
func (c *child) kill(t *testing.T) syscall.WaitStatus {
	t.Helper()
	if _, err := c.sigkill(); err != nil {
		t.Errorf("%v", err)
	}
	ws, ok := c.reaped(30 * time.Second)
	if !ok {
		t.Fatalf("child %s did not exit within 30 s of SIGKILL", c.name)
	}
	return ws
}

// log is everything the child wrote to stderr so far.
func (c *child) log() string {
	b, _ := os.ReadFile(c.stderrPath)
	return string(b)
}

// tail is the last n lines of the log.
func (c *child) tail(n int) string {
	lines := strings.Split(strings.TrimRight(c.log(), "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

// gone reports whether no process has the child's PID: it is reaped, and the
// kernel has not given the PID to another process.
func (c *child) gone() bool {
	return errors.Is(syscall.Kill(c.cmd.Process.Pid, 0), syscall.ESRCH)
}

// waitFor polls cond until it holds. It fails at once if a watched child exits
// meanwhile, with the end of that child's stderr, and after timeout.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool, watch ...*child) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		for _, c := range watch {
			if !c.running() {
				t.Fatalf("child %s exited (%v) while the test waited until %s; the end of its stderr:\n%s", c.name, c.waitErr, what, c.tail(30))
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting until %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// healthy reports whether the lifecycle controller's /healthz at addr answers
// 200.
func healthy(addr string) bool {
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// secondCopyFlags are the arguments of a second copy of the controller on the
// first's host: the same state directory (so the same kubeconfig, credentials
// and agent builds, as a stale copy of the same launcher has), another port
// base whose block does not touch the first's, and another health address.
func secondCopyFlags(t *testing.T, first flags) flags {
	t.Helper()
	return repickPorts(t, first, first)
}

// noProcessesNamed fails the test for every process, other than this one, whose
// command line names s (a state directory, which both copies carry).
func noProcessesNamed(t *testing.T, s string) {
	t.Helper()
	if pids := processesNaming(s); len(pids) > 0 {
		var cmds []string
		for _, pid := range pids {
			b, _ := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
			cmds = append(cmds, strconv.Itoa(pid)+": "+string(bytes.ReplaceAll(b, []byte{0}, []byte{' '})))
		}
		slices.Sort(cmds)
		t.Errorf("processes still run with %s on their command line: %v", s, cmds)
	}
}
