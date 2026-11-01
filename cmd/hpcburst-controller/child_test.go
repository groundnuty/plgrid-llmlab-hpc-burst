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
	"net"
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

// controllerChild is main(), with the fake Slurm's remote Runner as the batch
// platform's transport. It returns the exit code main would exit with.
func controllerChild(args []string, runnerAddr string) int {
	f, err := parse(args, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-controller:", err)
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
	d := deps{providerScheme: providerScheme, newPlatform: remotePlatforms(runner), frpcDigests: exampleFrpcDigests}
	if err := run(ctx, f, d); err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-controller:", err)
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
// hpcburst up would start. The kubeconfig is the default, <state-dir>/kubeconfig.
func childArgs(f flags) []string {
	args := []string{"--state-dir", f.host.StateDir, "--port-base", strconv.Itoa(f.host.PortBase), "--health-addr", f.healthAddr}
	if f.frontMode != "" {
		args = append(args, "--front-mode", f.frontMode, "--front-addr", f.frontAddr)
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
	// exited is closed once the process has been waited for; waitErr is set
	// before that.
	exited  chan struct{}
	waitErr error
}

// startChild starts a controller process with f's arguments against the fake
// Slurm server at runnerAddr. Its cleanup kills and reaps it if the test has
// not, and a failed test logs the end of its stderr.
func startChild(t *testing.T, name string, f flags, runnerAddr string) *child {
	t.Helper()
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
	c.cmd.Env = []string{childEnvRunner + "=" + runnerAddr, "PATH=" + os.Getenv("PATH"), "TMPDIR=" + os.TempDir(), "HOME=" + os.TempDir()}
	c.cmd.Stdout, c.cmd.Stderr = logFile, logFile
	// Pdeathsig: a -test.timeout panic skips t.Cleanup, and then the kernel
	// ends the child.
	c.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("start child %s: %v", name, err)
	}
	go func() {
		c.waitErr = c.cmd.Wait()
		close(c.exited)
	}()
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

// kill sends SIGKILL, waits for the process to be reaped and returns its wait
// status. A process that is already reaped is not signalled again.
func (c *child) kill(t *testing.T) syscall.WaitStatus {
	t.Helper()
	if c.running() {
		if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil && c.running() {
			t.Errorf("SIGKILL to child %s: %v", c.name, err)
		}
	}
	select {
	case <-c.exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("child %s did not exit within 30 s of SIGKILL", c.name)
	}
	ws, _ := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
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
	for range 50 {
		base := freePortBase(t)
		if d := base - first.host.PortBase; d > 12 || d < -12 {
			args := []string{"--state-dir", first.host.StateDir, "--port-base", strconv.Itoa(base),
				"--health-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
				"--front-mode", first.frontMode, "--front-addr", first.frontAddr}
			f, err := parse(args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	t.Fatal("found no port base whose block is clear of the first copy's in 50 tries")
	return flags{}
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
