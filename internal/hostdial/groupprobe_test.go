package hostdial

// Tests for the kernel probe the first default-path start runs
// (groupprobe.go; PR 365's probe, brought across by issue #370): each call the
// guards and the reap barrier make is tried on a real child, the exact blocking
// waitid of the barrier among them, a missing one is named, a probe that cannot
// clean up after itself fails, and a start on a kernel that fails the probe is
// refused before anything is started.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The probe names the call a kernel (or its seccomp filter) lacks. The control:
// with nothing missing, it passes.
func TestTheProbeNamesTheMissingCall(t *testing.T) {
	if err := probeGroupCalls(); err != nil {
		t.Fatalf("the probe on this kernel: %v", err)
	}
	orig := groupWaitid
	cases := []struct {
		name  string
		setup func(t *testing.T)
		want  string
	}{
		{"pidfd_open", func(t *testing.T) {
			swap(t, &groupPidfdOpen, func(int, int) (int, error) { return -1, syscall.ENOSYS })
		}, "pidfd_open"},
		{"poll", func(t *testing.T) {
			swap(t, &groupPoll, func([]unix.PollFd, int) (int, error) { return -1, syscall.EPERM })
		}, "poll on a pidfd"},
		{"waitid", func(t *testing.T) {
			swap(t, &groupWaitid, func(int, int) error { return syscall.ENOSYS })
		}, "waitid(P_PIDFD, WNOWAIT)"},
		// A filter that allows the polling waitid and refuses the blocking
		// flag combination the barrier uses: the probe must try that one.
		{"waitid_blocking_only", func(t *testing.T) {
			swap(t, &groupWaitid, func(fd, options int) error {
				if options&unix.WNOHANG == 0 {
					return syscall.EPERM
				}
				return orig(fd, options)
			})
		}, "waitid(P_PIDFD, WEXITED|WNOWAIT)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.setup(t)
			err := probeGroupCalls()
			if err == nil {
				t.Fatalf("the probe accepted a kernel without %s", c.name)
			}
			if !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "pinned pidfd") {
				t.Errorf("the error does not name %q: %v", c.want, err)
			}
		})
	}
}

// probeProgram writes an executable shell script and makes it the probe's
// child program.
func probeProgram(t *testing.T, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "probe-child")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	swap(t, &probePrograms, []string{path})
}

// A probe whose child does not end cleanly fails: the reap's error is not
// swallowed. The child here exits with status 3 after every call passed.
func TestTheProbeFailsWhenItsChildDoesNotEndCleanly(t *testing.T) {
	probeProgram(t, "exit 3")
	err := probeGroupCalls()
	if err == nil || !strings.Contains(err.Error(), "did not end cleanly") {
		t.Errorf("the probe with a child that exits 3: got %v, want a failure saying it did not end cleanly", err)
	}
}

// A probe child that does not exit is ended through its pidfd, bounded, and the
// probe fails saying so.
func TestTheProbeFailsWhenItsChildDoesNotExit(t *testing.T) {
	probeProgram(t, "exec sleep 20")
	swap(t, &probeBound, 300*time.Millisecond)
	start := time.Now()
	err := probeGroupCalls()
	if err == nil || !strings.Contains(err.Error(), "did not exit within") {
		t.Errorf("the probe with a child that never exits: got %v, want a failure saying it did not exit", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("the probe took %s", d)
	}
}

// A probe child that does not exit AND cannot be ended (its kill fails): the
// probe fails, bounded, saying the child may still run. The child sleeps for a
// few seconds only, so it ends and is reaped by itself after the test.
func TestTheProbeFailsWhenItsChildCannotBeEnded(t *testing.T) {
	probeProgram(t, "exec sleep 3")
	swap(t, &probeBound, 200*time.Millisecond)
	swap(t, &groupPidfdSend, func(int, unix.Signal, *unix.Siginfo, int) error { return syscall.EPERM })
	err := probeGroupCalls()
	if err == nil || !strings.Contains(err.Error(), "may still run") {
		t.Errorf("the probe with a child that cannot be ended: got %v, want a failure saying it may still run", err)
	}
}

// A start on a kernel that fails the probe is refused before anything is
// started, with the missing call named; a probe that failed is not
// remembered, so the next start tries again and succeeds once the kernel is
// as it was.
func TestAStartOnAKernelThatFailsTheProbeIsRefusedAndStartsNothing(t *testing.T) {
	swap(t, &groupProbed.ok, false)
	restore := swap(t, &groupWaitid, func(int, int) error { return syscall.ENOSYS })
	log, _ := capturedLog()
	cmd := exec.Command("cat")
	p, err := startGuardedProcess(cmd, log, killGroup)
	if p != nil || err == nil || !strings.Contains(err.Error(), "waitid(P_PIDFD, WNOWAIT)") {
		t.Fatalf("a start on a kernel without waitid on a pidfd: %v, %v; want a refusal naming it", p, err)
	}
	if cmd.Process != nil {
		t.Error("a relay was started although the probe failed")
	}
	// The failure is not remembered: the next start, on the same kernel, is
	// refused again.
	cmd2 := exec.Command("cat")
	if p, err := startGuardedProcess(cmd2, log, killGroup); p != nil || err == nil || cmd2.Process != nil {
		t.Fatalf("a second start on the same kernel: %v, %v (started %v); want it refused too", p, err, cmd2.Process != nil)
	}
	restore()
	cmd = exec.Command("cat")
	p, err = startGuardedProcess(cmd, log, killGroup)
	if err != nil {
		t.Fatalf("the start after the kernel passes the probe: %v", err)
	}
	t.Cleanup(func() { endThroughPidfd(p.lead); _ = p.Close(); <-p.done })
}
