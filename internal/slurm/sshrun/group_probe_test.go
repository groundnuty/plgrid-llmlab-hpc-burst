package sshrun

// Tests for the kernel probe New runs (group_pidfd.go; PR 365 review round and
// delta): each call the guards and the reap barrier make is tried on a real
// child, the exact blocking waitid of the barrier among them, and a probe that
// cannot clean up after itself fails New.

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// probeNew is New with the options a transport test uses.
func probeNew(t *testing.T) error {
	t.Helper()
	r := newRig(t)
	_, err := New(Options{Name: "prov-a", Config: exampleConfig, ControlDir: r.ctl})
	return err
}

// New probes every call the guards and the barrier need, on a real child, and
// names the one a kernel (or its seccomp filter) lacks. The control: with
// nothing missing, New succeeds.
func TestNewNamesTheMissingPidfdCall(t *testing.T) {
	if err := probeNew(t); err != nil {
		t.Fatalf("New on this kernel: %v", err)
	}
	orig := pidfdWaitid
	cases := []struct {
		name  string
		setup func(t *testing.T)
		want  string
	}{
		{"pidfd_open", func(t *testing.T) {
			swap(t, &pidfdOpen, func(int, int) (int, error) { return -1, syscall.ENOSYS })
		}, "pidfd_open"},
		{"pidfd_send_signal", func(t *testing.T) {
			swap(t, &pidfdSendSignal, func(int, unix.Signal, *unix.Siginfo, int) error { return syscall.EPERM })
		}, "pidfd_send_signal"},
		{"poll", func(t *testing.T) {
			swap(t, &pidfdPoll, func([]unix.PollFd, int) (int, error) { return -1, syscall.EPERM })
		}, "poll on a pidfd"},
		{"waitid", func(t *testing.T) {
			swap(t, &pidfdWaitid, func(int, int) error { return syscall.ENOSYS })
		}, "waitid(P_PIDFD, WNOWAIT)"},
		// A filter that allows the polling waitid and refuses the blocking
		// flag combination the barrier uses: the probe must try that one.
		{"waitid_blocking_only", func(t *testing.T) {
			swap(t, &pidfdWaitid, func(fd, options int) error {
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
			err := probeNew(t)
			if err == nil {
				t.Fatalf("New accepted a kernel without %s", c.name)
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

// A probe whose child does not end cleanly fails New: the reap's error is not
// swallowed. The child here exits with status 3 after every call passed.
func TestNewFailsWhenTheProbesChildDoesNotEndCleanly(t *testing.T) {
	probeProgram(t, "exit 3")
	err := probeNew(t)
	if err == nil || !strings.Contains(err.Error(), "did not end cleanly") {
		t.Errorf("New with a probe child that exits 3: got %v, want a failure saying it did not end cleanly", err)
	}
}

// A probe child that does not exit is ended through its pidfd, bounded, and
// New fails saying so.
func TestNewFailsWhenTheProbesChildDoesNotExit(t *testing.T) {
	probeProgram(t, "exec sleep 20")
	swap(t, &probeBound, 300*time.Millisecond)
	start := time.Now()
	err := probeNew(t)
	if err == nil || !strings.Contains(err.Error(), "did not exit within") {
		t.Errorf("New with a probe child that never exits: got %v, want a failure saying it did not exit", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("New took %s", d)
	}
}

// A probe child that does not exit AND cannot be ended (its kill fails): New
// fails, bounded, saying the child may still run, instead of reporting success
// or waiting for it. The child sleeps for a few seconds only, so it ends and
// is reaped by itself after the test.
func TestNewFailsWhenTheProbesChildCannotBeEnded(t *testing.T) {
	probeProgram(t, "exec sleep 3")
	swap(t, &probeBound, 200*time.Millisecond)
	swap(t, &pidfdSendSignal, func(int, unix.Signal, *unix.Siginfo, int) error { return syscall.EPERM })
	err := probeNew(t)
	if err == nil || !strings.Contains(err.Error(), "may still run") {
		t.Errorf("New with a probe child that cannot be ended: got %v, want a failure saying it may still run", err)
	}
}
