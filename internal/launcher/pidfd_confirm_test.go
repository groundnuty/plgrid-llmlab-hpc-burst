package launcher

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// These tests cover the pidfd confirmation of #690: on mainline Linux 6.15 and later, and on
// stable kernels that took the backport (6.12.31 and 6.14.9 onwards; a distro kernel of any
// number may carry it), one poll of a pidfd can read readable for an instant while its process
// runs execve from a non-leader thread, so sysPidfd.exited confirms a readable poll with waitid
// (and, for a process that is not the caller's child, one re-poll). The kernel's transient cannot
// be produced on a kernel without that poll test, this VM's included. So TestConfirmExit scripts
// the calls, TestIdentifyConfirmsATransientReadablePidfd injects the transient through sysPidfd's
// seams on real processes, and TestLivenessAcrossConcurrentExec is where a hosted runner on a
// kernel that has the poll test shows the real one. None of them is evidence that the fix works
// against the real race unless it ran on such a kernel beside the unfixed code.

type pollStep struct {
	readable bool
	err      error
}

type waitStep struct {
	res waitResult
	err error
}

// TestConfirmExit scripts poll and wait for each case and checks the verdict, the error, the
// exact sequence of calls and both counters. The expected values are written out from the rule,
// not computed.
func TestConfirmExit(t *testing.T) {
	boom := errors.New("scripted failure")
	yes, no := pollStep{readable: true}, pollStep{}
	cases := []struct {
		name       string
		polls      []pollStep
		waits      []waitStep
		want       bool
		wantErr    error  // the error must wrap this one
		wantMsg    string // or, for an error of the helper's own, contain this text
		wantCalls  string
		wantChild  uint64 // overturned by waitid: a child that is alive
		wantRepoll uint64 // overturned by the re-poll: not a child, and not readable again
	}{
		{"not readable: alive, one poll, no wait", []pollStep{no}, nil, false, nil, "", "poll", 0, 0},
		{"readable, the child has exited", []pollStep{yes}, []waitStep{{res: childExited}}, true, nil, "", "poll wait", 0, 0},
		{"readable, the child is alive: the transient", []pollStep{yes}, []waitStep{{res: childAlive}}, false, nil, "", "poll wait", 1, 0},
		{"readable, not a child, readable again: exited", []pollStep{yes, yes}, []waitStep{{res: notChild}}, true, nil, "", "poll wait poll", 0, 0},
		{"readable, not a child, not readable again: the transient", []pollStep{yes, no}, []waitStep{{res: notChild}}, false, nil, "", "poll wait poll", 0, 1},
		{"a poll error is returned, with no wait", []pollStep{{err: boom}}, nil, false, boom, "", "poll", 0, 0},
		{"a wait error is returned, not an exit and not alive", []pollStep{yes}, []waitStep{{err: boom}}, false, boom, "", "poll wait", 0, 0},
		{"EINVAL from wait (a kernel before 5.4) is a re-poll: exited", []pollStep{yes, yes}, []waitStep{{err: unix.EINVAL}}, true, nil, "", "poll wait poll", 0, 0},
		{"EINVAL from wait (a kernel before 5.4) is a re-poll: the transient", []pollStep{yes, no}, []waitStep{{err: unix.EINVAL}}, false, nil, "", "poll wait poll", 0, 1},
		{"EINVAL from wait, then a re-poll error, is that error", []pollStep{yes, {err: boom}}, []waitStep{{err: unix.EINVAL}}, false, boom, "", "poll wait poll", 0, 0},
		{"a re-poll error is returned", []pollStep{yes, {err: boom}}, []waitStep{{res: notChild}}, false, boom, "", "poll wait poll", 0, 0},
		{"a wait result with no meaning is an error", []pollStep{yes}, []waitStep{{res: 0}}, false, nil, "has no meaning", "poll wait", 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			child0, repoll0 := pidfdOverturnedChild.Load(), pidfdOverturnedRepoll.Load()
			var calls []string
			var polls, waits int
			poll := func() (bool, error) {
				if polls >= len(c.polls) {
					t.Fatalf("poll call %d, after %v; the script has %d polls", polls+1, calls, len(c.polls))
				}
				calls = append(calls, "poll")
				polls++
				return c.polls[polls-1].readable, c.polls[polls-1].err
			}
			wait := func() (waitResult, error) {
				if waits >= len(c.waits) {
					t.Fatalf("wait call %d, after %v; the script has %d waits", waits+1, calls, len(c.waits))
				}
				calls = append(calls, "wait")
				waits++
				return c.waits[waits-1].res, c.waits[waits-1].err
			}
			got, err := confirmExit(poll, wait)
			if got != c.want {
				t.Errorf("exited = %v; want %v", got, c.want)
			}
			switch {
			case c.wantErr != nil && !errors.Is(err, c.wantErr):
				t.Errorf("error = %v; want one wrapping %v", err, c.wantErr)
			case c.wantMsg != "" && (err == nil || !strings.Contains(err.Error(), c.wantMsg)):
				t.Errorf("error = %v; want one containing %q", err, c.wantMsg)
			case c.wantErr == nil && c.wantMsg == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			}
			if s := strings.Join(calls, " "); s != c.wantCalls {
				t.Errorf("calls = %q; want %q", s, c.wantCalls)
			}
			if d := pidfdOverturnedChild.Load() - child0; d != c.wantChild {
				t.Errorf("overturned-by-waitid counter rose by %d; want %d", d, c.wantChild)
			}
			if d := pidfdOverturnedRepoll.Load() - repoll0; d != c.wantRepoll {
				t.Errorf("overturned-by-re-poll counter rose by %d; want %d", d, c.wantRepoll)
			}
		})
	}
}

// transientPidfds stands in for the kernel's transient of #690. It supplies sysPidfd's poll and
// wait: the first poll of every exited check reads readable, whatever the process is doing, and
// the wait and any re-poll that follow are the real kernel's. A poll is a re-poll only right after
// a wait that said notChild; that is the one case where a check polls twice.
type transientPidfds struct {
	mu       sync.Mutex
	repoll   bool
	injected int
}

func (s *transientPidfds) poll(fd int) (bool, error) {
	s.mu.Lock()
	first := !s.repoll
	s.repoll = false
	if first {
		s.injected++
	}
	s.mu.Unlock()
	if first {
		return true, nil
	}
	return pollPidfd(fd)
}

func (s *transientPidfds) wait(fd int) (waitResult, error) {
	w, err := waitPidfd(fd)
	s.mu.Lock()
	s.repoll = err == nil && w == notChild
	s.mu.Unlock()
	return w, err
}

func (s *transientPidfds) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.injected
}

// useTransientPidfds installs a transientPidfds as the package's pidfds for the test.
func useTransientPidfds(t *testing.T) *transientPidfds {
	t.Helper()
	s := &transientPidfds{}
	old := pidfds
	pidfds = sysPidfd{poll: s.poll, wait: s.wait}
	t.Cleanup(func() { pidfds = old })
	return s
}

// waitForDone waits until the helper has written "done" to its report, which it does once it is
// running and about to wait.
func waitForDone(t *testing.T, report string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		if b, _ := os.ReadFile(report); strings.Contains(string(b), "done\n") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the helper never wrote its report")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startExecSleeper starts this test binary as a child that writes "done" to its report and sleeps
// (execLoopEnv=0), waits until it has, and returns it unreaped, with a function that reaps it. The
// cleanup kills it, and reaps it unless the test did.
func startExecSleeper(t *testing.T) (cmd *exec.Cmd, reap func()) {
	t.Helper()
	report := filepath.Join(t.TempDir(), "report")
	cmd = exec.Command("/proc/self/exe")
	cmd.Env = []string{execLoopEnv + "=0", execReportEnv + "=" + report}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var reaped atomic.Bool
	reap = func() {
		if reaped.CompareAndSwap(false, true) {
			_ = cmd.Wait()
		}
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		reap()
	})
	waitForDone(t, report)
	return cmd, reap
}

// ownPidfd opens a pidfd of the test's own for pid, closed at the end of the test.
func ownPidfd(t *testing.T, pid int) int {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd
}

// wantWait checks what the kernel's waitid says about the process behind fd.
func wantWait(t *testing.T, what string, fd int, want waitResult) {
	t.Helper()
	if got, err := waitPidfd(fd); err != nil || got != want {
		t.Fatalf("waitid of %s: %d, %v; want %d", what, got, err, want)
	}
}

// TestIdentifyConfirmsATransientReadablePidfd runs identify on real processes through the real
// kernel, with transientPidfds making the first poll of every check read readable, as the kernel
// does for an instant at an execve's PID hand-over. It covers all three outcomes of the
// confirmation:
//
//   - a live child: waitid says alive, so identify says Ours and pidfdOverturnedChild rises once
//     per check;
//   - a live process that is not the test's child, a reparented grandchild: waitid says ECHILD,
//     the re-poll reads not readable, so identify says Ours and pidfdOverturnedRepoll rises once
//     per check;
//   - the same child, killed and not reaped (a zombie): waitid reports the exit, so identify says
//     Gone, "has exited", and neither counter moves.
//
// It is deterministic on any kernel, and it shows the wiring, not that the race is closed.
func TestIdentifyConfirmsATransientReadablePidfd(t *testing.T) {
	cmd, reap := startExecSleeper(t)
	childRec, err := newRec("child", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	childFd := ownPidfd(t, cmd.Process.Pid)

	gpid, greport := startReparented(t, 0)
	waitForDone(t, greport)
	grandRec, err := newRec("grandchild", gpid)
	if err != nil {
		t.Fatal(err)
	}
	grandFd := ownPidfd(t, gpid)

	// What the kernel says about each, with no stand-in: these show the setup is what it claims.
	wantWait(t, "the live child", childFd, childAlive)
	wantWait(t, "the reparented grandchild", grandFd, notChild)
	for name, fd := range map[string]int{"child": childFd, "grandchild": grandFd} {
		if gone, err := (sysPidfd{}).exited(fd); err != nil || gone {
			t.Fatalf("the real adapter says the live %s exited=%v, err=%v", name, gone, err)
		}
	}

	stand := useTransientPidfds(t)
	const checks = 100
	var injected int
	step := func(what string, rec ProcRec, wantChild, wantRepoll uint64) {
		t.Helper()
		child0, repoll0 := pidfdOverturnedChild.Load(), pidfdOverturnedRepoll.Load()
		for i := 0; i < checks; i++ {
			if v, d := identify(rec); v != Ours {
				t.Fatalf("%s, check %d: identify = %v %q; want Ours (the stand-in's readable poll must be confirmed, not taken as an exit)", what, i, v, d)
			}
		}
		injected += checks
		if n := stand.count(); n != injected {
			t.Fatalf("%s: the stand-in injected %d transients in all; want %d (identify polls once per check)", what, n, injected)
		}
		if d := pidfdOverturnedChild.Load() - child0; d != wantChild {
			t.Fatalf("%s: the overturned-by-waitid counter rose by %d; want %d", what, d, wantChild)
		}
		if d := pidfdOverturnedRepoll.Load() - repoll0; d != wantRepoll {
			t.Fatalf("%s: the overturned-by-re-poll counter rose by %d; want %d", what, d, wantRepoll)
		}
	}
	step("a live child", childRec, checks, 0)
	step("a live non-child", grandRec, 0, checks)

	// A zombie child: killed through a pidfd and not reaped. Wait on the pidfd itself, not a sleep.
	if err := unix.PidfdSendSignal(childFd, unix.SIGKILL, nil, 0); err != nil {
		t.Fatal(err)
	}
	pfd := []unix.PollFd{{Fd: int32(childFd), Events: unix.POLLIN}}
	for {
		n, err := unix.Poll(pfd, 60_000)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil || n != 1 {
			t.Fatalf("waiting for the killed child's pidfd: n=%d err=%v", n, err)
		}
		break
	}
	wantWait(t, "the killed, unreaped child", childFd, childExited)
	child0, repoll0 := pidfdOverturnedChild.Load(), pidfdOverturnedRepoll.Load()
	if v, d := identify(childRec); v != Gone || !strings.Contains(d, "has exited") {
		t.Fatalf("identify of the killed child = %v %q; want Gone, pid N has exited", v, d)
	}
	if gone, err := (sysPidfd{}).exited(childFd); err != nil || !gone {
		t.Fatalf("the real adapter says the killed child exited=%v, err=%v", gone, err)
	}
	if d := pidfdOverturnedChild.Load() - child0; d != 0 {
		t.Fatalf("a real exit raised the overturned-by-waitid counter by %d; want 0", d)
	}
	if d := pidfdOverturnedRepoll.Load() - repoll0; d != 0 {
		t.Fatalf("a real exit raised the overturned-by-re-poll counter by %d; want 0", d)
	}
	reap()
}
