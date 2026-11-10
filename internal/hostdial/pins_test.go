package hostdial_test

// New for issue #343 (PR 1's review round): how this package's new tests
// signal a process. The science agent's rule for the round (10 Nov 2026),
// after the incident of #308: no signal ever goes to a numeric PID. Every
// target is pinned by a pidfd, and only a pin whose process was verified,
// while its pidfd was held, as this test process's descendant along the
// expected chain is ever signalled. A pin that fails its check is closed and
// never signalled.
//
// The check is internal/rehearsal's (signal.go, PR #309): each hop's parent
// is the next, ending at this test process, and each parent started no later
// than its child, so a reused PID, which starts later, fails it. The pidfd is
// read after the rows: if the pinned process had exited, and its PID been
// reused, during the reads, the pidfd shows it exited, and the check fails.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// pinned is a process pinned by a pidfd.
type pinned struct {
	pid, fd  int
	verified bool
}

// pin opens a pidfd on pid and registers its close, and only its close, at
// the test's end. Then it verifies, while it holds the pidfd, that the
// process descends from this test process through parents, its parent first:
// each process's parent is the next one (the last one's is this test
// process), and each started no earlier than its parent. Only a pin that
// passes is verified, and only then is its SIGKILL at the test's end
// registered, which runs before the close. A pin that fails is returned with
// its error, and is never signalled.
func pin(t *testing.T, pid int, parents ...int) (*pinned, error) {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("pidfd_open of process %d: %w", pid, err)
	}
	p := &pinned{pid: pid, fd: fd}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if err := p.check(parents); err != nil {
		return p, err
	}
	p.verified = true
	t.Cleanup(func() { _ = p.signal(syscall.SIGKILL) })
	return p, nil
}

func (p *pinned) check(parents []int) error {
	chain := append([]int{p.pid}, parents...)
	self := os.Getpid()
	for i, q := range chain {
		want := self
		if i+1 < len(chain) {
			want = chain[i+1]
		}
		ppid, ticks, err := statOf(q)
		if err != nil {
			return err
		}
		if ppid != want {
			return fmt.Errorf("process %d's parent is %d, not %d", q, ppid, want)
		}
		_, wantTicks, err := statOf(want)
		if err != nil {
			return err
		}
		if wantTicks > ticks {
			return fmt.Errorf("process %d started before its parent %d: a reused PID", q, want)
		}
	}
	if p.exited() {
		return fmt.Errorf("process %d exited during the check", p.pid)
	}
	return nil
}

// errUnverified is a signal refused to a pin that failed its check.
var errUnverified = errors.New("the pin was not verified: not signalled")

// signal sends sig through the pin's pidfd; an unverified pin is refused.
func (p *pinned) signal(sig syscall.Signal) error {
	if !p.verified {
		return errUnverified
	}
	return unix.PidfdSendSignal(p.fd, sig, nil, 0)
}

// exited reports whether the pinned process has exited.
func (p *pinned) exited() bool {
	pfd := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(pfd, 0)
	return err == nil && n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0
}

// waitExit waits until the pinned process has exited.
func (p *pinned) waitExit(t *testing.T, d time.Duration, what string) {
	t.Helper()
	waitPidfdExit(t, p.fd, d, what)
}

// statOf reads a process's parent and start time from /proc/<pid>/stat
// (fields 4 and 22, counted after the command name's last ')').
func statOf(pid int) (ppid int, ticks uint64, err error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, 0, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, 0, fmt.Errorf("/proc/%d/stat is short", pid)
	}
	if ppid, err = strconv.Atoi(f[1]); err != nil {
		return 0, 0, err
	}
	ticks, err = strconv.ParseUint(f[19], 10, 64)
	return ppid, ticks, err
}

// A pin that fails its check is never signalled: here it names a wrong
// parent. Its signal is refused, and once its subtest has ended, and with it
// the pin's clean-up, the process is still alive. Closing its input ends it
// with exit 0, so no signal reached it. The positive control pins the same
// kind of process along its true chain, and its TERM ends it. The test holds
// each process by the pidfd its own start took from clone.
func TestAPinThatFailsItsCheckSignalsNothing(t *testing.T) {
	for _, wrong := range []bool{true, false} {
		name := "true chain"
		if wrong {
			name = "wrong parent"
		}
		t.Run(name, func(t *testing.T) {
			fd := -1
			cmd := exec.Command("/bin/sh", "-c", "read x; exit 0")
			cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
			in, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if fd < 0 {
				t.Fatal("the test's own start got no pidfd from clone")
			}
			own := &pinned{pid: cmd.Process.Pid, fd: fd, verified: true} // born with the child
			t.Cleanup(func() {
				_ = own.signal(syscall.SIGKILL)
				_ = in.Close()
				_ = cmd.Wait()
				_ = unix.Close(fd)
			})
			var parents []int
			if wrong {
				parents = []int{1}
			}
			var refused error
			t.Run("pin", func(t *testing.T) {
				p, err := pin(t, cmd.Process.Pid, parents...)
				switch {
				case wrong && err == nil:
					t.Fatal("a pin naming a wrong parent passed its check")
				case !wrong && err != nil:
					t.Fatalf("a pin along its true chain failed its check: %v", err)
				}
				refused = p.signal(syscall.SIGTERM)
				if !wrong {
					// Before the verified pin's own SIGKILL at its end.
					p.waitExit(t, 30*time.Second, "the process behind the verified pin")
				}
			})
			if wrong {
				if !errors.Is(refused, errUnverified) {
					t.Errorf("the unverified pin's signal: %v, want refused", refused)
				}
				if own.exited() {
					t.Fatal("the process exited after its unverified pin's clean-up: it was signalled")
				}
				_ = in.Close()
			}
			own.waitExit(t, 30*time.Second, "the pinned process")
			_ = cmd.Wait()
			ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
			switch {
			case wrong && !(ws.Exited() && ws.ExitStatus() == 0):
				t.Errorf("the process behind an unverified pin ended %v, want exit 0: no signal", cmd.ProcessState)
			case !wrong && !(ws.Signaled() && ws.Signal() == syscall.SIGTERM):
				t.Errorf("the process behind a verified pin ended %v, want its TERM", cmd.ProcessState)
			}
		})
	}
}
