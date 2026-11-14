package hostdial

// New for issue #343 (PR 1): the relay keeper's process start and stop
// (pidfd.go). The Owner-level tests, over the real path, are in
// options_test.go.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// logged is a slog handler that hands each record's message to a channel.
type logged struct{ msgs chan string }

func newLogged() *logged { return &logged{msgs: make(chan string, 16)} }

func (l *logged) Enabled(context.Context, slog.Level) bool { return true }

func (l *logged) Handle(_ context.Context, r slog.Record) error {
	select {
	case l.msgs <- r.Message:
	default:
	}
	return nil
}

func (l *logged) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logged) WithGroup(string) slog.Handler      { return l }

// A pidfd relay's stop sends nothing to a process whose PID no longer shows
// the start time recorded when it started, as a PID reaped and reused would.
// Here the record is altered: the stop logs that it does not signal, and the
// process, which waits on its standard input, ends by itself with exit 0 once
// the test closes that input. The positive control, the record as it was,
// ends the same process with the stop's TERM. In both, the process leads no
// process group of its own: it is in the test's (HBEP-6, Signals: "Alpha's
// signal to a relay's process group is dropped").
func TestAPidfdStopSignalsNoProcessWhoseStartTimeDiffers(t *testing.T) {
	for _, altered := range []bool{true, false} {
		name := "record kept"
		if altered {
			name = "record altered"
		}
		t.Run(name, func(t *testing.T) {
			l := newLogged()
			cmd := exec.Command("/bin/sh", "-c", "read x; exit 0")
			p, err := startPidfdProcess(cmd, slog.New(l))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = p.in.Close()
				p.pf.mu.Lock()
				if !p.pf.closed {
					_ = unix.PidfdSendSignal(p.pf.fd, syscall.SIGKILL, nil, 0)
				}
				p.pf.mu.Unlock()
				<-p.done
			})
			if pgid, err := syscall.Getpgid(p.pid); err != nil || pgid != syscall.Getpgrp() {
				t.Errorf("the pidfd relay %d is in process group %d (%v), want the test's %d", p.pid, pgid, err, syscall.Getpgrp())
			}
			if altered {
				p.pf.mu.Lock()
				p.pf.ticks++
				p.pf.mu.Unlock()
			}
			stopped := make(chan struct{})
			go func() { p.stop(30 * time.Second); close(stopped) }()
			if altered {
				select {
				case m := <-l.msgs:
					if !strings.Contains(m, "not signalled") {
						t.Fatalf("the stop logged %q, want that it does not signal", m)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("the stop logged nothing within 30 s")
				}
				_ = p.in.Close()
			}
			select {
			case <-stopped:
			case <-time.After(60 * time.Second):
				t.Fatal("the stop did not return within 60 s")
			}
			ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
			switch {
			case altered && !(ws.Exited() && ws.ExitStatus() == 0):
				t.Errorf("the process ended %v, want exit 0: a stop that does not signal it", cmd.ProcessState)
			case !altered && !(ws.Signaled() && ws.Signal() == syscall.SIGTERM):
				t.Errorf("the process ended %v, want the stop's TERM", cmd.ProcessState)
			}
			p.pf.mu.Lock()
			closed := p.pf.closed
			p.pf.mu.Unlock()
			if !closed {
				t.Error("the pidfd is still open after the process was reaped")
			}
		})
	}
}

// An Owner whose relays are signalled only through a pidfd refuses a kernel
// that gives none, rather than signal a PID. Alpha's NewOwner still succeeds
// on such a kernel, as asserted below, but its relays no longer start there
// (issue #370): their group signals are guarded by a pinned pidfd, so the
// probe of the first default-path start (groupCalls, groupprobe.go) refuses
// every start, naming the missing call, instead of NewOwner refusing (see
// TestAStartOnAKernelThatFailsTheProbeIsRefusedAndStartsNothing). Before that
// change alpha signalled its relays' groups by number and the same kernel ran
// it. This comment was corrected for issue #370; the assertions are as they
// were.
func TestAPidfdOwnerRefusesAKernelWithNoPidfd(t *testing.T) {
	saved := pidfdOpen
	pidfdOpen = func(int, int) (int, error) { return -1, unix.ENOSYS }
	t.Cleanup(func() { pidfdOpen = saved })
	o := OwnerOptions{
		Ports: map[agentapi.RelayChannel]int{agentapi.RelayControl: 28000, agentapi.RelayServing: 28001},
		Epoch: func(context.Context) (int64, error) { return 1, nil },
	}
	if _, err := NewOwner(o); err != nil {
		t.Errorf("alpha's Owner on a kernel with no pidfd: %v, want it to run", err)
	}
	o.Pidfd = true
	if _, err := NewOwner(o); !errors.Is(err, unix.ENOSYS) {
		t.Errorf("a pidfd Owner on a kernel with no pidfd: %v, want a refusal naming ENOSYS", err)
	}
}

// A start that gets no pidfd from clone is refused and reported, and nothing
// signals its process: no signal ever goes to a numeric PID (the science
// agent's rule for #349's round). Here pidfdAttr asks clone for no pidfd. The
// process traps every signal it can trap into a log, and ends by itself, with
// exit 0, once its input closes, which the refusal does. So the log stays
// empty and the exit is 0. The other tests here are the positive control:
// with the pidfd clone gives, the stop's TERM ends the same process.
func TestAStartWithNoClonePidfdIsRefusedAndNothingIsSignalled(t *testing.T) {
	savedAttr, savedReaped := pidfdAttr, unpinnedReaped
	pidfdAttr = func(*int) *syscall.SysProcAttr { return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL} }
	reaped := make(chan *os.ProcessState, 1)
	unpinnedReaped = func(c *exec.Cmd) { reaped <- c.ProcessState }
	t.Cleanup(func() { pidfdAttr, unpinnedReaped = savedAttr, savedReaped })
	trapLog := filepath.Join(t.TempDir(), "signals")
	script := `for s in HUP INT QUIT USR1 USR2 PIPE ALRM TERM; do trap "echo $s >> $1" $s; done; read x; exit 0`
	l := newLogged()
	cmd := exec.Command("/bin/sh", "-c", script, "sh", trapLog)
	p, err := startPidfdProcess(cmd, slog.New(l))
	if p != nil || err == nil || !strings.Contains(err.Error(), "without a pidfd from clone: refused, and not signalled") {
		t.Fatalf("a start with no clone pidfd: %v, %v; want a refusal naming the missing pidfd", p, err)
	}
	select {
	case m := <-l.msgs:
		if !strings.Contains(m, "refused") {
			t.Errorf("the refusal was logged as %q", m)
		}
	default:
		t.Error("the refusal was not logged")
	}
	select {
	case ps := <-reaped:
		ws := ps.Sys().(syscall.WaitStatus)
		if !ws.Exited() || ws.ExitStatus() != 0 {
			t.Errorf("the refused process ended %v, want exit 0 by itself: no signal", ps)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the refused process did not end within 30 s of its input closing")
	}
	if b, err := os.ReadFile(trapLog); err == nil && len(b) != 0 {
		t.Errorf("the refused process got signals %q, want none", b)
	}
}
