package hostdial

// The kernel calls behind groupsig.go's guards, as variables so a test can show
// what each failure does, and the probe that the first default-path relay start
// runs (issue #370, as PR 365's probe at sshrun.New): every call the guards and
// the reap barrier make is tried on a real child before a relay is started, and
// a missing one is named. pidfd_open alone admits Linux 5.3, which has no
// waitid(P_PIDFD), and a seccomp filter can allow some of these and not others.
//
// The probe runs at the first start and not in NewOwner, and a failure refuses
// every start (it is tried again at the next one) instead of refusing the
// Owner: TestAPidfdOwnerRefusesAKernelWithNoPidfd pins that alpha's NewOwner
// succeeds on a kernel with no pidfd. What the ruling forbids stays forbidden:
// on such a kernel no relay starts, since nothing may signal a group unguarded.
//
// What stays Go's: os/exec holds its own handle on each child and signals
// through it for WaitDelay's last resort; when Go's own probe or the
// duplication fails, os/exec falls back to a numeric-PID kill of that one
// child. That is Go's code, it signals one child and never a group, and the
// scan cannot see it.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var (
	groupPidfdOpen = unix.PidfdOpen
	groupPidfdSend = unix.PidfdSendSignal // only the probe's cleanup of its own child
	groupPoll      = unix.Poll
	groupWaitid    = func(fd, options int) error { return unix.Waitid(unix.P_PIDFD, fd, nil, options, nil) }
	groupProcRead  = os.ReadFile
)

// probePrograms are tried in order as the probe's child: any program that
// starts and exits will do. None is the ssh client: a test's ssh is a fake.
var probePrograms = []string{"/bin/sh", "/usr/bin/sh", "/bin/true", "/usr/bin/true"}

// probeBound is how long the probe waits for its child to end.
var probeBound = 10 * time.Second

// groupProbed remembers a probe that passed; a failed one is tried again.
var groupProbed struct {
	mu sync.Mutex
	ok bool
}

// groupCalls fails when this kernel (or its filter) lacks a call the guarded
// stop needs. A pass is remembered for the life of the process.
func groupCalls() error {
	groupProbed.mu.Lock()
	defer groupProbed.mu.Unlock()
	if groupProbed.ok {
		return nil
	}
	if err := probeGroupCalls(); err != nil {
		return err
	}
	groupProbed.ok = true
	return nil
}

// probeGroupCalls tries, on a real child that it starts, pidfd_open, poll on a
// pidfd, waitid(P_PIDFD, WNOHANG|WNOWAIT) and the very call the reap barrier
// makes, a blocking waitid(P_PIDFD, WEXITED|WNOWAIT), and names the first that
// fails. A probe whose child cannot be reaped also fails.
func probeGroupCalls() error {
	prog := ""
	for _, p := range probePrograms {
		if _, err := os.Stat(p); err == nil {
			prog = p
			break
		}
	}
	if prog == "" {
		return fmt.Errorf("hostdial: no program among %s to probe this kernel's pidfd support with", strings.Join(probePrograms, ", "))
	}
	cmd := exec.Command(prog, "-c", "exit 0")
	cmd.Env = []string{}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("hostdial: the pidfd probe could not start %s: %w", prog, err)
	}
	pid := cmd.Process.Pid
	var failed error
	fd, err := groupPidfdOpen(pid, 0)
	if err != nil {
		failed = probeMissing("pidfd_open", err)
		fd = -1
	} else {
		failed = probeChild(fd)
	}
	// A probe that failed may have left its child running: end it through its
	// pidfd (the child is ours and unreaped, so that signals it alone), so the
	// wait below is short.
	if failed != nil && fd >= 0 {
		_ = groupPidfdSend(fd, syscall.SIGKILL, nil, 0)
	}
	// The child has nobody's attention but ours: reap it, bounded. A reap that
	// fails is a failure of the probe, not something to swallow.
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var waitErr, cleanupErr error
	select {
	case waitErr = <-waited:
	case <-time.After(probeBound):
		if fd >= 0 {
			_ = groupPidfdSend(fd, syscall.SIGKILL, nil, 0)
		}
		select {
		case waitErr = <-waited:
			cleanupErr = fmt.Errorf("did not end by itself within %s", probeBound)
		case <-time.After(probeBound):
			cleanupErr = fmt.Errorf("could not be ended within %s and may still run", 2*probeBound)
		}
	}
	if fd >= 0 {
		_ = unix.Close(fd)
	}
	switch {
	case failed != nil && cleanupErr != nil:
		return fmt.Errorf("%w; and the probe's child %v", failed, cleanupErr)
	case failed != nil:
		return failed
	case cleanupErr != nil:
		return fmt.Errorf("hostdial: the pidfd probe's child %v", cleanupErr)
	case waitErr != nil:
		return fmt.Errorf("hostdial: the pidfd probe's child did not end cleanly: %w", waitErr)
	}
	return nil
}

func probeMissing(what string, err error) error {
	return fmt.Errorf("hostdial: the relays' process groups are signalled only behind a pinned pidfd, and this kernel (or its filter) does not allow %s on a child: %w", what, err)
}

// probeChild runs the calls on the child whose pidfd is fd. The child is not
// reaped yet, so it is alive or a zombie, and either answers.
func probeChild(fd int) error {
	missing := probeMissing
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	if _, err := retryEINTR(func() (int, error) { return groupPoll(fds, 0) }); err != nil {
		return missing("poll on a pidfd", err)
	}
	if fds[0].Revents&unix.POLLNVAL != 0 {
		return missing("poll on a pidfd", errors.New("the descriptor is not usable"))
	}
	if _, err := retryEINTR(func() (int, error) {
		return 0, groupWaitid(fd, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT)
	}); err != nil {
		return missing("waitid(P_PIDFD, WNOWAIT)", err)
	}
	// The very call the reap barrier makes, awaitExit: a BLOCKING waitid, on a
	// child that exits. A filter can allow the polling call above and refuse
	// this flag combination, and then every relay's barrier would fail on a
	// live relay. The child exits by itself; the wait for that is bounded, so
	// the blocking call returns at once.
	if _, err := retryEINTR(func() (int, error) { return groupPoll(fds, int(probeBound.Milliseconds())) }); err != nil {
		return missing("poll on a pidfd", err)
	}
	if fds[0].Revents&unix.POLLIN == 0 {
		return fmt.Errorf("hostdial: the pidfd probe's child did not exit within %s, so the barrier's blocking waitid could not be tried", probeBound)
	}
	if err := awaitExit(fd); err != nil {
		return missing("waitid(P_PIDFD, WEXITED|WNOWAIT) (the blocking call that awaits an exit without reaping)", err)
	}
	return nil
}

// awaitExit blocks until the child behind the pidfd has exited, WITHOUT
// reaping it: waitid(P_PIDFD, WEXITED|WNOWAIT), which leaves a zombie whose
// PID, and so its group's number, cannot be handed out again.
func awaitExit(fd int) error {
	for {
		err := groupWaitid(fd, unix.WEXITED|unix.WNOWAIT)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func retryEINTR(f func() (int, error)) (int, error) {
	for {
		n, err := f()
		if !errors.Is(err, syscall.EINTR) {
			return n, err
		}
	}
}
