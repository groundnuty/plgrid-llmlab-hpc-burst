package sshrun

// The kernel calls behind group.go's guards, as variables so a test can show
// what each failure does, and the probe that New runs (PR 365 review round,
// 11 Nov 2026): every call the guards and the reap barrier need is tried on a
// real child before the transport accepts the kernel, and a missing one is
// named. pidfd_open alone admits Linux 5.3, which has no waitid(P_PIDFD), and
// a seccomp filter can allow some of these and not others.
//
// What stays Go's: os/exec holds its own handle on each child (a duplicate of
// our pidfd), and signals through it for WaitDelay's last resort and for
// runClient's default Cancel. When Go's own probe or the duplication fails
// (no descriptor to spare, say), os/exec falls back to a numeric-PID kill of
// that child alone. That is Go's code and not ours, and the package scan
// cannot see it; after this probe passes, Go's probe passes on the same
// kernel, so it takes the pidfd route.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var (
	pidfdOpen       = unix.PidfdOpen
	pidfdSendSignal = unix.PidfdSendSignal
	pidfdPoll       = unix.Poll
	pidfdWaitid     = func(fd, options int) error { return unix.Waitid(unix.P_PIDFD, fd, nil, options, nil) }
	procRead        = os.ReadFile
)

// probePrograms are tried in order as the probe's child: any program that
// starts and exits will do. None is the ssh client: a test's ssh is a fake,
// and a real one has no business being started for this.
var probePrograms = []string{"/bin/sh", "/usr/bin/sh", "/bin/true", "/usr/bin/true"}

// probeBound is how long the probe waits for its child to end.
var probeBound = 10 * time.Second

// probePidfd tries, on a real child that it starts, clone's pidfd,
// pidfd_open, pidfd_send_signal (signal 0), poll on a pidfd and
// waitid(P_PIDFD, WNOWAIT), and names the first that fails. A transport whose
// groups are signalled only behind a pinned pidfd refuses to run rather than
// signal unpinned: for an alpha that ran on kernels without them, that is a
// deliberate change (the ruling requires the pidfd).
func probePidfd() error {
	prog := ""
	for _, p := range probePrograms {
		if _, err := os.Stat(p); err == nil {
			prog = p
			break
		}
	}
	if prog == "" {
		return fmt.Errorf("sshrun: no program among %s to probe this kernel's pidfd support with", strings.Join(probePrograms, ", "))
	}
	cmd := exec.Command(prog, "-c", "exit 0")
	cmd.Env = []string{}
	fd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("sshrun: the pidfd probe could not start %s with a pidfd from clone (CLONE_PIDFD): %w", prog, err)
	}
	failed := probeChild(cmd.Process.Pid, fd)
	// The child has nobody's attention but ours: end and reap it, bounded.
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(probeBound):
		if fd >= 0 {
			_ = pidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
		}
		select {
		case <-done:
		case <-time.After(probeBound):
		}
	}
	if fd >= 0 {
		_ = unix.Close(fd)
	}
	return failed
}

// probeChild runs the calls on the child pid, whose pidfd from clone is fd.
// The child is not reaped yet, so it is alive or a zombie, and either answers.
func probeChild(pid, fd int) error {
	missing := func(what string, err error) error {
		return fmt.Errorf("sshrun: the ssh process groups are signalled only behind a pinned pidfd, and this kernel (or its filter) does not allow %s on a child: %w", what, err)
	}
	if fd < 0 {
		return missing("a pidfd from clone (CLONE_PIDFD)", errors.New("clone returned none"))
	}
	pfd, err := pidfdOpen(pid, 0)
	if err != nil {
		return missing("pidfd_open", err)
	}
	_ = unix.Close(pfd)
	// Signal 0 only asks; ESRCH is an answer too (the kernel took the call).
	if err := pidfdSendSignal(fd, 0, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
		return missing("pidfd_send_signal", err)
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	if _, err := retryEINTR(func() (int, error) { return pidfdPoll(fds, 0) }); err != nil {
		return missing("poll on a pidfd", err)
	}
	if fds[0].Revents&unix.POLLNVAL != 0 {
		return missing("poll on a pidfd", errors.New("the descriptor is not usable"))
	}
	if _, err := retryEINTR(func() (int, error) {
		return 0, pidfdWaitid(fd, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT)
	}); err != nil {
		return missing("waitid(P_PIDFD, WNOWAIT)", err)
	}
	return nil
}

// awaitExit blocks until the child behind the pidfd has exited, WITHOUT
// reaping it: waitid(P_PIDFD, WEXITED|WNOWAIT), which leaves a zombie whose
// PID, and so its group's number, cannot be handed out again.
func awaitExit(fd int) error {
	for {
		err := unix.Waitid(unix.P_PIDFD, fd, nil, unix.WEXITED|unix.WNOWAIT, nil)
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
