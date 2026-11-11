package sshrun

// New for issue 363: every process-group signal in this package goes through
// groupLeader.signal, behind four guards. sshrun starts its ssh processes
// (the master, and "ssh -O check") in a process group of their own and used
// to signal the group by its leader's PID with nothing in between, the class
// of bug that issue 308 was: a group signal with a PGID of 0 or 1, or with
// our own group's, reaches every same-uid process (kill(2): -1 and 0 mean
// "all" and "my group"), and a negative PGID names a single process.
//
// The guards (science agent's ruling on issue 363, 11 Nov 2026), in order,
// each refusing with an error that names it and the PGID, and a log line:
//
//  1. the PGID is above 1 and is not this process's own group;
//  2. the leader was pinned when it was started: its pidfd (from clone, so
//     it names that process and nothing else while it is open) and its start
//     time (field 22 of /proc/<pid>/stat), read before anything can reap it;
//  3. through the pidfd, the leader is still alive (poll shows no exit) and
//     still our child (waitid on the pidfd gives ECHILD for a process that
//     is not, or was reaped);
//  4. /proc still shows the pinned start time for the leader's PID.
//
// A refused signal is never sent another way: no fallback to an unguarded
// kill, and none to a signal on the leader alone.
//
// What stays open, stated plainly: there is no pidfd group signal, so the
// last step is kill(-pgid), and the guards and that call are not one atomic
// step. For the group to be the wrong one in that gap, the leader would have
// to exit and be reaped, and its number be handed to a new process, all
// within microseconds. The kernel allocates PIDs cyclically up to pid_max,
// and keeps a PGID allocated for as long as any member of the group lives.
//
// The start time is read as hostdial reads it (internal/hostdial/pidfd.go,
// procStartTicks, PR 349): copied rather than imported, since hostdial is
// the keeper's package and that PR is in review.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// killGroup is the package's only numeric signal: it sends sig to every
// process of the group pgid. It is reached only from groupLeader.signal,
// after all four guards (TestNoUnguardedSignalsInThePackage keeps it so).
func killGroup(pgid int, sig syscall.Signal) error {
	return unix.Kill(-pgid, sig)
}

// pidfdOpen is the kernel's pidfd_open; a test replaces it to show that New
// refuses a kernel with none.
var pidfdOpen = unix.PidfdOpen

// checkPidfd fails when this kernel gives no pidfd: a transport whose process
// groups are signalled only behind a pinned pidfd refuses to run rather than
// signal unpinned.
func checkPidfd() error {
	fd, err := pidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("sshrun: the ssh process groups are signalled only behind a pinned pidfd, and this kernel gives none: %w", err)
	}
	_ = unix.Close(fd)
	return nil
}

// The guards' names, as an error and the log carry them.
const (
	guardPGID       = "pgid-above-1"        // 1: the PGID is 1 or less
	guardOwnGroup   = "not-our-own-group"   // 1: the PGID is this process's own group
	guardUnpinned   = "leader-pinned"       // 2: the leader could not be pinned when it started
	guardLeaderGone = "leader-alive"        // 3: the leader exited (or was reaped)
	guardNotChild   = "leader-is-our-child" // 3: the pidfd names a process that is not our child
	guardStartTime  = "leader-start-time"   // 4: the PID shows another start time than the pin
	guardCannotTell = "leader-checkable"    // a check itself failed: refused, never assumed
)

// groupGuardError is a refused group signal.
type groupGuardError struct {
	Guard  string
	PGID   int
	Detail string
}

func (e *groupGuardError) Error() string {
	return fmt.Sprintf("sshrun: process group %d not signalled: guard %q refused: %s", e.PGID, e.Guard, e.Detail)
}

// Is makes a refusal because the leader exited an os.ErrProcessDone, which
// exec.Cmd's Cancel contract treats as "nothing left to cancel".
func (e *groupGuardError) Is(target error) bool {
	return e.Guard == guardLeaderGone && target == os.ErrProcessDone
}

// groupLeader is a process group's leader, pinned when it was started.
type groupLeader struct {
	log  *slog.Logger
	sink func(pgid int, sig syscall.Signal) error // killGroup, or a test's

	// ready closes once the leader is pinned or pinning failed. signal waits
	// for it: exec's context watcher can call Cancel the moment Start returns.
	ready chan struct{}
	once  sync.Once

	// Set before ready closes, read after.
	pinErr error
	pid    int    // the leader's PID: read through /proc, never a signal target
	pgid   int    // the group to signal: pid, for a leader that Setpgid made
	ticks  uint64 // the leader's start time, in clock ticks since boot
	fd     int    // the leader's pidfd; closed by release, after the reap

	mu     sync.Mutex // held across the checks and the signal, and by release
	closed bool       // after release: fd is not ours any more
}

func newGroupLeader(log *slog.Logger, sink func(int, syscall.Signal) error) *groupLeader {
	return &groupLeader{log: log, sink: sink, ready: make(chan struct{}), fd: -1}
}

// settle ends the pinning, with err when it failed.
func (g *groupLeader) settle(err error) {
	g.once.Do(func() {
		g.pinErr = err
		close(g.ready)
	})
}

// pin records the leader pid, whose pidfd is fd, and its start time. The
// process must not have been reaped yet, so its PID is its own.
func (g *groupLeader) pin(pid, fd int) error {
	g.pid, g.pgid = pid, pid
	ticks, err := procStartTicks(pid)
	if err == nil && ticks == 0 {
		err = errors.New("its start time is 0")
	}
	if err != nil {
		err = fmt.Errorf("sshrun: the ssh process %d has no pinned start time: %w", pid, err)
		g.settle(err)
		return err
	}
	g.ticks, g.fd = ticks, fd
	g.settle(nil)
	return nil
}

// start starts cmd as the leader of a process group of its own, with a pidfd
// from clone, and pins it. cmd.SysProcAttr's other fields are kept. A start
// whose leader cannot be pinned is ended through its pidfd (the leader alone,
// never a group) and refused, so nothing runs that could not be signalled
// safely later.
func (g *groupLeader) start(cmd *exec.Cmd) error {
	var attr syscall.SysProcAttr
	if cmd.SysProcAttr != nil {
		attr = *cmd.SysProcAttr
	}
	fd := -1
	attr.Setpgid, attr.PidFD = true, &fd
	cmd.SysProcAttr = &attr
	if err := cmd.Start(); err != nil {
		g.settle(err)
		return err
	}
	pid := cmd.Process.Pid
	if fd < 0 {
		// Cannot happen on a kernel that passed checkPidfd: clone's pidfd
		// (Linux 5.2) predates pidfd_open (5.3), and Go's Start fails when
		// clone refuses the flag. Should it happen anyway, nothing here can
		// signal the process safely: it is reaped in the background, and
		// ends by itself or with this process (a master's parent-death
		// signal).
		err := fmt.Errorf("sshrun: the ssh process %d started without a pidfd from clone: refused, and never signalled", pid)
		g.settle(err)
		g.log.Error("an ssh process started without a pidfd from clone: refused, and never signalled", "pid", pid)
		go func() { _ = cmd.Wait() }()
		return err
	}
	if err := g.pin(pid, fd); err != nil {
		g.log.Error("an ssh process could not be pinned: it is ended through its pidfd and refused", "pid", pid, "err", err)
		_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
		_ = cmd.Wait()
		_ = unix.Close(fd)
		return err
	}
	return nil
}

// release closes the pidfd; call it once the leader was reaped (after
// cmd.Wait returned). A signal that comes later is refused, so it never goes
// through a descriptor number the kernel may have given to another file.
func (g *groupLeader) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed && g.fd >= 0 {
		g.closed = true
		_ = unix.Close(g.fd)
	}
}

// signal sends sig to the leader's process group, or, when a guard refuses,
// sends nothing and returns a *groupGuardError naming it. It logs every
// refusal.
func (g *groupLeader) signal(sig syscall.Signal) error {
	<-g.ready
	g.mu.Lock()
	defer g.mu.Unlock()
	err := g.checked(sig)
	var refusal *groupGuardError
	switch {
	case err == nil:
	case errors.As(err, &refusal):
		level := slog.LevelWarn
		if refusal.Guard == guardLeaderGone {
			level = slog.LevelInfo // the ordinary race: it ended as we were about to stop it
		}
		g.log.Log(context.Background(), level, "a process group is not signalled: a guard refused", "guard", refusal.Guard, "pgid", refusal.PGID, "signal", sig.String(), "detail", refusal.Detail)
	case !errors.Is(err, syscall.ESRCH):
		g.log.Warn("a signal to a process group failed", "pgid", g.pgid, "signal", sig.String(), "err", err)
	}
	return err
}

// checked runs the guards, then the sink. The caller holds g.mu.
func (g *groupLeader) checked(sig syscall.Signal) error {
	refuse := func(guard, detail string) error {
		return &groupGuardError{Guard: guard, PGID: g.pgid, Detail: detail}
	}
	// Guard 1: no group whose number reaches more than one group, or none.
	switch {
	case g.pgid <= 1:
		return refuse(guardPGID, "kill(2) would reach every process, this group, or a single process by a negative number")
	case g.pgid == unix.Getpgrp():
		return refuse(guardOwnGroup, "it is this process's own group")
	}
	// Guard 2: a leader that was pinned.
	if g.pinErr != nil {
		return refuse(guardUnpinned, g.pinErr.Error())
	}
	if g.closed {
		return refuse(guardLeaderGone, "its pidfd was closed after it was reaped")
	}
	// Guard 3: alive and our child, through the pidfd.
	fds := []unix.PollFd{{Fd: int32(g.fd), Events: unix.POLLIN}}
	n, err := retryEINTR(func() (int, error) { return unix.Poll(fds, 0) })
	switch {
	case err != nil:
		return refuse(guardCannotTell, "poll on its pidfd failed: "+err.Error())
	case n > 0 && fds[0].Revents&(unix.POLLNVAL|unix.POLLERR) != 0:
		return refuse(guardCannotTell, "its pidfd is not usable")
	case n > 0:
		return refuse(guardLeaderGone, "its pidfd shows that it exited")
	}
	// WNOWAIT: look, do not reap; the process is Go's to wait for. ECHILD is
	// the answer for a process that is not our child, or was reaped.
	_, err = retryEINTR(func() (int, error) {
		return 0, unix.Waitid(unix.P_PIDFD, g.fd, nil, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
	})
	switch {
	case errors.Is(err, unix.ECHILD):
		return refuse(guardNotChild, "waitid on its pidfd says it is not our child")
	case err != nil:
		return refuse(guardCannotTell, "waitid on its pidfd failed: "+err.Error())
	}
	// Guard 4: the PID is still the one pinned. Fails closed: with no pidfd
	// group signal, an unreadable start time is a refusal.
	ticks, err := procStartTicks(g.pid)
	switch {
	case errors.Is(err, errProcGone):
		return refuse(guardLeaderGone, "/proc has no row for it")
	case err != nil:
		return refuse(guardCannotTell, "its start time cannot be read: "+err.Error())
	case ticks != g.ticks:
		return refuse(guardStartTime, fmt.Sprintf("its start time is %d, the pinned one %d", ticks, g.ticks))
	}
	if err := g.sink(g.pgid, sig); err != nil {
		return fmt.Errorf("sshrun: signal %s to process group %d: %w", sig, g.pgid, err)
	}
	return nil
}

func retryEINTR(f func() (int, error)) (int, error) {
	for {
		n, err := f()
		if !errors.Is(err, syscall.EINTR) {
			return n, err
		}
	}
}

// errProcGone: /proc has no row for the PID.
var errProcGone = errors.New("no such process")

// procStartTicks reads a process's start time, in clock ticks since boot,
// from /proc/<pid>/stat: field 22, counted after the command name, which may
// hold spaces and parentheses, so after its last ')'.
func procStartTicks(pid int) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return 0, errProcGone
	case err != nil:
		return 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	// f[0] is the state (field 3), so the start time (field 22) is f[19].
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, fmt.Errorf("/proc/%d/stat has %d fields after the command name, want at least 20", pid, len(f))
	}
	ticks, err := strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("/proc/%d/stat has an unreadable start time", pid)
	}
	return ticks, nil
}
