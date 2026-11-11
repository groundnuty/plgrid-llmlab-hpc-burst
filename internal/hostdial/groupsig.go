package hostdial

// New for issue #370, which brings #363's design (internal/slurm/sshrun,
// group.go, PR 365) to alpha's relays. A default-path relay is started in a
// process group of its own (startProcess) and used to be stopped by TERM and
// KILL to that group's number, with nothing in between: the class of bug that
// issue 308 was (a group signal with a PGID of 0 or 1, or our own group's,
// reaches every same-uid process). Every group signal on that path now goes
// through groupLeader.signal, behind four guards; science agent's ruling of 11
// Nov 2026, which lifts, for this change, the condition that alpha's hostdial
// code stay as it was ("same guards as #365 plus its reap barrier, on the
// default path"). The relay keeper's own path (pidfd.go, process.pf) is not
// touched: it signals one process through its pidfd and never a group.
//
// The guards, in order, each refusing with an error that names it and the PGID,
// and a log line:
//
//  1. the PGID is above 1 and is not this process's own group;
//  2. the leader was pinned when it started: its pidfd (pidfd_open on its PID,
//     taken before anything could reap it, so the number is its own) and its
//     start time (field 22 of /proc/<pid>/stat);
//  3. through the pidfd, the leader is still alive (poll shows no exit) and
//     still our child (waitid on the pidfd gives ECHILD for a process that is
//     not, or was reaped);
//  4. /proc still shows the pinned start time for the leader's PID.
//
// A refused signal is never sent another way. The pidfd is taken with
// pidfd_open and not from clone because the pin on alpha's attributes
// (TestAlphasRelayLeadsItsOwnGroupAndItsStopSignalsIt) says alpha's command
// asks clone for none.
//
// The reap barrier (the pattern of internal/agent/procgroup, as in PR 365):
// there is no pidfd group signal, so the last step is kill(-pgid), and a
// leader reaped between the guards and that call would free its group's
// number. The relay's waiter therefore waits for the exit WITHOUT reaping
// (waitid with WNOWAIT leaves a zombie, so the number stays ours), then sets
// reaping under the mutex every signal holds through its kill, and only then
// reaps. A signal after that is refused.
//
// A relay's ssh starts no group member of its own that a guard could miss when
// the leader is gone; what a leader-only end leaves running is its children,
// as before. The start time is read as procStartTicks (pidfd.go) reads it,
// copied for a seam (groupProcRead) so a test can make /proc unreadable.

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
	"time"

	"golang.org/x/sys/unix"
)

// killGroup is this file's only numeric signal: it sends sig to every process
// of the group pgid. It is reached only from groupLeader.signal, after all
// four guards (TestNoUnguardedGroupSignalsInTheDefaultPath keeps it so).
func killGroup(pgid int, sig syscall.Signal) error {
	return unix.Kill(-pgid, sig)
}

// The guards' names, as an error and the log carry them.
const (
	guardPGID       = "pgid-above-1"
	guardOwnGroup   = "not-our-own-group"
	guardUnpinned   = "leader-pinned"
	guardLeaderGone = "leader-alive"
	guardNotChild   = "leader-is-our-child"
	guardStartTime  = "leader-start-time"
	guardCannotTell = "leader-checkable" // a check itself failed: refused, never assumed
)

// groupGuardError is a refused group signal.
type groupGuardError struct {
	Guard  string
	PGID   int
	Detail string
}

func (e *groupGuardError) Error() string {
	return fmt.Sprintf("hostdial: process group %d not signalled: guard %q refused: %s", e.PGID, e.Guard, e.Detail)
}

// Is makes a refusal because the leader exited an os.ErrProcessDone.
func (e *groupGuardError) Is(target error) bool {
	return e.Guard == guardLeaderGone && target == os.ErrProcessDone
}

// groupLeader is a relay's process group leader, pinned when it started.
type groupLeader struct {
	log  *slog.Logger
	sink func(pgid int, sig syscall.Signal) error // killGroup, or a test's

	// Set by pin, before the leader is used.
	pinErr error
	pid    int    // the leader's PID: read through /proc, never a signal target
	pgid   int    // the group to signal: pid, for a leader that Setpgid made
	ticks  uint64 // the leader's start time, in clock ticks since boot
	fd     int    // the leader's pidfd; closed by release, after the reap

	mu      sync.Mutex // held across the checks and the signal, and by markReaping and release
	reaping bool       // set before any reap: no signal after it
	closed  bool       // after release: fd is not ours any more
}

func newGroupLeader(log *slog.Logger, sink func(int, syscall.Signal) error) *groupLeader {
	return &groupLeader{log: log, sink: sink, fd: -1}
}

// pin takes the leader's pidfd and start time. The process was just started
// and nobody has waited for it, so its PID is its own. A failure leaves pinErr
// set, which refuses every group signal.
func (g *groupLeader) pin(pid int) error {
	g.pid, g.pgid = pid, pid
	fail := func(err error) error {
		g.pinErr = fmt.Errorf("hostdial: the relay's ssh %d could not be pinned: %w", pid, err)
		return g.pinErr
	}
	fd, err := groupPidfdOpen(pid, 0)
	if err != nil {
		return fail(fmt.Errorf("pidfd_open: %w", err))
	}
	// A process that is our child answers waitid on its pidfd; one that is not
	// gives ECHILD.
	if _, err := retryEINTR(func() (int, error) {
		return 0, groupWaitid(fd, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT)
	}); err != nil {
		_ = unix.Close(fd)
		return fail(fmt.Errorf("it is not our child (waitid on its pidfd): %w", err))
	}
	ticks, err := groupStartTicks(pid)
	if err == nil && ticks == 0 {
		err = errors.New("its start time is 0")
	}
	if err != nil {
		_ = unix.Close(fd)
		return fail(fmt.Errorf("no start time: %w", err))
	}
	g.ticks, g.fd = ticks, fd
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

// markReaping is the barrier: it takes the mutex every signal holds, so it
// waits for a signal that is under way, and sets reaping, which refuses every
// later one. Every reap of a pinned leader comes after it.
func (g *groupLeader) markReaping() {
	g.mu.Lock()
	g.reaping = true
	g.mu.Unlock()
}

// reap is how a pinned leader is reaped: it waits for the exit without
// reaping, sets reaping under the signal mutex, reaps (cmd.Wait) and releases
// the pidfd. If the exit cannot be awaited that way, reaping is set all the
// same and the process is waited for, so its group is not signalled from then
// on: fail closed.
func (g *groupLeader) reap(cmd *exec.Cmd) error {
	if err := awaitExit(g.fd); err != nil {
		g.log.Error("the exit of a relay's ssh could not be awaited without reaping it: its group is not signalled from now on", "pid", g.pid, "err", err)
	}
	g.markReaping()
	err := cmd.Wait()
	g.release()
	return err
}

// exited reports whether the leader is known to have exited: its pidfd was
// released (after the reap), or polls readable. It is evidence, and false
// means no evidence, not that it runs.
func (g *groupLeader) exited() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return true
	}
	if g.fd < 0 {
		return false
	}
	fds := []unix.PollFd{{Fd: int32(g.fd), Events: unix.POLLIN}}
	n, err := retryEINTR(func() (int, error) { return groupPoll(fds, 0) })
	return err == nil && n > 0 && fds[0].Revents&(unix.POLLNVAL|unix.POLLERR) == 0
}

// signal sends sig to the leader's process group, or, when a guard refuses,
// sends nothing and returns a *groupGuardError naming it. It logs every
// refusal.
func (g *groupLeader) signal(sig syscall.Signal) error {
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
		g.log.Log(context.Background(), level, "a relay's process group is not signalled: a guard refused", "guard", refusal.Guard, "pgid", refusal.PGID, "signal", sig.String(), "detail", refusal.Detail)
	case !errors.Is(err, syscall.ESRCH):
		g.log.Warn("a signal to a relay's process group failed", "pgid", g.pgid, "signal", sig.String(), "err", err)
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
	if g.reaping {
		return refuse(guardLeaderGone, "it is being reaped: its group's number may be handed out again")
	}
	if g.closed {
		return refuse(guardLeaderGone, "its pidfd was closed after it was reaped")
	}
	// Guard 3: alive and our child, through the pidfd.
	fds := []unix.PollFd{{Fd: int32(g.fd), Events: unix.POLLIN}}
	n, err := retryEINTR(func() (int, error) { return groupPoll(fds, 0) })
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
		return 0, groupWaitid(g.fd, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT)
	})
	switch {
	case errors.Is(err, unix.ECHILD):
		return refuse(guardNotChild, "waitid on its pidfd says it is not our child")
	case err != nil:
		return refuse(guardCannotTell, "waitid on its pidfd failed: "+err.Error())
	}
	// Guard 4: the PID is still the one pinned. Fails closed: with no pidfd
	// group signal, an unreadable start time is a refusal.
	ticks, err := groupStartTicks(g.pid)
	switch {
	case errors.Is(err, errProcGone):
		return refuse(guardLeaderGone, "/proc has no row for it")
	case err != nil:
		return refuse(guardCannotTell, "its start time cannot be read: "+err.Error())
	case ticks != g.ticks:
		return refuse(guardStartTime, fmt.Sprintf("its start time is %d, the pinned one %d", ticks, g.ticks))
	}
	if err := g.sink(g.pgid, sig); err != nil {
		return fmt.Errorf("hostdial: signal %s to process group %d: %w", sig, g.pgid, err)
	}
	return nil
}

// groupStartTicks reads a process's start time, in clock ticks since boot,
// from /proc/<pid>/stat: field 22, counted after the command name, which may
// hold spaces and parentheses, so after its last ')'. procStartTicks
// (pidfd.go) reads it the same way, through os.ReadFile.
func groupStartTicks(pid int) (uint64, error) {
	b, err := groupProcRead("/proc/" + strconv.Itoa(pid) + "/stat")
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

// stopRetryPause is how long a refused group stop waits before its one retry.
var stopRetryPause = 200 * time.Millisecond

// stopGroup stops a pinned relay by its process group: TERM, then KILL after
// grace, and returns once it is reaped. Each signal goes through the leader's
// guards; one that is refused is retried once after stopRetryPause, and a
// refusal that stays ends the stop with an error, without a signal sent
// another way: the relay may still run. A leader that exited is not a refusal
// to act on, but "gone" is accepted only on evidence: the waiter reaped it, or
// its pidfd shows the exit, never after a wait alone.
func (p *process) stopGroup(grace time.Duration) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	if err := p.signalOnce(syscall.SIGTERM, grace); err != nil {
		return err
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
		return nil
	case <-t.C:
	}
	if err := p.signalOnce(syscall.SIGKILL, grace); err != nil {
		return err
	}
	select {
	case <-p.done:
	case <-time.After(killReapBound):
		return fmt.Errorf("hostdial: the relay's ssh %d was sent KILL and was not reaped within %s", p.pid, killReapBound)
	}
	return nil
}

// killReapBound is how long a stop waits for a relay it sent KILL to be
// reaped: a KILL to a live group ends its leader at once, so it never takes
// long, and the wait is bounded so that a stop can never hold its caller up
// forever.
var killReapBound = 30 * time.Second

// signalOnce sends sig through the guards, with the one retry, and says
// whether the relay is gone when the signal cannot be sent. nil means the
// signal was sent, or the relay is known to be gone.
func (p *process) signalOnce(sig syscall.Signal, grace time.Duration) error {
	err := p.lead.signal(sig)
	if err != nil {
		time.Sleep(stopRetryPause)
		err = p.lead.signal(sig)
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	// The leader is said to be gone: believe that only with evidence.
	select {
	case <-p.done:
		return nil
	case <-time.After(grace):
	}
	if p.lead.exited() {
		return nil
	}
	return fmt.Errorf("hostdial: the relay's ssh %d was not signalled (%w), and it has not been seen to exit", p.pid, err)
}
