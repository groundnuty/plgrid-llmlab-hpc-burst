package rehearsal

// New, after internal/slurm/fakeslurm/procsweep.go (signalOne): what the nodes
// signal, and how (issue #308, guard 3 in procs.go's header).
//
// Ownership is by construction. The test process is a child subreaper, so an
// orphan of anything it started is reparented to it, never to PID 1 or the
// user manager: every process a node started stays below the test process
// for as long as it lives. signalProcs signals a process only when a walk up
// its parents, made while the process is pinned with a pidfd, reaches the test
// process, and only through that pidfd.
//
// Each hop of the walk is checked against PID reuse by start-time order: a
// parent started no later than its child, so a "parent" that started later is
// a reused PID, and the process is refused. A hop that has exited, or started
// later, can also mean the walk raced a reparenting, as when an sshd ends
// while its session child is read. The pinned process is then read again, and
// only a parent that changed earns another walk. A row that cannot be read
// refuses at once. The residual: start times have the clock tick's resolution
// (10 ms), so a parent that exited, gave up its PID, and had it reused within
// the tick its child started in would pass. That takes a wrap of the PID space
// in 10 ms.

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// procAPI is what the nodes ask of the kernel. hostProcs is the host's; a test
// replaces single fields to script a row or an exited sshd, and records the
// signals in place of sending them. Every signal the nodes send goes through
// send or kill.
type procAPI struct {
	// self is the PID of the process the nodes run in.
	self int
	// row reads one process: live is false for one that has exited, a zombie
	// included, and err is set for one whose row exists but cannot be read.
	row func(pid int) (r procRow, live bool, err error)
	// open pins a process with a pidfd; ESRCH when it is gone, and EINVAL
	// for one that is mid-exit on some kernels (fakeslurm, issue #211).
	open func(pid int) (fd int, err error)
	// probe is kill(pid, 0), sent only after open answered EINVAL.
	probe func(pid int) error
	// send signals the process the pidfd names.
	send    func(fd int, sig syscall.Signal) error
	closeFd func(fd int)
	// kill ends an sshd the pool started, through its os.Process (a pidfd
	// since go 1.23), never by its number.
	kill func(p *os.Process) error
	// reap collects one zombie child of this process by its PID, without
	// waiting (waitid P_PID, WNOHANG); never any other PID.
	reap func(pid int) error
	// started, when set, is called with each sshd's PID after it starts and
	// before its row is read (tests).
	started func(pid int)
}

// hostProcs is the kernel's own /proc.
func hostProcs() *procAPI {
	return &procAPI{
		self:    os.Getpid(),
		row:     statRow,
		open:    func(pid int) (int, error) { return unix.PidfdOpen(pid, 0) },
		probe:   func(pid int) error { return syscall.Kill(pid, 0) },
		send:    func(fd int, sig syscall.Signal) error { return unix.PidfdSendSignal(fd, sig, nil, 0) },
		closeFd: func(fd int) { _ = unix.Close(fd) },
		kill: func(p *os.Process) error {
			if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return err
			}
			return nil
		},
		reap: func(pid int) error {
			var info unix.Siginfo
			return unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOHANG, nil)
		},
	}
}

var subreaper struct {
	once sync.Once
	err  error
}

// ensureSubreaper makes this process a child subreaper, once: every orphan of
// what it starts is then reparented to it, so what a node started stays below
// it after its sshd has gone. The attribute is the whole process's and stays
// for its life, so the later tests of a binary that started a node run under
// it too (the audit of issue #308's fix lists what that changes). Each node
// calls it before it starts its first process.
func ensureSubreaper() error {
	subreaper.once.Do(func() {
		if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
			subreaper.err = fmt.Errorf("rehearsal: make this process a child subreaper: %w", err)
		}
	})
	return subreaper.err
}

// maxDepth bounds a walk up the parents, which a listing that changes while
// it is read must not make endless.
const maxDepth = 4096

// maxWalks bounds the walks of one process whose parent changed during them.
const maxWalks = 3

// owner is who the signals are for: the process the nodes run in, with its
// start time, and its ancestors.
type owner struct {
	api       *procAPI
	self      procRow
	ancestors map[int]bool
}

// ownerOf reads the process's row and its ancestors, up to and excluding PID
// 1. An ancestry that cannot be read is an error: an ancestor that is not
// known could be signalled, so every signal is refused.
func ownerOf(api *procAPI) (*owner, error) {
	o := &owner{api: api, ancestors: map[int]bool{}}
	pid := api.self
	for depth := 0; ; depth++ {
		r, live, err := api.row(pid)
		switch {
		case err != nil:
			return nil, fmt.Errorf("the ancestry of process %d: %w", api.self, err)
		case !live:
			return nil, fmt.Errorf("the ancestry of process %d: process %d is not live", api.self, pid)
		case depth >= maxDepth:
			return nil, fmt.Errorf("the ancestry of process %d is deeper than %d", api.self, maxDepth)
		}
		if depth == 0 {
			o.self = r
		}
		if r.ppid <= 1 {
			return o, nil
		}
		o.ancestors[r.ppid] = true
		pid = r.ppid
	}
}

// walk goes up from a live process to the test process. Each parent must have
// started no later than its child. moved reports a hop that has exited or
// started later than its child, which a reparenting during the walk can also
// explain; a hop that cannot be read is an error.
func (o *owner) walk(t procRow) (below, moved bool, err error) {
	child := t
	for depth := 0; depth < maxDepth; depth++ {
		if child.ppid == o.self.pid {
			return o.self.ticks <= child.ticks, false, nil
		}
		if child.ppid <= 1 {
			return false, false, nil
		}
		p, live, perr := o.api.row(child.ppid)
		switch {
		case perr != nil:
			return false, false, fmt.Errorf("its ancestor %d: %w", child.ppid, perr)
		case !live || p.ticks > child.ticks:
			return false, true, nil
		}
		child = p
	}
	return false, false, nil
}

// isBelow reports whether a pinned, live process is below the test process. A
// walk that moved reads the process again, and walks again only when its
// parent changed. gone reports that it ended meanwhile.
func (o *owner) isBelow(t procRow) (below, gone bool, err error) {
	for walks := 1; ; walks++ {
		below, moved, err := o.walk(t)
		if err != nil || below || !moved || walks >= maxWalks {
			return below, false, err
		}
		again, live, err := o.api.row(t.pid)
		switch {
		case err != nil:
			return false, false, err
		case !live || again.ticks != t.ticks:
			return false, true, nil
		case again.ppid == t.ppid:
			return false, false, nil
		}
		t = again
	}
}

// refusal is the error for a process signalProcs will not signal.
func refusal(r procRow, sig syscall.Signal, why string) error {
	return fmt.Errorf("rehearsal: refused to send %v to process %d: %s (issue #308)", sig, r.pid, why)
}

// signalProcs sends sig to each listed process that is still the one listed
// and is strictly below the test process. Each is pinned with a pidfd first
// and checked after that: a PID that now names a process with another start
// time was recycled and is left alone, and the signal goes through the pidfd,
// never to a PID. It refuses PID 1 and below, a row with no start time, the
// process itself, its ancestors, and a process that is not below it. A
// refusal, and a process that cannot be pinned, read or signalled, is an
// error, and the others are still signalled. It returns the rows it neither
// refused nor failed on.
func signalProcs(api *procAPI, rows []procRow, sig syscall.Signal) ([]procRow, error) {
	o, err := ownerOf(api)
	if err != nil {
		return nil, fmt.Errorf("rehearsal: refused to send %v to %d processes: %w (issue #308)", sig, len(rows), err)
	}
	var kept []procRow
	var errs []error
	for _, r := range rows {
		if err := o.signalOne(r, sig); err != nil {
			errs = append(errs, err)
			continue
		}
		kept = append(kept, r)
	}
	return kept, errors.Join(errs...)
}

func (o *owner) signalOne(r procRow, sig syscall.Signal) error {
	switch {
	case r.pid <= 1:
		return refusal(r, sig, "PID 1 and below are the host's")
	case r.pid == o.self.pid:
		return refusal(r, sig, "it is the process that sends it")
	case r.ticks == 0:
		return refusal(r, sig, "its row has no start time")
	}
	fd, err := o.api.open(r.pid)
	if errors.Is(err, syscall.ESRCH) {
		return nil // it ended since the listing
	}
	if errors.Is(err, syscall.EINVAL) {
		// A process that exits between the listing and the pidfd can answer EINVAL, not ESRCH (fakeslurm,
		// issue #211). EINVAL is taken as "ended" only when a re-check agrees.
		gone, cerr := o.endedSince(r.pid)
		switch {
		case cerr != nil:
			return fmt.Errorf("rehearsal: pidfd_open of process %d: %w (re-check: %v)", r.pid, err, cerr)
		case gone:
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("rehearsal: pidfd_open of process %d: %w", r.pid, err)
	}
	defer o.api.closeFd(fd)
	now, live, err := o.api.row(r.pid)
	switch {
	case err != nil:
		return fmt.Errorf("rehearsal: read process %d: %w", r.pid, err)
	case !live || now.ticks != r.ticks:
		return nil // gone, or the PID names another process now
	case o.ancestors[r.pid]:
		return refusal(r, sig, "it is an ancestor of the process that sends it")
	}
	below, gone, err := o.isBelow(now)
	switch {
	case err != nil:
		return refusal(r, sig, "its ancestry cannot be read: "+err.Error())
	case gone:
		return nil
	}
	if !below {
		return refusal(r, sig, "it is not below the process that sends it")
	}
	if err := o.api.send(fd, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("rehearsal: send %v to process %d: %w", sig, r.pid, err)
	}
	return nil
}

// endedSince re-checks a listed process after pidfd_open answered EINVAL: it
// has ended when kill(pid, 0) says no such process, or when its row shows it
// is not live. One that kill(0) finds and whose row is live has not ended, and
// a re-check that cannot be made is an error.
func (o *owner) endedSince(pid int) (bool, error) {
	switch err := o.api.probe(pid); {
	case errors.Is(err, syscall.ESRCH):
		return true, nil
	case err != nil && !errors.Is(err, syscall.EPERM):
		return false, fmt.Errorf("kill(%d, 0): %w", pid, err)
	}
	_, live, err := o.api.row(pid)
	if err != nil {
		return false, fmt.Errorf("read process %d: %w", pid, err)
	}
	return !live, nil
}

// reapAdopted reaps the zombies among rows that this process adopted as a
// subreaper. A row qualifies only when it was recorded with another parent, so
// this process never forked it and no exec.Cmd waits for it, and it is now
// this process's zombie child with the start time it was recorded with. It
// reaps by that PID alone, never -1, so no exec.Cmd loses its status. The rest
// stay zombies until this process exits, holding a PID and nothing else: a
// row already adopted when it was listed (its parent this process, as an
// exec.Cmd's is), and a process the nodes started that no sample recorded.
func reapAdopted(api *procAPI, rows []procRow) error {
	var errs []error
	for _, r := range rows {
		if r.ppid == api.self || r.ppid <= 0 || r.ticks == 0 {
			continue
		}
		state, now, gone, err := readStat(r.pid)
		if err != nil || gone || state != "Z" || now.ppid != api.self || now.ticks != r.ticks {
			continue
		}
		if err := api.reap(r.pid); err != nil && !errors.Is(err, syscall.ECHILD) {
			errs = append(errs, fmt.Errorf("rehearsal: reap adopted process %d: %w", r.pid, err))
		}
	}
	return errors.Join(errs...)
}
