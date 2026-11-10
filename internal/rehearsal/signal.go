package rehearsal

// New, after internal/slurm/fakeslurm/procsweep.go (signalOne): what the nodes
// signal, and how (issue #308, guard 3 in procs.go's header).
//
// A process is signalled only when a walk up its parents, made while it is
// pinned with a pidfd, reaches the test process, and only through that pidfd.
// Each hop is checked against PID reuse by start-time order: a parent started
// no later than its child, so a "parent" that started later is a reused PID.
// A process that is not below the test any more, such as one whose parent
// exited and left it to PID 1 or the user manager, is never signalled: it is
// a leak, counted and reported with its command name, start time and node.
// The forced commands cannot become one (relay.go: a parent-death signal), so
// a passing run reports none.
//
// The residual: start times have the clock tick's resolution (10 ms), so a
// parent that exited, gave up its PID, and had it reused within the tick its
// child started in would pass. That takes a wrap of the PID space in 10 ms.

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
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
	// comm is a process's command name, for a report.
	comm func(pid int) string
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
	// started, when set, is called with each sshd's PID after it starts and
	// before its row is read (tests).
	started func(pid int)
	// leaks counts the processes left outside the test's tree: the process's
	// total for the host's, a test's own otherwise.
	leaks *atomic.Int64
}

// hostProcs is the kernel's own /proc.
func hostProcs() *procAPI {
	return &procAPI{
		self:    os.Getpid(),
		row:     statRow,
		comm:    readComm,
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
		leaks: &leaked,
	}
}

func readComm(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	if err != nil {
		return "?"
	}
	return strings.TrimSpace(string(b))
}

// leaked counts the processes the nodes of this process left outside the
// test's tree; ReportLeaks prints it.
var leaked atomic.Int64

// maxDepth bounds a walk up the parents, which a listing that changes while
// it is read must not make endless.
const maxDepth = 4096

// owner is who the signals are for: the process the nodes run in, with its
// start time, and its ancestors.
type owner struct {
	api       *procAPI
	node      string
	self      procRow
	ancestors map[int]bool
}

// ownerOf reads the process's row and its ancestors, up to and excluding PID
// 1. An ancestry that cannot be read is an error: an ancestor that is not
// known could be signalled, so every signal is refused.
func ownerOf(api *procAPI, node string) (*owner, error) {
	o := &owner{api: api, node: node, ancestors: map[int]bool{}}
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
// started no later than its child. why says, when the process is not below
// the test, where the walk left the test's tree; a hop that cannot be read is
// an error.
func (o *owner) walk(t procRow) (below bool, why string, err error) {
	child := t
	for depth := 0; depth < maxDepth; depth++ {
		if child.ppid == o.self.pid {
			if o.self.ticks > child.ticks {
				return false, fmt.Sprintf("process %d started before the test process", child.pid), nil
			}
			return true, "", nil
		}
		if child.ppid <= 1 {
			return false, fmt.Sprintf("its ancestry reaches PID %d without the test process", child.ppid), nil
		}
		p, plive, perr := o.api.row(child.ppid)
		switch {
		case perr != nil:
			return false, "", fmt.Errorf("its ancestor %d: %w", child.ppid, perr)
		case !plive:
			return false, fmt.Sprintf("its ancestor %d has exited", child.ppid), nil
		case p.ticks > child.ticks:
			return false, fmt.Sprintf("its ancestor %d started after process %d: a reused PID", child.ppid, child.pid), nil
		}
		child = p
	}
	return false, fmt.Sprintf("its ancestry is deeper than %d", maxDepth), nil
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
// process itself and its ancestors. A process that is not below the test is a
// leak: never signalled, counted, and reported. Every refusal and leak, and a
// process that cannot be pinned, read or signalled, is an error, and the
// others are still signalled. It returns the rows it neither refused nor
// failed on. node names the node in a report.
func signalProcs(api *procAPI, node string, rows []procRow, sig syscall.Signal) ([]procRow, error) {
	o, err := ownerOf(api, node)
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
	below, why, err := o.walk(now)
	if err != nil {
		return refusal(r, sig, "its ancestry cannot be read: "+err.Error())
	}
	if !below {
		o.api.leaks.Add(1)
		return fmt.Errorf("rehearsal: the %s left process %d (%s, started at tick %d) outside the test's tree: %s; it was not signalled (issue #308)",
			o.node, r.pid, o.api.comm(r.pid), r.ticks, why)
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

// ReportLeaks is for the TestMain of a package whose tests start a node,
// after m.Run: it prints how many processes the nodes left outside the test's
// tree, and returns the exit code, 1 in place of 0 when there was any. Each
// leak has failed its node's Close already; this is the run's total, which a
// passing run shows as 0.
func ReportLeaks(code int) int {
	n := leaked.Load()
	fmt.Printf("rehearsal: %d leaked (processes the nodes left outside the test's tree, never signalled)\n", n)
	if n > 0 {
		return max(code, 1)
	}
	return code
}
