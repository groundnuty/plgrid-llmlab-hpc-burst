package fakeslurm

// New. Run mode: finding a job's processes and signalling them without ever
// signalling one the fake did not start.
//
// Two identities have to hold. The session ID is the batch shell's PID, and
// the kernel keeps that number allocated while the shell is a zombie or any
// session member lives, so a scan by SID selects the job's session only while
// the shell has not been reaped (runwait.go keeps it unreaped until the last
// scan; jobRun.scanLive). And a member, once listed, is signalled through a
// pidfd opened before its start time is checked again, so the signal reaches
// the process that was checked.
//
// The one window left is the listing itself: a descendant is any process whose
// parent's PID is a member's, read over a few milliseconds. To signal a
// stranger by it, a member would have to exit, its PID be given to an
// unrelated process, and that process fork, all inside one listing, which
// takes a wrap of the whole PID space (pid_max, 4 million) in that time. It
// cannot be shown by a test, and the pidfd check does not close it.

import (
	"errors"
	"fmt"
	"syscall"
	"time"
)

// sessionRows lists the live processes of the job whose batch shell is sid:
// the members of its session and, transitively, their children. The caller
// holds the shell unreaped.
func sessionRows(api *procAPI, sid int) ([]procRow, error) {
	if api.scanned != nil {
		api.scanned(sid)
	}
	rows, err := api.list()
	if err != nil {
		return nil, err
	}
	in := map[int]bool{}
	for _, r := range rows {
		if r.sid == sid {
			in[r.pid] = true
		}
	}
	for changed := len(in) > 0; changed; {
		changed = false
		for _, r := range rows {
			if !in[r.pid] && in[r.ppid] {
				in[r.pid] = true
				changed = true
			}
		}
	}
	var out []procRow
	for _, r := range rows {
		if in[r.pid] {
			out = append(out, r)
		}
	}
	return out, nil
}

// signalProcs sends sig to each listed process that is still the one listed.
// Each is pinned with a pidfd first, and its start time is read after that: a
// PID that now names a process with another start time was recycled and is
// left alone, and the signal goes through the pidfd, to the process that was
// checked, never to a PID. A process that has exited is not an error; one
// that cannot be pinned, read or signalled is, and the others are still
// signalled.
func signalProcs(api *procAPI, rows []procRow, sig syscall.Signal) error {
	var errs []error
	for _, r := range rows {
		if err := signalOne(api, r, sig); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func signalOne(api *procAPI, r procRow, sig syscall.Signal) error {
	fd, err := api.open(r.pid)
	if errors.Is(err, syscall.ESRCH) {
		return nil // it ended since the listing
	}
	if err != nil {
		return fmt.Errorf("pidfd_open of process %d: %w", r.pid, err)
	}
	defer api.closeFd(fd)
	now, live, err := api.row(r.pid)
	if err != nil {
		return fmt.Errorf("read process %d: %w", r.pid, err)
	}
	if !live || now.ticks != r.ticks {
		return nil // gone, or the PID names another process now
	}
	if err := api.send(fd, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal %v to process %d: %w", sig, r.pid, err)
	}
	return nil
}

// sweepSession kills every process left in the job's session until none is
// left. One listing that finds none is not believed: a process forked after
// the last KILL can fork a child and exit between the directory read and its
// own stat read, and the listing then misses the child, so a second listing,
// after a pause, has to agree. Processes that survive KILL for limit (stuck in
// the kernel), and a /proc or a signal that cannot be read or sent, are
// errors: the job's processes are not known to be gone.
func sweepSession(api *procAPI, sid int, limit, interval time.Duration) error {
	deadline := time.Now().Add(limit)
	empty := 0
	for {
		rows, err := sessionRows(api, sid)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			if empty++; empty >= 2 {
				return nil
			}
		} else {
			empty = 0
			if time.Now().After(deadline) {
				return fmt.Errorf("%d processes of the job survive SIGKILL", len(rows))
			}
			if err := signalProcs(api, rows, syscall.SIGKILL); err != nil {
				return err
			}
		}
		time.Sleep(interval)
	}
}
