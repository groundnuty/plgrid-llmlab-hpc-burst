package launcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
)

// DefaultGrace is how long down waits after SIGTERM before it sends SIGKILL.
const DefaultGrace = 30 * time.Second

// killWait is how long down waits for a process to disappear after SIGKILL.
const killWait = 10 * time.Second

// socketReleaseWait is how long down waits for the kernel to release the sockets of a dead process.
const socketReleaseWait = 5 * time.Second

func logf(w io.Writer, format string, a ...any) {
	if w != nil {
		fmt.Fprintf(w, format+"\n", a...)
	}
}

// ProcStop is what happened to one recorded process.
type ProcStop struct {
	Name    string `json:"name"`
	PID     int    `json:"pid"`
	Before  string `json:"before"`           // identity verdict before any signal: ours, gone or foreign
	Detail  string `json:"detail,omitempty"` // why, for gone and foreign
	Signals string `json:"signals,omitempty"`
	TookMS  int64  `json:"took_ms"`
}

// StopReport describes a stop. ForeignOnPorts lists listeners that sit on the recorded ports
// afterwards but belong to someone else: down leaves them alone and says so.
type StopReport struct {
	Procs          []ProcStop `json:"processes"`
	GoneListeners  int        `json:"listeners_gone"`
	ForeignOnPorts []string   `json:"foreign_listeners_on_ports,omitempty"`
}

// signalOurs sends sig to the process only if it is still the recorded one. The identity is
// checked, then a handle to the process is taken (a pidfd, where the kernel has one, so the
// signal cannot reach a process that reuses the PID later), then the identity is checked
// again: a PID that was reused in between is caught by the second check.
func signalOurs(rec ProcRec, sig syscall.Signal) (bool, error) {
	if v, _ := identify(rec); v != Ours {
		return false, nil
	}
	p, err := os.FindProcess(rec.PID)
	if err != nil {
		return false, err
	}
	defer p.Release()
	if v, _ := identify(rec); v != Ours {
		return false, nil
	}
	if err := p.Signal(sig); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// waitGone polls until none of the records is a live process of ours, or the time is up.
func waitGone(ctx context.Context, recs []ProcRec, d time.Duration) bool {
	end := time.Now().Add(d)
	for {
		alive := false
		for _, r := range recs {
			if v, _ := identify(r); v == Ours {
				alive = true
				break
			}
		}
		if !alive {
			return true
		}
		if time.Now().After(end) || ctx.Err() != nil {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stopRecorded stops the processes in the record, and only those. For each it verifies PID,
// start time and boot ID before any signal: a process that no longer matches is reported and
// never signalled. A verified one gets SIGTERM, and SIGKILL (verified again) after the grace
// period. Its descendants, found while it was verified, are held to the same rule. Then it
// confirms that the processes are gone and that the sockets they held no longer listen.
func stopRecorded(ctx context.Context, st *State, grace time.Duration, log io.Writer) (*StopReport, error) {
	// Capacity is fixed: stops holds pointers into rep.Procs, which must not be reallocated.
	rep := &StopReport{Procs: make([]ProcStop, 0, len(st.Procs))}
	var ours, kidRecs []ProcRec
	inodes := map[string]bool{}
	stops := map[int]*ProcStop{}
	starts := map[int]time.Time{}
	for _, rec := range st.Procs {
		v, detail := identify(rec)
		ps := ProcStop{Name: rec.Name, PID: rec.PID, Before: v.String(), Detail: detail}
		rep.Procs = append(rep.Procs, ps)
		stops[rec.PID] = &rep.Procs[len(rep.Procs)-1]
		switch v {
		case Ours:
			ours = append(ours, rec)
			owned, _, err := ownedListeners([]int{rec.PID})
			if err != nil {
				return rep, fmt.Errorf("cannot list the listeners of %s (pid %d), so could not confirm later that they are gone: %w", rec.Name, rec.PID, err)
			}
			for _, s := range owned {
				inodes[s.inode] = true
			}
			for _, pid := range descendants([]int{rec.PID}) {
				if pid == rec.PID {
					continue
				}
				if kr, err := newRec(fmt.Sprintf("%s-child", rec.Name), pid); err == nil {
					kidRecs = append(kidRecs, kr)
				}
			}
		case Foreign:
			logf(log, "not signalling %s: %s", rec.Name, detail)
		case Gone:
			logf(log, "%s (pid %d) is already gone", rec.Name, rec.PID)
		}
	}
	for _, rec := range ours {
		starts[rec.PID] = time.Now()
		if sent, err := signalOurs(rec, syscall.SIGTERM); err != nil {
			return rep, fmt.Errorf("SIGTERM to %s (pid %d): %w", rec.Name, rec.PID, err)
		} else if sent {
			stops[rec.PID].Signals = "TERM"
			logf(log, "sent SIGTERM to %s (pid %d)", rec.Name, rec.PID)
		}
	}
	all := append(append([]ProcRec(nil), ours...), kidRecs...)
	if !waitGone(ctx, all, grace) {
		for _, rec := range all {
			sent, err := signalOurs(rec, syscall.SIGKILL)
			if err != nil {
				return rep, fmt.Errorf("SIGKILL to %s (pid %d): %w", rec.Name, rec.PID, err)
			}
			if sent {
				if s, ok := stops[rec.PID]; ok {
					s.Signals += "+KILL"
				}
				logf(log, "grace period of %s over: sent SIGKILL to %s (pid %d)", grace, rec.Name, rec.PID)
			}
		}
		if !waitGone(ctx, all, killWait) {
			for _, rec := range all {
				if v, _ := identify(rec); v == Ours {
					return rep, fmt.Errorf("%s (pid %d) is still alive %s after SIGKILL", rec.Name, rec.PID, killWait)
				}
			}
		}
	}
	for _, rec := range ours {
		stops[rec.PID].TookMS = time.Since(starts[rec.PID]).Milliseconds()
	}
	// Confirm: none of the sockets they held still listens. The kernel releases a dead process's
	// sockets slightly after the process is gone (a zombie can briefly still show its listeners),
	// so a socket that is still there is given a few seconds before it counts as a failure.
	ports := map[int]bool{}
	for _, p := range st.Ports.All() {
		ports[p] = true
	}
	var socks []sock
	for deadline := time.Now().Add(socketReleaseWait); ; {
		var err error
		if socks, err = listenSockets(); err != nil {
			return rep, fmt.Errorf("cannot confirm that the listeners are gone: %w", err)
		}
		var held *sock
		for i, s := range socks {
			if inodes[s.inode] {
				held = &socks[i]
			}
		}
		if held == nil {
			break
		}
		if time.Now().After(deadline) {
			return rep, fmt.Errorf("the processes are gone, but a socket they held still listens on %s", held.Listener)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, s := range socks {
		if ports[s.Port] {
			rep.ForeignOnPorts = append(rep.ForeignOnPorts, s.Listener.String())
		}
	}
	rep.GoneListeners = len(inodes)
	for _, f := range rep.ForeignOnPorts {
		logf(log, "warning: %s listens on one of the recorded ports, but not as a launched process; left alone", f)
	}
	return rep, nil
}

// DownOptions are the inputs of Down.
type DownOptions struct {
	StateDir string
	Grace    time.Duration
	Log      io.Writer
}

// Down stops the recorded processes of the host in the state directory and removes the
// record. With no record there is nothing to stop. The data directory is kept.
func Down(ctx context.Context, o DownOptions) (*StopReport, error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(l.dir); errors.Is(err, os.ErrNotExist) {
		logf(o.Log, "no state directory %s: nothing to stop", l.dir)
		return nil, nil
	}
	unlock, err := l.lockState()
	if err != nil {
		return nil, err
	}
	defer unlock()
	st, err := l.loadState()
	if err != nil {
		return nil, err
	}
	if st == nil {
		logf(o.Log, "no launcher record in %s: nothing to stop", l.dir)
		return nil, nil
	}
	if o.Grace <= 0 {
		o.Grace = DefaultGrace
	}
	rep, err := stopRecorded(ctx, st, o.Grace, o.Log)
	if err != nil {
		return rep, err
	}
	if err := l.clearRecord(); err != nil {
		return rep, err
	}
	return rep, nil
}

// clearRecord removes the record and the kubeconfigs, which point at a server that is gone. The
// data directory stays.
func (l layout) clearRecord() error {
	for _, f := range []string{l.stateFile(), l.kubeconfig(), l.adminKubeconfig()} {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
