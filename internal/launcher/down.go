package launcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
)

// DefaultGrace is how long down waits after SIGTERM before it sends SIGKILL. It is longer than
// SupervisorGraceWithFront, the least a supervisor that runs the front needs.
const DefaultGrace = 40 * time.Second

// ErrGraceTooShort is returned by Down and Upgrade, before anything is stopped, when the grace is
// shorter than SupervisorGraceWithFront and the supervisor runs a front.
var ErrGraceTooShort = errors.New("the grace is too short for the front to stop")

// checkGrace refuses a grace the recorded front cannot stop within.
func checkGrace(st *State, grace time.Duration) error {
	if st.FrontRun == nil || grace >= SupervisorGraceWithFront {
		return nil
	}
	for _, p := range st.Procs {
		if p.Name == supervisorName {
			return fmt.Errorf("%w: --grace %s, but the supervisor runs the front, which needs up to %s to stop and flush its usage records, so give at least %s (nothing was stopped)",
				ErrGraceTooShort, grace, FrontTermGrace, SupervisorGraceWithFront)
		}
	}
	return nil
}

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

// stopRecorded stops the processes in the record, and only those: the supervisor first, which
// stops its children and is waited for, then the rest (k3s). So the controllers never lose their
// API server while they shut down. Each group is stopped by stopGroup. A process whose identity
// cannot be established does not keep the rest from being stopped; the stop fails at the end.
func stopRecorded(ctx context.Context, st *State, grace time.Duration, log io.Writer) (*StopReport, error) {
	var first, rest []ProcRec
	for _, rec := range st.Procs {
		if rec.Name == supervisorName {
			first = append(first, rec)
		} else {
			rest = append(rest, rec)
		}
	}
	out := &StopReport{Procs: make([]ProcStop, 0, len(st.Procs))}
	var unknown []string
	for _, group := range [][]ProcRec{first, rest} {
		if len(group) == 0 {
			continue
		}
		g := grace
		if group[0].Name == supervisorName && st.FrontRun != nil && g < SupervisorGraceWithFront {
			// The supervisor gives the front FrontTermGrace to flush its usage records; killing the
			// supervisor sooner kills the front with it (Pdeathsig). Down and Upgrade refuse such a
			// grace (checkGrace); this covers the internal stops, such as an aborted up.
			logf(log, "the supervisor runs the front, which needs %s to stop: waiting up to %s for it, not %s", FrontTermGrace, SupervisorGraceWithFront, g)
			g = SupervisorGraceWithFront
		}
		rep, unk, err := stopGroup(ctx, st, group, g, log)
		out.Procs = append(out.Procs, rep.Procs...)
		out.GoneListeners += rep.GoneListeners
		out.ForeignOnPorts = append(out.ForeignOnPorts, rep.ForeignOnPorts...)
		unknown = append(unknown, unk...)
		if err != nil {
			return out, err
		}
	}
	// Report in the order of the record, whatever the order of stopping.
	at := map[int]int{}
	for i, rec := range st.Procs {
		at[rec.PID] = i
	}
	sort.SliceStable(out.Procs, func(i, j int) bool { return at[out.Procs[i].PID] < at[out.Procs[j].PID] })
	if len(unknown) > 0 {
		return out, fmt.Errorf("could not check the identity of %s, so nothing was sent to it and the record is kept", strings.Join(unknown, "; "))
	}
	return out, nil
}

// stopGroup stops recs, and only those. For each it takes a pidfd and
// verifies PID, start time and boot ID while holding it, before any signal: a process that no
// longer matches is reported and never signalled, and one whose identity cannot be established
// (Unknown) is not signalled either; it is returned in unknown. A verified
// process gets SIGTERM through its pidfd, and SIGKILL (verified again) after the grace period. Its
// descendants, adopted through their own verified pidfds while it was held, are held to the same
// rule. Exits are read from the pidfds. Then it confirms that the sockets they held no longer
// listen. A cancelled ctx cuts the grace period short, never the wait after SIGKILL.
func stopGroup(ctx context.Context, st *State, recs []ProcRec, grace time.Duration, log io.Writer) (rep *StopReport, unknown []string, err error) {
	rep = &StopReport{Procs: make([]ProcStop, 0, len(recs))}
	var roots, all []*procHandle
	defer func() { closeAll(all) }()
	rootIdx := map[*procHandle]int{} // index into rep.Procs
	inodes := map[string]bool{}
	for _, rec := range recs {
		h, v, detail := openRec(rec)
		rep.Procs = append(rep.Procs, ProcStop{Name: rec.Name, PID: rec.PID, Before: v.String(), Detail: detail})
		switch v {
		case Ours:
			tree := openTree(h)
			all = append(all, tree...)
			roots = append(roots, h)
			rootIdx[h] = len(rep.Procs) - 1
			owned, _, err := ownedListeners(tree)
			if err != nil {
				return rep, unknown, fmt.Errorf("cannot list the listeners of %s (pid %d), so could not confirm later that they are gone: %w", rec.Name, rec.PID, err)
			}
			for _, s := range owned {
				inodes[s.inode] = true
			}
		case Foreign:
			logf(log, "not signalling %s: %s", rec.Name, detail)
		case Gone:
			logf(log, "%s (pid %d) is already gone", rec.Name, rec.PID)
		case Unknown:
			logf(log, "not signalling %s: %s", rec.Name, detail)
			unknown = append(unknown, fmt.Sprintf("%s (pid %d): %s", rec.Name, rec.PID, detail))
		}
	}
	starts := map[*procHandle]time.Time{}
	for _, h := range roots {
		starts[h] = time.Now()
		if sent, err := signalHandle(h, syscall.SIGTERM); err != nil {
			return rep, unknown, fmt.Errorf("SIGTERM to %s (pid %d): %w", h.rec.Name, h.rec.PID, err)
		} else if sent {
			rep.Procs[rootIdx[h]].Signals = "TERM"
			logf(log, "sent SIGTERM to %s (pid %d)", h.rec.Name, h.rec.PID)
		}
	}
	if !waitExited(ctx, all, grace) {
		for _, h := range all {
			sent, err := signalHandle(h, syscall.SIGKILL)
			if err != nil {
				return rep, unknown, fmt.Errorf("SIGKILL to %s (pid %d): %w", h.rec.Name, h.rec.PID, err)
			}
			if sent {
				if i, ok := rootIdx[h]; ok {
					rep.Procs[i].Signals += "+KILL"
				}
				logf(log, "grace period over (or interrupted): sent SIGKILL to %s (pid %d)", h.rec.Name, h.rec.PID)
			}
		}
		// Not ctx: an interrupted stop still waits for what it killed to be gone.
		if !waitExited(context.Background(), all, killWait) {
			for _, h := range all {
				if gone, err := h.exited(); err != nil || !gone {
					return rep, unknown, fmt.Errorf("%s (pid %d) is still alive %s after SIGKILL", h.rec.Name, h.rec.PID, killWait)
				}
			}
		}
	}
	for _, h := range roots {
		rep.Procs[rootIdx[h]].TookMS = time.Since(starts[h]).Milliseconds()
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
			return rep, unknown, fmt.Errorf("cannot confirm that the listeners are gone: %w", err)
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
			return rep, unknown, fmt.Errorf("the processes are gone, but a socket they held still listens on %s", held.Listener)
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
	return rep, unknown, nil
}

// DownOptions are the inputs of Down.
type DownOptions struct {
	StateDir string
	Grace    time.Duration
	Log      io.Writer
}

// Down stops the recorded processes of the host in the state directory and removes the
// record. With no record there is nothing to stop. The data directory is kept, and so is the
// record of the release that wrote the store. A process whose identity could not be checked makes
// Down fail and keeps the record.
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
	if err := checkGrace(st, o.Grace); err != nil {
		return nil, err
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

// clearRecord removes the record, the supervisor's record of its children and the kubeconfigs,
// which point at a server that is gone. The data directory stays.
func (l layout) clearRecord() error {
	for _, f := range []string{l.stateFile(), l.kubeconfig(), l.adminKubeconfig(), l.supervisorFile()} {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
