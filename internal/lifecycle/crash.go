package lifecycle

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The crash-correlation event (HBEP-7, Health and restarts: Crash correlation; #539, from spike S18, as
// amended by #550): a worker exit the controller didn't ask for, of a worker that was serving (its
// Instance ready or draining, and the worker verified and warmed up since its last start), within crashWindow after
// such an exit on another Instance of the same ModelDeployment, raises a failing event CrashCorrelated,
// regarding the Instance whose exit made the pair and related to the other. It is an event only: each
// Instance still restarts by its own ladder. One Instance's own crash loop is not counted (that is
// RestartLoop's), nor are two workers of one Instance, nor a failed start, which belongs to the
// verification and provisioning loops.
const (
	crashWindow = 30 * time.Second
	// crashKeep is how long an exit is kept to pair with, and its key to recognise it again, after it
	// was first seen: past the window, with room for reports that arrive late or out of order.
	crashKeep = 10 * time.Minute
)

// exitSeen is one unrequested worker exit of an Instance.
type exitSeen struct {
	key    string             // the process: Instance, worker, generation, PID and start time
	inst   *v1alpha1.Instance // a copy, the event's object or its related one
	worker string
	at     time.Time // the agent's exit time (WorkerReport.ExitedAt), or the exit's first sight
	seen   time.Time // when the controller first saw it
	paired bool      // it made a pair, and the event was raised for it
}

// correlateExits records the unrequested exits of serving workers in the Instance's current report, each
// once, and raises CrashCorrelated for each that makes a pair with an exit on another Instance of its
// ModelDeployment within crashWindow. A requested stop is reported as stopped, never exited, so it never
// counts, and nor does the router, which is not a worker. A stale Instance, of an earlier deployment of
// the same name, never counts, though it drains and so serves: it would pair with the new deployment's
// Instances.
func (p *pass) correlateExits(in *v1alpha1.Instance, av agentView) {
	if p.md == nil || av.report == nil || p.stale[in.UID] {
		return
	}
	for i := range av.report.Workers {
		w := &av.report.Workers[i]
		if w.Phase != agentapi.PartExited || !servingWorker(in, w.Name) {
			continue
		}
		at := av.reportAt
		if w.ExitedAt != nil {
			at = *w.ExitedAt
		}
		start := ""
		if w.StartTime != nil {
			start = w.StartTime.UTC().Format(time.RFC3339Nano)
		}
		e := &exitSeen{key: fmt.Sprintf("%s/%s/%d/%d/%s", in.UID, w.Name, w.Generation, w.PID, start), inst: in.DeepCopy(),
			worker: w.Name, at: at, seen: p.now}
		later, earlier := p.mem.recordExit(p.md.UID, e)
		if later == nil {
			continue
		}
		gap := later.at.Sub(earlier.at)
		p.r.Recorder.Event(later.inst, earlier.inst, eventFailing, v1alpha1.ReasonCrashCorrelated,
			fmt.Sprintf("%s of Instance %s exited without being told to %s after %s of Instance %s, another Instance of ModelDeployment %s (within %s): a request may be crashing every engine it reaches",
				later.worker, later.inst.Name, gap.Round(time.Millisecond), earlier.worker, earlier.inst.Name, p.md.Name, crashWindow))
	}
}

// servingWorker reports whether worker w of Instance in was serving: the Instance ready or draining
// (a draining Instance still serves its in-flight requests), and w verified and warmed up at its current
// generation. With one worker the Instance's conditions are the worker's (verifiedWorker); with several,
// its status.workers entry (workerWarmed). It is read at the start of the pass that first sees the exit,
// before the restart it causes changes either. The drain's own stop of a worker is requested, reported
// stopped, never exited, so it never counts.
func servingWorker(in *v1alpha1.Instance, w string) bool {
	if in.Status.Phase != v1alpha1.InstanceReady && in.Status.Phase != v1alpha1.InstanceDraining {
		return false
	}
	if several(in) {
		return workerWarmed(in, w)
	}
	return verifiedWorker(in)
}

// recordExit records e for ModelDeployment md, unless its process was recorded before. It returns the
// pair e makes, the later exit first, when an exit on another Instance of md lies within crashWindow of
// it and the later of the two has made no pair yet: the nearest such exit, if several; nil otherwise. e
// is the later exit when it came after; when a report brings an earlier exit late, the later one is the
// recorded exit, if it made no pair of its own. Every deployment's exits are kept crashKeep after they
// were first seen, and no longer, a deleted deployment's included.
func (m *memory) recordExit(md types.UID, e *exitSeen) (later, earlier *exitSeen) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.exitKeys[e.key]; ok {
		return nil, nil
	}
	m.exitKeys[e.key] = e.seen
	for k, seen := range m.exitKeys {
		if e.seen.Sub(seen) > crashKeep {
			delete(m.exitKeys, k)
		}
	}
	for d, xs := range m.exits {
		kept := xs[:0]
		for _, x := range xs {
			if e.seen.Sub(x.seen) <= crashKeep {
				kept = append(kept, x)
			}
		}
		if len(kept) == 0 {
			delete(m.exits, d)
		} else {
			m.exits[d] = kept
		}
	}
	var pair, pairEarlier *exitSeen
	best := time.Duration(-1)
	for _, x := range m.exits[md] {
		d := x.at.Sub(e.at).Abs()
		if x.inst.UID == e.inst.UID || d > crashWindow {
			continue
		}
		l, ear := e, x
		if x.at.After(e.at) {
			l, ear = x, e
		}
		if l.paired || (best >= 0 && d >= best) {
			continue
		}
		pair, pairEarlier, best = l, ear, d
	}
	m.exits[md] = append(m.exits[md], e)
	if pair == nil {
		return nil, nil
	}
	pair.paired = true
	return pair, pairEarlier
}
