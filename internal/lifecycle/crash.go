package lifecycle

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The crash-correlation event (HBEP-7, Health and restarts: Crash correlation; #539, from spike S18): a
// worker exit the controller didn't ask for, within crashWindow after such an exit on another Instance
// of the same ModelDeployment, raises a failing event CrashCorrelated, regarding the Instance whose exit
// made the pair and related to the other. It is an event only: each Instance still restarts by its own
// ladder. One Instance's own crash loop is not counted (that is RestartLoop's), and neither are two
// workers of one Instance.
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

// correlateExits records the unrequested worker exits in the Instance's current report, each once, and
// raises CrashCorrelated for each that makes a pair with an exit on another Instance of its
// ModelDeployment within crashWindow. A requested stop is reported as stopped, never exited, so it never
// counts, and nor does the router, which is not a worker.
func (p *pass) correlateExits(in *v1alpha1.Instance, av agentView) {
	if p.md == nil || av.report == nil {
		return
	}
	for i := range av.report.Workers {
		w := &av.report.Workers[i]
		if w.Phase != agentapi.PartExited {
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

// recordExit records e for ModelDeployment md, unless its process was recorded before. It returns the
// pair e makes, the later exit first, when an exit on another Instance of md lies within crashWindow of
// it (the nearest, if several) and the later of the two has made no pair yet; nil otherwise. e is the
// later exit when it came after; when a report brings an earlier exit late, the later one is the
// recorded exit, if it made no pair of its own.
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
	kept := m.exits[md][:0]
	var near *exitSeen
	for _, x := range m.exits[md] {
		if e.seen.Sub(x.seen) > crashKeep {
			continue
		}
		kept = append(kept, x)
		d := x.at.Sub(e.at).Abs()
		if x.inst.UID != e.inst.UID && d <= crashWindow && (near == nil || d < near.at.Sub(e.at).Abs()) {
			near = x
		}
	}
	m.exits[md] = append(kept, e)
	if near == nil {
		return nil, nil
	}
	later, earlier = e, near
	if near.at.After(e.at) {
		later, earlier = near, e
	}
	if later.paired {
		return nil, nil
	}
	later.paired = true
	return later, earlier
}
