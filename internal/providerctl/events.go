package providerctl

// The Provider controller's typed Allocation events (beta; HBEP-8, Design
// details, Events, Allocation events (beta); #625).
//
// They are raised through a second recorder, client-go's events.k8s.io one
// (Options.TypedRecorder), which carries the type in its action, and are
// counted under the controller label provider-<name> by the wrapper
// eventcount.Events where the recorder is built. The Provider's other events,
// HBEP-4's diagnostics, keep going through crossplane-runtime's recorder with no
// action and are untyped.
//
// One map, allocationEventType, is the only place the types are set, as
// internal/managed's badAction is for the managed catalog controller: the
// helpers below read it, and the drift guard (internal/reasondrift) reads it
// from the source to check each emit site's type against HBEP-8's table. A
// reason is a named constant, never a literal, since the guard refuses a
// literal.

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// HBEP-8's types, as they go in an event's action (Events, Form).
const (
	eventStarted    = "started"
	eventReady      = "ready"
	eventDegraded   = "degraded"
	eventFailing    = "failing"
	eventEndingSoon = "ending-soon"
	eventEnded      = "ended"
)

// The reasons of the typed events that api/v1alpha1 does not declare. AllocationFailed,
// AllocationEndedEarly, AllocationRefused and Requeued are its
// constants (the lifecycle controller raises the first four as Instance events too).
const (
	evJobSubmitted     = "JobSubmitted"
	evJobAdopted       = "JobAdopted"
	evHoldLifted       = "HoldLifted"
	evJobStarted       = "JobStarted"
	evHardEndNear      = "HardEndNear"
	evJobEnded         = "JobEnded"
	evSubmittedAsOther = "SubmittedAsOther"
	evJobLost          = "JobLost"
	evJobReappeared    = "JobReappeared"
	evAdminHold        = "AdminHold"
	// evJobCompleting is the event's reason. api/v1alpha1's ReasonJobCompleting
	// is the JobCompleting condition's reason, "Completing", a different word.
	evJobCompleting = "JobCompleting"
)

// allocationEventType maps each reason of the typed Allocation events to its
// type (HBEP-8, Allocation events (beta), table). It is the only place the
// types are set. The drift guard reads this literal from the source, so each
// key and value must be a named string constant.
var allocationEventType = map[string]string{
	evJobSubmitted:                      eventStarted,
	evJobAdopted:                        eventStarted,
	evHoldLifted:                        eventStarted,
	evJobStarted:                        eventReady,
	evJobReappeared:                     eventReady,
	evHardEndNear:                       eventEndingSoon,
	evJobEnded:                          eventEnded,
	v1alpha1.ReasonAllocationEndedEarly: eventFailing,
	v1alpha1.ReasonAllocationFailed:     eventFailing,
	v1alpha1.ReasonRequeued:             eventFailing,
	evSubmittedAsOther:                  eventFailing,
	v1alpha1.ReasonAllocationRefused:    eventFailing,
	evJobLost:                           eventFailing,
	evAdminHold:                         eventDegraded,
	evJobCompleting:                     eventDegraded,
}

// RaisedEvents returns the series a batch Provider's controller can raise
// typed, each with the Kubernetes type its action gives: the series created at
// 0 under provider-<name> when its manager starts (HBEP-8, Raised events). The
// drift guard checks the list against the emit sites.
func RaisedEvents() []eventcount.Series {
	out := make([]eventcount.Series, 0, len(allocationEventType)+1)
	for reason, action := range allocationEventType {
		out = append(out, eventcount.Series{Reason: reason, Action: action, Type: provider.EventKindOf(action)})
	}
	// ObservationGap, which each Provider manager raises at its start through the
	// same recorder (gap.go), regarding its Lease.
	out = append(out, eventcount.Series{Reason: reasonObservationGap, Action: eventDegraded, Type: provider.EventKindWarning})
	// EventsNotLogged, which the durable log's publisher raises through the same recorder when the
	// manager has one (internal/eventcount).
	out = append(out, eventcount.Series{Reason: eventcount.ReasonEventsNotLogged, Action: eventDegraded, Type: provider.EventKindWarning})
	sort.Slice(out, func(i, k int) bool { return out[i].Reason < out[k].Reason })
	return out
}

// typed raises the typed event reason regarding a, with related as its second
// object (nil for none), and the id <Allocation UID>/<reason>/<fact> that marks
// the transition (HBEP-8, The durable log, Streams, The event's id): fact is
// the status value that marks it, so a transition raised again after a failed
// status write keeps its id. The recorder's wrapper (eventcount.Events) publishes
// the event to the durable log under the id as its Nats-Msg-Id when the process runs
// with the log, where a second publish of the id inside the log's duplicate window
// stores nothing; the API's Event holds no id (PR E2, #625).
//
// The type comes from allocationEventType, and the note is cut to
// provider.MaxEventNote. A note that holds the Allocation's job ID as a token
// is refused: it is counted (refusedNotes) and logged, and not handed over,
// since a typed note goes to the durable log, which is kept and may be exported
// (HBEP-8, Allocation events, No job ID in a note; R2 of #625). Every note is
// built without the ID, so this is the last line, not the first.
func (e *external) typed(a *v1alpha1.Allocation, reason, fact, note string, related runtime.Object) {
	action, ok := allocationEventType[reason]
	if !ok {
		e.s.o.Log.Info("a typed event with no type in allocationEventType is not raised", "reason", reason)
		return
	}
	if e.refuseNote(a, reason, note) {
		return
	}
	eventtype := provider.EventKindOf(action)
	id := string(a.UID) + "/" + reason + "/" + fact
	e.s.o.TypedRecorder.EventfID(id, a, related, eventtype, reason, action, "%s", provider.CutNote(note))
}

// typedUnmarked raises a typed event that marks no transition, so it carries no
// id of its own: the repeating conditions AdminHold and JobCompleting, each
// emission an event of its own, which the log's publisher gives a fresh UUID.
// The note is checked and cut as typed does.
func (e *external) typedUnmarked(a *v1alpha1.Allocation, reason, note string, related runtime.Object) {
	action, ok := allocationEventType[reason]
	if !ok {
		e.s.o.Log.Info("a typed event with no type in allocationEventType is not raised", "reason", reason)
		return
	}
	if e.refuseNote(a, reason, note) {
		return
	}
	eventtype := provider.EventKindOf(action)
	e.s.o.TypedRecorder.Eventf(a, related, eventtype, reason, action, "%s", provider.CutNote(note))
}

// refuseNote reports whether note names the Allocation's job ID as a token, and
// counts and logs the refusal if so. The refused event reaches neither the API nor
// the durable log's publisher, so the recorder it was to go through counts it as not
// logged (eventcount.IDRecorder.NotLogged): in hpcburst_events_unlogged_total under
// provider-<name>, with the rest of the events the log did not take, and never silently
// lost (HBEP-8, The durable log, When it is down; team-lead, 16 Nov 2026).
func (e *external) refuseNote(a *v1alpha1.Allocation, reason, note string) bool {
	for _, id := range recordedJobIDs(a) {
		if containsToken(note, id) {
			e.s.refusedNotes.Add(1)
			e.s.o.Log.Info("a typed event whose note names the job ID is not raised", "reason", reason, "allocation", a.Namespace+"/"+a.Name)
			e.s.o.TypedRecorder.NotLogged(a, reason)
			return true
		}
	}
	return false
}

// RefusedNotes is how many typed events the controller has not raised because
// their note named the job ID (typed's defence). It is for tests and
// diagnostics; HBEP-8 lists no metric for it.
func (c *Controller) RefusedNotes() int64 { return c.shared.refusedNotes.Load() }

// recordedJobIDs are the job IDs the Allocation names: its annotation and its
// status.
func recordedJobIDs(a *v1alpha1.Allocation) []string {
	var out []string
	if id := a.Annotations[v1alpha1.AnnotationJobID]; id != "" {
		out = append(out, id)
	}
	if id := a.Status.AtProvider.JobID; id != "" && id != a.Annotations[v1alpha1.AnnotationJobID] {
		out = append(out, id)
	}
	return out
}

// factTime is the fact a time marks a transition by: RFC 3339 in UTC to the
// second, the precision a status time keeps, so the id is the same whether the
// pass read the time from the scheduler or from the stored status. "unknown"
// for a time that is not there.
func factTime(t *time.Time) string {
	if t == nil {
		return "unknown"
	}
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// attemptFact is the submission attempt's number the record names, 0 for none:
// the marking fact of JobSubmitted, JobAdopted, SubmittedAsOther and
// AllocationRefused (HBEP-8, The event's id; R1 of #625).
func attemptFact(r record) string {
	if r.sub == nil {
		return "0"
	}
	return fmt.Sprintf("%d", r.sub.Attempt)
}

// baseState is the first word of the scheduler's state: the base state before
// any flag.
func baseState(j provider.Job) string {
	if len(j.State) == 0 {
		return "unknown"
	}
	return j.State[0]
}

// eventGates are the in-memory gates of the typed events: they are kept in
// memory, so a restart or a new leader raises an event once more, which is
// "when it starts" for that process (HBEP-8, Repeating conditions; HardEndNear).
type eventGates struct {
	mu sync.Mutex
	// hardEnd holds, by Allocation UID, the hard ends HardEndNear was raised for.
	hardEnd map[types.UID]map[int64]bool
	// hourly holds, by kind and Allocation UID, when a repeating condition's
	// event was last raised.
	hourly map[gateKind]map[types.UID]time.Time
	// deletingEnded holds the Allocations whose JobEnded a deleting pass raised.
	deletingEnded map[types.UID]bool
}

type gateKind int

const (
	gateAdminHold gateKind = iota
	gateCompleting
)

// hardEndDue reports whether HardEndNear is to be raised for the Allocation's
// hard end, and records it if so: once per hard end, so a hard end that moves
// later raises it again before the new one.
func (g *eventGates) hardEndDue(uid types.UID, end time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.hardEnd == nil {
		g.hardEnd = map[types.UID]map[int64]bool{}
	}
	if g.hardEnd[uid] == nil {
		g.hardEnd[uid] = map[int64]bool{}
	}
	k := end.UTC().Truncate(time.Second).Unix()
	if g.hardEnd[uid][k] {
		return false
	}
	g.hardEnd[uid][k] = true
	return true
}

// hourlyDue reports whether the repeating condition's event is due for the
// Allocation at now, and records the emission if so: the first emission of a
// run is due at once, and the next once an hour has passed since the last
// (HBEP-8, Repeating conditions).
func (g *eventGates) hourlyDue(kind gateKind, uid types.UID, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.hourly == nil {
		g.hourly = map[gateKind]map[types.UID]time.Time{}
	}
	if g.hourly[kind] == nil {
		g.hourly[kind] = map[types.UID]time.Time{}
	}
	if last, ok := g.hourly[kind][uid]; ok && now.Sub(last) < provider.EventRepeatInterval {
		return false
	}
	g.hourly[kind][uid] = now
	return true
}

// clearHourly ends the repeating condition's run for the Allocation, so its next
// start raises the event at once.
func (g *eventGates) clearHourly(kind gateKind, uid types.UID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.hourly[kind], uid)
}

// endedOnce reports whether a deleting pass is to raise JobEnded for the
// Allocation, and records it: once per process (R3 of #625).
func (g *eventGates) endedOnce(uid types.UID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.deletingEnded == nil {
		g.deletingEnded = map[types.UID]bool{}
	}
	if g.deletingEnded[uid] {
		return false
	}
	g.deletingEnded[uid] = true
	return true
}

// forget drops every gate of the Allocation, as its finalizer goes.
func (g *eventGates) forget(uid types.UID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.hardEnd, uid)
	for _, m := range g.hourly {
		delete(m, uid)
	}
	delete(g.deletingEnded, uid)
}

// nopTypedRecorder is the typed recorder when none is configured: nothing is
// raised, as for Options.Recorder.
type nopTypedRecorder struct{}

// The methods have pointer receivers, so the scan sees no compiler-made wrapper
// of a value method (it reports a recorder call it cannot name in one).
func (*nopTypedRecorder) Eventf(runtime.Object, runtime.Object, string, string, string, string, ...interface{}) {
}

func (*nopTypedRecorder) EventfID(string, runtime.Object, runtime.Object, string, string, string, string, ...interface{}) {
}

func (*nopTypedRecorder) NotLogged(runtime.Object, string) {}

var _ eventcount.IDRecorder = &nopTypedRecorder{}
