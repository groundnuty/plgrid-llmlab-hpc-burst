package alpha

// The events row A's flows ask of an Instance (nowEvents, in now_flows_test.go), driven without a cluster: the ending-soon event's side is drawn at the
// time of each read, and Athena's expectation does not read the Instance's lease end (the review of PR 770, points 2 and 3). The events a run raised are
// fixtures here, with a manual clock and a sleep that advances it, so that a loop that never ends fails the test instead of hanging it.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// EndingSoonOf is the rule on Sano's serve row and Athena's unconditional expectation everywhere else: a lease end that is wrong, here five hours after
// ready, must not turn Athena's mandatory event into a forbidden one.
func TestAthenasEndingSoonIsUnconditionalAndSanosFollowsTheRule(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	m := time.Minute
	const athena = "alpha-athena-now-serve"
	for _, c := range []struct {
		name, row    string
		ready, drain time.Duration // after t0
		at           time.Duration
		want         EndingSoonVerdict
	}{
		{"Athena, the real 48-minute lease (drain start at 33)", athena, 3 * m, 33 * m, 14 * m, EndingSoonExpected},
		{"Athena, a wrongly late lease end", athena, 3 * m, 5 * time.Hour, 14 * m, EndingSoonExpected},
		{"Athena, a lease end before ready", athena, 3 * m, 0, 14 * m, EndingSoonExpected},
		{"Athena, read at the very start of the window", athena, 3 * m, 5 * time.Hour, 0, EndingSoonExpected},
		{"Athena's no-overlap row, a late lease end", "alpha-athena-now-nooverlap", 3 * m, 5 * time.Hour, 14 * m, EndingSoonExpected},
		{"Sano, the real 106-minute lease (drain start at 91): the rule says forbidden", SanoServeName, 4 * m, 91 * m, 12 * m, EndingSoonForbidden},
		{"Sano, the same lease, well past the boundary", SanoServeName, 4 * m, 91 * m, 40 * m, EndingSoonExpected},
		{"Sano, inside the band of the boundary", SanoServeName, 4 * m, 91 * m, 31*m + 5*time.Second, EndingSoonOptional},
	} {
		if got := EndingSoonOf(c.row, t0.Add(c.ready), t0.Add(c.drain), t0.Add(c.at)); got != c.want {
			t.Errorf("%s: EndingSoonOf = %v, want %v", c.name, got, c.want)
		}
	}
}

// nowEventsWorld is a run's events over time, driven by a manual clock.
type nowEventsWorld struct {
	t      *testing.T
	now    time.Time
	in     v1alpha1.Instance
	rises  map[string]time.Duration // an event's name, by action, and when it is there (after the polling began); an absent name never is
	begun  time.Time
	sleeps int
}

func (w *nowEventsWorld) event(action string) eventsv1.Event {
	typ := provider.EventKindOrdinary
	if action == "failing" {
		typ = provider.EventKindWarning
	}
	reason := map[string]string{"started": v1alpha1.ReasonAgentRegistered, "ready": "Ready", "failing": v1alpha1.ReasonWorkerRestart, "ending-soon": v1alpha1.EventReasonNoReplacementReady,
		"replaced": v1alpha1.EventReasonHandedOver}[action]
	return eventsv1.Event{ObjectMeta: metav1.ObjectMeta{Name: action}, Action: action, Reason: reason, Type: typ, Note: "an invented note",
		Regarding:           corev1.ObjectReference{Kind: "Instance", Namespace: w.in.Namespace, Name: w.in.Name},
		ReportingController: obscheck.LifecycleController}
}

func (w *nowEventsWorld) events() []eventsv1.Event {
	var out []eventsv1.Event
	for action, after := range w.rises {
		if !w.now.Before(w.begun.Add(after)) {
			out = append(out, w.event(action))
		}
	}
	return out
}

// poll runs pollNowEvents for a row, the Instance's ready time and lease end given after t0, the polling beginning at begin after t0 and bounded by wait.
func (w *nowEventsWorld) poll(row string, ready, drain, begin, wait time.Duration) (missing []obscheck.WantEvent, problems []string) {
	t0 := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	w.now, w.begun = t0.Add(begin), t0.Add(begin)
	sets := nowEventSets(w.in, row, t0.Add(ready), t0.Add(drain))
	return pollNowEvents(w.events, sets, func() time.Time { return w.now }, func(d time.Duration) {
		w.sleeps++
		if w.sleeps > 100000 {
			w.t.Fatal("the polling never ended")
		}
		w.now = w.now.Add(d)
	}, w.begun.Add(wait))
}

func names(ms []obscheck.WantEvent) string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Action)
	}
	return strings.Join(out, ",")
}

// Point 2 of the review of PR 770: the expectation is drawn at each read, not once before the polling. Sano's drain start is at 91 minutes after the
// job's start and the boundary (an hour before) at 31, so a polling that begins five seconds before it begins inside the band, where the event is
// neither expected nor forbidden. If the last expected event, the worker-rung restart, comes 20 seconds later, the boundary is by then more than a
// status interval behind, the event is due, and its absence is a failure of the row.
func TestAnEndingSoonEventThatBecameDueDuringThePollingIsExpectedAtTheLastRead(t *testing.T) {
	const m = time.Minute
	begin := 31*m - 5*time.Second
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: "default"}}
	always := map[string]time.Duration{"started": 0, "ready": 0}

	// The event never comes, and the restart arrives after the band: missing.
	w := &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 20 * time.Second}}
	if missing, problems := w.poll(SanoServeName, 4*m, 91*m, begin, 10*m); names(missing) != "ending-soon" || len(problems) != 0 {
		t.Errorf("polling that begins inside the band, the event never raised: missing %q, problems %v; want ending-soon missing and nothing else", names(missing), problems)
	}
	// The event comes inside the band: nothing missing.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 20 * time.Second, "ending-soon": 10 * time.Second}}
	if missing, problems := w.poll(SanoServeName, 4*m, 91*m, begin, 10*m); len(missing) != 0 || len(problems) != 0 {
		t.Errorf("polling that begins inside the band, the event raised within it: missing %q, problems %v; want a clean run", names(missing), problems)
	}
	// Everything is there at once, inside the band, with no event: neither way is judged, and the polling ends at once.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0}}
	if missing, problems := w.poll(SanoServeName, 4*m, 91*m, begin, 10*m); len(missing) != 0 || len(problems) != 0 || w.sleeps != 0 {
		t.Errorf("everything present at the start, inside the band: missing %q, problems %v, %d sleeps; want a clean run that did not wait", names(missing), problems, w.sleeps)
	}
	// A polling that begins well past the boundary expects it from the start: missing, to the deadline.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0}}
	if missing, _ := w.poll(SanoServeName, 4*m, 91*m, 40*m, 2*m); names(missing) != "ending-soon" {
		t.Errorf("polling that begins past the boundary, the event never raised: missing %q, want ending-soon", names(missing))
	}
	// Before the boundary it is forbidden at the last read: present, a problem; absent, nothing.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0, "ending-soon": 0}}
	if missing, problems := w.poll(SanoServeName, 4*m, 91*m, 12*m, 2*m); len(missing) != 0 || len(problems) != 1 || !strings.Contains(problems[0], "forbidden event ending-soon") {
		t.Errorf("an ending-soon event on Sano before the boundary: missing %q, problems %v; want one forbidden-event problem", names(missing), problems)
	}
	w = &nowEventsWorld{t: t, in: in, rises: always}
	w.rises["failing"] = 0
	if missing, problems := w.poll(SanoServeName, 4*m, 91*m, 12*m, 2*m); len(missing) != 0 || len(problems) != 0 {
		t.Errorf("no ending-soon event on Sano before the boundary: missing %q, problems %v; want a clean run", names(missing), problems)
	}
	// A handover is forbidden on every row.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0, "replaced": 0}}
	if _, problems := w.poll(SanoServeName, 4*m, 91*m, 12*m, 2*m); len(problems) != 1 || !strings.Contains(problems[0], "forbidden event replaced") {
		t.Errorf("a handover: problems %v, want one forbidden-event problem", problems)
	}
}

// Point 3: Athena's ending-soon event stays expected whatever the Instance's lease end says. A lease end five hours after ready is not Athena's (its job
// limit is 48 minutes), and under the rule it would make the event forbidden: a run that raised it would fail, and one that did not would pass.
func TestAthenasEndingSoonEventIsStillExpectedWithAWronglyLateLeaseEnd(t *testing.T) {
	const m = time.Minute
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: "default"}}
	const athena = "alpha-athena-now-serve"
	// Not raised: missing, as it was before #735.
	w := &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0}}
	if missing, _ := w.poll(athena, 3*m, 5*time.Hour, 14*m, 2*m); names(missing) != "ending-soon" {
		t.Errorf("Athena, no ending-soon event, a wrongly late lease end: missing %q, want ending-soon", names(missing))
	}
	// Raised: no problem, and not forbidden.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0, "ending-soon": 0}}
	if missing, problems := w.poll(athena, 3*m, 5*time.Hour, 14*m, 2*m); len(missing) != 0 || len(problems) != 0 {
		t.Errorf("Athena, the event raised, a wrongly late lease end: missing %q, problems %v; want a clean run", names(missing), problems)
	}
	// The real lease end gives the same.
	w = &nowEventsWorld{t: t, in: in, rises: map[string]time.Duration{"started": 0, "ready": 0, "failing": 0, "ending-soon": 0}}
	if missing, problems := w.poll(athena, 3*m, 33*m, 14*m, 2*m); len(missing) != 0 || len(problems) != 0 {
		t.Errorf("Athena, the event raised, the real lease end: missing %q, problems %v; want a clean run", names(missing), problems)
	}
}
