package e2e

// HBEP-8's beta rows that need the API and the log together (Test plan, Beta; issue #625, PR E2): the log is a REAL
// child process, killed with SIGKILL through its pidfd, and the emitter is a real Provider manager's typed recorder, the
// events.k8s.io recorder of client-go writing into the test control plane, wrapped by eventcount.Events with the
// publisher. What the log holds is read through a fresh client; what the API holds is listed from the API server.

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// leaseAPIEvents lists the EventsNotLogged events the API holds about the Provider's own Lease.
func (r *evRig) leaseAPIEvents(reason string) []eventsv1.Event {
	r.t.Helper()
	var list eventsv1.EventList
	if err := c.List(r.ctx, &list, client.InNamespace(v1alpha1.ControllerNamespace)); err != nil {
		r.t.Fatal(err)
	}
	var out []eventsv1.Event
	for _, e := range list.Items {
		if e.Regarding.UID == r.lease.UID && e.Reason == reason {
			out = append(out, e)
		}
	}
	return out
}

// Down past the retries (the publisher's budget shortened to 2 s), with the API: the emitter is a real Provider
// manager's typed recorder, its 500 events are about five Allocations, and the log is SIGKILLed at a seeded random point
// and stays down until the retries have run out. Whatever the node's speed, a fast one can store all 500 before the kill,
// so a further 25 events are raised after it: they meet a dead log and are lost, and the branch always has losses. The
// stream's ids plus the counter's value total at least all the events raised, and every stream id is one the emitter raised. One EventsNotLogged follows the log's return, in the API and in the log, regarding
// the Provider's own provider-<name> Lease.
func TestEventsSurviveALogCrashDownPastTheRetries(t *testing.T) {
	const crashAfterKill = 25
	seed := time.Now().UnixNano()
	if s := os.Getenv("HPCBURST_TEST_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseInt(s, 10, 64); err != nil {
			t.Fatalf("HPCBURST_TEST_SEED=%q: %v", s, err)
		}
	}
	t.Logf("seed %d (set HPCBURST_TEST_SEED to repeat the run)", seed)
	rng := rand.New(rand.NewSource(seed))
	kill := uint64(1 + rng.Intn(450)) // early enough that events are still queued when the log dies

	r := newEvRig(t, rigOpts{budget: 2 * time.Second, withLease: true})
	controller := eventcount.ControllerProvider(r.prov)
	var allocs []*v1alpha1.Allocation
	for i := 0; i < 5; i++ {
		allocs = append(allocs, r.newAlloc(fmt.Sprintf("o%d", i), "3h"))
	}
	raised := map[string]string{} // id -> the Allocation's UID
	raise := func(n int) {
		a := allocs[n%5]
		id := fmt.Sprintf("crash-%03d", n)
		raised[id] = string(a.UID)
		r.typed.EventfID(id, a, nil, provider.EventKindOrdinary, "JobStarted", "ready", "event %d", n)
	}
	for n := 0; n < 500; n++ {
		raise(n)
	}
	_, js := r.logDir.Connect(t)
	eventually(t, fmt.Sprintf("the stream to hold %d events", kill), 60*time.Second, func() bool {
		return nodetest.StreamInfo(t, js, eventlog.StreamEvents).State.Msgs >= kill
	})
	r.logProc.Kill()
	// The log is dead now, so these are lost whatever was stored before the kill.
	for n := 500; n < 500+crashAfterKill; n++ {
		raise(n)
	}

	// The retries run out. The first loss raises EventsNotLogged, which the API shows; its own publish, to the
	// dead log, runs out too, so wait for it to be shown and then for the publisher to hold nothing.
	eventually(t, "EventsNotLogged to reach the API", 60*time.Second, func() bool { return len(r.leaseAPIEvents(eventcount.ReasonEventsNotLogged)) > 0 })
	r.idle()
	lost, _ := r.counter.Unlogged(controller)
	if lost < 1 {
		t.Fatalf("no event was lost with the log down past the retries (kill at %d)", kill)
	}
	// The counter also counts the EventsNotLogged that ran out against the dead log: each was raised while the
	// log was down, so each was lost, and none is among the events raised. The raised-events counter holds how many were
	// raised (exactly, by HBEP-8's rule); subtracting them takes away the unit of slack they would give an event
	// that was neither stored nor counted.
	lostEvents := lost - r.count(r.key(eventcount.ActionDegraded, eventcount.ReasonEventsNotLogged, provider.EventKindWarning))
	r.startLogProc()
	// The log is back: the next publish it takes closes the run of losses with one more EventsNotLogged.
	trigger := r.newAlloc("trigger", "3h")
	r.typed.EventfID("after-return", trigger, nil, provider.EventKindOrdinary, "JobStarted", "ready", "after the return")
	leaseSubject := "hb.ev.lease." + string(r.lease.UID)
	eventually(t, "the closing EventsNotLogged to be in the log", 60*time.Second, func() bool {
		for _, m := range r.logAll() {
			if m.Subject == leaseSubject && strings.Contains(m.Data.Note, "takes events again") {
				return true
			}
		}
		return false
	})
	r.idle()
	all := r.logAll()
	stored, closing := 0, 0
	for _, m := range all {
		if uid, ok := raised[m.ID]; ok {
			stored++
			if m.Subject != "hb.ev.allocation."+uid {
				t.Errorf("%s is on %q, want its Allocation's subject", m.ID, m.Subject)
			}
			continue
		}
		switch {
		case m.ID == "after-return":
		case m.Data.Reason == eventcount.ReasonEventsNotLogged && m.Subject == leaseSubject:
			if strings.Contains(m.Data.Note, "takes events again") {
				closing++
			}
			if m.Data.Action != eventcount.ActionDegraded || m.Data.Type != provider.EventKindWarning {
				t.Errorf("EventsNotLogged is %s/%s, want degraded/Warning", m.Data.Action, m.Data.Type)
			}
		default:
			t.Errorf("the log holds %s (%s on %s), which the emitter did not raise", m.ID, m.Data.Reason, m.Subject)
		}
	}
	t.Logf("%d raised: %d in the stream; the counter reads %v, %v of it EventsNotLogged that ran out: %v accounted for", len(raised), stored, lost, lost-lostEvents, float64(stored)+lostEvents)
	if float64(stored)+lostEvents < float64(len(raised)) {
		t.Errorf("stream %d + counter %v less the lost EventsNotLogged = %v, want at least %d: an event is neither in the log nor counted", stored, lost, float64(stored)+lostEvents, len(raised))
	}
	if closing != 1 {
		t.Errorf("the log holds %d EventsNotLogged closing the run, want exactly one after the log's return", closing)
	}
	// The API's half: EventsNotLogged, degraded, a Warning, regarding the Provider's own Lease, with the count and since
	// when in its note.
	var found bool
	for _, e := range r.leaseAPIEvents(eventcount.ReasonEventsNotLogged) {
		if e.Action == eventcount.ActionDegraded && e.Type == provider.EventKindWarning && e.ReportingController == v1alpha1.FieldManagerProvider &&
			e.Regarding.Kind == "Lease" && e.Regarding.Name == v1alpha1.ProviderLeaseName(r.prov) && strings.Contains(e.Note, " events not logged since ") {
			found = true
		}
	}
	if !found {
		t.Errorf("the API holds no degraded Warning EventsNotLogged regarding the Lease %s, with the count and since when: %+v", v1alpha1.ProviderLeaseName(r.prov), r.leaseAPIEvents(eventcount.ReasonEventsNotLogged))
	}
}

// The caps fit the disk (HBEP-8, Test plan, Beta; R10): with no log running at the start, because it was refused for
// space or is not up yet, the first typed event's loss raises EventsNotLogged, shown in the API, regarding the Provider's
// own Lease, with the count and since when. The publisher's retries are shortened, not skipped: the event is retried
// against a log that is not there, and counted when they run out.
func TestWithNoLogRunningTheFirstLossRaisesEventsNotLogged(t *testing.T) {
	r := newEvRig(t, rigOpts{logDown: true, budget: 1500 * time.Millisecond, withLease: true})
	controller := eventcount.ControllerProvider(r.prov)
	a := r.newAlloc("a", "3h")
	r.typed.EventfID(eid(a, "JobStarted", "0"), a, nil, provider.EventKindOrdinary, "JobStarted", "ready", "the first typed event")
	if v, _ := r.counter.Unlogged(controller); v != 0 {
		t.Fatalf("the event counts as lost at once (%v), before its retries have run out", v)
	}
	eventually(t, "the event to be counted as not logged", 60*time.Second, func() bool { v, _ := r.counter.Unlogged(controller); return v >= 1 })
	var got []eventsv1.Event
	eventually(t, "EventsNotLogged to reach the API", 60*time.Second, func() bool {
		got = r.leaseAPIEvents(eventcount.ReasonEventsNotLogged)
		return len(got) > 0
	})
	e := got[0]
	if e.Action != eventcount.ActionDegraded || e.Type != provider.EventKindWarning || e.ReportingController != v1alpha1.FieldManagerProvider ||
		e.Regarding.Kind != "Lease" || e.Regarding.Name != v1alpha1.ProviderLeaseName(r.prov) || e.Regarding.Namespace != v1alpha1.ControllerNamespace {
		t.Errorf("EventsNotLogged is %s/%s regarding %+v reported by %s, want a degraded Warning regarding the Lease %s/%s, reported by hpcburst-provider",
			e.Action, e.Type, e.Regarding, e.ReportingController, v1alpha1.ControllerNamespace, v1alpha1.ProviderLeaseName(r.prov))
	}
	if !strings.Contains(e.Note, "1 events not logged since ") {
		t.Errorf("the note is %q, want the count and since when", e.Note)
	}
	r.idle()
}
