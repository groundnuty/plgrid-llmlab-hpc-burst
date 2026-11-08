package lifecycle

// HBEP-8's raised-events counter on two real copies of the lifecycle
// controller (Test plan, Raised events and A standby): built by NewManager
// with the production wiring, one in this process and one in a child, each
// read on its own /metrics. The copies bind their metrics and health servers
// in #256's block, 29760-29779 (raisedPortBase), with #61's envtest block,
// under envtest.lock.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// raisedPortBase is #256's block, 29760-29779: ten pairs of ports.
const raisedPortBase = 29760

// copyRead reads a copy's /metrics for one series of the counter.
func copyRead(t *testing.T, addr string, k eventcount.Key) eventcount.Read {
	t.Helper()
	sent := time.Now()
	code, body := scrapeText(addr)
	answered := time.Now()
	if code != http.StatusOK {
		t.Fatalf("GET %s/metrics: %d", addr, code)
	}
	r, err := eventcount.ParseRead([]byte(body), k)
	if err != nil {
		t.Fatal(err)
	}
	r.Sent, r.Answered = sent, answered
	return r
}

// neverRaisedHere are lifecycle series no pass of this rig can raise: it has
// no agent, no tunnel, no handover and no second deployment, and copy A's
// start is the first, with no Lease before it.
var neverRaisedHere = []eventcount.Series{
	{Reason: v1alpha1.EventReasonHandedOver, Action: eventReplaced, Type: provider.EventKindOrdinary},
	{Reason: v1alpha1.EventReasonTunnelOrphaned, Action: eventDegraded, Type: provider.EventKindWarning},
	{Reason: v1alpha1.ReasonAgentRegistered, Action: eventStarted, Type: provider.EventKindOrdinary},
	{Reason: reasonObservationGap, Action: eventDegraded, Type: provider.EventKindWarning},
}

// TestRaisedEventsOnTwoManagers: the serving copy exports every series the
// lifecycle controller can raise, those never raised at 0; a standby exports
// none of them (exportsNothing: no hpcburst_ series at all). When the serving
// copy stops, the other takes the Lease, raises its start's ObservationGap
// and counts it in its own counter: 1 on its /metrics. An absence check whose
// reads straddle that hand-over is inconclusive, whatever the counts say
// (HBEP-8, Raised events, condition 2). A fixture with one replica, or one
// that injects the serving predicate, would prove nothing here.
func TestRaisedEventsOnTwoManagers(t *testing.T) {
	g := newServingRigAt(t, raisedPortBase)
	defer g.logPorts(t)
	a, pa := g.startCopy(t, "copy A", Options{})
	g.awaitServing(t, "copy A", pa)

	// Every series of the lifecycle controller is on the serving copy; the
	// ones this rig cannot raise read 0.
	_, body := scrapeText(pa.metrics)
	for _, s := range RaisedEvents() {
		k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: s.Reason, Action: s.Action, Type: s.Type}
		r, err := eventcount.ParseRead([]byte(body), k)
		if err != nil || !r.HasCount {
			t.Errorf("copy A, serving, has no series %s (%v)", k, err)
		}
	}
	for _, s := range neverRaisedHere {
		k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: s.Reason, Action: s.Action, Type: s.Type}
		if r, _ := eventcount.ParseRead([]byte(body), k); !r.HasCount || r.Count != 0 {
			t.Errorf("copy A: %s = %g (present %v), want a real 0", k, r.Count, r.HasCount)
		}
	}

	b, pb := g.startChildCopy(t)
	eventually(t, 2*time.Minute, "copy B is a standby with synced caches", func() (bool, string) {
		caches, leading := readyzCodes(t, pb.health)
		return caches == http.StatusOK && leading == http.StatusInternalServerError, fmt.Sprintf("/readyz/caches %d, /readyz/leading %d; output:\n%s", caches, leading, b.out.String())
	})
	exportsNothing(t, "copy B, a standby", pb.metrics)

	gap := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reasonObservationGap, Action: eventDegraded, Type: provider.EventKindWarning}
	first := copyRead(t, pa.metrics, gap)
	change := time.Now()
	if !a.stop(t, "copy A") {
		t.FailNow()
	}
	in, alloc := g.currentInstance(t)
	eventually(t, 2*time.Minute, "copy B takes the Lease and serves", func() (bool, string) {
		return servesState(pb.metrics, in, alloc)
	})
	var second eventcount.Read
	eventually(t, time.Minute, "copy B counts its start's ObservationGap", func() (bool, string) {
		second = copyRead(t, pb.metrics, gap)
		return second.HasCount && second.Count == 1, fmt.Sprintf("%s = %g (present %v)", gap, second.Count, second.HasCount)
	})
	b.stop(t)

	check := eventcount.Absence{
		Key: gap, First: first, Second: second, Change: change, Collectors: eventcount.LifecycleCollectors,
		Barriers: []eventcount.Barrier{{What: "copy B's gap emitter, after its take", PassStart: change, Observed: second.Sent}},
		Quiet:    "only the gap emitter raises ObservationGap",
	}
	v, why := check.Judge()
	if v != eventcount.Inconclusive || !strings.Contains(why, "a restart or a hand-over") {
		t.Fatalf("reads across the hand-over from copy A to copy B: %s (%s), want inconclusive for the hand-over", v, why)
	}
	t.Logf("across the hand-over: %s", why)
}
