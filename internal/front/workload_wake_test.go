package front

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The wakes of a Workload's FIFO (HBEP-5, Workloads, Waiting; HBEP-764's PR 5, the science agent's rulings Q5 and Q6): a request that no route of its Workload
// can take waits in the Workload's FIFO, and each event that frees a route places the head. The five events are a completion on a managed target, a
// completion on an Instance of the Workload, a join, an Instance that is eligible again, and the cap going off. Each row holds the waiter where it waits,
// shows from the doubles' counts and the router's count of waiters that it has not been placed, fires its one trigger, and shows where it went.
//
// queueTimeout is ten minutes in every row (wkQueueLong), so a wake path that is removed leaves the waiter waiting until errWaitFor's deadline, which fails the
// row: no row can pass by the time-out or by polling. Each request is held where it went and is cut by its client's cancel, which completes it on its target.

// wkQueueLong sets queueTimeout far beyond any row, so that no placement is the time-out's.
func wkQueueLong(c *Config) { c.QueueTimeout = Duration(10 * time.Minute) }

// wkSessions is the session key of each request an engine has received, in the order they arrived: the front sets x-session-id toward an Instance to the
// request's key (proxy.go, sendWith), and the engine logs a request on arrival, held or not.
func wkSessions(e *enginetest.Engine) []string {
	var out []string
	for _, r := range e.Requests() {
		out = append(out, r.Header.Get("X-Session-Id"))
	}
	return out
}

// wkAddInstance adds an Instance of md, a ModelDeployment of the fixture's snapshot, with an engine double of its own, as newPwFixOn makes them, and holds its
// requests on the fixture's hold when hold is set. The caller applies the snapshot.
func wkAddInstance(f *wrFix, md, name string, hold bool) *enginetest.Engine {
	f.t.Helper()
	key := newKey(f.t)
	e := enginetest.Start(f.t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: f.clk})
	f.uids["ns/"+name] = f.instance(md, name, "helios-a", e, key, instanceOpts{})
	f.engines["ns/"+name] = e
	if hold {
		e.Set(enginetest.Behaviour{Hold: f.hold})
	}
	return e
}

// wkUnreachable sets the AgentUnreachable condition of an Instance to status, as the control plane does (HBEP-5, Request path, Routable: the Instance stays a
// member, not eligible). The caller applies the snapshot.
func wkUnreachable(f *wrFix, inst string, status metav1.ConditionStatus) {
	f.t.Helper()
	f.inst(f.uids["ns/"+inst]).Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionAgentUnreachable, Status: status, Reason: "Test", LastTransitionTime: metav1.Now()}}
}

// wkSettledOurs waits until open requests are open on our engines and waiting wait at the front: the fixture's settled for a front with no managed target,
// which has no LLMLab double to count.
func wkSettledOurs(f *wrFix, open, waiting int) {
	f.t.Helper()
	errWaitFor(f.t, "the requests to settle", func() bool { return f.openOurs() == open && f.waiting() == waiting })
}

// wkWantRequests fails the test unless the engine has received exactly the requests of these session keys, in this order.
func wkWantRequests(t *testing.T, what string, e *enginetest.Engine, want ...string) {
	t.Helper()
	got := wkSessions(e)
	if len(got) != len(want) {
		t.Fatalf("%s: the engine received requests of sessions %q, want %q", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: the engine received requests of sessions %q, want %q", what, got, want)
		}
	}
}

// Wake 1, a completion on a managed target (HBEP-5, Workloads, Waiting: a managed target is one of the Workload's routes, and its completion frees a unit
// of the managedSessions count). alpha is OwnFirst with managedSessions 1. a-0 (cap 1) holds one request and the managed target holds one, so the next
// waits; cutting the managed one frees the count, and the waiter goes to the managed target, where it is held. a-0 still has its one.
func TestACompletionOnAManagedTargetPlacesTheWaitingRequestOnTheManagedTarget(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1, edit: wkQueueLong})
	f.start(f.alpha(), "wk1-own")
	f.settled(0, 1, 0) // a-0 holds its one
	_, cutManaged := f.start(f.alpha(), "wk1-over")
	f.settled(1, 1, 0) // the overflow went to the managed target: count 1 of 1
	f.start(f.alpha(), "wk1-wait")
	f.settled(1, 1, 1) // a-0 is at its cap and the managed target at the bound: it waits, and has not arrived anywhere
	if got := f.managedCount(); got != 1 {
		t.Fatalf("the Workload's count is %d before the cut, want 1", got)
	}

	cutManaged()       // the one on the managed target ends: the trigger
	f.settled(2, 1, 0) // the waiter is on the LLMLab double now (arrived typed 2), and a-0 has still only its one
	wkWantRequests(t, "a-0", f.engines["ns/a-0"], "wk1-own")
	errWaitFor(t, "the count to be 1 again, the waiter's", func() bool { return f.managedCount() == 1 })
}

// Wake 2, a completion on an Instance of the Workload: a-0 (cap 1) holds one request and no managed target exists, so the next waits; cutting the holder
// frees a-0, and the waiter is placed on it.
func TestACompletionOnAnInstanceOfTheWorkloadPlacesTheWaitingRequestOnIt(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	_, cutHolder := f.start(f.alpha(), "wk2-holder")
	wkSettledOurs(f, 1, 0)
	f.start(f.alpha(), "wk2-wait")
	wkSettledOurs(f, 1, 1) // a-0 is full and nothing else is a route: it waits
	wkWantRequests(t, "a-0 before the cut", f.engines["ns/a-0"], "wk2-holder")

	cutHolder() // a-0 frees its one: the trigger
	wkSettledOurs(f, 1, 0)
	errWaitFor(t, "the waiter to reach a-0", func() bool { return len(f.engines["ns/a-0"].Requests()) == 2 })
	wkWantRequests(t, "a-0 after the cut", f.engines["ns/a-0"], "wk2-holder", "wk2-wait")
}

// Wake 3, a join: a-0 (cap 1) is full and no managed target exists, so the request waits; a new Instance a-1 joins md-a, and the waiter is placed on it.
func TestAJoinPlacesTheWaitingRequestOnTheNewInstance(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.start(f.alpha(), "wk3-holder")
	wkSettledOurs(f, 1, 0)
	f.start(f.alpha(), "wk3-wait")
	wkSettledOurs(f, 1, 1)

	a1 := wkAddInstance(f, "md-a", "a-1", true)
	f.apply() // a-1 joins: the trigger
	wkSettledOurs(f, 2, 0)
	errWaitFor(t, "the waiter to reach a-1", func() bool { return len(a1.Requests()) == 1 })
	wkWantRequests(t, "a-1", a1, "wk3-wait")
	wkWantRequests(t, "a-0", f.engines["ns/a-0"], "wk3-holder")
}

// Wake 4, an Instance eligible again. The waiter must really wait, so two ModelDeployments serve alpha: md-a (a-0, cap 1, held) is full, and md-b's only
// Instance b-0 is a member that is not eligible (AgentUnreachable true: HBEP-5, Request path, Routable), so nothing takes the request, and a full
// ModelDeployment is what makes it wait, not "no ready target". The condition clears, and the waiter is placed on b-0.
func TestAnInstanceEligibleAgainPlacesTheWaitingRequestOnIt(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.servedMDIn("ns", "md-b", scModel, 1, "helios-a")
	b0 := wkAddInstance(f, "md-b", "b-0", true)
	f.apply() // b-0 joins, eligible
	wkUnreachable(f, "b-0", metav1.ConditionTrue)
	f.apply() // b-0 stays a member and is not eligible now

	f.start(f.alpha(), "wk4-holder")
	wkSettledOurs(f, 1, 0) // a-0, the only eligible Instance, holds its one
	f.start(f.alpha(), "wk4-wait")
	wkSettledOurs(f, 1, 1)
	if got := len(b0.Requests()); got != 0 {
		t.Fatalf("b-0 is not eligible and has received %d requests, want 0", got)
	}

	wkUnreachable(f, "b-0", metav1.ConditionFalse)
	f.apply() // b-0 is eligible again: the trigger
	wkSettledOurs(f, 2, 0)
	errWaitFor(t, "the waiter to reach b-0", func() bool { return len(b0.Requests()) == 1 })
	wkWantRequests(t, "b-0", b0, "wk4-wait")
	wkWantRequests(t, "a-0", f.engines["ns/a-0"], "wk4-holder")
}

// Wake 5, the cap going off (HBEP-5, Load bound and cap: the count cap is in force only while the eligible Instances meet spec.instances). md-a has
// spec.instances 2 and two Instances of cap 1, both held: ready 2 of desired 2, so the cap is in force and the next request waits. a-1 becomes
// ineligible: ready 1 of desired 2, the loose bound and no cap, so the waiter is placed on a-0, which then holds two requests.
func TestTheCapGoingOffPlacesTheWaitingRequestOnTheRemainingInstance(t *testing.T) {
	f := newWrFix(t, wrOpts{instances: []string{"a-0", "a-1"}, cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.start(f.alpha(), "wk5-first")
	f.start(f.alpha(), "wk5-second")
	wkSettledOurs(f, 2, 0) // both held, one on each: a request whose home is full spills to the other
	if a0, a1 := f.engines["ns/a-0"].Open(), f.engines["ns/a-1"].Open(); a0 != 1 || a1 != 1 {
		t.Fatalf("open on a-0 and a-1: %d and %d, want 1 and 1 (cap 1 each)", a0, a1)
	}
	f.start(f.alpha(), "wk5-wait")
	wkSettledOurs(f, 2, 1)

	wkUnreachable(f, "a-1", metav1.ConditionTrue)
	f.apply() // 1 eligible of 2 desired: the cap is off, the trigger
	wkSettledOurs(f, 3, 0)
	if a0, a1 := f.engines["ns/a-0"].Open(), f.engines["ns/a-1"].Open(); a0 != 2 || a1 != 1 {
		t.Errorf("open on a-0 and a-1: %d and %d, want 2 and 1 (the waiter went to a-0, the only eligible one)", a0, a1)
	}
	if got := wkSessions(f.engines["ns/a-0"]); got[len(got)-1] != "wk5-wait" {
		t.Errorf("the last request a-0 received is of session %q, want wk5-wait", got[len(got)-1])
	}
}

// Q5, the order of a freed slot (the science agent's ruling Q5 on #764; HBEP-5, Workloads, Waiting): a group's own waiters, which name no Workload, are
// placed first, and then the Workload FIFOs, the one whose head is oldest first. So a freed slot goes to the unnamed waiter U even though the Workload's
// waiter W arrived before it. a-0 (cap 1) is held by H; W waits in alpha's FIFO and then U in the group's queue; H is cut: a-0 takes U, and W still waits.
// Cutting U then frees the slot for W, which shows W was waiting and not dropped.
func TestAFreedSlotGoesToTheGroupsOwnWaiterBeforeAWorkloadsOlderWaiter(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	_, cutH := f.start(scModel, "q5-h")
	wkSettledOurs(f, 1, 0)
	f.start(f.alpha(), "q5-w") // the Workload's waiter, the older
	wkSettledOurs(f, 1, 1)
	_, cutU := f.start(scModel, "q5-u") // the group's own waiter, the younger
	wkSettledOurs(f, 1, 2)
	if got := f.wlState().waiting.Load(); got != 1 {
		t.Fatalf("alpha's FIFO holds %d, want 1 (W; U is in the group's queue)", got)
	}

	cutH() // one slot frees
	errWaitFor(t, "a-0 to take its second request", func() bool { return len(f.engines["ns/a-0"].Requests()) == 2 })
	wkWantRequests(t, "a-0 after H is cut", f.engines["ns/a-0"], "q5-h", "q5-u")
	wkSettledOurs(f, 1, 1) // U holds the slot, and W still waits
	if got := f.wlState().waiting.Load(); got != 1 {
		t.Errorf("alpha's FIFO holds %d, want 1: W still waits", got)
	}

	cutU() // the slot frees again: now the Workload's waiter is the only one
	errWaitFor(t, "a-0 to take its third request", func() bool { return len(f.engines["ns/a-0"].Requests()) == 3 })
	wkWantRequests(t, "a-0 after U is cut", f.engines["ns/a-0"], "q5-h", "q5-u", "q5-w")
	wkSettledOurs(f, 1, 0)
}

// wkOlderHeadTakesTheSlot is Q5's second half: of two Workloads' FIFOs the one whose head arrived first is served first, whatever the Workloads' names.
// a-0 (cap 1) is held by H; a waiter of the Workload older arrives, then one of younger; H is cut, and the slot goes to older's waiter.
func wkOlderHeadTakesTheSlot(t *testing.T, older, younger string) {
	t.Helper()
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.workload("ns", "beta", scModel)
	f.apply()
	_, cutH := f.start(scModel, "q5w-h")
	wkSettledOurs(f, 1, 0)
	f.start(scModel+"#"+older, "q5w-"+older)
	wkSettledOurs(f, 1, 1)
	f.start(scModel+"#"+younger, "q5w-"+younger)
	wkSettledOurs(f, 1, 2)

	cutH()
	errWaitFor(t, "a-0 to take its second request", func() bool { return len(f.engines["ns/a-0"].Requests()) == 2 })
	wkWantRequests(t, "a-0 after H is cut", f.engines["ns/a-0"], "q5w-h", "q5w-"+older)
	wkSettledOurs(f, 1, 1) // the older's waiter holds the slot; the younger's waits
}

// Q5 across Workloads: beta's waiter arrives before alpha's, so beta's head is older, and the freed slot is beta's though alpha sorts first by name.
func TestAFreedSlotGoesToTheWorkloadWhoseWaiterIsOlderEvenWhenItsNameSortsLater(t *testing.T) {
	wkOlderHeadTakesTheSlot(t, "beta", "alpha")
}

// The mirror of the row above, so that no order of names or of arrival passes both: alpha's waiter is the older, and the slot is alpha's.
func TestAFreedSlotGoesToTheWorkloadWhoseWaiterIsOlderWhenItsNameSortsFirst(t *testing.T) {
	wkOlderHeadTakesTheSlot(t, "alpha", "beta")
}
