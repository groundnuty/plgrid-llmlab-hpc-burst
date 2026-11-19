package front

import (
	"slices"
	"testing"

	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// Edits and deletes of a Workload that has requests waiting or in flight (HBEP-5, Workloads; the science agent's ruling Q15 on #764): a request keeps the
// definition it resolved with. A waiter keeps its charge, its route order and its managed bound whatever an edit does meanwhile; a Workload deleted from
// the view keeps its state, and so its FIFO, until the FIFO has drained and no request of it is in flight on a managed target; and the state, with the
// FIFO and the count, outlives a rebuild of the view that has nothing to do with it. Every name here starts with wedit, or fifo for what workload_fifo_test.go
// holds, so that it cannot meet another file's.

// weditAlpha applies edit to alpha in the snapshot. The snapshot's Workloads are copied first, so the router, which holds the slice it was last given,
// never sees the change before apply.
func weditAlpha(t *testing.T, f *wrFix, edit func(w *v1alpha1.Workload)) {
	t.Helper()
	f.snap.Workloads = slices.Clone(f.snap.Workloads)
	for i := range f.snap.Workloads {
		if w := &f.snap.Workloads[i]; w.Namespace == "ns" && w.Name == "alpha" {
			edit(w)
			return
		}
	}
	t.Fatal("no Workload ns/alpha in the snapshot")
}

// weditApply hands the snapshot to the router and waits until the rebuild has finished and its wake pass, which the rebuild hands to a goroutine, has run
// and gone idle: the router's rebuild count has advanced and neither wakeRunning nor wakeAgain is set. A test then asserts what the wake did, or did not.
func weditApply(t *testing.T, f *wrFix) {
	t.Helper()
	before := f.router.rebuilds.Load()
	f.apply()
	errWaitFor(t, "the rebuild to finish", func() bool { return f.router.rebuilds.Load() > before })
	errWaitFor(t, "the wake pass after the rebuild to go idle", func() bool { return !f.router.wakeRunning.Load() && !f.router.wakeAgain.Load() })
}

// A waiter keeps the charge it arrived with (Q15; HBEP-5, The count in tokens). a-0 has kvCacheTokens 1,000 (one worker: B = 1,000) at
// routing.instanceCap 10, and alpha's shape is typical context 800 + typical output 100, so c = 900. A Workload request is admitted only when
// 10 * (N + c) + u * 1,000 <= 10 * 1,000, which with u = 0 is N + c <= 1,000.
//
//	H, first:     N = 0:   0 + 900 = 900 <= 1,000: admitted, N = 900.
//	W, at c 900:  900 + 900 = 1,800 > 1,000: refused, so it waits.
//
// alpha is then edited to context 40 + output 10, so c = 50. A request that carried the edited charge would be admitted: 900 + 50 = 950 <= 1,000. W
// carries 900, so after the edit and the wake it still waits. H is cut: N = 0, and W is admitted at its own charge, 0 + 900 = 900 <= 1,000, so a-0 holds
// N = 900 and not 50. A request of alpha that arrives after that carries the edited charge: 900 + 50 = 950 <= 1,000, admitted, N = 950. That last request is
// the proof that the edit had reached the front all along, which the waiter's wait would show as well if the edit had been lost.
func TestAWaiterKeepsTheChargeItArrivedWithWhenItsWorkloadIsEdited(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 10, holdOurs: true, noManaged: true, edit: fifoLong})
	kv := int64(1000)
	f.snap.Instances = slices.Clone(f.snap.Instances)
	for i := range f.snap.Instances {
		if f.snap.Instances[i].UID == f.uids["ns/a-0"] {
			f.snap.Instances[i].Status.KVCacheTokens = &kv
		}
	}
	weditAlpha(t, f, func(w *v1alpha1.Workload) {
		w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 800, 100
	})
	f.apply()
	charged := func(when string, wantN int64) {
		t.Helper()
		n, u, ok := f.scGroup("ns/md-a").Charged("ns/a-0")
		if !ok || n != wantN || u != 0 {
			t.Fatalf("%s: a-0 holds N = %d, u = %d (member %v), want N = %d, u = 0", when, n, u, ok, wantN)
		}
	}

	_, cutH := f.start(f.alpha(), "edit-h")
	fifoSettled(t, f, 1, 0)
	charged("H admitted at c 900", 900)
	f.start(f.alpha(), "edit-w")
	ws := fifoWaitState(t, f, 1) // W: 900 + 900 = 1,800 > 1,000

	weditAlpha(t, f, func(w *v1alpha1.Workload) {
		w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 40, 10
	})
	weditApply(t, f)
	if n := ws.waiting.Load(); n != 1 {
		t.Fatalf("the FIFO holds %d after the edit and its wake, want 1: the waiter carries the charge it arrived with, 900, and 900 + 900 > 1,000", n)
	}
	if got := fifoSessions(f, "ns/a-0"); len(got) != 1 {
		t.Fatalf("a-0 has received %v after the edit, want only edit-h: the waiter was admitted at the edited charge", got)
	}
	charged("after the edit, the waiter still waits", 900)

	cutH() // N falls to 0 and the waiter is admitted at its own charge
	if got := fifoWaitSessions(t, f, "ns/a-0", 2); got[1] != "edit-w" {
		t.Fatalf("a-0 was given %v after H ended, want edit-w second", got)
	}
	charged("the waiter admitted: its charge is the old 900, not the edited 50", 900)

	f.start(f.alpha(), "edit-n") // arrives after the edit: c = 50
	if got := fifoWaitSessions(t, f, "ns/a-0", 3); got[2] != "edit-n" {
		t.Fatalf("a-0 was given %v, want edit-n third: 900 + 50 = 950 <= 1,000 admits a request that carries the edited charge", got)
	}
	charged("a request that arrived after the edit carries 50", 950)
}

// weditRouteOrderRow is a Workload's routes with a-0 at cap 1 and the managed target at managedSessions 1, three requests of alpha H on a-0, M on the managed
// target and W waiting, where the Workload's route order at the start is managedFirst and is edited to the other one after W waits. The wake of the edit
// places nothing, since both routes are taken. Both routes are then freed, M's and H's completions each held at the start of its wake pass (the gate), so
// that the first pass that runs finds a-0 and the managed target both free and the route order is the only thing that says where W goes. It returns the
// sessions a-0 has received and how many requests have reached the LLMLab double.
func weditRouteOrderRow(t *testing.T, managedFirst bool) (a0 []string, managed int) {
	t.Helper()
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1, managedFirst: managedFirst, edit: fifoLong})
	gate := newFifoGate(t, f.router, false)
	var cutH, cutM func()
	if managedFirst {
		_, cutM = f.start(f.alpha(), "edit-m")
		f.settled(1, 0, 0) // ManagedFirst: the first request goes to the managed target
		_, cutH = f.start(f.alpha(), "edit-h")
		f.settled(1, 1, 0) // the managed target is at its bound, so the second goes to ours
	} else {
		_, cutH = f.start(f.alpha(), "edit-h")
		f.settled(0, 1, 0) // OwnFirst: the first goes to a-0
		_, cutM = f.start(f.alpha(), "edit-m")
		f.settled(1, 1, 0) // a-0 is at its cap, so the second overflows to the managed target
	}
	f.start(f.alpha(), "edit-w")
	f.settled(1, 1, 1) // both routes are taken: W waits

	weditAlpha(t, f, func(w *v1alpha1.Workload) {
		if managedFirst {
			w.Spec.RouteOrder = v1alpha1.RouteOwnFirst
		} else {
			w.Spec.RouteOrder = v1alpha1.RouteManagedFirst
		}
		w.Spec.ManagedSessions = ptr.To(int32(1)) // a ManagedFirst Workload carries a bound; the edit keeps the same one
	})
	weditApply(t, f)
	f.settled(1, 1, 1) // nothing was free, so the wake of the edit placed nothing

	gate.arm()
	cutM()
	errWaitFor(t, "the managed target's slot to be free", func() bool { return f.managedCount() == 0 })
	cutH()
	errWaitFor(t, "a-0's slot to be free", func() bool { return f.scLoad("ns/md-a", "ns/a-0") == 0 })
	gate.waitEntered(t, 2) // both completions are held at their wake pass: a-0 and the managed target are both free, and W is not placed
	if f.waiting() != 1 {
		t.Fatalf("%d requests wait with the wake held, want 1", f.waiting())
	}
	gate.release()
	// W is placed on exactly one of the two: a-0 (a second request in its log) or the managed target (a second at the LLMLab double).
	errWaitFor(t, "W to be placed", func() bool { return len(fifoSessions(f, "ns/a-0")) >= 2 || f.arrivedManaged() >= 2 })
	errWaitFor(t, "the FIFO to drain", func() bool { return f.waiting() == 0 })
	return fifoSessions(f, "ns/a-0"), f.arrivedManaged()
}

// A waiter keeps its route order (Q15; HBEP-5, Route order). W arrived under OwnFirst with managedSessions 1, and alpha is then edited to ManagedFirst.
// With a-0 and the managed target both free, an OwnFirst request goes to a-0, so by hand a-0 has received H and W and the LLMLab double only M (1). A
// waiter that took the edited order would go to the managed target first: a-0 would have H alone and the LLMLab double two.
func TestAWaiterThatArrivedUnderOwnFirstKeepsItWhenItsWorkloadIsEditedToManagedFirst(t *testing.T) {
	a0, managed := weditRouteOrderRow(t, false)
	if len(a0) != 2 || a0[0] != "edit-h" || a0[1] != "edit-w" || managed != 1 {
		t.Errorf("a-0 received %v and the LLMLab double %d, want [edit-h edit-w] and 1: an OwnFirst waiter takes the free a-0 before the managed target", a0, managed)
	}
}

// The converse: W arrived under ManagedFirst and alpha is edited to OwnFirst. With both routes free a ManagedFirst request goes to the managed target, so by
// hand the LLMLab double has received M and W (2) and a-0 only H.
func TestAWaiterThatArrivedUnderManagedFirstKeepsItWhenItsWorkloadIsEditedToOwnFirst(t *testing.T) {
	a0, managed := weditRouteOrderRow(t, true)
	if len(a0) != 1 || a0[0] != "edit-h" || managed != 2 {
		t.Errorf("a-0 received %v and the LLMLab double %d, want [edit-h] and 2: a ManagedFirst waiter takes the free managed target before a-0", a0, managed)
	}
}

// A waiter keeps its managed bound (Q15; HBEP-5, managedSessions). alpha is OwnFirst with managedSessions 1; a-0 (cap 1) holds H, the managed target holds
// M (the Workload's count is 1, at its bound), and W waits. alpha is edited to managedSessions 5. Under the bound W arrived with, 1, the count 1 is at it,
// so after the edit and its wake W still waits and the LLMLab double has received only M. M is cut: the count is 0, below the bound of 1, and W is placed
// on the managed target (the LLMLab double has received 2) and the count is 1 again. A waiter that took the edited bound, 5, would have been placed by the
// edit's own wake, with the count 1 < 5.
func TestAWaiterKeepsTheManagedBoundItArrivedWithWhenItsWorkloadIsEdited(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1, edit: fifoLong})
	f.start(f.alpha(), "edit-h")
	f.settled(0, 1, 0)
	_, cutM := f.start(f.alpha(), "edit-m")
	f.settled(1, 1, 0)
	if got := f.managedCount(); got != 1 {
		t.Fatalf("the Workload's count is %d with M on the managed target, want 1", got)
	}
	f.start(f.alpha(), "edit-w")
	f.settled(1, 1, 1) // a-0 is at its cap and the count is at the bound: W waits

	weditAlpha(t, f, func(w *v1alpha1.Workload) { w.Spec.ManagedSessions = ptr.To(int32(5)) })
	weditApply(t, f)
	if got := f.arrivedManaged(); got != 1 {
		t.Fatalf("the LLMLab double has received %d requests after the edit, want 1: W took the edited bound 5 and was placed on the managed target, though it arrived with 1", got)
	}
	if got := f.waiting(); got != 1 {
		t.Fatalf("%d requests wait after the edit, want 1", got)
	}

	cutM() // the count falls to 0, below the bound W arrived with
	f.settled(2, 1, 0)
	if got := f.managedCount(); got != 1 {
		t.Errorf("the Workload's count is %d with W on the managed target, want 1", got)
	}
}

// A deleted Workload (Q15; HBEP-5, Workloads). One request of alpha, W, waits behind H on a-0 (cap 1). alpha is deleted from the view. A new request that
// names alpha is refused, 400 invalid_model_name with detail unknown_workload, since the view has no such Workload. W is not refused: it still waits, and
// its Workload's state still exists, since its FIFO has not drained. H is cut and W is served under the definition it came with: 200 from a-0, its usage
// record carrying workload alpha. Only then does the state go (nothing waits and nothing of it is on a managed target), and with it the waiting series'
// sample for alpha. A drain that dropped the FIFO without serving W fails the 200.
func TestAWorkloadDeletedWhileARequestWaitsStillServesItAndThenItsStateGoes(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: fifoLong})
	_, cutH := f.start(f.alpha(), "edit-h")
	fifoSettled(t, f, 1, 0)
	f.engines["ns/a-0"].Set(enginetest.Behaviour{}) // H is held; what comes after it is answered
	waiter, _ := f.start(f.alpha(), "edit-w")
	ws := fifoWaitState(t, f, 1)

	f.dropWorkload("ns", "alpha")
	weditApply(t, f)
	f.refused("a new request naming the deleted Workload", f.alpha(), "unknown_workload")
	if got := f.wlState(); got != ws {
		t.Fatalf("alpha's state after the delete is %p, want the one the waiter is in, %p", got, ws)
	}
	if n := ws.waiting.Load(); n != 1 {
		t.Fatalf("the FIFO holds %d after the delete and its wake, want 1: W is neither served nor dropped", n)
	}
	if n := f.waiting(); n != 1 {
		t.Fatalf("Status().Waiting is %d after the delete, want 1", n)
	}
	wantSample(t, scrape(t, f.metrics), 1, fifoWaitingSeries, "workload", "alpha", "model_deployment", "")

	cutH() // a-0 is free
	resp, body := waiter.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("the waiter: status %d from %q, want 200 from ns/a-0 under the definition it came with: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	wantWorkload(t, "served after the delete", f.record(resp.Header.Get("hpcburst-request-id")), str("alpha"))
	errWaitFor(t, "alpha's state to be dropped", func() bool { return f.wlState() == nil })
	wantNoSample(t, scrape(t, f.metrics), fifoWaitingSeries, "workload", "alpha", "model_deployment", "")
}

// The FIFO and the count survive a rebuild (Q15). a-0 (cap 1) holds H, the managed target holds M (managedSessions 1: the count is 1), and W waits. A
// ModelDeployment of another model with an Instance joins, and the view is rebuilt: the router's state of alpha is the same object, its FIFO still holds
// one and its count is still 1. H is cut and W is placed on a-0, then served, and the count is still 1, M still being on the managed target.
func TestTheWorkloadFIFOAndCountSurviveARebuildThatAnUnrelatedInstanceCauses(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1, edit: fifoLong})
	_, cutH := f.start(f.alpha(), "edit-h")
	f.settled(0, 1, 0)
	f.start(f.alpha(), "edit-m")
	f.settled(1, 1, 0)
	waiter, _ := f.start(f.alpha(), "edit-w")
	f.settled(1, 1, 1)
	ws := f.wlState()
	if ws == nil || ws.waiting.Load() != 1 || f.managedCount() != 1 {
		t.Fatalf("before the rebuild: state %v, want one waiting and a count of 1", ws)
	}

	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: f.clk})
	f.servedMD("md-other", "an-unrelated-model-9b", 1, "helios-a")
	f.instance("md-other", "o-0", "helios-a", e, key, instanceOpts{})
	weditApply(t, f)
	if got := f.wlState(); got != ws {
		t.Fatalf("alpha's state after the rebuild is %p, want the same object, %p", got, ws)
	}
	if n := ws.waiting.Load(); n != 1 {
		t.Errorf("the FIFO holds %d after the rebuild, want 1", n)
	}
	if n := f.managedCount(); n != 1 {
		t.Errorf("the Workload's count is %d after the rebuild, want 1", n)
	}
	if n := f.waiting(); n != 1 {
		t.Errorf("Status().Waiting is %d after the rebuild, want 1", n)
	}

	cutH() // a-0 is free and W takes it
	if got := fifoWaitSessions(t, f, "ns/a-0", 2); got[1] != "edit-w" {
		t.Fatalf("a-0 was given %v after H ended, want edit-w second", got)
	}
	f.freeOurs()
	resp, body := waiter.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("the waiter: status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	if n := f.managedCount(); n != 1 {
		t.Errorf("the Workload's count is %d after W was served, want 1: M is still on the managed target", n)
	}
}
