package lifecycle

// HBEP-7's hedge slot rows that no merged test carried (Test plan, The hedge's slots, and #725's first two rows; #604
// PR 2): a hedge member and another deployment's new primary racing for a Provider's last slot, in both interleavings
// and in host-dialed mode, and two same-Provider members of one hedge made by two leaders. Every count is worked out by
// hand, as a literal, never by the slot code.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// syncWorld is rg.Step without the deployments' passes: the Providers' passes, then the agents'.
func syncWorld(rg *rig) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range rg.extra {
		if err := x.prov.Sync(rg.ctx); err != nil {
			rg.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
	rg.syncAgents()
}

// holdersOn counts the Allocations on the Provider that hold a slot, whatever the controller's reader shows.
func holdersOn(rg *rig, provider string) int {
	rg.t.Helper()
	n := 0
	for _, a := range rg.allAllocations() {
		if ref := a.Spec.ProviderConfigReference; ref != nil && ref.Name == provider && holdsSlot(&a) {
			n++
		}
	}
	return n
}

// TestAHedgeMemberAndAnotherDeploymentsPrimaryRaceForTheLastSlot (#725's row 1, #604 PR 2's "a concurrent case"): D's
// replacement hedge is due a member W on B at 08:00:20, and a new deployment md-f, which allows B alone, makes its first
// primary there in the same moment. B holds E's primary and nothing reserved (E ends at 09:30, before its drain start
// 09:50:20), so B's limit of 2 leaves ONE slot beyond what it holds: W needs it, and so does md-f's primary. Both passes
// are reconciled AT ONCE (reconcileAtOnce) against a cache that never shows the first creation, and each interleaving is
// forced by a barrier BETWEEN the first pass's reservation and its Allocation's create (A7 of #751): the first pass runs on
// to the create of its Allocation on B and waits there, in the window where the reservation is made and the Allocation is
// not; the second pass, held at its first list of its own Allocations until the first arrives there, then runs its whole pass,
// reads inside that window, and ends; and only then is the first pass's create released. A reservation that were recorded
// only after the create would let the second pass take the same slot, so the row would then find three Allocations on B, not
// two. Whichever reserves first creates, and the other is refused:
//   - W first: 1 listed (E) + W's expectation = 2 >= 2 refuses md-f, which has no Instance and shows SubmitLimitReached
//     naming B; D keeps R1 on A (in P's reserved slot, host-dialed) and has W;
//   - md-f first: 1 listed + md-f's expectation = 2 >= 2 refuses W, silently (a member passed over sets no reason), so D has R1
//     alone and md-f has its primary.
//
// Either way B holds E's Allocation and exactly one more, 2, the limit, never 3, also once the cache has caught up and one more
// pass has run. The same rows run on a submit limit of 2 set by B's QOS and in host-dialed mode (2 per Provider).
func TestAHedgeMemberAndAnotherDeploymentsPrimaryRaceForTheLastSlot(t *testing.T) {
	for _, mode := range []struct {
		name string
		rig  func(t *testing.T, opts ...rigOption) *hedgeRig
	}{
		{"a submit limit of 2 on B", func(t *testing.T, opts ...rigOption) *hedgeRig {
			return newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b2", MaxSubmitPerUser: i32(2)}, opts...)
		}},
		{"host-dialed, 2 per Provider", func(t *testing.T, opts ...rigOption) *hedgeRig {
			return newHedgeRig(t, append([]rigOption{withHostDialed()}, opts...)...)
		}},
	} {
		for _, first := range []string{hoMD, "md-f"} {
			t.Run(mode.name+", "+first+" reserves first", func(t *testing.T) {
				second := map[string]string{hoMD: "md-f", "md-f": hoMD}[first]
				atCreate := make(chan struct{})
				ended := map[string]chan struct{}{hoMD: make(chan struct{}), "md-f": make(chan struct{})}
				var armed atomic.Bool
				var once sync.Once
				barrier := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if a, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() && a.Spec.ProviderConfigReference != nil && a.Spec.ProviderConfigReference.Name == hedgeB &&
						a.Labels[v1alpha1.LabelModelDeployment] == first {
						reached := false
						once.Do(func() { reached = true })
						if reached {
							close(atCreate) // the reservation is made and the Allocation is not: the second pass may read now
							if err := waitOn(t, ended[second], second+"'s pass ending"); err != nil {
								return err
							}
						}
					}
					return c.Create(ctx, obj, opts...)
				}}
				h := mode.rig(t, withInterceptors(barrier))
				h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour}) // A: P starts at once, R1 pends
				h.Step()                                                                 // the Providers' first status
				e := mdSpec("md-e")
				e.Spec.Providers.Names = []string{hedgeB}
				end := metav1.NewTime(hoT(9, 30, 0))
				e.Spec.EndTime = &end
				h.create(e)
				d := mdSpec(hoMD)
				onBoth(d)
				h.create(d)
				h.clock.Advance(10 * time.Second)
				h.Step() // 08:00:10: P on A, E's primary on B
				setWaits(h.rig, rigProvider, 20, 10*time.Hour)
				f := mdSpec("md-f")
				f.Spec.Providers.Names = []string{hedgeB}
				h.create(f)
				h.clock.Advance(10 * time.Second) // 08:00:20
				syncWorld(h.rig)                  // P's job starts; D is due R1 on A and W on B
				if n := holdersOn(h.rig, hedgeB); n != 1 {
					t.Fatalf("B holds %d Allocations before the race, want E's alone", n)
				}

				g := &passGate{Reader: newStaleReader(t, h.c), hold: map[string]func() error{}, done: map[string]bool{}}
				g.hold[second] = func() error { return waitOn(t, atCreate, first+"'s pass reaching its Allocation's create") }
				h.r.Reader = g
				armed.Store(true)
				for i, err := range h.reconcileAtOnceThen(h.r, func(name string) { close(ended[name]) }, hoMD, "md-f") {
					if err != nil {
						t.Fatalf("reconcile %d: %v", i, err)
					}
				}
				select {
				case <-atCreate:
				default:
					t.Fatalf("%s's pass never reached its Allocation's create: the two passes did not meet", first)
				}
				var onB []v1alpha1.Instance
				for _, in := range h.onProvider(hoMD, hedgeB) {
					onB = append(onB, in)
				}
				wantW, wantF := 1, 0
				if first == "md-f" {
					wantW, wantF = 0, 1
				}
				if len(onB) != wantW || len(h.instances("md-f")) != wantF {
					t.Fatalf("D has %d Instances on B and md-f %d, want %d and %d: %s reserved first", len(onB), len(h.instances("md-f")), wantW, wantF, first)
				}
				if n := len(h.onProvider(hoMD, rigProvider)); n != 2 {
					t.Fatalf("D has %d Instances on A, want P and R1", n)
				}
				if n := holdersOn(h.rig, hedgeB); n != 2 {
					t.Fatalf("B holds %d Allocations, want E's and exactly one more: its limit is 2", n)
				}
				if first == hoMD {
					waitsNaming(t, h.rig, "md-f", "")
				} else if c := h.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
					t.Fatalf("D Degraded = %s %q: a hedge member passed over sets no reason", c.Reason, c.Message)
				}
				// The cache catches up; the next pass changes nothing.
				g.Reader.(*staleReader).catchUp(t)
				h.clock.Advance(10 * time.Second)
				h.Step()
				if n := holdersOn(h.rig, hedgeB); n != 2 {
					t.Fatalf("after the cache caught up, B holds %d Allocations, want still 2", n)
				}
			})
		}
	}
}

// newLeader is a second controller replica over the rig's store: its own Reconciler, with a slot table and a Backend of its
// own, as a leader hand-over leaves two (a Lease does not fence).
func (rg *rig) newLeader() *Reconciler {
	return &Reconciler{
		Client: rg.c, Reader: rg.c, Clock: rg.clock, Backend: &Backend{Reader: rg.c, Clock: rg.clock, CredentialsDir: rg.creds},
		Checkpoints: rg.source, Recorder: rg.r.Recorder, Config: rg.r.Config,
	}
}

// seedCutShortMember makes a member of the hedge whose first member is first (""), for the need of pred, on Provider prov,
// through provisionWith, with its Allocation's create cut short by the armed cutter: the member's Instance exists with no
// Allocation, as a pass killed between the two leaves it. It returns the member's name.
func (rg *rig) seedCutShortMember(md, first, pred, prov string) string {
	rg.t.Helper()
	p := rg.passOf(md)
	var predIn *v1alpha1.Instance
	for _, in := range p.insts {
		if in.Name == pred {
			predIn = in
		}
	}
	if predIn == nil {
		rg.t.Fatalf("no predecessor %s", pred)
	}
	before := map[string]bool{}
	for _, in := range rg.instances(md) {
		before[in.Name] = true
	}
	if _, err := p.provisionWith(predIn, placeOpts{only: prov, hedgeGroup: first}); err == nil || !errorsIsKilled(err) {
		rg.t.Fatalf("seeding a cut-short member on %s: %v, want the cut create's error", prov, err)
	}
	for _, in := range rg.instances(md) {
		if !before[in.Name] {
			return in.Name
		}
	}
	rg.t.Fatalf("no new Instance of %s after seeding on %s", md, prov)
	return ""
}

func errorsIsKilled(err error) bool {
	for ; err != nil; err = unwrap(err) {
		if err == errHOKilled {
			return true
		}
	}
	return false
}

func unwrap(err error) error {
	if u, ok := err.(interface{ Unwrap() error }); ok {
		return u.Unwrap()
	}
	return nil
}

// TestTwoHedgeMembersWhoseInstancesBothExistRetryOlderFirst (#725's row 2, retitled to what it proves, A5 of #751; HBEP-7,
// Hedging (beta), Slots reserved atomically: once both members' Instances exist, the older, by creation time and then UID,
// is that Provider's member, and the newer waits, never cancelled for the race). P of D serves on A. X, the first member of
// P's replacement hedge, is made on B at 08:01:10 and Y, the hedge's second, at 08:01:20, each Instance with its Allocation
// cut short (no Allocation yet) while B's limit is 2. The limit is then lowered to 1, the second leader has a slot table of
// its own and reads through a cache that shows no Allocation made after the first leader's pass began. Each leader passes
// once, in both orders (the retry of an Allocation that two Instances wait for):
//   - X, the older, takes B's one slot: its Allocation is made;
//   - Y, the newer, finds it taken, counting X's own slot where X's Allocation is not shown: it waits, with D
//     SubmitLimitReached naming B;
//   - NEITHER Instance is ended or deleted, and B holds one Allocation, the limit, never two.
//
// With the limit raised to 2 again, Y's Allocation is made on the next pass: Y waited, it was never cancelled.
//
// It does NOT prove the window before both Instances exist: two leaders can each reserve B's last slot for a member whose
// Instance does not exist yet, and the younger may reach Slurm first. That race is HBEP-7's stated limit, for any hedge
// (the one extra creation of Per-user limits), bounded by the two rows below for a missing primary's hedge.
func TestTwoHedgeMembersWhoseInstancesBothExistRetryOlderFirst(t *testing.T) {
	for _, order := range []struct {
		name     string
		firstOne bool // the rig's own leader passes first
	}{
		{"the rig's leader passes first, the other leader second", true},
		{"the other leader passes first, the rig's leader second", false},
	} {
		t.Run(order.name, func(t *testing.T) {
			cut := &allocCutter{}
			h := newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b2", MaxSubmitPerUser: i32(2)}, withInterceptors(cut.funcs(interceptor.Funcs{})))
			h.batch.Enqueue(pfake.Script{})
			h.Step()
			d := mdSpec(hoMD)
			onBoth(d)
			h.create(d)
			for range 7 {
				tick10(h.rig) // to 08:01:10: P is ready on A
			}
			p := h.liveInstances(hoMD)[0].Name
			cut.arm(hoMD, 0)
			x := h.seedCutShortMember(hoMD, "", p, hedgeB) // 08:01:10
			h.clock.Advance(10 * time.Second)
			y := h.seedCutShortMember(hoMD, x, p, hedgeB) // 08:01:20
			cut.disarm()
			xi, yi := h.mustInstance(x), h.mustInstance(y)
			if !xi.CreationTimestamp.Time.Before(yi.CreationTimestamp.Time) {
				t.Fatalf("X created %v, Y %v, want X the older", xi.CreationTimestamp, yi.CreationTimestamp)
			}
			xAlloc, yAlloc := xi.Spec.AllocationRef.Name, yi.Spec.AllocationRef.Name
			if _, ok := h.allocation(xAlloc); ok {
				t.Fatal("X has its Allocation: the cut did not hold")
			}
			if _, ok := h.allocation(yAlloc); ok {
				t.Fatal("Y has its Allocation: the cut did not hold")
			}
			h.setSubmitLimit("example-b2", 1)
			other := h.newLeader()
			stale := newStaleReader(t, h.c) // shows no Allocation made from now on
			other.Reader = stale
			leaders := []*Reconciler{h.r, other}
			if !order.firstOne {
				leaders = []*Reconciler{other, h.r}
			}
			for _, r := range leaders {
				h.clock.Advance(10 * time.Second)
				if _, err := r.Reconcile(h.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}}); err != nil {
					t.Fatalf("reconcile: %v", err)
				}
			}
			if _, ok := h.allocation(xAlloc); !ok {
				t.Fatal("X, the older, has no Allocation: it takes the one slot")
			}
			if _, ok := h.allocation(yAlloc); ok {
				t.Fatal("Y, the newer, has its Allocation: B's one slot is X's")
			}
			if n := holdersOn(h.rig, hedgeB); n != 1 {
				t.Fatalf("B holds %d Allocations, want X's alone: its limit is 1", n)
			}
			for _, n := range []string{x, y} {
				if in := instOrNil(h.rig, n); in == nil || ended(in) || !in.DeletionTimestamp.IsZero() {
					t.Fatalf("member %s is %+v after the race, want it kept: a member that waits for the race is no loser", n, in)
				}
			}
			if c := h.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonSubmitLimitReached {
				t.Fatalf("D Degraded = %s %q, want SubmitLimitReached: Y waits for B's slot", c.Reason, c.Message)
			}
			// The slot comes: Y was never cancelled, so its Allocation is made.
			stale.catchUp(t)
			h.setSubmitLimit("example-b2", 2)
			h.clock.Advance(10 * time.Second)
			h.Step()
			if _, ok := h.allocation(yAlloc); !ok {
				t.Fatal("Y has no Allocation once the limit is 2: it waited for a slot")
			}
		})
	}
}

// TestTheOlderWaitingHedgeMemberTakesTheSlot is TestTheOlderWaitingReplacementTakesTheSlot's row for the two members of one
// replacement hedge on one Provider whose effective submit limit is 1, which only a leader hand-over race can make: X, the
// hedge's first member, and Y, which names X's hedge, both naming the primary P and both waiting for their Allocations. Two
// leaders, each with a slot table of its own, reserve one for each, in both orders. Written out by hand: the older counts
// nothing (0 < 1) and takes the slot, the newer counts the older's own slot (1) and waits. The creation times decide, with
// UIDs that run against them, and a tie on the time is broken by the UID.
func TestTheOlderWaitingHedgeMemberTakesTheSlot(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	member := func(name, hedge string, created time.Time, uid string) v1alpha1.Instance {
		ann := map[string]string{v1alpha1.AnnotationPredecessor: "p"}
		if hedge != "" {
			ann[v1alpha1.AnnotationHedgeGroup] = hedge
		}
		in := v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created),
				Annotations: ann, Labels: map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: hoMD}},
			Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}},
		}
		in.Status.Phase = v1alpha1.InstancePending
		return in
	}
	early, late := hoT(8, 0, 40), hoT(8, 0, 50)
	for _, c := range []struct {
		name           string
		older, younger v1alpha1.Instance
	}{
		{"by creation time", member("x", "", early, "uid-b"), member("y", "x", late, "uid-a")},
		{"a tie on creation time, broken by UID", member("x", "", early, "uid-a"), member("y", "x", early, "uid-b")},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := fixedInstances{insts: []v1alpha1.Instance{c.older, c.younger}, md: md}
			one := i32(1)
			for _, order := range [][2]v1alpha1.Instance{{c.older, c.younger}, {c.younger, c.older}} {
				leaderA, leaderB := newSlotTable(), newSlotTable()
				for i, in := range order {
					tab := leaderA
					if i == 1 {
						tab = leaderB
					}
					a, err := tab.reserveAsk(context.Background(), r, slotAsk{provider: rigProvider, key: rigNS + "/" + in.Spec.AllocationRef.Name, limit: one}, fixedAt(early))
					if err != nil {
						t.Fatal(err)
					}
					isOlder := in.Name == c.older.Name
					if a.ok != isOlder {
						t.Fatalf("%s (older %v) reserves: ok %v with a count of %d, want ok only for the older", in.Name, isOlder, a.ok, a.count)
					}
					if want := map[bool]int{true: 0, false: 1}[isOlder]; a.count != want {
						t.Fatalf("%s counts %d, want %d", in.Name, a.count, want)
					}
				}
			}
		})
	}
}

// fixedAllocs is fixedInstances that also shows the Allocations a test lists, as the Provider's Allocation list does.
type fixedAllocs struct {
	fixedInstances
	allocs []v1alpha1.Allocation
}

func (f fixedAllocs) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if al, ok := list.(*v1alpha1.AllocationList); ok {
		al.Items = f.allocs
		return nil
	}
	return f.fixedInstances.List(ctx, list, opts...)
}

// TestAMissingPrimarysHedgeRaceIsBoundedByTheLimitPlusOne (#725's R2, ruled a stated limit, HBEP-7 Hedging, Slots reserved
// atomically): the members of a MISSING PRIMARY's hedge name no predecessor, so they hold no slot of their own until
// their Allocations exist, and two leaders' slot tables, each knowing nothing of the other's, can both reserve. The
// race is bounded, by hand, at a limit of 1:
//   - x, the older member, asks on leader A's table: ok, a count of 0; y, the younger, asks on leader B's: ok, a count
//     of 0. Two reservations, the limit plus the one extra creation that Per-user limits accepts, never three;
//   - z, a third member, asked on either table, is refused with a count of 1: each table holds its own expectation, and
//     a table counts it, so a table never reserves a second past the limit;
//   - once x's Allocation is listed (a pending one holds its slot), a fresh table, a third leader's, refuses y with a
//     count of 1: past the race window, the older member's Allocation is what the count reads.
func TestAMissingPrimarysHedgeRaceIsBoundedByTheLimitPlusOne(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	member := func(name, hedge string, created time.Time, uid string) v1alpha1.Instance {
		ann := map[string]string{}
		if hedge != "" {
			ann[v1alpha1.AnnotationHedgeGroup] = hedge
		}
		in := v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created),
				Annotations: ann, Labels: map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: hoMD}},
			Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}},
		}
		in.Status.Phase = v1alpha1.InstancePending
		return in
	}
	early := hoT(8, 0, 40)
	r := fixedInstances{insts: []v1alpha1.Instance{member("x", "", early, "uid-a"), member("y", "x", hoT(8, 0, 50), "uid-b"), member("z", "x", hoT(8, 1, 0), "uid-c")}, md: md}
	one := i32(1)
	ask := func(tab *slotTable, rd client.Reader, name string) slotAnswer {
		t.Helper()
		a, err := tab.reserveAsk(context.Background(), rd, slotAsk{provider: rigProvider, key: rigNS + "/alloc-" + name, limit: one}, fixedAt(early))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	leaderA, leaderB := newSlotTable(), newSlotTable()
	if a := ask(leaderA, r, "x"); !a.ok || a.count != 0 {
		t.Fatalf("x on leader A: ok %v, count %d, want ok with a count of 0", a.ok, a.count)
	}
	if a := ask(leaderB, r, "y"); !a.ok || a.count != 0 {
		t.Fatalf("y on leader B: ok %v, count %d, want ok with a count of 0: the stated limit, one past the limit of 1", a.ok, a.count)
	}
	for name, tab := range map[string]*slotTable{"A": leaderA, "B": leaderB} {
		if a := ask(tab, r, "z"); a.ok || a.count != 1 {
			t.Fatalf("z on leader %s: ok %v, count %d, want refused with a count of 1: a third reservation would pass the bound", name, a.ok, a.count)
		}
	}
	listed := fixedAllocs{fixedInstances: r, allocs: []v1alpha1.Allocation{{
		ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "alloc-x"},
		Spec:       v1alpha1.AllocationSpec{ProviderConfigReference: providerRef(rigProvider)},
		Status:     v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: v1alpha1.AllocationPending}},
	}}}
	if a := ask(newSlotTable(), listed, "y"); a.ok || a.count != 1 {
		t.Fatalf("y on a fresh table once x's Allocation is listed: ok %v, count %d, want refused with a count of 1", a.ok, a.count)
	}
}

// TestAMissingPrimarysRefusedHedgeMemberWaitsAndEndsHedgeLostAtTheWin (the same R2 limit, its binding condition): of a
// missing primary's hedge, M1 on A and M2 on B, Slurm's submit limit on A is held by a job outside the framework, so A
// refuses M1's Allocation with QOSMaxSubmitJobPerUserLimit, code SubmitLimitReached, every minute. By hand:
//   - M1's Allocation stays Submitting with a Rejected record carrying that code, and M1's Instance is NOT ended,
//     cancelled or failed while the hedge has no winner: it waits;
//   - M2's job starts on B and M2 becomes ready: the hedge is resolved, and M1, a pending loser, ends with HedgeLost, its
//     pending Allocation cancelled;
//   - HedgeLost is no failure: M1 is no failedEnd, the class's streak has no end time, and the deployment's status keeps
//     one start outcome, M2's (Ready), none for M1's Allocation.
func TestAMissingPrimarysRefusedHedgeMemberWaitsAndEndsHedgeLostAtTheWin(t *testing.T) {
	h := newHedgeRig(t, withBatchConfig(oneJobUser))
	h.batch.AddOutsideJob("example-other-project", rigPartition, 1, 4*time.Hour)
	h.Step() // the Providers' first status
	md := mdSpec(hoMD)
	seededOnly(md)
	h.create(md)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: M1 on A
	firsts := h.onProvider(hoMD, rigProvider)
	if len(firsts) != 1 {
		t.Fatalf("%d Instances on A after the first pass, want the missing primary's one", len(firsts))
	}
	m1 := firsts[0].Name
	m1Alloc := firsts[0].Spec.AllocationRef.Name
	m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
	sawRefusal := false
	h.until(3*time.Minute, 10*time.Second, "M2 ready", func() bool {
		if a, ok := h.allocation(m1Alloc); ok {
			if sub := a.Status.AtProvider.Submission; sub != nil && sub.State == v1alpha1.SubmissionRejected && sub.Code == v1alpha1.ReasonSubmitLimitReached {
				sawRefusal = true
			}
		}
		if h.mustInstance(m2).Status.Phase == v1alpha1.InstanceReady {
			return true
		}
		if in := h.mustInstance(m1); ended(in) {
			t.Fatalf("M1 ended (%s) at %s, before the hedge had a winner: a member refused for the limit waits", in.Status.EndReason, h.clock.Now().Format(time.TimeOnly))
		}
		return false
	})
	if !sawRefusal {
		t.Fatal("M1's Allocation never showed a Rejected record with code SubmitLimitReached: the row proves nothing")
	}
	got := h.mustInstance(m1) // the pass in which M2 is ready resolved the hedge
	if !ended(got) || got.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("M1 is %s, ended for %q, want ended for HedgeLost", got.Status.Phase, got.Status.EndReason)
	}
	if !h.released(m1Alloc) {
		t.Fatalf("M1's pending Allocation %s was not cancelled", m1Alloc)
	}
	if failedEnd(got) {
		t.Fatal("M1's HedgeLost end counts as a failed end: a refused loser must be no failure")
	}
	if streak := h.passOf(hoMD).classStreak(h.passOf(hoMD).pvOf(got.Labels).class); len(streak) != 0 {
		t.Fatalf("the class's failure streak is %v, want none", streak)
	}
	for _, c := range h.md(hoMD).Status.StartOutcomes {
		for _, o := range c.Outcomes {
			if o.Result != "Ready" {
				t.Fatalf("a start outcome %+v is not M2's Ready one: M1's refused Allocation adds none", o)
			}
		}
	}
	var n int
	for _, c := range h.md(hoMD).Status.StartOutcomes {
		n += len(c.Outcomes)
	}
	if n != 1 {
		t.Fatalf("%d start outcomes, want M2's alone", n)
	}
}
