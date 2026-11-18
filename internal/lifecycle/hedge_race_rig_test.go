package lifecycle

// HBEP-7's hedge slot rows that no merged test carried (Test plan, The hedge's slots, and #725's first two rows; #604
// PR 2): a hedge member and another deployment's new primary racing for a Provider's last slot, in both interleavings
// and in host-dialed mode, and two same-Provider members of one hedge made by two leaders. Every count is worked out by
// hand, as a literal, never by the slot code.

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
// forced by gates on the Allocation Lists of the passes (passGate): the pass that goes first waits at its gate until the
// other has arrived at its own, so the two meet, and the pass that goes second is released only when the first's has
// ended. Whichever reserves first creates, and the other is refused:
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
		rig  func(t *testing.T) *hedgeRig
	}{
		{"a submit limit of 2 on B", func(t *testing.T) *hedgeRig {
			return newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b2", MaxSubmitPerUser: i32(2)})
		}},
		{"host-dialed, 2 per Provider", func(t *testing.T) *hedgeRig { return newHedgeRig(t, withHostDialed()) }},
	} {
		for _, first := range []string{hoMD, "md-f"} {
			t.Run(mode.name+", "+first+" reserves first", func(t *testing.T) {
				h := mode.rig(t)
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

				second := map[string]string{hoMD: "md-f", "md-f": hoMD}[first]
				atGate := map[string]chan struct{}{hoMD: make(chan struct{}), "md-f": make(chan struct{})}
				ended := map[string]chan struct{}{hoMD: make(chan struct{}), "md-f": make(chan struct{})}
				g := &passGate{Reader: newStaleReader(t, h.c), hold: map[string]func() error{}, done: map[string]bool{}}
				g.hold[first] = func() error {
					close(atGate[first])
					return waitOn(t, atGate[second], second+"'s pass arriving at its gate")
				}
				g.hold[second] = func() error {
					close(atGate[second])
					return waitOn(t, ended[first], first+"'s pass ending")
				}
				h.r.Reader = g
				for i, err := range h.reconcileAtOnceThen(h.r, func(name string) { close(ended[name]) }, hoMD, "md-f") {
					if err != nil {
						t.Fatalf("reconcile %d: %v", i, err)
					}
				}
				for n, ch := range atGate {
					select {
					case <-ch:
					default:
						t.Fatalf("%s's pass never reached its gate: the two passes did not meet", n)
					}
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

// TestTwoMembersOfAHedgeOnOneProviderFromTwoLeaders (#725's row 2; HBEP-7, Hedging (beta), Slots reserved atomically: "Two
// members on one Provider, which only a leader hand-over race can make, are one too many. The older, by creation time and then
// UID, is that Provider's member, and the newer waits ... It is never cancelled for the race"). P of D serves on A. X, the first
// member of P's replacement hedge, is made on B at 08:00:40 by one leader, and Y, the hedge's second, at 08:00:50 by the
// other, each Instance with its Allocation cut short (no Allocation yet) while B's limit is 2. The limit is then lowered to
// 1, the slot table of each leader holds nothing of the other's, and the second leader reads through a cache that shows no
// Allocation made after the first leader's pass began. Each leader passes once, in both orders:
//   - X, the older, takes B's one slot: its Allocation is made;
//   - Y, the newer, finds it taken, counting X's own slot where X's Allocation is not shown: it waits, with D
//     SubmitLimitReached naming B;
//   - NEITHER Instance is ended or deleted, and B holds one Allocation, the limit, never two.
//
// With the limit raised to 2 again, Y's Allocation is made on the next pass: Y waited, it was never cancelled.
func TestTwoMembersOfAHedgeOnOneProviderFromTwoLeaders(t *testing.T) {
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
