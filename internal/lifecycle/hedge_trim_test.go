package lifecycle

// What step 7 does with a surplus hedge (D9; the science agent's two rulings on #616 and #627, 17 Nov 2026): "Queue age
// is capacity in waiting. Where k allows, step 7 leaves the displaced pending unit as the surplus it may keep", and
// "the trim is per Provider, not all or nothing: a surplus hedge's member is kept where its Provider has room under k,
// taking one unit there; only the members whose Provider has no room are trimmed, as own trims beyond k; the whole
// hedge goes only when no member's Provider has room. A hedge left with one member is a hedge of one." The chosen
// primary's own hedge is never surplus, so its members are never trimmed.
//
// Every row is a population and the targets it gives, by hand ("Instance:effect:cause", least advanced first), never
// computed by scaleDownTargets, selectPrimaries or trimSurplusMembers. The selection's fixed order among non-ready
// surplus is the earlier job start, then the older creation time, then the UID; the rows set creation times apart, so
// no row depends on a UID. k is 1: one non-ready surplus Instance per Provider is left alone.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestASurplusHedgeIsTrimmedPerProvider: instances 1, R chosen and ready, and the surplus below. Providers are prov-a
// (the default) and prov-b.
func TestASurplusHedgeIsTrimmedPerProvider(t *testing.T) {
	t.Run("a member whose Provider has room is kept, the one whose Provider has none is cancelled alone", func(t *testing.T) {
		// S, pending on prov-b since 09:00, is the older surplus and takes prov-b's room. H, pending on prov-a, and its
		// member H-b on prov-b were made at 09:05 and 09:06. In the fixed order S comes first: prov-b has no room left
		// for H-b, which is cancelled with the trim cause. prov-a has room for H, which stays pending with its queue age.
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("S", v1alpha1.InstancePending, createdAt(hoT(9, 0, 0)), onProv("prov-b"))
		f.add("H", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		f.add("H-b", v1alpha1.InstancePending, createdAt(hoT(9, 6, 0)), inHedge("H"), onProv("prov-b"))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "H-b:cancelPending:overAllocationTrim")
	})
	t.Run("no room on either Provider: the second surplus hedge goes whole, the first is kept", func(t *testing.T) {
		// H (09:05) takes prov-a's room with H and prov-b's with H-b. H2 (09:10) and H2-b (09:11) find none on either:
		// both are cancelled. Charging room on the representative's Provider alone would leave prov-b's unspent and keep
		// H2-b.
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("H", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		f.add("H-b", v1alpha1.InstancePending, createdAt(hoT(9, 6, 0)), inHedge("H"), onProv("prov-b"))
		f.add("H2", v1alpha1.InstancePending, createdAt(hoT(9, 10, 0)))
		f.add("H2-b", v1alpha1.InstancePending, createdAt(hoT(9, 11, 0)), inHedge("H2"), onProv("prov-b"))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "H2:cancelPending:overAllocationTrim", "H2-b:cancelPending:overAllocationTrim")
	})
	t.Run("the chosen primary's own hedge stays whole whatever the room: older surplus holds both Providers' room", func(t *testing.T) {
		// C, started on prov-a at 10:05, and its hedge member C-b, pending on prov-b, are the chosen primary's own hedge:
		// a started Instance outranks the older pending ones. S-a and S-b, pending since 09:00 and 09:01, are surplus and
		// each left alone on its own Provider, using both rooms. Nothing is taken: the chosen hedge's members are never
		// trimmed.
		f := newSelFx()
		f.add("C", v1alpha1.InstanceStarting, createdAt(hoT(9, 20, 0)), jobStartedAt(hoT(10, 5, 0)))
		f.add("C-b", v1alpha1.InstancePending, createdAt(hoT(9, 21, 0)), inHedge("C"), onProv("prov-b"))
		f.add("S-a", v1alpha1.InstancePending, createdAt(hoT(9, 0, 0)))
		f.add("S-b", v1alpha1.InstancePending, createdAt(hoT(9, 1, 0)), onProv("prov-b"))
		p := f.pass(1, 1)
		sameNames(t, "primaries", p.primaries(), "C")
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0))
	})
	t.Run("a hedge left with one member is a hedge of one: the pass after the trim cancels nothing more", func(t *testing.T) {
		// The first row's population a pass later: H-b is gone, H is a hedge of one on prov-a, which has room for it.
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("S", v1alpha1.InstancePending, createdAt(hoT(9, 0, 0)), onProv("prov-b"))
		f.add("H", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0))
	})
}

// TestAHedgeDisplacedByAnOlderSurplusKeepsItsQueueAge: the queue-age ruling's row. instances 1. P served and had a
// replacement hedge, R on prov-a and R-b on prov-b, both pending since 09:30. S, a pending surplus Instance left alone
// by a lowered count, is older, 09:00. P has drained and ended, so the candidates are S and the replacement hedge, and S
// outranks it by age: S is chosen, and the hedge is surplus. With k = 1 neither of its members is cancelled: each is the
// only surplus on its own Provider. Before D9 the hedge went whole and lost its queue age.
func TestAHedgeDisplacedByAnOlderSurplusKeepsItsQueueAge(t *testing.T) {
	f := newSelFx()
	f.add("P", v1alpha1.InstanceEnded)
	f.add("S", v1alpha1.InstancePending, createdAt(hoT(9, 0, 0)))
	f.add("R", v1alpha1.InstancePending, createdAt(hoT(9, 30, 0)), replacementOf("P"))
	f.add("R-b", v1alpha1.InstancePending, createdAt(hoT(9, 31, 0)), replacementOf("P"), inHedge("R"), onProv("prov-b"))
	p := f.pass(1, 1)
	sameNames(t, "primaries", p.primaries(), "S")
	wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0))
}

// TestAHedgeWithAReadyMemberGoesWhole: a ready surplus is always taken, a hedge's included: the winner drains. The
// per-Provider trim concerns members that wait.
func TestAHedgeWithAReadyMemberGoesWhole(t *testing.T) {
	f := newSelFx()
	f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)))
	f.add("W", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)), createdAt(hoT(9, 5, 0)), readyAt(hoT(9, 40, 0)))
	f.add("W-b", v1alpha1.InstancePending, createdAt(hoT(9, 6, 0)), inHedge("W"), onProv("prov-b"))
	p := f.pass(1, 1)
	sameNames(t, "primaries", p.primaries(), "R")
	wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "W:drain:lowered", "W-b:cancelPending:lowered")
}
