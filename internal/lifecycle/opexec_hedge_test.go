package lifecycle

// A hedge among an Executing Operation's committed targets gets no member, through the pass's own Operations (HBEP-7,
// Hedging (beta), Interactions, Operations; frozenByOperation wired as hedgeFrozen's default, hedge_grow.go), with no
// Reconciler.hedgeFrozen seam set. Once the Operation is finished the hedge grows as the hedge rule says.
//
// THE FREEZE IS UNREACHABLE END TO END WITH TODAY'S PLANNER. A Scale's committed hedge member is surplus once its spec lands, and
// a surplus hedge gets no member (hedge_grow.go: only a chosen primary's hedge grows); before the spec lands step 7 is held
// (opsHold) and adds nothing; an End shuts the deployment down, which grows nothing; and a Release holds no Instances. So
// frozenByOperation is HBEP-7's defensive rule, and this row proves its wiring in growHedges at pass level: the pass the rig
// builds (passOf) is given the Operations the Operations step would have loaded (p.ops), the same state, growth and clock in
// every case, and only the Operation differs.
//
// An earlier row ran a whole Scale through the rig and was VACUOUS: the mutant that returns false from hedgeFrozen survived it,
// with the baseline and the mutant both at 0 failed and 34 passed, because nothing adds a member there with or without the
// freeze. This row fails that mutant, and the one that ignores an open PartlyFailed Operation.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// An Operation of ops is committed to the hedge's first member, M1, a hedge that is due its member on B: the missing primary's
// hedge, with A's waits 10 h, so that B's member comes in the pass. While it is Executing the pass adds none; the same pass
// with the same Operation Done adds it.
func TestAHedgeAmongAnExecutingOperationsCommittedTargetsGetsNoMemberAndAFinishedOneReleasesIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase v1alpha1.OperationPhase
		done  bool // finishedAt is set
		want  int  // B's members of M1's hedge after the pass
	}{
		{"Executing: frozen", v1alpha1.OperationPhaseExecuting, false, 0},
		{"PartlyFailed and still retried: frozen", v1alpha1.OperationPhasePartlyFailed, false, 0},
		{"Done: the hedge grows", v1alpha1.OperationPhaseDone, true, 1},
		{"PartlyFailed and finished: the hedge grows", v1alpha1.OperationPhasePartlyFailed, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHedgeRig(t)
			// The first pass of the world adds no member either (the seam), so that M1 is the hedge's only member, due its next.
			h.r.hedgeFrozen = func(hedgeGroup) bool { return true }
			m1 := missingPrimary(t, h, 10*time.Hour, nil)
			if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 0 {
				t.Fatalf("B has %d members of M1's hedge before the pass", n)
			}
			h.r.hedgeFrozen = nil // the pass's own default, from its Operations
			p := h.passOf(hoMD)
			p.now = h.clock.Now().UTC().Truncate(time.Second)
			op := &v1alpha1.Operation{
				ObjectMeta: metav1.ObjectMeta{Name: "scale", Namespace: rigNS, UID: "uid-scale"},
				Spec:       v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: 1}},
				Status: v1alpha1.OperationStatus{Phase: tc.phase, Committed: []v1alpha1.CommittedTarget{
					{Kind: v1alpha1.TargetKindInstance, Name: m1, UID: h.mustInstance(m1).UID, Generation: 1, Effect: v1alpha1.PlanEffectCancelPending}}},
			}
			if tc.done {
				op.Status.FinishedAt = &metav1.Time{Time: p.now}
			}
			p.ops = []*v1alpha1.Operation{op}
			if err := p.growHedges(); err != nil {
				t.Fatal(err)
			}
			if n := len(h.membersOn(hoMD, m1, hedgeB)); n != tc.want {
				t.Errorf("B has %d members of M1's hedge after the pass with the Operation %s, want %d", n, tc.name, tc.want)
			}
		})
	}
}
