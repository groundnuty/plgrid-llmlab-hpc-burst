package lifecycle

// The two level conditions of the kept-warm mark and the level rule of its finalizer (HBEP-10, The report, Kept warm: protect first),
// each read from the cluster's state, never from the order of writes that led to it:
//   - the mark is cleared when an Instance is placed on the Allocation, and when it hosts a live Instance that isn't ending while no
//     committed, unfinished Operation targets it (the serving pair of a Scale above zero);
//   - the deployment carries held-by-kept-warm exactly while it has an End with delete that is committed and not finished, or while it
//     is being deleted and one of its Allocations is marked and started, and the rule reads no Operation that is gone.

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestTheMarkIsClearedByPlacementAndByAServingPairAndByNothingElse(t *testing.T) {
	for _, tc := range []struct {
		name        string
		op          func() *v1alpha1.Operation // the marking Operation, nil when it is gone
		phase       v1alpha1.OperationPhase
		finished    bool
		committed   bool                   // the live Instance is a committed target of the marking Operation
		shutdown    bool                   // the deployment is in shutdown (HBEP-7, Reconcile loop, step 4)
		instPhase   v1alpha1.InstancePhase // "" for no live Instance
		wantCleared bool
	}{
		{"no live Instance: nothing placed, nothing serving", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhaseExecuting, false, true, false, "", false},
		{"its own Instance, ending, under an End that is unfinished", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhaseExecuting, false, true, false, v1alpha1.InstanceDraining, false},
		{"its own Instance, ending, under an End that is finished", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhasePartlyFailed, true, true, false, v1alpha1.InstanceDraining, false},
		{"its own Instance still serving under an Operation that is unfinished", func() *v1alpha1.Operation { return scaleOp(1) }, v1alpha1.OperationPhaseExecuting, false, true, false, v1alpha1.InstanceReady, false},
		{"its own Instance still serving under a PartlyFailed Operation that is not finished", func() *v1alpha1.Operation { return scaleOp(1) }, v1alpha1.OperationPhasePartlyFailed, false, true, false, v1alpha1.InstanceReady, false},
		{"its own Instance serving on once the Scale above zero has finished: cleared", func() *v1alpha1.Operation { return scaleOp(1) }, v1alpha1.OperationPhasePartlyFailed, true, true, false, v1alpha1.InstanceReady, true},
		{"its own Instance still starting once the Scale above zero has finished: cleared", func() *v1alpha1.Operation { return scaleOp(1) }, v1alpha1.OperationPhasePartlyFailed, true, true, false, v1alpha1.InstanceStarting, true},
		{"the same, but the deployment is in shutdown: the spec keeps nothing, the mark stays", func() *v1alpha1.Operation { return scaleOp(1) }, v1alpha1.OperationPhasePartlyFailed, true, true, true, v1alpha1.InstanceReady, false},
		{"an Instance placed on it that the Operation did not commit to, though the Operation is unfinished: cleared", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhaseExecuting, false, false, false, v1alpha1.InstanceStarting, true},
		{"an Instance placed on it that is already ending is not a placement: the mark stays", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhaseExecuting, false, false, false, v1alpha1.InstanceDraining, false},
		{"the marking Operation is gone and a live Instance serves on: cleared", nil, "", false, true, false, v1alpha1.InstanceReady, true},
		{"the marking Operation is gone, finished and removed, and its Instance is draining: the mark stays", nil, "", false, true, false, v1alpha1.InstanceDraining, false},
		{"the marking Operation is gone and the deployment is in shutdown: the mark stays", nil, "", false, true, true, v1alpha1.InstanceReady, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &v1alpha1.Allocation{}
			a.Name, a.UID = "alloc-p", types.UID("uid-alloc-p")
			a.Status.AtProvider.StartTime = opTime(opT0)
			md := &v1alpha1.ModelDeployment{}
			md.Name, md.UID = opMD, "uid-md"
			md.Spec.Instances = 1
			p := &pass{md: md, allocs: map[string]*v1alpha1.Allocation{"alloc-p": a}, shutdown: tc.shutdown}
			const opUID = types.UID("9c4f9840-fe6c-3951-a310-dd5d74680a7b") // the release-cause form takes a UUID
			mark := v1alpha1.ReleaseCauseOperation(opNS, "op", opUID)
			a.Annotations = map[string]string{v1alpha1.AnnotationKeptWarm: mark}
			in := &v1alpha1.Instance{}
			in.Name, in.UID = "in-p", types.UID("uid-in-p")
			in.Spec.AllocationRef.Name = "alloc-p"
			if tc.op != nil {
				var committed []v1alpha1.CommittedTarget
				committed = append(committed, committedOf(v1alpha1.TargetKindAllocation, "alloc-p", v1alpha1.PlanEffectCancelPending))
				if tc.committed {
					committed = append(committed, v1alpha1.CommittedTarget{Kind: v1alpha1.TargetKindInstance, Name: "in-p", UID: in.UID, Effect: v1alpha1.PlanEffectCancelPending})
				}
				op := executingOp("op", tc.op(), committed)
				op.UID = opUID
				op.Status.Phase = tc.phase
				if tc.finished {
					now := metav1.Now()
					op.Status.FinishedAt = &now
				}
				p.ops = []*v1alpha1.Operation{op}
			}
			if tc.instPhase != "" {
				in.Status.Phase = tc.instPhase
				p.insts = []*v1alpha1.Instance{in}
			}
			if got := p.markCleared(a); got != tc.wantCleared {
				t.Errorf("markCleared = %v, want %v", got, tc.wantCleared)
			}
		})
	}
}

func TestTheKeptWarmFinalizerIsALevelRuleThatReadsNoOperationThatIsGone(t *testing.T) {
	for _, tc := range []struct {
		name      string
		op        func() *v1alpha1.Operation // nil: no Operation
		phase     v1alpha1.OperationPhase
		finished  bool
		deleting  bool
		started   bool
		marked    bool
		allocGone bool
		want      bool
	}{
		{"an End with delete, committed and unfinished, on a live deployment", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhaseExecuting, false, false, false, false, false, true},
		{"the same, PartlyFailed and not finished", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhasePartlyFailed, false, false, false, false, false, true},
		{"the same, while the deployment is being deleted with nothing marked: held until the End finishes", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhaseExecuting, false, true, false, false, false, true},
		{"an End with delete that finished without deleting leaves no finalizer", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhasePartlyFailed, true, false, false, false, false, false},
		{"an End with delete that is only planned never holds", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhasePlanned, false, false, false, false, false, false},
		{"an End without delete holds nothing", func() *v1alpha1.Operation { return endOp(false) }, v1alpha1.OperationPhaseExecuting, false, false, false, false, false, false},
		{"a finished End with delete, the deployment being deleted, a marked and started Allocation: held", func() *v1alpha1.Operation { return endOp(true) }, v1alpha1.OperationPhasePartlyFailed, true, true, true, true, false, true},
		{"the same with no Operation at all, it is gone: still held, the rule reads no Operation", nil, "", false, true, true, true, false, true},
		{"a marked Allocation whose job is still pending does not hold", nil, "", false, true, false, true, false, false},
		{"a started Allocation with no mark does not hold", nil, "", false, true, true, false, false, false},
		{"a marked and started Allocation that is itself going does not hold", nil, "", false, true, true, true, true, false},
		{"a marked and started Allocation on a LIVE deployment holds nothing: nothing is deleting it", nil, "", false, false, true, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := &v1alpha1.ModelDeployment{}
			md.Name, md.UID = opMD, "uid-md"
			if tc.deleting {
				now := metav1.Now()
				md.DeletionTimestamp = &now
			}
			a := &v1alpha1.Allocation{}
			a.Name, a.UID = "alloc-p", types.UID("uid-alloc-p")
			if tc.started {
				a.Status.AtProvider.StartTime = opTime(opT0)
			}
			if tc.marked {
				a.Annotations = map[string]string{v1alpha1.AnnotationKeptWarm: v1alpha1.ReleaseCauseOperation(opNS, "op", "uid-op")}
			}
			if tc.allocGone {
				now := metav1.Now()
				a.DeletionTimestamp = &now
			}
			p := &pass{md: md, allocs: map[string]*v1alpha1.Allocation{"alloc-p": a}}
			if tc.op != nil {
				op := executingOp("op", tc.op(), []v1alpha1.CommittedTarget{committedOf(v1alpha1.TargetKindAllocation, "alloc-p", v1alpha1.PlanEffectCancelPending)})
				op.Status.Phase = tc.phase
				op.Status.Plan.ModelDeployment.UID = md.UID
				if tc.finished {
					now := metav1.Now()
					op.Status.FinishedAt = &now
				}
				p.ops = []*v1alpha1.Operation{op}
			}
			if got := p.keptWarmFinalizerWanted(); got != tc.want {
				t.Errorf("keptWarmFinalizerWanted = %v, want %v", got, tc.want)
			}
		})
	}
}

// From an Operation's commit until its spec write lands the controller creates no Allocation and no Instance (HBEP-7, Reconcile loop,
// steps 7 and 8): the guard reads the committed, unfinished Operations' SpecWritten condition, and an unknown commit holds too.
func TestCreationIsHeldFromTheCommitUntilTheSpecWriteLands(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       v1alpha1.OperationPhase
		finished    bool
		specWritten bool
		unknown     bool
		none        bool
		want        bool
	}{
		{"no Operation", "", false, false, false, true, false},
		{"committed, the spec not written", v1alpha1.OperationPhaseExecuting, false, false, false, false, true},
		{"committed, the spec written", v1alpha1.OperationPhaseExecuting, false, true, false, false, false},
		{"PartlyFailed and not finished, the spec not written", v1alpha1.OperationPhasePartlyFailed, false, false, false, false, true},
		{"finished, the spec not written", v1alpha1.OperationPhasePartlyFailed, true, false, false, false, false},
		{"only planned", v1alpha1.OperationPhasePlanned, false, false, false, false, false},
		{"a commit of this pass that may or may not have landed", "", false, false, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pass{commitUnknown: tc.unknown}
			if !tc.none {
				op := executingOp("op", scaleOp(1), nil)
				op.Status.Phase = tc.phase
				if tc.finished {
					now := metav1.Now()
					op.Status.FinishedAt = &now
				}
				if tc.specWritten {
					op.Status.Conditions = append(op.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionSpecWritten, Status: metav1.ConditionTrue, Reason: "Written"})
				}
				p.ops = []*v1alpha1.Operation{op}
			}
			if got := p.creationHeld(); got != tc.want {
				t.Errorf("creationHeld = %v, want %v", got, tc.want)
			}
		})
	}
}
