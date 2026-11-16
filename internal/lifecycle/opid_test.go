package lifecycle

// The plan's ID, by properties only (#589, PR 4, AC 4). No literal hash is written anywhere: the ID is
// opaque, so a test says only what must and must not move it.

import (
	"regexp"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// idFixture is a deployment of two Instances, one ready and one that has not started, with the global
// policy: Scale(1) takes the pending one.
func idFixture(t *testing.T) *opFixture {
	f := newOpFixture(t, 2)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	return f
}

func mustPlan(t *testing.T, f *opFixture, at time.Time) *v1alpha1.OperationPlan {
	t.Helper()
	plan, refusal := f.pass().planOperation(f.request(scaleOp(1), at))
	if refusal != "" {
		t.Fatalf("refused: %s", refusal)
	}
	return plan
}

func TestThePlanIDIsSixteenHexCharacters(t *testing.T) {
	if id := mustPlan(t, idFixture(t), opAt).ID; !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(id) {
		t.Errorf("ID %q is not 16 lowercase hex characters", id)
	}
}

func TestTheSameStateGivesTheSameID(t *testing.T) {
	a := mustPlan(t, idFixture(t), opAt)
	b := mustPlan(t, idFixture(t), opAt)
	if a.ID != b.ID {
		t.Errorf("one state, two IDs: %s and %s", a.ID, b.ID)
	}
	// Another observation time of the same state: times are left out of the ID.
	if c := mustPlan(t, idFixture(t), opAt.Add(7*time.Minute)); c.ID != a.ID {
		t.Errorf("the plan's time moved the ID: %s and %s", a.ID, c.ID)
	}
	// The order the Instances were listed in is not part of the state.
	f := idFixture(t)
	f.insts[0], f.insts[1] = f.insts[1], f.insts[0]
	if d := mustPlan(t, f, opAt); d.ID != a.ID {
		t.Errorf("the list order moved the ID: %s and %s", a.ID, d.ID)
	}
}

func TestAChangeOfStateGivesAnotherID(t *testing.T) {
	base := mustPlan(t, idFixture(t), opAt).ID
	for _, tc := range []struct {
		name   string
		mutate func(*opFixture)
	}{
		{"a pending Allocation started", func(f *opFixture) {
			a := f.allocs["alloc-p"]
			a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
			a.Status.AtProvider.StartTime = opTime(opT0)
			a.Status.AtProvider.HardEnd = opTime(opHardEnd)
		}},
		{"another Instance became ready", func(f *opFixture) {
			f.insts[1].Status.Phase = v1alpha1.InstanceReady
			f.insts[1].Status.LeaseEnd = opTime(opHardEnd.Add(time.Hour))
			f.allocs["alloc-p"].Status.AtProvider.StartTime = opTime(opT0)
		}},
		{"the global policy was edited (its generation moved)", func(f *opFixture) { f.policies[0].Generation++ }},
		{"the global policy was replaced (its UID is another)", func(f *opFixture) { f.policies[0].UID = "uid-global-2" }},
		{"the ModelDeployment's spec changed (its generation moved)", func(f *opFixture) { f.md.Generation++ }},
		{"the ModelDeployment was deleted and created again (its UID)", func(f *opFixture) { f.md.UID = "uid-md-2" }},
		{"the Allocation was replaced (its UID)", func(f *opFixture) { f.allocs["alloc-p"].UID = "uid-alloc-p-2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := idFixture(t)
			tc.mutate(f)
			if got := mustPlan(t, f, opAt).ID; got == base {
				t.Errorf("the ID did not move: %s", got)
			}
		})
	}
}

// A new queue wait, a new start-to-ready, a moved hard end of a target the plan keeps: a cost input changes
// the figures, not the ID.
func TestOnlyACostInputChangedKeepsTheID(t *testing.T) {
	base := mustPlan(t, idFixture(t), opAt)
	f := idFixture(t)
	f.pvs["sano"].prov.Status.QueueWaits = waitsOf(5, 9)
	f.pvs["sano"].prov.Status.LastSuccessfulStatusRead = opTime(opAt)
	f.md.Status.StartToReady = []v1alpha1.ClassStartToReady{{AcceleratorClass: f.pvs["sano"].class, Duration: "12m0s", MeasuredTime: metav1.NewTime(opT0)}}
	f.allocs["alloc-p"].Status.AtProvider.SubmitTime = opTime(opT0.Add(20 * time.Minute)) // a queue age that differs
	changed := mustPlan(t, f, opAt)
	if changed.ID != base.ID {
		t.Fatalf("a cost input moved the ID: %s and %s", base.ID, changed.ID)
	}
	if sameFigures(base, changed) {
		t.Error("the figures should differ: a queue wait, a start-to-ready and a submit time changed")
	}
}

// The backstop of confirmedOperation recomputes the ID from the plan it sits on.
func TestThePlanIDIsTheHashOfThePlanItSitsOn(t *testing.T) {
	plan := mustPlan(t, idFixture(t), opAt)
	if got := planID(v1alpha1.OperationActionScale, plan.ModelDeployment, plan.Targets); got != plan.ID {
		t.Fatalf("recomputing gives %s, the plan holds %s", got, plan.ID)
	}
	forged := plan.DeepCopy()
	forged.Targets[0].Effect = v1alpha1.PlanEffectKeep
	if planID(v1alpha1.OperationActionScale, forged.ModelDeployment, forged.Targets) == plan.ID {
		t.Error("an edited plan keeps its ID")
	}
	if planID(v1alpha1.OperationActionEnd, plan.ModelDeployment, plan.Targets) == plan.ID {
		t.Error("another action keeps the ID")
	}
}
