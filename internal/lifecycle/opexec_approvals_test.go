package lifecycle

// Approval, staleness and order on the scenario rig (#589, PR 5): an Operation turns Stale when what it was planned against
// changed, two Operations on one deployment run one at a time, one name gives one execution, and the owner's Sano example
// runs end to end.

import (
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// Whatever changed in what the plan was made against, between the approval and the execution, the Operation turns Stale:
// nothing is written to the ModelDeployment, the Allocations or the Instances by the Operation, a PlanStale event goes
// out, and an approval added afterwards changes nothing.
func TestAnApprovedOperationTurnsStaleWhenWhatItWasPlannedAgainstChanged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		queue  time.Duration
		change func(t *testing.T, w *execWorld)
	}{
		{"the pending Allocation started", 30 * time.Minute, func(t *testing.T, w *execWorld) {
			w.slow[w.pendingAlloc] = true // its Instance stays starting: the Allocation has started and nothing else has
			w.until(time.Hour, 30*time.Second, "P's job started", func() bool {
				a, ok := w.allocation(w.pendingAlloc)
				return ok && a.Status.AtProvider.StartTime != nil
			})
			if in := w.instance(w.pending); in.Status.Phase == v1alpha1.InstanceReady {
				t.Fatal("P is ready: this case changes only the Allocation")
			}
		}},
		{"another Instance became ready", 30 * time.Minute, func(t *testing.T, w *execWorld) {
			w.until(time.Hour, 30*time.Second, "both Instances ready", func() bool { return w.readyCount(hoMD) == 2 })
		}},
		{"the policy that applies was edited", 10 * time.Hour, func(t *testing.T, w *execWorld) {
			// The person stays an approver, so the approval still counts: only the policy's generation moved.
			var pol v1alpha1.ApprovalPolicy
			if err := w.c.Get(w.ctx, client.ObjectKey{Name: "global"}, &pol); err != nil {
				t.Fatal(err)
			}
			pol.Spec.Approvers = append(pol.Spec.Approvers, v1alpha1.Approver{Group: "operators"})
			pol.Generation++
			if err := w.c.Update(w.ctx, &pol); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newExecWorld(t, tc.queue)
			w.plan("scale", scaleTo(1))
			w.confirm("scale")
			w.ticks(2)
			if got := w.getOp("scale").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
				t.Fatalf("phase %q before the change, want AwaitingApproval", got)
			}
			tc.change(t, w)
			if got := w.getOp("scale").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
				t.Fatalf("phase %q after the change and before the approval: the controller does not plan again", got)
			}
			specBefore := w.md(hoMD).Spec.Instances

			var mu sync.Mutex
			var made []string
			w.fc.set(nil, func(method string, obj client.Object) {
				mu.Lock()
				defer mu.Unlock()
				switch obj.(type) {
				case *v1alpha1.Operation:
				default:
					if !strings.HasPrefix(method, "Status.") {
						made = append(made, method+" "+obj.GetName())
					}
				}
			})
			w.approve("scale", "person", userPerson)
			w.tick()
			op := w.getOp("scale")
			if op.Status.Phase != v1alpha1.OperationPhaseStale {
				t.Fatalf("phase %q, want Stale", op.Status.Phase)
			}
			mu.Lock()
			if len(made) != 0 {
				t.Errorf("the pass wrote %v: a stale Operation writes nothing but its own status", made)
			}
			mu.Unlock()
			if len(op.Status.Committed) != 0 || len(op.Status.Targets) != 0 || op.Status.FinishedAt == nil {
				t.Errorf("committed %d, targets %d, finishedAt %v: a stale Operation commits nothing and is finished", len(op.Status.Committed), len(op.Status.Targets), op.Status.FinishedAt)
			}
			if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonPlanStale {
				t.Errorf("Executable %+v, want False/PlanStale", c)
			}
			if got := w.md(hoMD).Spec.Instances; got != specBefore {
				t.Errorf("spec.instances went from %d to %d", specBefore, got)
			}
			if _, ok := w.allocation(w.pendingAlloc); !ok {
				t.Error("P's Allocation was released")
			}
			stale := 0
			for _, e := range w.rec.Events() {
				if e.Code == v1alpha1.ReasonPlanStale {
					stale++
					if e.Type != "degraded" || !e.Warning || e.Kind != "*v1alpha1.Operation" || e.Name != "scale" || !strings.Contains(e.Message, "stale") {
						t.Errorf("the PlanStale event is %+v, want a degraded Warning regarding Operation scale", e)
					}
				}
			}
			if stale != 1 {
				t.Errorf("%d PlanStale events, want one", stale)
			}
			// An approval added afterwards changes nothing.
			rv := op.ResourceVersion
			w.approve("scale", "late", userPerson)
			w.ticks(3)
			if got := w.getOp("scale"); got.Status.Phase != v1alpha1.OperationPhaseStale || got.ResourceVersion != rv || len(got.Status.Committed) != 0 {
				t.Errorf("after a later approval: phase %q, resourceVersion %s -> %s, committed %d: it changes nothing", got.Status.Phase, rv, got.ResourceVersion, len(got.Status.Committed))
			}
			if got := w.md(hoMD).Spec.Instances; got != specBefore {
				t.Errorf("spec.instances went from %d to %d after a later approval", specBefore, got)
			}
		})
	}
}

// Two confirmed Operations on one deployment run one at a time: the second waits while the first is Executing, and is
// then checked, and turns Stale, since the first changed the state: the count it would lower to is no longer below the
// deployment's. Each effect happens once.
func TestTwoConfirmedOperationsRunOneAtATimeAndTheSecondTurnsStale(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("op-1", scaleTo(1))
	w.plan("op-2", scaleTo(1))
	w.confirm("op-1")
	w.confirm("op-2")
	w.ticks(2)
	w.approve("op-1", "person", userPerson)
	w.approve("op-2", "person", userPerson)
	sbatches := w.submissions()

	w.tick() // the first commits and acts; P's Allocation is being deleted, held by its finalizer
	first, second := w.getOp("op-1"), w.getOp("op-2")
	if first.Status.Phase != v1alpha1.OperationPhaseExecuting || len(first.Status.Committed) == 0 {
		t.Fatalf("op-1 is %q with %d committed targets, want Executing", first.Status.Phase, len(first.Status.Committed))
	}
	if second.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || len(second.Status.Committed) != 0 {
		t.Fatalf("op-2 is %q with %d committed targets, want it waiting in AwaitingApproval", second.Status.Phase, len(second.Status.Committed))
	}
	if c := condOf(second, v1alpha1.ConditionExecutable); c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonAnotherOperationExecuting || !strings.Contains(c.Message, "op-1") {
		t.Errorf("op-2's Executable is %+v, want False/AnotherOperationExecuting naming op-1", c)
	}

	w.tick() // the Allocation is gone: op-1 is done, and op-2 is checked
	first, second = w.getOp("op-1"), w.getOp("op-2")
	if first.Status.Phase != v1alpha1.OperationPhaseDone {
		t.Fatalf("op-1 is %q, want Done", first.Status.Phase)
	}
	if second.Status.Phase != v1alpha1.OperationPhaseStale || len(second.Status.Committed) != 0 {
		t.Fatalf("op-2 is %q with %d committed targets, want Stale with none", second.Status.Phase, len(second.Status.Committed))
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want the first's 1", got)
	}
	if w.submissions() != sbatches {
		t.Error("a submission was made")
	}
	if got := w.batch.Count("scancel"); got != 1 {
		t.Errorf("the fake Slurm saw %d scancel calls, want one: the effect happened once", got)
	}
	stale := 0
	for _, e := range w.rec.Events() {
		if e.Code == v1alpha1.ReasonPlanStale && e.Name == "op-2" {
			stale++
		}
	}
	if stale != 1 {
		t.Errorf("%d PlanStale events for op-2, want one", stale)
	}
}

// Creating an Operation, then creating it again under the same name, gives one Operation and one execution.
func TestCreatingAnOperationTwiceGivesOneOperationAndOneExecution(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("scale", scaleTo(1))
	again := opRigOp("scale", scaleTo(1))
	if err := w.c.Create(w.ctx, again); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("the second create: %v, want AlreadyExists", err)
	}
	var l v1alpha1.OperationList
	if err := w.c.List(w.ctx, &l, client.InNamespace(rigNS)); err != nil || len(l.Items) != 1 {
		t.Fatalf("%d Operations (%v), want one", len(l.Items), err)
	}
	w.confirm("scale")
	w.ticks(2)
	w.approve("scale", "person", userPerson)
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	w.ticks(5)
	if got := w.batch.Count("scancel"); got != 1 {
		t.Errorf("%d scancel calls, want one: one Operation, one execution", got)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}
}

// The owner's Sano example, end to end (HBEP-10, Approval policy): a Provider policy names agent A for Provider P. A's
// Scale on P runs on A's own approval; A's Scale on Q waits for the person, since the global policy names the person alone,
// and runs on the person's approval. Another agent's approval, B's, counts under neither policy. The requester's own
// approval alone would prove nothing, so it is tested on both Providers.
func TestTheOwnersSanoExampleEndToEnd(t *testing.T) {
	const qMD = "md-q"
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}
	w := newExecWorld(t, 10*time.Hour)
	// Provider P is A (the rig's helios-gpu), whose policy names agent A; Provider Q is B, under the global policy.
	provP := providerPolicy(rigProvider, agentA)
	w.create(&provP)
	w.b.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	q := noOverAllocation(mdSpec(qMD))
	q.Spec.Instances = 2
	q.Spec.Providers.Names = []string{hedgeB}
	w.create(q)
	w.until(5*time.Minute, 10*time.Second, "Q's Instances", func() bool { return len(w.liveInstances(qMD)) == 2 && w.readyCount(qMD) == 1 })

	// A's Scale on P: the targets are on Provider P, under its policy, which names A.
	w.plan("scale-p", scaleTo(1))
	w.confirm("scale-p")
	w.ticks(2)
	if got := w.getOp("scale-p").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
		t.Fatalf("A's Scale on P is %q, want AwaitingApproval until it is approved", got)
	}
	w.approve("scale-p", "agent-a", userAgentA)
	w.untilPhase("scale-p", v1alpha1.OperationPhaseDone)
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("after A's own approval, P's deployment has %d Instances asked, want 1", got)
	}

	// A's Scale on Q: A's own approval, and B's, count under no policy of Q.
	w.planOn(qMD, "scale-q", scaleTo(1))
	w.confirm("scale-q")
	w.ticks(2)
	w.approve("scale-q", "agent-a", userAgentA)
	w.approve("scale-q", "agent-b", userAgentB)
	w.ticks(5)
	if op := w.getOp("scale-q"); op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || len(op.Status.Committed) != 0 {
		t.Fatalf("A's Scale on Q is %q with %d committed targets after A's and B's approvals, want it waiting for the person", op.Status.Phase, len(op.Status.Committed))
	}
	if got := w.md(qMD).Spec.Instances; got != 2 {
		t.Errorf("Q's deployment has %d Instances asked, want 2 while it waits", got)
	}
	// The person's approval runs it.
	w.approve("scale-q", "person", userPerson)
	w.untilPhase("scale-q", v1alpha1.OperationPhaseDone)
	if got := w.md(qMD).Spec.Instances; got != 1 {
		t.Errorf("after the person's approval, Q's deployment has %d Instances asked, want 1", got)
	}
}
