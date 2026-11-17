package lifecycle

// A warm-up that fails on one worker of an Instance of several, before the Instance's first ready (HBEP-7,
// Several workers, "Verification, qualification and warm-up are per worker"; Release; issue #672). The branch is
// workers_start.go's callback to warmWorkers ("Before the first ready, as with one worker: the Instance fails"),
// which no other row reaches: the other QualificationFailed writes are the Instance's own verdicts
// (check_conditions_test.go), a worker's failed qualification (workers_start_test.go) and the throughput gate's
// second miss (gate_workers_test.go, whose failure goes through failGate, not through this callback).

import (
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// warmUpFailureText is the error the engine double gives the failed request. It is a plain sentence of the row's
// own, so that the Instance's WarmedUp message can be told to carry the engine's words and not a code's.
const warmUpFailureText = "the engine answered 500"

// TestAWorkersFailedWarmUpBeforeTheFirstReadyFailsTheInstance runs an Instance of two workers. Both workers
// warm up in lockstep, one round a Step, worker-0 asked first (the same order the rows in gate_workers_test.go
// and TestOneWorkersFailedQualificationFailsTheInstance rely on). Round 1 is the default one for both. In round 2
// worker-0's round is the default again, so it settles against round 1 and is warmed up, and worker-1's first
// request fails. warmWorkers judges worker-0 first in the same pass and marks it warmed up, then judges worker-1
// and fails the Instance. The expected values are each rule's own:
//   - the Instance ends QualificationFailed, never ready, with its WarmedUp condition False/Failed and the message
//     naming worker-1 and the engine's words, and Verified and Qualified left True: it is the warm-up's failure,
//     not a verification's, not a qualification's (workers_start.go's other failCheck call) and not the gate's
//     (status.gate stays nil, status.restarts empty: nothing restarted a worker);
//   - the start-up outcome is a failure (outcomes.go: an Instance that ends before ready because its
//     qualification or warm-up failed is a failure), carrying the reason QualificationFailed and no gate verdict,
//     the class's counts 1 failure, 1 qualification failure of 1 outcome;
//   - the Allocation is released at once, in the very pass that ends the Instance (read with no further Step: a
//     release left to the next pass's tombstone, which deletes with the same cause, must fail the row), with the
//     failure's reason as its cause (HBEP-7, Release: "An Allocation whose Instance failed serves nothing, so it
//     is released at once"; failNoted ends the Instance with release set, and deleteAllocation writes the cause
//     first, endCause: "Instance <name> failed: <reason>"). It is not kept under its retention policy, which an
//     Allocation left empty by an ordinary end follows.
func TestAWorkersFailedWarmUpBeforeTheFirstReadyFailsTheInstance(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	var failedShape agentapi.Shape
	rg.engine = func(string) afake.Engine {
		calls := map[int32]int{}
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			round := afake.DefaultWarmUpRound(n, req)
			worker := calls[n] // the first call of a round is worker-0's, the second worker-1's
			calls[n]++
			if n == 2 && worker == 1 {
				round.Requests[0].Error = warmUpFailureText
				failedShape = round.Requests[0].Shape
			}
			return round
		}}
	}
	rg.create(withWorkers(mdSpec("md"), 2))
	startUntil(rg, "md", 2*time.Minute, "an Instance and its Allocation", func() bool {
		ins := rg.instances("md")
		if len(ins) == 0 {
			return false
		}
		_, ok := rg.allocation(ins[0].Spec.AllocationRef.Name)
		return ok
	})
	first := rg.instances("md")[0]
	name, allocName := first.Name, first.Spec.AllocationRef.Name
	alloc, _ := rg.allocation(allocName)
	allocUID := alloc.UID
	if w := first.Spec.Launch.Workers; w == nil || *w != 2 || first.Status.ReadyTime != nil {
		t.Fatalf("the Instance's launch.workers %s and ready time %v at its creation, want 2 workers and not ready: the row needs a two-worker Instance still starting", countOf(first.Spec.Launch.Workers), first.Status.ReadyTime)
	}

	startUntil(rg, "md", 20*time.Minute, "the Instance ended", func() bool { return ended(rg.instance(name)) })
	got := rg.instance(name)

	// The release (HBEP-7, Release): at once, so it is read here, with no Step and no wait after the pass that ended
	// the Instance, and nothing below steps before the outcome's wait. end() deletes the Allocation in that pass. A
	// wait would let a release the ending pass skipped pass anyway: the next pass's tombstone deletes the Allocation
	// of a failed Instance with the same cause (instances.go). The cause is the failure's reason, which the store
	// held when the delete was made.
	if cause, deleted := log.deletedWith(allocName); !deleted {
		t.Errorf("the Allocation was not deleted in the pass that ended the Instance, want it released at once: a delete made by a later pass is not at once")
	} else if want := "Instance " + name + " failed: QualificationFailed"; cause != want {
		t.Errorf("the Allocation was deleted with the cause %q in the store, want it written first: %s", cause, want)
	}

	// What ended it.
	if got.Status.EndReason != v1alpha1.ReasonQualificationFailed || got.Status.ReadyTime != nil {
		t.Fatalf("end reason %q, ready %v, want QualificationFailed and never ready", got.Status.EndReason, got.Status.ReadyTime)
	}
	if len(got.Status.Workers) != 2 {
		t.Errorf("status.workers %+v, want the two workers' entries: not an Instance of one", got.Status.Workers)
	}
	if failedShape == "" {
		t.Fatal("the engine double failed no request: the Instance ended for another reason")
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionWarmedUp)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonFailed {
		t.Fatalf("WarmedUp is %+v, want False/Failed", c)
	}
	for _, want := range []string{"worker-1: ", "warm-up round 2", warmUpFailureText, string(failedShape)} {
		if !strings.Contains(c.Message, want) {
			t.Errorf("WarmedUp's message %q does not hold %q", c.Message, want)
		}
	}
	if strings.Contains(c.Message, "worker-0") {
		t.Errorf("WarmedUp's message %q blames worker-0, which warmed up", c.Message)
	}
	for _, typ := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified} {
		if !isTrue(got.Status.Conditions, typ) {
			t.Errorf("%s is %+v after the warm-up's failure, want True: it is the warm-up that failed", typ, apimeta.FindStatusCondition(got.Status.Conditions, typ))
		}
	}
	if !workerWarmed(got, "worker-0") || workerWarmed(got, "worker-1") {
		t.Errorf("worker entries %+v, want worker-0 warmed up (it settled in round 2) and worker-1 not", got.Status.Workers)
	}
	if got.Status.Gate != nil || len(got.Status.Restarts) != 0 {
		t.Errorf("status.gate %+v, restarts %+v, want neither: nothing but the warm-up's failure acted", got.Status.Gate, got.Status.Restarts)
	}
	var events int
	for _, e := range rg.rec.Events() {
		if e.Kind == "*v1alpha1.Instance" && e.Name == name && e.Code == v1alpha1.ReasonQualificationFailed {
			events++
		}
	}
	if events != 1 {
		t.Errorf("the Instance recorded %d QualificationFailed events, want 1", events)
	}

	// The start-up outcome (outcomes.go): a failure, kept for its Allocation. The classifier's table says it, and
	// the pass records it.
	if got := endOutcome[v1alpha1.ReasonQualificationFailed]; got != v1alpha1.StartOutcomeFailed {
		t.Errorf("endOutcome[QualificationFailed] is %q, want %q", got, v1alpha1.StartOutcomeFailed)
	}
	outcome := func() (v1alpha1.StartOutcome, bool) {
		for _, class := range rg.md("md").Status.StartOutcomes {
			for _, o := range class.Outcomes {
				if o.AllocationUID == allocUID {
					return o, true
				}
			}
		}
		return v1alpha1.StartOutcome{}, false
	}
	rg.until(5*time.Minute, 10*time.Second, "the Allocation's start-up outcome", func() bool { _, ok := outcome(); return ok })
	o, _ := outcome()
	if o.Result != v1alpha1.StartOutcomeFailed || o.Reason != v1alpha1.ReasonQualificationFailed || o.Gate != "" || o.ReferenceDate != "" {
		t.Errorf("outcome %+v, want Failed, QualificationFailed and no gate verdict: the Instance never reached the gate", o)
	}
	if outs := rg.md("md").Status.StartOutcomes[0]; outs.Failures != 1 || outs.QualificationFailures != 1 || outs.Count != 1 {
		t.Errorf("counts %d, %d of %d, want 1 failure, 1 qualification failure, of 1", outs.Failures, outs.QualificationFailures, outs.Count)
	}
}
