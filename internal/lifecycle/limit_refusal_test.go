package lifecycle

// HBEP-7's Test plan items "A limit refusal is no failure", "A lost reply
// under a one-job limit" and the leader hand-over case of "Per-user limits,
// reserved atomically", through the lifecycle controller against the
// provider fake, whose user has a one-job submit limit (pfake.Config's
// MaxSubmitPerUser 1) as Athena's plgrid-now does. Slurm's reason line for a
// full submit limit is "sbatch: error: QOSMaxSubmitJobPerUserLimit" (HBEP-4,
// QOS limits, A limit refusal).

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const submitLimitLine = "sbatch: error: QOSMaxSubmitJobPerUserLimit"

// oneJobUser: the fake Slurm's user may have one job pending or running.
func oneJobUser(c *pfake.Config) { c.MaxSubmitPerUser = 1 }

// rejectedAttempts returns how many sbatch calls the fake received, and the
// Allocation's current submission record.
func onlyAllocation(rg *rig, md string) v1alpha1.Allocation {
	rg.t.Helper()
	as := rg.allocations(md)
	if len(as) != 1 {
		rg.t.Fatalf("%d Allocations of %s, want exactly one", len(as), md)
	}
	return as[0]
}

// noRestartLoop fails when the deployment's condition carries RestartLoop or a
// RestartLoop event fired.
func noRestartLoop(t *testing.T, rg *rig, md string) {
	t.Helper()
	c := rg.condition(md, v1alpha1.ConditionDegraded)
	if strings.Contains(c.Message, v1alpha1.ReasonRestartLoop) || c.Reason == v1alpha1.ReasonRestartLoop {
		t.Fatalf("Degraded = %s %q: a refused submission counted toward RestartLoop", c.Reason, c.Message)
	}
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonRestartLoop)); n != 0 {
		t.Fatalf("%d RestartLoop events, want none", n)
	}
}

// TestLimitRefusalIsNoFailure: the fake's one-job QOS is held by a job outside
// the framework, as the owner's other projects would hold it. Three attempts
// in a row are refused with QOSMaxSubmitJobPerUserLimit (the reject backoff is
// 1 min). After each, the Allocation stays Submitting, its Instance stays
// pending, the deployment has SubmitLimitReached with the reason line, and no
// RestartLoop is set and no class pauses. Once the outside job ends, the next
// attempt is accepted and the Instance starts.
func TestLimitRefusalIsNoFailure(t *testing.T) {
	rg := newRig(t, oneSlotOpts(withBatchConfig(oneJobUser))...)
	outside := rg.batch.AddOutsideJob("example-other-project", rigPartition, 1, 4*time.Hour)
	rg.create(mdSpec("md"))
	refusals := 0
	rg.until(10*time.Minute, 10*time.Second, "three refusals", func() bool {
		as := rg.allocations("md")
		if len(as) == 0 {
			return false
		}
		sub := as[0].Status.AtProvider.Submission
		if sub == nil || sub.State != v1alpha1.SubmissionRejected || int(sub.Attempt) <= refusals {
			return false
		}
		refusals = int(sub.Attempt)
		if sub.Code != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(sub.Reason, submitLimitLine) {
			t.Fatalf("attempt %d record %+v, want Rejected with code SubmitLimitReached and the reason line", sub.Attempt, sub)
		}
		if ph := as[0].Status.AtProvider.Phase; ph != v1alpha1.AllocationSubmitting {
			t.Fatalf("after refusal %d the Allocation is %s, want Submitting", refusals, ph)
		}
		ins := rg.instances("md")
		if len(ins) != 1 || ins[0].Status.Phase != v1alpha1.InstancePending {
			t.Fatalf("after refusal %d the Instances are %+v, want one pending", refusals, ins)
		}
		c := rg.condition("md", v1alpha1.ConditionDegraded)
		if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, submitLimitLine) {
			t.Fatalf("after refusal %d Degraded = %s/%s %q, want SubmitLimitReached with the reason line", refusals, c.Status, c.Reason, c.Message)
		}
		if strings.Contains(c.Message, v1alpha1.ReasonSubmissionRejected+":") {
			t.Fatalf("a submit-limit refusal also shows as SubmissionRejected: %q", c.Message)
		}
		noRestartLoop(t, rg, "md")
		return refusals == 3
	})
	if n := rg.submissions(); n != 3 {
		t.Fatalf("%d sbatch calls, want the three refused", n)
	}
	// The outside job ends: the next attempt, after the backoff, is accepted,
	// and the Instance starts. A paused class would hold it back.
	rg.batch.CancelExternally(outside)
	rg.until(5*time.Minute, 10*time.Second, "the Instance starts", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && ins[0].Status.Phase != v1alpha1.InstancePending && ins[0].Status.Phase != ""
	})
	a := onlyAllocation(rg, "md")
	if a.Status.AtProvider.Submission == nil || a.Status.AtProvider.Submission.Attempt != 4 || a.Annotations[v1alpha1.AnnotationJobID] == "" {
		t.Fatalf("Allocation %s record %+v job %q, want attempt 4 accepted", a.Name, a.Status.AtProvider.Submission, a.Annotations[v1alpha1.AnnotationJobID])
	}
	if in := rg.instances("md")[0]; in.Status.Phase == v1alpha1.InstanceEnded {
		t.Fatalf("the Instance ended (%s), want it started", in.Status.EndReason)
	}
	noRestartLoop(t, rg, "md")
}

// TestLimitRefusalOtherPolicyReason: a refusal whose reason line names another
// limit gives SubmissionRejected, with that reason in the message, retried
// with backoff, and sets no RestartLoop.
func TestLimitRefusalOtherPolicyReason(t *testing.T) {
	rg := newRig(t)
	other := pfake.Script{Outcome: provider.Rejected, Policy: "QOSMaxWallDurationPerJobLimit"}
	rg.batch.Enqueue(other, other, other)
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "three refusals", func() bool {
		as := rg.allocations("md")
		if len(as) == 0 || as[0].Status.AtProvider.Submission == nil {
			return false
		}
		sub := as[0].Status.AtProvider.Submission
		return sub.State == v1alpha1.SubmissionRejected && sub.Attempt == 3
	})
	sub := onlyAllocation(rg, "md").Status.AtProvider.Submission
	if sub.Code != "" {
		t.Fatalf("record code %q for another limit, want none", sub.Code)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSubmissionRejected || !strings.Contains(c.Message, "QOSMaxWallDurationPerJobLimit") || strings.Contains(c.Message, v1alpha1.ReasonSubmitLimitReached) {
		t.Fatalf("Degraded = %s/%s %q, want SubmissionRejected naming QOSMaxWallDurationPerJobLimit", c.Status, c.Reason, c.Message)
	}
	noRestartLoop(t, rg, "md")
	// Retried with backoff: the fourth attempt, unscripted, is accepted.
	rg.until(5*time.Minute, 10*time.Second, "the fourth attempt is accepted", func() bool {
		return onlyAllocation(rg, "md").Annotations[v1alpha1.AnnotationJobID] != ""
	})
}

// TestLostReplyUnderOneJobLimit: the reply to the first submission is lost
// but the scheduler registered the job, under the fake user's one-job limit.
// The attempt resolves by tag: exactly one Allocation and one job with the tag
// remain, its hold is lifted once, the Instance starts, and no second
// Allocation is created.
func TestLostReplyUnderOneJobLimit(t *testing.T) {
	rg := newRig(t, oneSlotOpts(withBatchConfig(oneJobUser))...)
	rg.batch.Enqueue(pfake.Script{Outcome: provider.Unknown, Registered: true})
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "the Instance starts", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && ins[0].Status.Phase != v1alpha1.InstancePending && ins[0].Status.Phase != ""
	})
	a := onlyAllocation(rg, "md")
	tag := a.Annotations[v1alpha1.AnnotationExternalName]
	jobs, err := rg.batch.FindByTag(rg.ctx, tag, rigStart.Add(-time.Hour))
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs with tag %s = %v, %v; want exactly one", tag, jobs, err)
	}
	if n := rg.submissions(); n != 1 {
		t.Fatalf("%d sbatch calls, want the one whose reply was lost", n)
	}
	lifts := 0
	for _, c := range rg.batch.Calls() {
		if c.Op == "scontrol-release" {
			lifts++
		}
	}
	if lifts != 1 {
		t.Fatalf("%d hold lifts, want one", lifts)
	}
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("%d Allocations, want one", n)
	}
	if c := rg.condition("md", v1alpha1.ConditionDegraded); strings.Contains(c.Message, v1alpha1.ReasonSubmitLimitReached) {
		t.Fatalf("Degraded %q: the lost reply was read as a refusal", c.Message)
	}
}

// TestSlotsLeaderHandOver: an old and a new leader, each with its own
// expectations and a cache that does not show the other's creation, each
// create one Allocation on the Provider whose effective submit limit is 1. A
// Lease does not fence, so both exist (HBEP-7, Slots are reserved
// atomically). The fake user's one-job limit accepts one job; the other
// attempt is refused with QOSMaxSubmitJobPerUserLimit and waits as
// SubmitLimitReached.
func TestSlotsLeaderHandOver(t *testing.T) {
	rg := newRig(t, oneSlotOpts(withBatchConfig(oneJobUser))...)
	rg.Step() // the Provider's first status: Ready
	rg.create(mdSpec("md-a"))
	rg.create(mdSpec("md-b"))
	old := rg.r
	old.Reader = newStaleReader(t, rg.c)
	rg.startController(true) // the new leader: fresh memory and expectations
	fresh := rg.r
	fresh.Reader = newStaleReader(t, rg.c)
	reconcile := func(r *Reconciler, md string) {
		t.Helper()
		if _, err := r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: md}}); err != nil {
			t.Fatalf("reconcile %s: %v", md, err)
		}
	}
	reconcile(old, "md-a")
	reconcile(fresh, "md-b")
	if n := len(rg.allAllocations()); n != 2 {
		t.Fatalf("%d Allocations, want two: each leader's expectations are its own", n)
	}
	// From here the new leader alone, its cache caught up.
	fresh.Reader = rg.c
	for range 3 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	accepted, refused := 0, ""
	for _, md := range []string{"md-a", "md-b"} {
		a := onlyAllocation(rg, md)
		switch sub := a.Status.AtProvider.Submission; {
		case a.Annotations[v1alpha1.AnnotationJobID] != "":
			accepted++
		case sub != nil && sub.State == v1alpha1.SubmissionRejected && sub.Code == v1alpha1.ReasonSubmitLimitReached:
			refused = md
		default:
			t.Fatalf("%s's Allocation is neither accepted nor refused for the limit: %+v", md, a.Status.AtProvider)
		}
	}
	if accepted != 1 || refused == "" {
		t.Fatalf("%d accepted and refused %q, want one of each", accepted, refused)
	}
	if n := rg.submissions(); n < 2 {
		t.Fatalf("%d sbatch calls, want both submitted", n)
	}
	c := rg.condition(refused, v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, submitLimitLine) {
		t.Fatalf("%s Degraded = %s/%s %q, want SubmitLimitReached with the reason line", refused, c.Status, c.Reason, c.Message)
	}
	if ph := onlyAllocation(rg, refused).Status.AtProvider.Phase; ph != v1alpha1.AllocationSubmitting {
		t.Fatalf("the refused Allocation is %s, want Submitting: it waits", ph)
	}
}
