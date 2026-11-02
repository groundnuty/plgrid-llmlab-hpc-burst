package lifecycle

// Issue #174, as re-scoped by `@hpc-burst-science-agent` on 1 Nov 2026: a
// refused interval adds nothing to a queue wait. A queue wait is one recorded
// job's wait from the scheduler's recorded submission to its start (HBEP-4,
// Provider status, queueWaits: hbep/0004-provider-interface/README.md:373
// here, :374 on main after PR #196), and a refusal records no job (HBEP-4,
// Notes, "A limit refusal is no failure": :633 here, :636 on main). HBEP-7
// states the rule in Handover, No-overlap mode (PR #196, kept by PR #205), and
// its Test plan item "Queue waits on the queued path" says: "A refused interval
// before acceptance adds nothing to any wait, so a longer refusal gives a
// smaller recorded wait."
//
// Why a normal-mode replacement. Under #195 (HBEP-7 amendment PR #205) a
// queued-path no-overlap successor records no wait at all, so a refusal test
// on that path would prove nothing once #195 lands. Here the Provider's QOS
// allows 2 GPUs per user, so the replacement may run beside its predecessor:
// the deployment is in normal mode, the replacement carries no no-overlap
// mark (the test checks that), and #195 does not mark it. The queued-path rule
// and its test are #195's.
//
// Why a GPU held outside the framework. With only the scheduler's own queue
// wait, a job's wait counts from its accepted submission, so a short and a
// long refusal would record the same wait. To show that the refused time is
// left out, the start must not move with the acceptance: a job of the owner's
// other project holds one GPU from 08:00:00 until its 100 min limit ends it
// at 09:40:00 ("a GPU they hold makes our job pend": HBEP-7, Provisioning,
// Per-user limits). The fake's cap of 2 GPUs then holds an accepted
// replacement pending until 09:40:00, beside the predecessor's GPU.
//
// The timeline is handover_test.go's (hoStart): the primary P0's job starts
// at 08:00:20 with no wait (its GPU fits beside the outside job's), it is
// ready at 08:01:10 (start-to-ready 50 s), its hard end is 12:00:20, and its
// drain start 09:50:20. The Provider's one wait is P0's 0 s, so its
// replacement R1 is created at 09:50:20 - (0 + 50 s + 30 min) = 09:19:30. The
// fake refuses R1's first attempts with QOSMaxSubmitJobPerUserLimit. The
// provider's controller makes attempt 1 at 09:19:40, the pass after R1's
// creation, and each next one after the 1 min reject backoff, so attempt n
// runs at 09:19:40 + (n - 1) min.
//
//	SHORT, one refusal: attempt 2, at 09:20:40, is accepted. R1 pends on the
//	  GPU cap until 09:40:00 and starts then. Its wait is 09:40:00 - 09:20:40
//	  = 19m20s: the refused minute, 09:19:40 to 09:20:40, is not in it. R1 is
//	  ready at 09:40:50 (start-to-ready 50 s), its hard end is 13:40:00 and its
//	  drain start 11:30:00. The planning wait is the largest of 0 s and
//	  19m20s, so R1's replacement R2 is created at 11:30:00 - (19m20s + 50 s +
//	  30 min) = 11:30:00 - 50m10s = 10:39:50, 59m50s after R1's start.
//	LONG, 22 refusals: attempts 1 to 22, 09:19:40 to 09:40:40, are refused.
//	  Attempt 23, at 09:41:40, comes after the GPU freed, so R1 is accepted and
//	  starts at once: its wait is 0 s. It is ready at 09:42:30, its hard end
//	  is 13:41:40 and its drain start 11:31:40. The planning wait is 0 s, so R2
//	  is created at 11:31:40 - (0 s + 50 s + 30 min) = 11:00:50, 1h19m10s
//	  after R1's start.
//
// So 21 more minutes of refusals make the recorded wait SMALLER (0 s against
// 19m20s), and R2's plan later after R1's start (1h19m10s against 59m50s).
// Both R1 starts come before P0's lateness deadline, 09:50:20 - 50 s =
// 09:49:30.
//
// The pass order. The provider's pass handles Allocations in the order its
// client lists them, and their names are random (HBEP-3). No start here
// falls in a pass that also cancels a job, so the order changes none of the
// times above, but the provider's client lists Allocations oldest first all
// the same (oldestFirst), so the runs never depend on the names.
//
// Why the expected values are independent of the code under test: every time
// above is worked out by hand from the rig's timeline and HBEP-7's rule, and
// written as a literal. The test first asserts the facts the arithmetic rests
// on (the outside job's end, R1's attempts, submission, start, lease end and
// start-to-ready) as literals, so a different timeline fails as a
// precondition, not as a wrong wait. It never calls replacementTime or
// p90Wait to get an expected value (p90Wait's result is checked against a
// literal), never sets the waits itself (hoSetWaits): they come from the
// provider controller's recording of the jobs, and never derives the
// expected wait by subtracting the fake job's submit time from its start.

import (
	"context"
	"sort"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

type refusalWaitCase struct {
	name     string
	refusals int
	// accepted is when R1's accepted attempt runs; r1Start is when its job
	// starts, and wait is its recorded queue wait.
	accepted, r1Start time.Time
	wait              time.Duration
	r1LeaseEnd        time.Time
	// r2At is R2's expected creation time.
	r2At time.Time
}

var refusalWaitCases = []refusalWaitCase{{
	name: "short", refusals: 1,
	accepted: hoT(9, 20, 40), r1Start: hoT(9, 40, 0), wait: 19*time.Minute + 20*time.Second,
	r1LeaseEnd: hoT(13, 40, 0), r2At: hoT(10, 39, 50),
}, {
	name: "long", refusals: 22,
	accepted: hoT(9, 41, 40), r1Start: hoT(9, 41, 40), wait: 0,
	r1LeaseEnd: hoT(13, 41, 40), r2At: hoT(11, 0, 50),
}}

// TestQueueWaitAfterRefusal: a normal-mode replacement refused for a full
// submit limit, then accepted. The refused interval stays out of its recorded
// queue wait and out of the next replacement's plan, for a SHORT and a LONG
// refusal.
func TestQueueWaitAfterRefusal(t *testing.T) {
	for _, tc := range refusalWaitCases {
		t.Run(tc.name, func(t *testing.T) { runRefusalWait(t, tc) })
	}
}

// oldestFirst is a client whose Allocation lists come oldest first, by
// creation time, then by name, whatever their random names.
type oldestFirst struct{ client.Client }

func (o oldestFirst) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := o.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if l, ok := list.(*v1alpha1.AllocationList); ok {
		sort.SliceStable(l.Items, func(i, k int) bool {
			a, b := l.Items[i], l.Items[k]
			if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
				return a.CreationTimestamp.Before(&b.CreationTimestamp)
			}
			return a.Name < b.Name
		})
	}
	return nil
}

// refusalWaitRig is a rig with a 4 h limit whose Provider's QOS allows 2 GPUs
// per user, as the fake's user QOS does, with the provider's pass in a fixed
// order (oldest Allocation first; only the provider's client is wrapped) and
// the outside job holding one GPU until 09:40:00. It returns the outside
// job's ID.
func refusalWaitRig(t *testing.T) (*rig, string) {
	qos := []v1alpha1.QOSProfile{{Name: "example-two", MaxGPUsPerUser: i32(2)}}
	opts := append(qosOpts(qos, "", "example-two"), withMaxTime(4*time.Hour),
		withBatchConfig(func(c *pfake.Config) { c.MaxGPUsPerUser = 2 }))
	rg := newRig(t, opts...)
	rg.prov.Client = oldestFirst{rg.prov.Client}
	outside := rg.batch.AddOutsideJob("example-other-project", rigPartition, 1, 100*time.Minute)
	return rg, outside
}

func runRefusalWait(t *testing.T, tc refusalWaitCase) {
	rg, outside := refusalWaitRig(t)
	p := hoStart(rg, nil)
	if w := hoWaits(rg); len(w) != 1 || w[0] != 0 {
		t.Fatalf("waits %v after the primary's start, want its own 0s alone", w)
	}
	// The primary's submission has taken its script: the refusals are R1's.
	for range tc.refusals {
		rg.batch.Enqueue(pfake.Script{Outcome: provider.Rejected, Policy: "QOSMaxSubmitJobPerUserLimit"})
	}

	// R1: created at 09:19:30, in normal mode, and refused at 09:19:40.
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	if n := len(hoRepl(rg, p.Name)); n != 0 {
		t.Fatalf("a replacement exists before its planned time 09:19:30")
	}
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(hoT(9, 19, 30)) {
		t.Fatalf("replacements %+v, want one created at 09:19:30", reps)
	}
	r1 := reps[0]
	if r1.Annotations[annotationNoOverlapSuccessor] != "" {
		t.Fatalf("R1 is marked as a no-overlap successor (%q), want a normal-mode replacement", r1.Annotations[annotationNoOverlapSuccessor])
	}
	ra := r1.Spec.AllocationRef.Name
	if a, ok := rg.allocation(ra); !ok || a.Annotations[v1alpha1.AnnotationJobID] != "" || a.Status.AtProvider.Submission == nil ||
		a.Status.AtProvider.Submission.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("R1's Allocation after its first attempt: %+v, want refused with SubmitLimitReached and no job", a)
	}

	// Step until R1 is ready, in 10 s steps where the attempts and the start
	// are timed, noting when R1's job is first recorded.
	jobAt := time.Time{}
	watch := func() {
		if a, ok := rg.allocation(ra); ok && a.Annotations[v1alpha1.AnnotationJobID] != "" && jobAt.IsZero() {
			jobAt = rg.clock.Now()
		}
	}
	hoStepUntil(rg, hoT(9, 49, 30), 10*time.Second, func() bool {
		return rg.instance(r1.Name).Status.Phase == v1alpha1.InstanceReady
	}, watch)

	// The facts the expected values rest on.
	if j, ok := rg.batch.Job(outside); !ok || j.EndTime == nil || !j.EndTime.Equal(hoT(9, 40, 0)) {
		t.Fatalf("the outside job is %+v, want it ended at 09:40:00", j)
	}
	if !jobAt.Equal(tc.accepted) {
		t.Fatalf("R1's job was recorded at %s, want at its accepted attempt, %s", jobAt.Format(time.TimeOnly), tc.accepted.Format(time.TimeOnly))
	}
	a, _ := rg.allocation(ra)
	if s := a.Status.AtProvider.Submission; s == nil || int(s.Attempt) != tc.refusals+1 {
		t.Fatalf("R1's submission record %+v, want attempt %d accepted", s, tc.refusals+1)
	}
	rj, ok := rg.batch.Job(a.Annotations[v1alpha1.AnnotationJobID])
	if !ok || !rj.SubmitTime.Equal(tc.accepted) || rj.StartTime == nil || !rj.StartTime.Equal(tc.r1Start) {
		t.Fatalf("R1's job is %+v, want submitted at %s and started at %s", rj, tc.accepted.Format(time.TimeOnly), tc.r1Start.Format(time.TimeOnly))
	}
	r1i := rg.instance(r1.Name)
	if r1i.Status.LeaseEnd == nil || !r1i.Status.LeaseEnd.Time.Equal(tc.r1LeaseEnd) {
		t.Fatalf("R1's leaseEnd is %v, want %s", r1i.Status.LeaseEnd, tc.r1LeaseEnd.Format(time.TimeOnly))
	}
	if r1i.Status.ReadyTime == nil || !r1i.Status.ReadyTime.Time.Equal(tc.r1Start.Add(50*time.Second)) {
		t.Fatalf("R1 was ready at %v, want 50 s after its start %s", r1i.Status.ReadyTime, tc.r1Start.Format(time.TimeOnly))
	}
	if s := rg.md(hoMD).Status.StartToReady; len(s) != 1 || s[0].Duration != "50s" {
		t.Fatalf("the class's start-to-ready is %+v, want 50s", s)
	}

	// The estimate: the accepted job's wait alone, and with fewer than 20
	// waits it is the planning wait.
	if w := hoWaits(rg); len(w) != 2 || w[0] != 0 || w[1] != tc.wait {
		t.Fatalf("waits %v once R1 started, want [0s %s]", w, tc.wait)
	}
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	if got, ok := p90Wait(prov.Status.QueueWaits); !ok || got != tc.wait {
		t.Fatalf("planning wait %s (%v), want %s", got, ok, tc.wait)
	}

	// R2 at its planned time, not before, in normal mode.
	hoStepTo(rg, tc.r2At.Add(-10*time.Second), 5*time.Minute, nil)
	if n := len(hoRepl(rg, r1.Name)); n != 0 {
		t.Fatalf("at %s R2 exists before its planned time %s", rg.clock.Now().Format(time.TimeOnly), tc.r2At.Format(time.TimeOnly))
	}
	hoStepTo(rg, tc.r2At, 10*time.Second, nil)
	rs := hoRepl(rg, r1.Name)
	if len(rs) != 1 || !rs[0].CreationTimestamp.Time.Equal(tc.r2At) || rs[0].Annotations[annotationNoOverlapSuccessor] != "" {
		t.Fatalf("R2 %+v, want one created at %s, not a no-overlap successor", rs, tc.r2At.Format(time.TimeOnly))
	}
}
