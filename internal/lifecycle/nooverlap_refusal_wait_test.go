package lifecycle

// Issue #174, the refusal variant: a queued replacement whose submissions are
// refused for a full submit limit (SubmitLimitReached) before one is accepted.
// A queue wait is one job's wait from the scheduler's recorded submission to
// its start (HBEP-4, Provider status, queueWaits:
// hbep/0004-provider-interface/README.md:373 here, :374 on main after PR #196),
// and a refusal records no job (HBEP-4, Notes, "A limit refusal is no
// failure": :633 here, :636 on main). So the refused interval stays out of the
// replacement's queue wait and out of the planning wait, and a longer refusal
// gives a SMALLER recorded wait, not a larger one (`@hpc-burst-science-agent`'s
// ruling on #174, 1 Nov 2026, now in HBEP-7, Handover, No-overlap mode, and
// its Test plan item "Queue waits on the queued path", by PR #196).
//
// The timeline is nooverlap_wait_test.go's, with the idle limit 5 min: R1 is
// created at 09:19:30, P0 ends at 09:56:00, and its job leaves the GPU at
// 10:01:10. The fake refuses R1's first attempts with
// QOSMaxSubmitJobPerUserLimit. The provider's controller makes attempt 1 at
// 09:19:40 and each next one after the 1 min reject backoff, so attempt n
// runs at 09:19:40 + (n - 1) min (the clock moves in 10 s steps, which land
// on those times).
//
//	SHORT, one refusal: attempt 2, at 09:20:40, is accepted. R1 pends on the
//	  GPU cap until 10:01:10 and starts then. Its wait is 10:01:10 - 09:20:40
//	  = 40m30s: the refused minute is not in it, and the wait is a minute
//	  shorter than on the path with no refusal (41m30s). With the provider's
//	  pass in a fixed order, oldest Allocation first (capWaitRig, as in
//	  nooverlap_wait_test.go), R1's start is seen in the pass that cancels
//	  P0's job, so R1 is ready 50 s after its start. R1's drain start is
//	  10:01:10 + 4 h - 2h10m = 11:51:10, so R2 is created at 11:51:10 -
//	  (40m30s + 50 s + 30 min) = 11:51:10 - 1h11m20s = 10:39:50.
//	LONG, 42 refusals: attempts 1 to 42, 09:19:40 to 10:00:40, are refused.
//	  Attempt 43, at 10:01:40, comes after P0's job left the GPU, so R1 is
//	  accepted and starts at once: its wait is 0 s. Its drain start is
//	  10:01:40 + 1h50m = 11:51:40. R1 starts in a pass that cancels nothing,
//	  so it is ready 50 s after its start, and R2 is created at 11:51:40 -
//	  (0 s + 50 s + 30 min) = 11:20:50.
//
// So 41 more minutes of refusals leave the recorded wait SMALLER (0 s against
// 40m30s), and R2's plan later (11:20:50 against 10:39:50). Every value is
// worked out by hand; the independence argument of nooverlap_wait_test.go
// holds here too.

import (
	"testing"
	"time"

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
	accepted: hoT(9, 20, 40), r1Start: hoT(10, 1, 10), wait: 40*time.Minute + 30*time.Second,
	r1LeaseEnd: hoT(14, 1, 10), r2At: hoT(10, 39, 50),
}, {
	name: "long", refusals: 42,
	accepted: hoT(10, 1, 40), r1Start: hoT(10, 1, 40), wait: 0,
	r1LeaseEnd: hoT(14, 1, 40), r2At: hoT(11, 20, 50),
}}

// TestNoOverlapQueueWaitAfterRefusal: a queued replacement refused for a full
// submit limit, then accepted. The refused time stays out of its queue wait,
// for a SHORT and a LONG refusal.
func TestNoOverlapQueueWaitAfterRefusal(t *testing.T) {
	for _, tc := range refusalWaitCases {
		t.Run(tc.name, func(t *testing.T) { runRefusalWait(t, tc) })
	}
}

func runRefusalWait(t *testing.T, tc refusalWaitCase) {
	rg := capWaitRig(t)
	p := capWaitPrimary(t, rg, 5*time.Minute)
	// The primary's submission has taken its script: the refusals are R1's.
	for range tc.refusals {
		rg.batch.Enqueue(pfake.Script{Outcome: provider.Rejected, Policy: "QOSMaxSubmitJobPerUserLimit"})
	}
	r1 := capWaitR1(t, rg, p)
	ra := r1.Spec.AllocationRef.Name
	if a, ok := rg.allocation(ra); !ok || a.Annotations[v1alpha1.AnnotationJobID] != "" || a.Status.AtProvider.Submission == nil ||
		a.Status.AtProvider.Submission.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("R1's Allocation after its first attempt: %+v, want refused with SubmitLimitReached and no job", a)
	}

	// Step until R1 is ready, in 10 s steps where the attempts and the
	// release are timed, checking that no attempt before the accepted one
	// recorded a job.
	jobAt := time.Time{}
	watch := func() {
		if a, ok := rg.allocation(ra); ok && a.Annotations[v1alpha1.AnnotationJobID] != "" && jobAt.IsZero() {
			jobAt = rg.clock.Now()
		}
	}
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool {
		return rg.instance(r1.Name).Status.Phase == v1alpha1.InstanceReady
	}, watch)
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

	// The estimate: the accepted job's wait alone.
	capWaitPlan(t, rg, tc.wait)

	if r1i.Status.ReadyTime == nil || !r1i.Status.ReadyTime.Time.Equal(tc.r1Start.Add(50*time.Second)) {
		t.Fatalf("R1 was ready at %v, want 50 s after its start %s", r1i.Status.ReadyTime, tc.r1Start.Format(time.TimeOnly))
	}
	if s := rg.md(hoMD).Status.StartToReady; len(s) != 1 || s[0].Duration != "50s" {
		t.Fatalf("the class's start-to-ready is %+v, want 50s", s)
	}
	capWaitR2At(t, rg, r1, tc.r2At)
}
