package lifecycle

// Issue #174: how the queue-wait estimate changes after a replacement has
// waited on a QOS cap, on no-overlap mode's queued path (HBEP-7, Handover,
// No-overlap mode, the queued-path item: "Its recorded queue wait includes
// that time, so the Provider's observed waits grow and later plans submit
// earlier"). A queue wait is one job's wait from the scheduler's recorded
// submission to its start (HBEP-4, queueWaits). The replacement's planned
// creation time is its primary's drain start minus (the planning wait + the
// class's start-to-ready time + handoverMargin), and never before the
// primary's Allocation is Running; the planning wait is the p90 of the last 20
// waits, or the largest while fewer than 20 are observed (HBEP-7, Handover;
// open question 2).
//
// The timeline, up to the first replacement's submission, is the one of
// nooverlap_test.go: the primary P0's job starts at 08:00:20 with no wait (the
// Provider's one observed wait, 0 s), it is ready at 08:01:10 (start-to-ready
// 50 s), its hard end is 12:00:20 and its drain start 09:50:20. Its
// replacement R1 is created at 09:50:20 - (0 + 50 s + 30 min) = 09:19:30, and
// its job is submitted by the provider's next pass, at 09:19:40. The fake's
// one-GPU cap keeps R1 pending while P0's job holds the GPU (the queued path).
// P0 drains at 09:50:20 with its stream in flight and ends on the first report
// after the stream, at 09:56:00. The deployment releases when idle: at
// 09:56:00 + the idle limit the controller deletes P0's Allocation, and the
// provider's next pass, 10 s later, cancels its job. That frees the GPU, and
// R1's job starts at that same moment (the fake starts a capped job when the
// cap frees). So R1's whole wait is the cap wait:
//
//	SHORT, idle limit 5 min: P0's job ends at 09:56:00 + 5 min + 10 s =
//	  10:01:10. R1's wait is 10:01:10 - 09:19:40 = 41m30s.
//	LONG, idle limit 60 min: P0's job ends at 09:56:00 + 60 min + 10 s =
//	  10:56:10. R1's wait is 10:56:10 - 09:19:40 = 1h36m30s.
//
// The Provider then holds two waits, 0 s and R1's. With fewer than 20, the
// planning wait is the largest, R1's. R1's job runs the rig's 4 h limit, so
// its hard end is its start + 4 h and its drain start that minus the default
// 2h10m drain window, its start + 1h50m. Its replacement R2 is planned at:
//
//	SHORT: drain start 10:01:10 + 1h50m = 11:51:10. With R1's start-to-ready
//	  S1, R2 is created at 11:51:10 - (41m30s + S1 + 30 min). S1 is 50 s when
//	  the provider's pass sees R1's start in the pass that cancels P0's job,
//	  and 60 s when it sees it one pass later: the pass handles Allocations in
//	  the order the client lists them, and their names are random (HBEP-3).
//	  S1 = 50 s gives 11:51:10 - 1h12m20s = 10:38:50; S1 = 60 s gives
//	  11:51:10 - 1h12m30s = 10:38:40. Both are after R1's start, so neither is
//	  clamped.
//	LONG: drain start 10:56:10 + 1h50m = 12:46:10. R1 is not ready yet when
//	  R2 is due, so the start-to-ready is still P0's 50 s: 12:46:10 -
//	  (1h36m30s + 50 s + 30 min) = 12:46:10 - 2h7m20s = 10:38:50, before R1's
//	  start at 10:56:10. The plan reaches back to R1's start, and R2 is created
//	  in the first pass that sees R1's Allocation Running (10:56:10 or
//	  10:56:20, by the same order).
//
// So the 55 min longer cap wait moves R2's plan from about 37 min after R1's
// start to R1's start itself: the growth is visible. (Without the cap, R1
// would start 10 s after its creation, with a wait of 0 s, and R2's plan would
// be the drain start minus 50 s + 30 min.)
//
// Why the expected values are independent of the code under test: every time
// above is worked out by hand from the rig's timeline and HBEP-7's rule, and
// written as a literal. The test first asserts the scheduler facts the
// arithmetic rests on (P0's end, R1's submission and start, R1's lease end and
// start-to-ready) as literals, so a different timeline fails as a
// precondition, not as a wrong wait. It never calls replacementTime or
// p90Wait to get an expected value (p90Wait's result is checked against a
// literal), never sets the waits itself (hoSetWaits): they come from the
// provider controller's recording of the jobs, and never derives the
// expected wait by subtracting the fake job's submit time from its start.

import (
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

type capWaitCase struct {
	name string
	idle time.Duration
	// pJobEnd is when P0's job leaves the GPU, and R1's job starts.
	pJobEnd time.Time
	// wait is R1's recorded queue wait, the planning wait after it.
	wait time.Duration
	// r1LeaseEnd is R1's hard end, its start + 4 h.
	r1LeaseEnd time.Time
	// coarseTo is how far the clock moves in coarse steps after P0's end,
	// before 10 s steps around the release.
	coarseTo time.Time
	// r2At gives R2's expected creation time for R1's start-to-ready s1;
	// clamped cases give the zero time: R2 is due at R1's start.
	r2At func(s1 time.Duration) time.Time
}

var capWaitCases = []capWaitCase{{
	name: "short", idle: 5 * time.Minute,
	pJobEnd: hoT(10, 1, 10), wait: 41*time.Minute + 30*time.Second,
	r1LeaseEnd: hoT(14, 1, 10), coarseTo: hoT(10, 0, 0),
	r2At: func(s1 time.Duration) time.Time {
		switch s1 {
		case 50 * time.Second:
			return hoT(10, 38, 50)
		case 60 * time.Second:
			return hoT(10, 38, 40)
		}
		return time.Time{}
	},
}, {
	name: "long", idle: 60 * time.Minute,
	pJobEnd: hoT(10, 56, 10), wait: time.Hour + 36*time.Minute + 30*time.Second,
	r1LeaseEnd: hoT(14, 56, 10), coarseTo: hoT(10, 55, 0),
	r2At: nil,
}}

// TestNoOverlapQueueWaitAfterCapWait: a queued replacement waits on the GPU
// cap until its predecessor's job ends, then starts. Its whole cap wait is
// recorded as its queue wait, and the next replacement's planned time follows
// from it, for a SHORT and a LONG cap wait.
func TestNoOverlapQueueWaitAfterCapWait(t *testing.T) {
	for _, tc := range capWaitCases {
		t.Run(tc.name, func(t *testing.T) { runCapWait(t, tc) })
	}
}

func runCapWait(t *testing.T, tc capWaitCase) {
	rg := noOverlapQueuedRig(t)
	md := mdSpec(hoMD)
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(tc.idle)}
	rg.create(md)
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	p := rg.instances(hoMD)[0]
	if p.Status.ReadyTime == nil || !p.Status.ReadyTime.Time.Equal(hoT(8, 1, 10)) || p.Status.LeaseEnd == nil || !p.Status.LeaseEnd.Time.Equal(hoT(12, 0, 20)) {
		t.Fatalf("the primary should be ready at 08:01:10 with leaseEnd 12:00:20, got %v and %v", p.Status.ReadyTime, p.Status.LeaseEnd)
	}
	if w := hoWaits(rg); len(w) != 1 || w[0] != 0 {
		t.Fatalf("waits %v after the primary's start, want its own 0s alone", w)
	}

	// R1: created at 09:19:30, submitted at 09:19:40.
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	if n := len(hoRepl(rg, p.Name)); n != 0 {
		t.Fatalf("a replacement exists before its planned time 09:19:30")
	}
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(hoT(9, 19, 30)) || reps[0].Annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("replacements %+v, want one created at 09:19:30, marked as a no-overlap successor", reps)
	}
	r1 := reps[0]
	r1Job := capWaitJob(rg, r1.Spec.AllocationRef.Name)
	if j, ok := rg.batch.Job(r1Job); !ok || !j.SubmitTime.Equal(hoT(9, 19, 40)) || j.StartTime != nil {
		t.Fatalf("R1's job %s is %+v, want submitted at 09:19:40 and not started", r1Job, j)
	}
	pJob := capWaitJob(rg, p.Spec.AllocationRef.Name)

	// Through P0's drain and end, then to the release, in 10 s steps where
	// the times are measured.
	hoStepTo(rg, hoT(9, 49, 0), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 57, 0), 10*time.Second, nil)
	if pi := rg.instance(p.Name); pi.Status.Phase != v1alpha1.InstanceEnded || pi.Status.EndTime == nil || !pi.Status.EndTime.Time.Equal(hoT(9, 56, 0)) {
		t.Fatalf("at 09:57:00 the primary is %s, ended at %v, want ended at 09:56:00", pi.Status.Phase, pi.Status.EndTime)
	}
	hoStepTo(rg, tc.coarseTo, 5*time.Minute, nil)

	// R1 starts once P0's job leaves the GPU. Watch, in 10 s steps, for the
	// first pass that sees R1's Allocation started and for R2's creation.
	var r1Seen, r2Seen time.Time
	var r2 *v1alpha1.Instance
	watch := func() {
		now := rg.clock.Now()
		if a, ok := rg.allocation(r1.Spec.AllocationRef.Name); ok && a.Status.AtProvider.StartTime != nil && r1Seen.IsZero() {
			r1Seen = now
		}
		if rs := hoRepl(rg, r1.Name); len(rs) > 0 && r2 == nil {
			r2, r2Seen = &rs[0], now
		}
	}
	hoStepUntil(rg, tc.pJobEnd.Add(10*time.Minute), 10*time.Second, func() bool {
		return rg.instance(r1.Name).Status.Phase == v1alpha1.InstanceReady
	}, watch)

	// The scheduler facts the expected values rest on.
	pj, ok := rg.batch.Job(pJob)
	if !ok || pj.EndTime == nil || !pj.EndTime.Equal(tc.pJobEnd) {
		t.Fatalf("P0's job %s is %+v, want it ended at %s", pJob, pj, tc.pJobEnd.Format(time.TimeOnly))
	}
	rj, ok := rg.batch.Job(r1Job)
	if !ok || !rj.SubmitTime.Equal(hoT(9, 19, 40)) || rj.StartTime == nil || !rj.StartTime.Equal(tc.pJobEnd) {
		t.Fatalf("R1's job %s is %+v, want submitted at 09:19:40 and started at %s", r1Job, rj, tc.pJobEnd.Format(time.TimeOnly))
	}
	r1i := rg.instance(r1.Name)
	if r1i.Status.LeaseEnd == nil || !r1i.Status.LeaseEnd.Time.Equal(tc.r1LeaseEnd) {
		t.Fatalf("R1's leaseEnd is %v, want %s", r1i.Status.LeaseEnd, tc.r1LeaseEnd.Format(time.TimeOnly))
	}
	if !r1Seen.Equal(tc.pJobEnd) && !r1Seen.Equal(tc.pJobEnd.Add(10*time.Second)) {
		t.Fatalf("R1's start was first seen at %s, want in the pass that frees the GPU (%s) or the next", r1Seen.Format(time.TimeOnly), tc.pJobEnd.Format(time.TimeOnly))
	}
	t.Logf("R1's start first seen %s after the GPU freed", r1Seen.Sub(tc.pJobEnd))

	// The queue-wait estimate: R1's whole cap wait is recorded, and with
	// fewer than 20 waits it is the planning wait.
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

	if tc.r2At == nil {
		// LONG: the plan reaches back before R1's start, so R2 is created in
		// the first pass that sees R1's Allocation Running, and not before.
		if r2 == nil || !r2Seen.Equal(r1Seen) || !r2.CreationTimestamp.Time.Equal(r1Seen) {
			t.Fatalf("R2 %v first seen at %s, want created at %s, the first pass that saw R1 Running", r2, r2Seen.Format(time.TimeOnly), r1Seen.Format(time.TimeOnly))
		}
	} else {
		// SHORT: R2 at its planned time, from R1's start-to-ready.
		s1 := r1i.Status.ReadyTime.Time.Sub(tc.pJobEnd)
		want := tc.r2At(s1)
		if want.IsZero() {
			t.Fatalf("R1's start-to-ready is %s, want 50s or 60s (by the provider pass's order)", s1)
		}
		if s := rg.md(hoMD).Status.StartToReady; len(s) != 1 || s[0].Duration != v1alpha1.Duration(s1.String()) {
			t.Fatalf("the class's start-to-ready is %+v, want R1's %s", s, s1)
		}
		if r2 != nil {
			t.Fatalf("R2 was created at %s, before its planned time %s", r2Seen.Format(time.TimeOnly), want.Format(time.TimeOnly))
		}
		hoStepTo(rg, want.Add(-10*time.Second), 5*time.Minute, nil)
		if n := len(hoRepl(rg, r1.Name)); n != 0 {
			t.Fatalf("at %s R2 exists before its planned time %s", rg.clock.Now().Format(time.TimeOnly), want.Format(time.TimeOnly))
		}
		hoStepTo(rg, want, 10*time.Second, nil)
		rs := hoRepl(rg, r1.Name)
		if len(rs) != 1 || !rs[0].CreationTimestamp.Time.Equal(want) {
			t.Fatalf("R2 %+v, want one created at %s", rs, want.Format(time.TimeOnly))
		}
		r2 = &rs[0]
	}
	if r2.Annotations[annotationNoOverlapSuccessor] == "" || r2.Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("R2 %+v, want a no-overlap successor on %s (the queued path)", r2, rigProvider)
	}
}

// capWaitJob returns the job ID of the Allocation named alloc.
func capWaitJob(rg *rig, alloc string) string {
	rg.t.Helper()
	a, ok := rg.allocation(alloc)
	if !ok || a.Annotations[v1alpha1.AnnotationJobID] == "" {
		rg.t.Fatalf("the Allocation %s has no job: %+v", alloc, a)
	}
	return a.Annotations[v1alpha1.AnnotationJobID]
}
