package lifecycle

// The requeue of the reconcile loop (HBEP-7, Reconcile loop, step 11):
// "Requeue the key at the earliest coming deadline (a drain start, a
// replacement's planned submission time or lateness deadline, a registration or
// start timeout, a retention end), and after 30 s at the latest." The rig
// ignores RequeueAfter, so these tests call Reconcile themselves and read the
// Result. Every expected gap is a hand calculation from the clock.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// requeueOf runs one pass over the deployment and returns when it asks to be
// run again.
func requeueOf(t *testing.T, rg *rig, name string) time.Duration {
	t.Helper()
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: name}})
	if err != nil {
		t.Fatalf("reconcile %s at %s: %v", name, rg.clock.Now().Format(time.TimeOnly), err)
	}
	return res.RequeueAfter
}

// TestRequeueAtTheEndTime covers step 11 for the deployment's end time: the
// pass at 08:00:00 (the rig's clock, a whole second) of a deployment that ends
// at 08:00:17 asks to run again in 17 s (17 is none of 1, 15 or 30), with or
// without a front, whose Allocation and Instance bring deadlines of their own
// that are all later; one that ends in 10 minutes, or never, waits the longest
// the loop allows, 30 s.
func TestRequeueAtTheEndTime(t *testing.T) {
	end := func(d time.Duration) *metav1.Time { return &metav1.Time{Time: rigStart.Add(d)} }
	for _, tc := range []struct {
		name  string
		front bool
		end   *metav1.Time
		want  time.Duration
	}{
		{"end in 17 s, no front", false, end(17 * time.Second), 17 * time.Second},
		{"end in 17 s, with a front and a pending Instance", true, end(17 * time.Second), 17 * time.Second},
		{"end in 10 minutes: the 30 s cap", false, end(10 * time.Minute), 30 * time.Second},
		{"end in 10 minutes, with a front: the 30 s cap", true, end(10 * time.Minute), 30 * time.Second},
		{"no end time and nothing due: the 30 s cap", false, nil, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []rigOption
			if !tc.front {
				opts = append(opts, withoutFront())
			}
			rg := newRig(t, opts...)
			md := mdSpec("md")
			md.Spec.EndTime = tc.end
			rg.create(md)
			rg.Step() // the rig's clock stays at 08:00:00
			if tc.front && len(rg.instances("md")) != 1 {
				t.Fatalf("%d Instances after the first pass, want the pending one this row relies on", len(rg.instances("md")))
			}
			if got := requeueOf(t, rg, "md"); got != tc.want {
				t.Errorf("RequeueAfter = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestRequeueAtAReplacementsPlannedTimeAndTheDrainStart covers step 11 on the
// handover timeline of handover_test.go: a 4 h limit, the primary started at
// 08:00:20 and ready at 08:01:10 (start-to-ready 50 s), its drain start
// 09:50:20, the margin 30 min. One 40 min wait is on record, and the primary's
// own 0 s makes two, fewer than 20: the largest, 40 min. The replacement is
// planned for 09:50:20 - (40 min + 50 s + 30 min) = 08:39:30.
//
//   - At 08:39:10 the next deadline is that time, 20 s away, and a pass 20 s
//     later creates the replacement.
//   - The replacement is created at 08:39:30, starts at 08:39:40 and is ready at
//     08:40:30, so at 09:50:00 the next deadline is the primary's drain start,
//     09:50:20, 20 s away.
//   - Whatever the clock says between, a pass never asks for more than 30 s,
//     and for no less than 1 s.
func TestRequeueAtAReplacementsPlannedTimeAndTheDrainStart(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	hoSetWaits(rg, 40*time.Minute)
	primary := hoStart(rg, nil)
	if got := hoWaits(rg); len(got) != 2 || got[1] != 40*time.Minute {
		t.Fatalf("the Provider holds waits %v, want the 40 min one and the primary's 0 s", got)
	}
	capped := func() {
		if d := requeueOf(t, rg, hoMD); d < time.Second || d > 30*time.Second {
			t.Errorf("at %s RequeueAfter = %s, want between 1 s and 30 s", rg.clock.Now().Format(time.TimeOnly), d)
		}
	}
	hoStepTo(rg, hoT(8, 39, 10), time.Minute, capped)
	if n := len(rg.instances(hoMD)); n != 1 {
		t.Fatalf("%d Instances at 08:39:10, a replacement before its planned time 08:39:30", n)
	}
	got := requeueOf(t, rg, hoMD)
	if got != 20*time.Second {
		t.Fatalf("at 08:39:10 RequeueAfter = %s, want 20s (the replacement's planned time 08:39:30)", got)
	}
	rg.clock.Advance(got)
	rg.Step()
	r := hoRepl(rg, primary.Name)
	if len(r) != 1 {
		t.Fatalf("%d replacements at %s, want one created at the time the requeue asked for", len(r), rg.clock.Now().Format(time.TimeOnly))
	}
	hoWantTime(t, "the replacement's creation time", &r[0].CreationTimestamp, hoT(8, 39, 30))
	hoStepTo(rg, hoT(9, 50, 0), time.Minute, capped)
	if rg.readyCount(hoMD) != 2 {
		t.Fatalf("%d ready Instances at 09:50:00, want the primary and its replacement", rg.readyCount(hoMD))
	}
	if got := requeueOf(t, rg, hoMD); got != 20*time.Second {
		t.Errorf("at 09:50:00 RequeueAfter = %s, want 20s (the primary's drain start 09:50:20)", got)
	}
}
