package lifecycle

// The extra and the ranking (HBEP-7, Test plan, Over-allocation and the throughput gate, "The extra and the ranking";
// #616, PR 3). Every expected winner, time and reason is written out in the row; none is computed by the selection,
// the margin or scaleDownTargets. Each row runs a candidate and a peer that both pass, on both sides of the margin:
// 18.0 ms and 17.9 ms against 20 ms, where 10% of 20 ms is 2 ms, so a candidate must be lower than 18 ms to outrank.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	rk180 = 18000 * time.Microsecond // 18.0 ms: not lower than 20 ms by more than 10%
	rk179 = 17900 * time.Microsecond // 17.9 ms: lower than 18 ms, so by more than 10%
	rk200 = 20 * time.Millisecond
)

// rkCalibration checks the facts the rows' times rest on: P's job starts at 08:10:20 and X's at 08:15:20.
func rkCalibration(t *testing.T, h *rkRig, p, x string) {
	t.Helper()
	if got := h.jobStart(p); !got.Equal(rkAt(0, 10, 20)) {
		t.Fatalf("P's job started at %v, want 08:10:20 (a 10 min wait from the provider's pass at 08:00:20)", got)
	}
	if got := h.jobStart(x); !got.Equal(rkAt(0, 15, 20)) {
		t.Fatalf("X's job started at %v, want 08:15:20", got)
	}
}

// TestTwoPendingInstancesCreateAndCancelNothing: P and the extra X both pend for ten passes. Step 7 reads one
// chosen primary and one surplus Instance that k = 1 leaves alone: it creates no third Allocation and cancels
// neither, so X keeps its queue age. Which of the two is chosen while both pend is left to their UIDs, and the row
// asserts nothing that depends on it.
func TestTwoPendingInstancesCreateAndCancelNothing(t *testing.T) {
	h := newRkRig(t, false)
	p, x := h.start(nil, pfake.Script{QueueWait: 3 * time.Hour}, pfake.Script{QueueWait: 3 * time.Hour})
	for i := range 10 {
		h.tick(10 * time.Second)
		for _, n := range []string{p, x} {
			if in := h.mustInstance(n); ended(in) || !h.kept(n) {
				t.Fatalf("pass %d: %s ended (%s) or its Allocation was released", i+1, n, in.Status.EndReason)
			}
		}
		if n, s := len(h.allAllocations()), h.submissions(); n != 2 || s != 2 {
			t.Fatalf("pass %d: %d Allocations and %d submissions, want the two", i+1, n, s)
		}
	}
}

// TestACandidateRanksAgainstItsServingPeer: P starts at 00:10 and reaches its ranking point at 00:40 measuring 20 ms:
// it is chosen and ready. X starts at 00:15 and reaches its point at 00:45. P's stream runs from 00:40 to 00:47.
//
//	X at 18.0 ms: 18.0 x 10 = 180, not below 20 x 9 = 180. X ends RankedOut at 00:45 without ever being ready; its
//	  Allocation is kept with idle-since 00:45; one replaced RankedOut event regards X with P related. P serves on.
//	X at 17.9 ms: 179 is below 180. P is marked for X, X is ready at 00:45 and P is still ready; P drains at 00:45:10,
//	  the front's probe interval after, and keeps draining while its stream runs, to 00:47:00. It ends RankedOut, its
//	  Allocation kept with idle-since at its end. One event regards P with X related. X serves throughout.
func TestACandidateRanksAgainstItsServingPeer(t *testing.T) {
	for _, c := range []struct {
		name string
		x    time.Duration
	}{
		{"18.0 ms does not outrank 20 ms", rk180},
		{"17.9 ms outranks 20 ms", rk179},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newRkRig(t, false)
			p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
			h.engineOf(p, rk200, 0)
			h.engineOf(x, c.x, 0)
			busyFrom, busyTo := rkAt(0, 40, 0), rkAt(0, 47, 0)
			h.health[h.alloc(p)] = func(now time.Time, proc afake.Process) agentapi.Health {
				hl := afake.DefaultHealth(now, proc)
				if !now.Before(busyFrom) && now.Before(busyTo) {
					hl.RunningRequests, hl.GeneratedTokens = agentapi.Int64(4), agentapi.Int64(now.Unix())
				}
				return hl
			}
			h.stepTo(rkAt(0, 40, 0), 10*time.Second, nil)
			rkCalibration(t, h, p, x)
			if got := h.readyAt(p); !got.Equal(rkAt(0, 40, 0)) {
				t.Fatalf("P ready at %v, want 00:40 (08:40:00)", got)
			}
			if in := h.mustInstance(x); ended(in) || in.Status.ReadyTime != nil {
				t.Fatalf("X is %s before it passes, want it unfinished", in.Status.Phase)
			}
			h.stepTo(rkAt(0, 44, 50), 10*time.Second, nil)
			if in := h.mustInstance(x); ended(in) || in.Status.ReadyTime != nil {
				t.Fatalf("X is %s at 00:44:50, want it still verifying", in.Status.Phase)
			}
			h.tick(10 * time.Second) // 00:45:00
			if c.x == rk180 {
				rkAssertRankedOut(t, h, x, p, rkAt(0, 45, 0))
				if in := h.mustInstance(p); in.Status.Phase != v1alpha1.InstanceReady || in.Annotations[annotationRankedOut] != "" {
					t.Fatalf("P is %s, marked %q: an Instance that was not outranked is untouched", in.Status.Phase, in.Annotations[annotationRankedOut])
				}
				h.run(30*time.Minute, time.Minute)
				if h.readyCount(hoMD) != 1 || h.mustInstance(p).Status.Phase != v1alpha1.InstanceReady {
					t.Fatal("P stopped serving after X was ranked out")
				}
				return
			}
			if got := h.readyAt(x); !got.Equal(rkAt(0, 45, 0)) {
				t.Fatalf("X ready at %v, want 00:45 (08:45:00)", got)
			}
			if in := h.mustInstance(p); in.Status.Phase != v1alpha1.InstanceReady || in.Annotations[annotationRankedOut] != x {
				t.Fatalf("P is %s, marked %q at 00:45:00, want ready and marked for %s", in.Status.Phase, in.Annotations[annotationRankedOut], x)
			}
			h.tick(10 * time.Second) // 00:45:10
			if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceDraining {
				t.Fatalf("P is %s at 00:45:10, want draining", ph)
			}
			h.stepTo(rkAt(0, 46, 50), 10*time.Second, func() {
				if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceDraining {
					t.Fatalf("P is %s at %s, want draining while its stream runs (to 00:47:00)", ph, h.clock.Now().Format(time.TimeOnly))
				}
			})
			h.stepTo(rkAt(0, 47, 20), 10*time.Second, nil)
			in := h.mustInstance(p)
			if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonRankedOut {
				t.Fatalf("P is %s, ended for %q at 00:47:20, want ended for RankedOut", in.Status.Phase, in.Status.EndReason)
			}
			if !h.kept(p) {
				t.Fatal("P's Allocation was released: ranking never releases one")
			}
			if since, ok := h.idleSince(p); !ok || !since.Equal(in.Status.EndTime.Time) {
				t.Fatalf("P's Allocation idle-since %v (%v), want the end of its Instance, %v", since, ok, in.Status.EndTime)
			}
			evs := h.rankedOutEvents(p)
			if len(evs) != 1 || evs[0].RelatedName != x || evs[0].Warning {
				t.Fatalf("RankedOut events regarding P: %+v, want one, Normal, with %s related", evs, x)
			}
			if xi := h.mustInstance(x); xi.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("X is %s, want ready: it serves in P's place", xi.Status.Phase)
			}
		})
	}
}

// rkAssertRankedOut checks an Instance ranked out at the given time without ever being ready: its Allocation is
// kept with idle-since at that time, and one replaced RankedOut event regards it with peer related ("" for none).
func rkAssertRankedOut(t *testing.T, h *rkRig, x, peer string, at time.Time) {
	t.Helper()
	in := h.mustInstance(x)
	if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonRankedOut || in.Status.ReadyTime != nil {
		t.Fatalf("%s is %s, ended for %q, ready at %v: want ended for RankedOut and never ready", x, in.Status.Phase, in.Status.EndReason, in.Status.ReadyTime)
	}
	if in.Status.EndTime == nil || !in.Status.EndTime.Time.Equal(at) {
		t.Fatalf("%s ended at %v, want %v", x, in.Status.EndTime, at)
	}
	if !h.kept(x) {
		t.Fatalf("%s's Allocation was released at its ranking: a ranked-out Allocation is kept as warm capacity", x)
	}
	if since, ok := h.idleSince(x); !ok || !since.Equal(at) {
		t.Fatalf("%s's Allocation idle-since %v (%v), want %v", x, since, ok, at)
	}
	evs := h.rankedOutEvents(x)
	if len(evs) != 1 || evs[0].RelatedName != peer || evs[0].Warning {
		t.Fatalf("RankedOut events regarding %s: %+v, want one, Normal, with %q related", x, evs, peer)
	}
}

// TestTheFirstToPassIsChosenAndTheLaterOutranksIt: X, started later, reaches its ranking point first, at 00:40,
// measuring 20 ms. P, started at 00:10, is only started then, so X is the better of the two and is chosen: it
// becomes ready at once, as an Instance the deployment needs. P reaches its point at 00:45 measuring 17.9 ms, 179
// against 20 x 9 = 180: it outranks X. X is marked for P, P is ready at 00:45, and X drains from 00:45:10 and ends
// RankedOut with its Allocation kept. (The same with 18.0 ms leaves X serving: the first test's row.)
func TestTheFirstToPassIsChosenAndTheLaterOutranksIt(t *testing.T) {
	for _, c := range []struct {
		name     string
		p        time.Duration
		outranks bool
	}{
		{"17.9 ms outranks", rk179, true},
		{"18.0 ms does not", rk180, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newRkRig(t, false)
			p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
			// X: job at 08:15:20, ranking point 08:40:00 with a delay of 24 min. P: job at 08:10:20, point 08:45:00
			// with 34 min.
			h.engineOf(x, rk200, 24*time.Minute)
			h.engineOf(p, c.p, 34*time.Minute)
			h.stepTo(rkAt(0, 40, 0), 10*time.Second, nil)
			rkCalibration(t, h, p, x)
			if got := h.readyAt(x); !got.Equal(rkAt(0, 40, 0)) {
				t.Fatalf("X ready at %v, want 00:40: it is chosen while P is only started", got)
			}
			if in := h.mustInstance(p); in.Status.ReadyTime != nil || ended(in) {
				t.Fatalf("P is %s at 00:40, want unfinished", in.Status.Phase)
			}
			h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
			if !c.outranks {
				rkAssertRankedOut(t, h, p, x, rkAt(0, 45, 0))
				if h.mustInstance(x).Status.Phase != v1alpha1.InstanceReady {
					t.Fatal("X stopped serving though P did not outrank it")
				}
				return
			}
			if got := h.readyAt(p); !got.Equal(rkAt(0, 45, 0)) {
				t.Fatalf("P ready at %v, want 00:45", got)
			}
			h.run(time.Minute, 10*time.Second)
			in := h.mustInstance(x)
			if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonRankedOut {
				t.Fatalf("X is %s, ended for %q, want ended for RankedOut", in.Status.Phase, in.Status.EndReason)
			}
			if !h.kept(x) {
				t.Fatal("X's Allocation was released")
			}
			if pi := h.mustInstance(p); pi.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("P is %s, want ready", pi.Status.Phase)
			}
		})
	}
}

// TestAFailedPrimaryLeavesTheExtraAsThePrimary: X still pends when P's job fails at 00:30 (P started at 00:10 and
// runs 19 min 40 s). X is the only Instance left, so it is the primary, and nothing is created: the deployment is
// back at its count with the Allocation it already had, its queue age intact.
func TestAFailedPrimaryLeavesTheExtraAsThePrimary(t *testing.T) {
	h := newRkRig(t, false)
	fail := 19*time.Minute + 40*time.Second
	p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute, FailAfter: &fail}, pfake.Script{QueueWait: 3 * time.Hour})
	h.stepTo(rkAt(0, 29, 50), 10*time.Second, nil)
	if in := h.mustInstance(p); ended(in) {
		t.Fatalf("P ended (%s) before its job failed", in.Status.EndReason)
	}
	submitted := h.submissions()
	h.stepTo(rkAt(0, 40, 0), 10*time.Second, nil)
	if in := h.mustInstance(p); in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonAllocationFailed {
		t.Fatalf("P is %s, ended for %q, want ended for AllocationFailed once its job failed", in.Status.Phase, in.Status.EndReason)
	}
	if in := h.mustInstance(x); ended(in) || !h.kept(x) {
		t.Fatalf("X is %s: want it kept as the primary", in.Status.Phase)
	}
	if got := h.submissions(); got != submitted {
		t.Fatalf("%d submissions after P's failure, %d before: X is the primary, nothing is created", got, submitted)
	}
	live := 0
	for _, in := range h.liveInstances(hoMD) {
		live++
		if in.Name != x {
			t.Fatalf("live Instance %s: only X is wanted", in.Name)
		}
	}
	if live != 1 {
		t.Fatalf("%d live Instances, want X alone", live)
	}
}
