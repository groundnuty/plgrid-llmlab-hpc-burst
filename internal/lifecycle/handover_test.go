package lifecycle

// Scenario tests for HBEP-7's Handover items (Test plan, README lines 445-449
// and 469). Every expected time is worked out by hand in a comment and written
// as a literal; none is computed by the planner.
//
// The timeline every test shares. The rig's primary job starts at 08:00:20 with
// no queue wait (the rig lifts the hold in the pass that submits). The fake
// agent starts the worker in the Step its job starts, the worker runs one Step
// later, and the checks answer one Step after that, so the verification record
// is written 20 s after the start, at 08:00:40. An Instance is ready only after
// the agent reports its warm-up done, one Step after the controller judged the
// rounds settled: 50 s after the start, at 08:01:10, so the class's
// start-to-ready time is 50 s once measured (hoStart checks all of this). With a
// 4 h limit the hard end is 12:00:20, the default drain window is 2h10m, so the
// primary's drain start is 09:50:20. The default handover margin is 30 min. A
// replacement's planned time is the drain start minus (the planning wait +
// start-to-ready + margin), and not before the primary's start: with no wait,
// 09:50:20 - (0 + 50 s + 30 min) = 09:19:30. Its job starts at the next provider
// pass, 10 s after its creation, and it is ready 50 s after that.
//
// The clock moves in 10 s steps, and the creation time of a replacement is the
// time of the step that creates it. So a test that asserts a creation time steps
// to 10 s before the planned time in coarse steps, and then in 10 s steps.

import (
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const hoMD = "md"

// hoT is a time on the rig's first day, 2026-11-02, in UTC.
func hoT(h, m, s int) time.Time { return time.Date(2026, 11, 2, h, m, s, 0, time.UTC) }

func hoMinutes(ms ...int) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m) * time.Minute
	}
	return out
}

// hoStepTo moves the clock to target in steps of at most coarse (the last one
// lands on target) and runs one rig step after each, then each. A long wait
// takes coarse steps; the minute around an Instance start takes 10 s ones,
// because start-to-ready is measured in steps.
func hoStepTo(rg *rig, target time.Time, coarse time.Duration, each func()) {
	rg.t.Helper()
	for rg.clock.Now().Before(target) {
		rg.clock.Advance(min(coarse, target.Sub(rg.clock.Now())))
		rg.Step()
		if each != nil {
			each()
		}
	}
}

// hoSetWaits replaces the queue waits in the Provider's status with waits
// (none: clears them), oldest first, as the Provider's controller wrote them.
func hoSetWaits(rg *rig, waits ...time.Duration) {
	rg.t.Helper()
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &prov); err != nil {
		rg.t.Fatal(err)
	}
	prov.Status.QueueWaits = nil
	for i, w := range waits {
		submit := rigStart.Add(-24*time.Hour + time.Duration(i)*time.Minute)
		prov.Status.QueueWaits = append(prov.Status.QueueWaits, v1alpha1.QueueWait{
			JobID:      "old-" + string(rune('a'+i)),
			SubmitTime: metav1.NewTime(submit), StartTime: metav1.NewTime(submit.Add(w)),
		})
	}
	if err := rg.c.Status().Update(rg.ctx, &prov); err != nil {
		rg.t.Fatal(err)
	}
}

// hoWaits returns the waits in the Provider's status, sorted.
func hoWaits(rg *rig) []time.Duration {
	rg.t.Helper()
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &prov); err != nil {
		rg.t.Fatal(err)
	}
	var out []time.Duration
	for _, w := range prov.Status.QueueWaits {
		out = append(out, w.Wait())
	}
	slices.Sort(out)
	return out
}

// hoStepNoWaits is rg.Step with the Provider's status losing every queue wait
// right after the provider's pass: the controller plans with no wait observed.
func hoStepNoWaits(rg *rig) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	hoSetWaits(rg)
	rg.syncAgents()
	rg.reconcileAll()
}

// hoStart creates the deployment and runs 70 s of 10 s steps (to 08:01:10, when
// the primary is ready) with step (nil: rg.Step). It checks the facts the
// expected times rest on and returns the primary.
func hoStart(rg *rig, step func()) *v1alpha1.Instance {
	rg.t.Helper()
	if step == nil {
		step = rg.Step
	}
	rg.create(mdSpec(hoMD))
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		step()
	}
	p := rg.instances(hoMD)[0]
	a, ok := rg.allocation(p.Spec.AllocationRef.Name)
	switch {
	case !ok || a.Status.AtProvider.StartTime == nil || !a.Status.AtProvider.StartTime.Time.Equal(hoT(8, 0, 20)):
		rg.t.Fatalf("the primary's job should start at 08:00:20, got %+v", a)
	case p.Status.ReadyTime == nil || !p.Status.ReadyTime.Time.Equal(hoT(8, 1, 10)):
		rg.t.Fatalf("the primary should be ready at 08:01:10, got %v", p.Status.ReadyTime)
	case p.Status.LeaseEnd == nil || !p.Status.LeaseEnd.Time.Equal(hoT(12, 0, 20)):
		rg.t.Fatalf("the primary's leaseEnd should be 12:00:20, got %v", p.Status.LeaseEnd)
	}
	if recs := hoVerificationRecords(rg, p.UID); len(recs) != 1 || !recs[0].CreationTimestamp.Time.Equal(hoT(8, 0, 40)) {
		rg.t.Fatalf("the primary's verification record should be the one written at 08:00:40, got %+v", recs)
	}
	if s := rg.md(hoMD).Status.StartToReady; len(s) != 1 || s[0].Duration != "50s" {
		rg.t.Fatalf("start-to-ready should be 50s, got %+v", s)
	}
	return &p
}

// hoRepl returns the Instances that name pred as their predecessor.
func hoRepl(rg *rig, pred string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range rg.instances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == pred {
			out = append(out, in)
		}
	}
	return out
}

// hoAtMostOneReplacement fails when a primary has two Instances naming it as
// their predecessor (ended ones included).
func hoAtMostOneReplacement(rg *rig) {
	rg.t.Helper()
	seen := map[string]bool{}
	for _, in := range rg.instances(hoMD) {
		if p := in.Annotations[v1alpha1.AnnotationPredecessor]; p != "" {
			if seen[p] {
				rg.t.Fatalf("at %s: Instance %s has a second replacement %s", rg.clock.Now().Format(time.TimeOnly), p, in.Name)
			}
			seen[p] = true
		}
	}
}

func hoPhase(rg *rig, name string) v1alpha1.InstancePhase { return rg.instance(name).Status.Phase }

func hoWantTime(t *testing.T, what string, got *metav1.Time, want time.Time) {
	t.Helper()
	if got == nil || !got.Time.Equal(want) {
		t.Fatalf("%s = %v, want %s", what, got, want.Format(time.TimeOnly))
	}
}

// TestHandoverReplacementCreatedAtItsPlannedTime covers the first Test plan
// item of Handover: the replacement is created at its planned time, never
// before the primary's Allocation starts, and never twice for one primary.
//
//   - No wait observed: the replacement is created at the primary's start,
//     08:00:20 (the Provider's status loses the primary's own wait of 0 s).
//   - 14 waits preloaded (12 of 5 min, one of 10, one of 40) and the primary's
//     own 0 s make 15, fewer than 20: the largest, 40 min, is used (nearest
//     rank would be the 14th smallest, 10 min). Before the primary is ready
//     the plan uses the 30 min default start-to-ready and falls at 08:10:20
//     (09:50:20 - (40 min + 30 min + 30 min)), which has not come yet; once
//     50 s is measured (08:01:10) it is 09:50:20 - (40 min + 50 s + 30 min) =
//     08:39:30.
//   - 19 waits preloaded (1..19 min, scrambled) and the primary's 0 s make 20:
//     the 18th smallest, 17 min: 09:50:20 - (17 min + 50 s + 30 min) = 09:02:30.
//   - A 3 h wait: 09:50:20 - (3 h + 50 s + 30 min) = 06:19:30 is before the
//     primary started, so the replacement waits for the primary's start, 08:00:20.
func TestHandoverReplacementCreatedAtItsPlannedTime(t *testing.T) {
	cases := []struct {
		name   string
		waits  []time.Duration
		forget bool
		count  int // the Provider's waits once the primary started
		want   time.Time
	}{
		{"no wait observed", nil, true, 0, hoT(8, 0, 20)},
		{"fewer than 20 waits: the largest", append(slices.Repeat(hoMinutes(5), 12), hoMinutes(10, 40)...), false, 15, hoT(8, 39, 30)},
		{"20 waits: the 18th smallest", hoMinutes(7, 13, 2, 19, 11, 4, 16, 9, 1, 14, 6, 18, 3, 12, 17, 8, 15, 5, 10), false, 20, hoT(9, 2, 30)},
		// 3 waits: the preloaded 3 h, the primary's 0 s and the replacement's
		// own 0 s, since the replacement is created at 08:00:20 and its job
		// starts at the next step, before hoStart returns at 08:01:10.
		{"a plan before the primary's start is held to it", hoMinutes(180), false, 3, hoT(8, 0, 20)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			if tc.waits != nil {
				hoSetWaits(rg, tc.waits...)
			}
			step := rg.Step
			if tc.forget {
				step = func() { hoStepNoWaits(rg) }
			}
			p := hoStart(rg, step)
			got := hoWaits(rg)
			if len(got) != tc.count || (tc.count == 20 && got[17] != 17*time.Minute) {
				t.Fatalf("the Provider holds waits %v, want %d of them (the 18th smallest 17m)", got, tc.count)
			}
			if tc.want.After(rg.clock.Now()) {
				hoStepTo(rg, tc.want.Add(-10*time.Second), time.Minute, func() {
					if n := len(rg.instances(hoMD)); n != 1 {
						t.Fatalf("at %s: %d Instances, a replacement before its planned time %s", rg.clock.Now().Format(time.TimeOnly), n, tc.want.Format(time.TimeOnly))
					}
				})
				hoStepTo(rg, tc.want, 10*time.Second, nil)
			}
			r := hoRepl(rg, p.Name)
			if len(r) != 1 || !r[0].CreationTimestamp.Time.Equal(tc.want) {
				t.Fatalf("replacements %+v, want one created at %s", r, tc.want.Format(time.TimeOnly))
			}
			// Never twice: one replacement, one more job, up to the drain start.
			hoStepTo(rg, hoT(9, 50, 10), time.Minute, func() { hoAtMostOneReplacement(rg) })
			if n, s := len(rg.instances(hoMD)), rg.submissions(); n != 2 || s != 2 {
				t.Fatalf("at 09:50:10: %d Instances and %d sbatch calls, want 2 and 2", n, s)
			}
			hoStepTo(rg, hoT(9, 55, 0), time.Minute, func() { hoAtMostOneReplacement(rg) })
		})
	}
}

// TestHandoverShortQueueReplacementReadyAboutTheMarginBeforeDrain covers: on a
// short queue the replacement is ready about the margin before the drain
// starts. The only wait is the primary's 0 s, so the replacement is created at
// 09:50:20 - (0 + 50 s + 30 min) = 09:19:30, its job is submitted and starts at
// the next step, 09:19:40, and it is ready 50 s later, 09:20:30: 29m50s before
// the drain start, about the 30 min margin.
func TestHandoverShortQueueReplacementReadyAboutTheMarginBeforeDrain(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	p := hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 20, 40), 10*time.Second, nil)
	r := hoRepl(rg, p.Name)
	if len(r) != 1 {
		t.Fatalf("want one replacement, got %d", len(r))
	}
	hoWantTime(t, "replacement creation", &r[0].CreationTimestamp, hoT(9, 19, 30))
	a, _ := rg.allocation(r[0].Spec.AllocationRef.Name)
	if a == nil {
		t.Fatal("the replacement has no Allocation")
	}
	hoWantTime(t, "replacement's job start", a.Status.AtProvider.StartTime, hoT(9, 19, 40))
	hoWantTime(t, "replacement ready", r[0].Status.ReadyTime, hoT(9, 20, 30))
	lead := hoT(9, 50, 20).Sub(r[0].Status.ReadyTime.Time)
	if margin := 30 * time.Minute; lead > margin || lead < margin-time.Minute {
		t.Fatalf("replacement ready %s before the drain start, want about the %s margin", lead, margin)
	}
}

// TestHandoverBothServeThenPrimaryDrainsAndSecondReplacementIsPlanned covers:
// both serve, then the primary drains at its drain start, not earlier, and its
// replacement's own replacement is planned the same way. The first replacement
// R1 is created 09:19:30, starts 09:19:40 (hard end 13:19:40, drain start
// 11:09:40) and is ready 09:20:30. Both serve until 09:50:20, when the primary
// drains (no request runs, so it ends at once) and R1 becomes the primary. The
// waits are the two jobs' 0 s, start-to-ready 50 s again (R1's, 09:19:40 to
// 09:20:30), so R2 is planned at 11:09:40 - (0 + 50 s + 30 min) = 10:38:50.
func TestHandoverBothServeThenPrimaryDrainsAndSecondReplacementIsPlanned(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	p := hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 20, 40), 10*time.Second, nil)
	hoStepTo(rg, hoT(9, 50, 10), time.Minute, func() {
		hoAtMostOneReplacement(rg)
		if n := rg.readyCount(hoMD); n != 2 || hoPhase(rg, p.Name) != v1alpha1.InstanceReady {
			t.Fatalf("at %s: %d ready, primary %s: both must serve until the drain start", rg.clock.Now().Format(time.TimeOnly), n, hoPhase(rg, p.Name))
		}
	})
	r1 := hoRepl(rg, p.Name)[0]
	if r1.Spec.DeploymentID != p.Spec.DeploymentID {
		t.Fatalf("replacement ID %s, primary ID %s: same class, same ID", r1.Spec.DeploymentID, p.Spec.DeploymentID)
	}
	// At its drain start, 09:50:20, the primary drains; it ends on the first
	// report taken after the drain began, the agent's at 09:50:30, which shows
	// no request running (a report from before the drain never ends it).
	hoStepTo(rg, hoT(9, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the primary is %s, want draining", ph)
	}
	hoStepTo(rg, hoT(9, 50, 30), 10*time.Second, nil)
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("at 09:50:30 the primary is %s/%s, want ended/Drained", got.Status.Phase, got.Status.EndReason)
	}
	if n, c := len(rg.rec.Find("replaced", "HandedOver")), len(rg.rec.Find("degraded", "CapacityDropped")); n != 1 || c != 0 {
		t.Fatalf("HandedOver events %d (want 1), CapacityDropped events %d (want 0)", n, c)
	}
	if n := rg.readyCount(hoMD); n != 1 || len(hoRepl(rg, r1.Name)) != 0 {
		t.Fatalf("ready %d, R1's replacements %d: want R1 serving and no R2 yet", n, len(hoRepl(rg, r1.Name)))
	}
	hoWantTime(t, "R1 leaseEnd", rg.instance(r1.Name).Status.LeaseEnd, hoT(13, 19, 40))
	if s := rg.md(hoMD).Status.StartToReady; len(s) != 1 || s[0].Duration != "50s" {
		t.Fatalf("start-to-ready should be 50s, got %+v", s)
	}
	hoStepTo(rg, hoT(10, 38, 40), time.Minute, func() {
		if n := len(hoRepl(rg, r1.Name)); n != 0 {
			t.Fatalf("at %s: R2 exists before its planned time 10:38:50", rg.clock.Now().Format(time.TimeOnly))
		}
	})
	hoStepTo(rg, hoT(10, 38, 50), 10*time.Second, nil)
	r2 := hoRepl(rg, r1.Name)
	if len(r2) != 1 || !r2[0].CreationTimestamp.Time.Equal(hoT(10, 38, 50)) || r2[0].Spec.DeploymentID != r1.Spec.DeploymentID {
		t.Fatalf("R1's replacements %+v, want one created at 10:38:50 with R1's ID", r2)
	}
	hoAtMostOneReplacement(rg)
}

// TestHandoverFailedReplacementPredecessorServesUntilItsDrainStart covers: the
// replacement fails verification, so the predecessor keeps serving until its
// hard end minus the drain window (09:50:20), not earlier, then drains; no
// request is cut at the hard end; a degraded event fires. The replacement's
// engine reports max-model-len 16384 against the declared 32768, so it fails
// at once and provisioning in the class stops. The predecessor's engine runs 4
// requests from 09:50:20 to 10:10:00: it stays draining, and ends Drained at
// 10:10:00, long before its hard end 12:00:20. This is a handover whose
// replacement is never ready before the drain start.
func TestHandoverFailedReplacementPredecessorServesUntilItsDrainStart(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	busyFrom, busyTo := hoT(9, 50, 20), hoT(10, 10, 0)
	rg.engine = func(alloc string) afake.Engine {
		if alloc != rg.instances(hoMD)[0].Spec.AllocationRef.Name {
			return afake.Engine{Verify: afake.VerifyMaxModelLen("16384")}
		}
		return afake.Engine{Health: func(now time.Time, proc afake.Process) agentapi.Health {
			h := afake.DefaultHealth(now, proc)
			if !now.Before(busyFrom) && now.Before(busyTo) {
				h.RunningRequests, h.GeneratedTokens = 4, now.Unix()
			}
			return h
		}}
	}
	p := hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 30), 10*time.Second, nil)
	r1 := hoRepl(rg, p.Name)[0]
	hoStepTo(rg, hoT(9, 21, 0), 10*time.Second, nil)
	recs := hoVerificationRecords(rg, r1.UID)
	if len(recs) != 1 {
		t.Fatalf("want one verification record for the replacement, got %d", len(recs))
	}
	if d, found, err := rg.r.readVerification(rg.ctx, rigNS, recs[0].Name); err != nil || !found || d.Verdict != v1alpha1.VerdictFail || len(d.Contradictions) == 0 {
		t.Fatalf("the replacement's record %+v (found %v, err %v): want a failed verdict with a contradiction", d, found, err)
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("Degraded = %s/%s, want True/VerificationFailed (the class is stopped)", c.Status, c.Reason)
	}
	hoStepTo(rg, hoT(9, 50, 10), time.Minute, func() {
		if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceReady {
			t.Fatalf("at %s the predecessor is %s, want ready until 09:50:20", rg.clock.Now().Format(time.TimeOnly), ph)
		}
		if s, c := rg.submissions(), len(rg.rec.Find("degraded", "CapacityDropped")); s != 2 || c != 0 {
			t.Fatalf("at %s: %d sbatch calls (want 2: provisioning stopped), %d CapacityDropped events (want 0)", rg.clock.Now().Format(time.TimeOnly), s, c)
		}
	})
	hoStepTo(rg, hoT(9, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the predecessor is %s, want draining (4 requests run)", ph)
	}
	if n := len(rg.rec.Find("degraded", "CapacityDropped")); n != 1 {
		t.Fatalf("CapacityDropped events at the drain start = %d, want 1", n)
	}
	hoStepTo(rg, hoT(10, 9, 50), time.Minute, func() {
		if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
			t.Fatalf("at %s the predecessor is %s, want draining while requests run", rg.clock.Now().Format(time.TimeOnly), ph)
		}
	})
	hoStepTo(rg, hoT(10, 10, 0), 10*time.Second, nil)
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("at 10:10:00 the predecessor is %s/%s, want ended/Drained (before its hard end 12:00:20)", got.Status.Phase, got.Status.EndReason)
	}
	c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonVerificationFailed || !strings.Contains(c.Message, v1alpha1.ReasonCapacityDropped) {
		t.Fatalf("Degraded = %s %q, want VerificationFailed and the message to list CapacityDropped", c.Reason, c.Message)
	}
	if s := rg.submissions(); s != 2 || rg.readyCount(hoMD) != 0 {
		t.Fatalf("%d sbatch calls and %d ready, want 2 and 0", s, rg.readyCount(hoMD))
	}
}

// TestHandoverLateReplacementDegradesThenTakesOver covers: the replacement's
// Allocation has not started by the predecessor's drain start minus the class's
// start-to-ready time, so ReplacementLate is set; the primary still drains at
// its drain start and capacity drops. R1 is created at 09:50:20 - (0 + 50 s +
// 30 min) = 09:19:30 and submitted at the next provider pass, 09:19:40; its job
// (the second submission) waits 3 h in the queue, so it starts 12:19:40 (hard
// end 16:19:40) and is not ready before the primary's drain start. Lateness is
// 09:50:20 - 50 s = 09:49:30. The primary drains at 09:50:20. Later R1 serves
// and becomes the primary; its waits are 0 s and 3 h, so its replacement's plan,
// 14:09:40 - (3 h + 50 s + 30 min) = 10:38:50, is before R1's start and R2 is
// created when R1's Allocation starts, 12:19:40.
func TestHandoverLateReplacementDegradesThenTakesOver(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 3 * time.Hour})
	p := hoStart(rg, nil)
	notLate := func() {
		if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status == metav1.ConditionTrue {
			t.Fatalf("at %s Degraded = %s/%s before the lateness deadline 09:49:30", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason)
		}
	}
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, notLate)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, notLate)
	hoStepTo(rg, hoT(9, 49, 20), time.Minute, notLate)
	hoStepTo(rg, hoT(9, 49, 30), 10*time.Second, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonReplacementLate {
		t.Fatalf("at 09:49:30 Degraded = %s/%s, want True/ReplacementLate", c.Status, c.Reason)
	}
	hoStepTo(rg, hoT(9, 50, 10), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceReady {
		t.Fatalf("at 09:50:10 the primary is %s, want ready", ph)
	}
	// Draining from 09:50:20, on time; ended on the first report after it.
	hoStepTo(rg, hoT(9, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the primary is %s, want draining on time", ph)
	}
	hoStepTo(rg, hoT(9, 50, 30), 10*time.Second, nil)
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("at 09:50:30 the primary is %s/%s, want drained", got.Status.Phase, got.Status.EndReason)
	}
	if c, n := rg.condition(hoMD, v1alpha1.ConditionDegraded), len(rg.rec.Find("degraded", "CapacityDropped")); c.Reason != v1alpha1.ReasonCapacityDropped || n != 1 {
		t.Fatalf("Degraded reason %s and %d CapacityDropped events, want CapacityDropped and 1", c.Reason, n)
	}
	r1 := hoRepl(rg, p.Name)[0]
	// R1's job is eligible when the provider first syncs after R1's
	// Allocation is created at 09:19:30 (hold mode lifts the hold in that
	// pass, 09:19:40) and starts 3 h later, 12:19:40; the loop reads the start
	// and checks it against that.
	var r1Start time.Time
	hoStepTo(rg, hoT(12, 30, 0), 10*time.Second, func() {
		a, ok := rg.allocation(r1.Spec.AllocationRef.Name)
		if r1Start.IsZero() && ok && a.Status.AtProvider.StartTime != nil {
			r1Start = a.Status.AtProvider.StartTime.Time
			if a.Status.AtProvider.SubmitTime == nil || a.Status.AtProvider.StartTime.Sub(a.Status.AtProvider.SubmitTime.Time) < 3*time.Hour {
				t.Fatalf("R1's job started %s after its submission, want at least the scripted 3 h", a.Status.AtProvider.StartTime.Sub(a.Status.AtProvider.SubmitTime.Time))
			}
		}
		if r1Start.IsZero() {
			if n, s := rg.readyCount(hoMD), rg.submissions(); n != 0 || s != 2 {
				t.Fatalf("at %s: %d ready, %d sbatch calls, want 0 and 2 while R1 queues", rg.clock.Now().Format(time.TimeOnly), n, s)
			}
			if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionTrue {
				t.Fatalf("at %s the deployment is not degraded while capacity is down", rg.clock.Now().Format(time.TimeOnly))
			}
		}
	})
	if r1Start.IsZero() {
		t.Fatal("R1's job never started")
	}
	if want := hoT(12, 19, 40); !r1Start.Equal(want) {
		t.Fatalf("R1's job started at %s, want %s (submitted 09:19:40 + 3 h)", r1Start.Format(time.TimeOnly), want.Format(time.TimeOnly))
	}
	if got := rg.instance(r1.Name); got.Status.Phase != v1alpha1.InstanceReady || got.Status.ReadyTime == nil || got.Status.ReadyTime.Sub(r1Start) > time.Minute {
		t.Fatalf("R1 is %s (ready at %v), want ready within a minute of its start %s", got.Status.Phase, got.Status.ReadyTime, r1Start.Format(time.TimeOnly))
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("Degraded = %s/%s once R1 serves, want False", c.Status, c.Reason)
	}
	if n := len(rg.rec.Find("degraded", v1alpha1.ReasonReplacementLate)); n != 1 {
		t.Fatalf("ReplacementLate events = %d, want one", n)
	}
	// R1's replacement: the Provider's largest wait is now R1's own (over 3 h),
	// so the plan, R1's drain start minus (3 h + start-to-ready + 30 min),
	// falls before R1's start and is held to it.
	r2 := hoRepl(rg, r1.Name)
	if len(r2) != 1 || !r2[0].CreationTimestamp.Time.Equal(r1Start) {
		t.Fatalf("R1's replacements %+v, want one created at R1's start %s", r2, r1Start.Format(time.TimeOnly))
	}
}

// TestHandoverLeaseEndExtensionMovesDrainStart covers Lease end: every observe
// copies the Allocation's hard end into the Instance's status.leaseEnd, and a
// later hard end in the fake is copied and moves the drain start. At 08:10:00
// an administrator raises the primary's limit from 4 h to 6 h: the hard end
// moves from 12:00:20 to 14:00:20 (start 08:00:20 + 6 h) and the drain start
// from 09:50:20 to 11:50:20, 2 h later. The replacement is planned at
// 11:50:20 - (0 + 50 s + 30 min) = 11:19:30, and the primary drains at 11:50:20.
func TestHandoverLeaseEndExtensionMovesDrainStart(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	p := hoStart(rg, nil)
	hoStepTo(rg, hoT(8, 10, 0), time.Minute, nil)
	alloc, _ := rg.allocation(p.Spec.AllocationRef.Name)
	hoWantTime(t, "leaseEnd", rg.instance(p.Name).Status.LeaseEnd, hoT(12, 0, 20))
	rg.batch.SetTimeLimit(alloc.Annotations[v1alpha1.AnnotationJobID], 6*time.Hour)
	hoStepTo(rg, hoT(8, 10, 10), 10*time.Second, nil)
	hoWantTime(t, "leaseEnd after the extension", rg.instance(p.Name).Status.LeaseEnd, hoT(14, 0, 20))
	hoStepTo(rg, hoT(11, 19, 20), time.Minute, func() {
		if n := len(rg.instances(hoMD)); n != 1 {
			t.Fatalf("at %s: %d Instances, the replacement is planned for 11:19:30", rg.clock.Now().Format(time.TimeOnly), n)
		}
	})
	hoStepTo(rg, hoT(11, 19, 30), 10*time.Second, nil)
	if r := hoRepl(rg, p.Name); len(r) != 1 || !r[0].CreationTimestamp.Time.Equal(hoT(11, 19, 30)) {
		t.Fatalf("replacements %+v, want one created at 11:19:30", r)
	}
	hoStepTo(rg, hoT(11, 50, 10), time.Minute, func() {
		if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceReady {
			t.Fatalf("at %s the primary is %s, want ready (the old drain start 09:50:20 has passed)", rg.clock.Now().Format(time.TimeOnly), ph)
		}
	})
	hoStepTo(rg, hoT(11, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 11:50:20 the primary is %s, want draining at the moved drain start", ph)
	}
	hoStepTo(rg, hoT(11, 50, 30), 10*time.Second, nil)
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("at 11:50:30 the primary is %s/%s, want drained on the first report after the drain began", got.Status.Phase, got.Status.EndReason)
	}
}

// TestHandoverSameClassSameIDAndRollout covers Same class, same ID. Without a
// change the replacement carries the primary's ID, and the deployment's
// activeIDs lists that one ID with two Instances while both live. After
// serving-spec input flag dtype=bfloat16 is added at 08:10:00, the replacement
// (created 09:19:30) carries the new current ID while the primary keeps its
// own; activeIDs lists both IDs, one Instance each, from the replacement's
// creation until the primary ends at 09:50:20, and then only the new one.
func TestHandoverSameClassSameIDAndRollout(t *testing.T) {
	for _, change := range []bool{false, true} {
		name := "no change"
		if change {
			name = "serving spec changed"
		}
		t.Run(name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			p := hoStart(rg, nil)
			if change {
				hoStepTo(rg, hoT(8, 10, 0), time.Minute, nil)
				rg.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) {
					md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "dtype", Value: "bfloat16"})
				})
			}
			hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
			hoStepTo(rg, hoT(9, 30, 0), 10*time.Second, nil)
			r1 := hoRepl(rg, p.Name)[0]
			active := rg.md(hoMD).Status.ActiveIDs
			if !change {
				if r1.Spec.DeploymentID != p.Spec.DeploymentID || len(active) != 1 || active[0].ID != p.Spec.DeploymentID || active[0].Instances != 2 {
					t.Fatalf("replacement ID %s, primary %s, activeIDs %+v: want one ID with 2 Instances", r1.Spec.DeploymentID, p.Spec.DeploymentID, active)
				}
				return
			}
			cur := rg.md(hoMD).Status.CurrentIDs
			if r1.Spec.DeploymentID == p.Spec.DeploymentID || len(cur) != 1 || cur[0].ID != r1.Spec.DeploymentID {
				t.Fatalf("replacement ID %s, primary ID %s, currentIDs %+v: want the new current ID", r1.Spec.DeploymentID, p.Spec.DeploymentID, cur)
			}
			if got := rg.instance(p.Name).Spec.DeploymentID; got != p.Spec.DeploymentID {
				t.Fatalf("the primary's ID changed to %s", got)
			}
			if !strings.Contains(strings.Join(r1.Spec.Launch.Command, " "), "bfloat16") || strings.Contains(strings.Join(p.Spec.Launch.Command, " "), "bfloat16") {
				t.Fatalf("launch commands: replacement %v, primary %v: only the replacement runs with dtype", r1.Spec.Launch.Command, p.Spec.Launch.Command)
			}
			if len(active) != 2 || active[0].Instances != 1 || active[1].Instances != 1 || active[0].ID == active[1].ID {
				t.Fatalf("activeIDs %+v, want both IDs with one Instance each while both live", active)
			}
			hoStepTo(rg, hoT(9, 51, 0), 10*time.Second, nil)
			if active = rg.md(hoMD).Status.ActiveIDs; len(active) != 1 || active[0].ID != r1.Spec.DeploymentID {
				t.Fatalf("activeIDs %+v after the primary ended, want only the new ID", active)
			}
		})
	}
}
