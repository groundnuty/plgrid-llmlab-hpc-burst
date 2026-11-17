package lifecycle

// HBEP-7's Test plan row "A hedge's started loser" (Over-allocation and qualification (beta), Hedging's started losers;
// Hedging (beta), Started losers are kept; #616, PR 5), through the pass, with the hedging fixture's two Providers A
// and B and the ranking rows' measurements.
//
// The shared timeline, worked out by hand from handover_test.go's header and rank_rig_test.go's: P starts at 08:00:20
// and is ready at 08:01:10, lease end 12:00:20, drain start 09:50:20; with no wait observed on A its replacement R1 is
// created at 09:19:30, the member on A; B's member R2 is SEEDED with seedHedgeMember (no hedge time is worked out:
// hedgeWidth is 1, so growth adds no member) at 09:19:40. R1's job starts at the provider pass of 09:19:40 and its
// engine starts the worker after klDelay, 10 min, so R1 reaches its ranking point at 09:19:40 + 10:00 + 0:40 =
// 09:30:20. R2's job starts at the pass of 09:19:50 and its Instance is ready 50 s later, 09:20:40: the win, with R1
// still verifying (the rows' "00:30", "00:55" and "01:05" are 09:19:40, 09:20:40 and 09:30:20 on the rig's clock).
// The deployment's overAllocation is 0 until the win: the rig's deployment is created at k = 0, and set to 1 after P is
// ready, so PR 4 makes no extra beside P, and each row says what A holds at the win. A measurement is the time per
// output token the fake engine reports (rkWarmUp), so 20 ms, 18.0 ms and 17.9 ms are exact; P, which no ranking here
// compares with, measures a slower 40 ms.

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// klDelay is the StartDelay of the loser's engine: its ranking point comes 10 min 40 s after its job starts.
const klDelay = 10 * time.Minute

// klClassA is the accelerator class of Provider A's partition.
var klClassA = v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}

// klRig is the hedging fixture with a time per output token, an engine StartDelay and a corruption verdict per
// Allocation, all read when an Allocation's agent first appears. An Allocation with no entry measures 40 ms.
type klRig struct {
	*hedgeRig
	tpot    map[string]time.Duration
	delay   map[string]time.Duration
	corrupt map[string]int
}

func newKlRig(t *testing.T) *klRig {
	h := &klRig{hedgeRig: newHedgeRig(t), tpot: map[string]time.Duration{}, delay: map[string]time.Duration{}, corrupt: map[string]int{}}
	h.rig.engine = func(alloc string) afake.Engine {
		tp, ok := h.tpot[alloc]
		if !ok {
			tp = 40 * time.Millisecond
		}
		e := afake.Engine{StartDelay: h.delay[alloc], WarmUpRound: rkWarmUp(tp)}
		if n, ok := h.corrupt[alloc]; ok {
			e.GreedyOutputs = afake.GreedyDivergingAt(n) // the three outputs differ before the 32nd token: the check fails
		}
		return e
	}
	return h
}

// klMD is HBEP-7's deployment on A then B, hedgeWidth 1 (seededOnly), with n instances and k = overAllocation.
func klMD(n, k int32) func(*v1alpha1.ModelDeployment) {
	return func(md *v1alpha1.ModelDeployment) {
		seededOnly(md)
		md.Spec.Instances = n
		md.Spec.OverAllocation = ptrTo(k)
		md.Spec.Workload = &v1alpha1.Workload{
			ContextTokens: v1alpha1.ContextTokens{Typical: 1024}, OutputTokens: v1alpha1.OutputTokens{Typical: 128}, TargetSessions: 8,
		}
		md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "max-num-seqs", Value: "16"})
	}
}

// setK sets the deployment's overAllocation.
func (h *klRig) setK(k int32) {
	h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.OverAllocation = ptrTo(k) })
}

// uidOf is the Allocation's UID, read while it exists.
func (h *klRig) allocUID(name string) types.UID {
	h.t.Helper()
	a, ok := h.allocation(name)
	if !ok {
		h.t.Fatalf("Allocation %s is gone", name)
	}
	return a.UID
}

// outcomeOf is the start-up outcome of A's class that the Allocation (by UID) added, if any.
func (h *klRig) outcomeOf(uid types.UID) (v1alpha1.StartOutcome, bool) {
	for _, c := range h.md(hoMD).Status.StartOutcomes {
		if c.AcceleratorClass != klClassA {
			continue
		}
		for _, o := range c.Outcomes {
			if o.AllocationUID == uid {
				return o, true
			}
		}
	}
	return v1alpha1.StartOutcome{}, false
}

// classAFailures is A's class's failure count in the deployment's status.
func (h *klRig) classAFailures() int32 {
	for _, c := range h.md(hoMD).Status.StartOutcomes {
		if c.AcceleratorClass == klClassA {
			return c.Failures
		}
	}
	return 0
}

// at fails the test unless the rig's clock reads the time.
func (h *klRig) at(want time.Time) {
	h.t.Helper()
	if got := h.clock.Now().UTC(); !got.Equal(want) {
		h.t.Fatalf("the clock reads %s, want %s: the fixture's timeline is off", got.Format(time.TimeOnly), want.Format(time.TimeOnly))
	}
}

// hedgeLostEvents are the replaced HedgeLost events that regard the Instance.
func (h *klRig) hedgeLostEvents(name string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range h.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost) {
		if e.Name == name {
			out = append(out, e)
		}
	}
	return out
}

// idleSince is the Allocation's idle-since anchor.
func (h *klRig) idleSince(alloc string) (time.Time, bool) {
	a, ok := h.allocation(alloc)
	if !ok {
		return time.Time{}, false
	}
	since, found, err := v1alpha1.IdleSince(a)
	if err != nil {
		h.t.Fatal(err)
	}
	return since, found
}

// sbatches is the number of jobs submitted on A and B together.
func (h *klRig) sbatches() int { return h.batch.Count("sbatch") + h.b.batch.Count("sbatch") }

// klPrimary starts the deployment of one instance at k = 0: P is ready at 08:01:10 on A (scripts: P's, then the
// replacement R1's, both starting at once; B's member's starts at once too).
func klPrimary(t *testing.T) (*klRig, string) {
	t.Helper()
	h := newKlRig(t)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{})
	h.b.batch.Enqueue(pfake.Script{})
	reservedStart(h.rig, hoMD, klMD(1, 0))
	return h, h.instances(hoMD)[0].Name
}

// klWith is P ready at 08:01:10 on A beside what the row holds there: "extra" is a pending extra X (k = 1 from the
// creation: PR 4 makes X in the pass that makes P, and X pends behind a 10 h wait), "warm" is a warm Allocation of the
// deployment (two primaries start together, and the deployment is lowered to one with k = 1: the drained one's
// Allocation is kept, hard end 12:00:20, the same as P's, so P's replacement is not placed on it: its drain start is
// not later than P's). It returns P.
func klWith(t *testing.T, what string) (*klRig, string) {
	t.Helper()
	switch what {
	case "extra":
		h := newKlRig(t)
		h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{})
		h.b.batch.Enqueue(pfake.Script{})
		md := mdSpec(hoMD)
		klMD(1, 1)(md)
		h.create(md)
		for range 7 {
			h.clock.Advance(10 * time.Second)
			h.Step()
		}
		var p, x string
		for _, in := range h.instances(hoMD) {
			if in.Status.ReadyTime != nil {
				p = in.Name
			} else {
				x = in.Name
			}
		}
		if p == "" || x == "" || len(h.instances(hoMD)) != 2 {
			t.Fatalf("want P ready and the extra X pending on A at 08:01:10, got %+v", h.instances(hoMD))
		}
		if a, _ := h.allocation(h.mustInstance(x).Spec.AllocationRef.Name); a == nil || jobStarted(a) {
			t.Fatalf("the extra X's job started, want it pending behind its 10 h wait: %+v", a)
		}
		return h, p
	case "warm":
		h := newKlRig(t)
		h.batch.Enqueue(pfake.Script{}, pfake.Script{}, pfake.Script{})
		h.b.batch.Enqueue(pfake.Script{})
		md := mdSpec(hoMD)
		klMD(2, 0)(md)
		h.create(md)
		for range 7 {
			h.clock.Advance(10 * time.Second)
			h.Step()
		}
		if h.readyCount(hoMD) != 2 {
			t.Fatalf("%d ready at 08:01:10, want the two primaries", h.readyCount(hoMD))
		}
		h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances, md.Spec.OverAllocation = 1, ptrTo(int32(1)) })
		h.until(5*time.Minute, 10*time.Second, "one primary drained", func() bool {
			for _, in := range h.instances(hoMD) {
				if ended(&in) {
					return true
				}
			}
			return false
		})
		var p string
		for _, in := range h.instances(hoMD) {
			if ended(&in) {
				if in.Status.EndReason != v1alpha1.ReasonDrained || h.released(in.Spec.AllocationRef.Name) {
					t.Fatalf("the surplus primary ended %q, Allocation released %v: want Drained with its Allocation kept (warm)", in.Status.EndReason, h.released(in.Spec.AllocationRef.Name))
				}
			} else {
				p = in.Name
			}
		}
		return h, p
	}
	return klPrimary(t)
}

// klHedge is the replacement hedge at the win: P's replacement R1 on A (the loser, a member whose job started) and the
// seeded member R2 on B (the winner).
type klHedge struct {
	h      *klRig
	p      string
	r1, r2 string
	r1A    string // R1's Allocation
	r2A    string
	r1UID  types.UID
}

// klWin steps from P's ready time to the win, 09:20:40, in a replacement hedge: R1's engine measures tpotL and
// starts its worker after klDelay, R2's measures 20 ms. kAtWin, when not nil, is the deployment's overAllocation from
// just before the win. R1 is not ready at the win, and its job started at 09:19:40.
func klWin(t *testing.T, h *klRig, p string, tpotL time.Duration, kAtWin *int32, corrupt bool) *klHedge {
	t.Helper()
	hoStepTo(h.rig, hoT(9, 19, 30), 5*time.Minute, nil)
	reps := hoRepl(h.rig, p)
	if len(reps) != 1 || reps[0].Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("P's replacements at 09:19:30: %+v, want one, on A", reps)
	}
	w := &klHedge{h: h, p: p, r1: reps[0].Name, r1A: reps[0].Spec.AllocationRef.Name}
	h.delay[w.r1A], h.tpot[w.r1A] = klDelay, tpotL
	if corrupt {
		h.corrupt[w.r1A] = 5
	}
	h.clock.Advance(10 * time.Second)
	h.Step() // 09:19:40: R1's job starts
	if a, ok := h.allocation(w.r1A); !ok || a.Status.AtProvider.StartTime == nil || !a.Status.AtProvider.StartTime.Time.Equal(hoT(9, 19, 40)) {
		t.Fatalf("R1's job did not start at 09:19:40: %+v", a)
	}
	w.r1UID = h.allocUID(w.r1A)
	w.r2 = h.seedHedgeMember(hoMD, w.r1, p, hedgeB)
	w.r2A = h.mustInstance(w.r2).Spec.AllocationRef.Name
	h.tpot[w.r2A] = 20 * time.Millisecond
	if kAtWin != nil {
		h.setK(*kAtWin)
	}
	h.until(5*time.Minute, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(w.r2).Status.Phase == v1alpha1.InstanceReady })
	h.at(hoT(9, 20, 40))
	if in := h.mustInstance(w.r1); in.Status.Phase == v1alpha1.InstanceReady || in.Status.ReadyTime != nil {
		t.Fatalf("R1 is %s at the win, want it still verifying: the fixture proves nothing about keeping", in.Status.Phase)
	}
	return w
}

// onA is what A holds at the win, as the objects say: the deployment's live Instances and Allocations on A.
func (w *klHedge) onA() (insts, allocs []string) {
	for _, in := range w.h.onProvider(hoMD, rigProvider) {
		insts = append(insts, in.Name)
	}
	for _, a := range w.h.allAllocations() {
		if a.Labels[v1alpha1.LabelProvider] == rigProvider && a.DeletionTimestamp.IsZero() {
			allocs = append(allocs, a.Name)
		}
	}
	return insts, allocs
}

// keptAtTheWin asserts a loser kept at the win: not ended, marked with its winner, Allocation kept, no HedgeLost.
func (w *klHedge) keptAtTheWin() {
	t, h := w.h.t, w.h
	t.Helper()
	l := h.mustInstance(w.r1)
	if ended(l) || l.Annotations[annotationCandidateOf] != w.r2 {
		t.Fatalf("R1 is %s (end %q) with candidate-of %q at the win, want it kept as R2's candidate", l.Status.Phase, l.Status.EndReason, l.Annotations[annotationCandidateOf])
	}
	if h.released(w.r1A) {
		t.Fatal("R1's Allocation was released at the win")
	}
	if n := len(h.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost)); n != 0 {
		t.Fatalf("%d HedgeLost events at the win, want none: R1 is kept", n)
	}
	// What A holds, so the case is the one it names: P's Allocation and R1's, each hosting a live Instance, and
	// nothing else of the deployment: no surplus Instance and no warm Allocation.
	insts, allocs := w.onA()
	if len(insts) != 2 || len(allocs) != 2 {
		t.Fatalf("A holds Instances %v and Allocations %v at the win, want P's and R1's alone", insts, allocs)
	}
}

// endsAtTheWin asserts a loser ended at the win, as on main: HedgeLost at 09:20:40, no mark, its Allocation kept with
// idle-since 09:20:40.
func (w *klHedge) endsAtTheWin() {
	t, h := w.h.t, w.h
	t.Helper()
	l := h.mustInstance(w.r1)
	if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost || l.Annotations[annotationCandidateOf] != "" {
		t.Fatalf("R1 is %s (end %q) with candidate-of %q at the win, want ended for HedgeLost, unmarked", l.Status.Phase, l.Status.EndReason, l.Annotations[annotationCandidateOf])
	}
	if h.released(w.r1A) {
		t.Fatal("R1's started Allocation was released at the win, want it kept")
	}
	if since, ok := h.idleSince(w.r1A); !ok || !since.Equal(hoT(9, 20, 40)) {
		t.Fatalf("R1's Allocation idle-since %v (%v), want 09:20:40, the win's pass", since, ok)
	}
	if o, ok := h.outcomeOf(w.r1UID); ok {
		t.Fatalf("R1, ended at the win, added the outcome %+v: a loser ended at the win adds none", o)
	}
}

// waitsToItsPoint steps, 10 s at a time, to 09:30:10: R1 is neither ended nor ready until its ranking point.
func (w *klHedge) waitsToItsPoint() {
	h := w.h
	h.t.Helper()
	// 30 s steps while the worker has not started (09:29:40), 10 s from 09:28:40, where the checks are answered a round
	// at a time: coarse steps there would let the Instance reach its point a round early.
	check := func() {
		if in := h.mustInstance(w.r1); ended(in) || in.Status.Phase == v1alpha1.InstanceReady {
			h.t.Fatalf("R1 is %s (end %q) at %s, want it waiting for its checks until 09:30:20", in.Status.Phase, in.Status.EndReason, h.clock.Now().Format(time.TimeOnly))
		}
	}
	hoStepTo(h.rig, hoT(9, 28, 40), 30*time.Second, check)
	hoStepTo(h.rig, hoT(9, 30, 10), 10*time.Second, check)
	// R1's warm-up is judged in the pass of 09:30:10 and the agent reports it done one round later, so the Instance reaches
	// its ranking point at 09:30:20, where the loop above found it neither ended nor ready.
}

// TestAStartedLoserIsRankedAgainstItsWinnerAlone is HBEP-7's row "A hedge's started loser", in P's replacement
// hedge: R1 on A, whose job started at 09:19:40 and which still verifies at 09:20:40 when R2 on B wins, measuring
// 20 ms, is not ended; it passes at 09:30:20. Measuring 18.0 ms, not faster than 20 by more than 2 ms, it ends with
// HedgeLost, kept with idle-since 09:30:20, and adds a success to A's class: R2 serves on and P's handover goes on
// with R2. Measuring 17.9 ms, it becomes ready, and R2 drains, ends with RankedOut and is kept, while P's handover
// goes on with R1. P, the deployment's other primary on A, measures 40 ms: a comparison with it would change the
// result. (P has a live replacement here, so PR 3's rule excludes it from any peer set; the missing-primary row,
// TestAStartedLoserIsNeverComparedWithAnotherPrimary, puts a peer-eligible P beside the loser.) Members are seeded.
func TestAStartedLoserIsRankedAgainstItsWinnerAlone(t *testing.T) {
	for _, c := range []struct {
		name     string
		tpot     time.Duration
		outranks bool
	}{{"18.0 ms against 20 ms", rk180, false}, {"17.9 ms against 20 ms", rk179, true}} {
		t.Run(c.name, func(t *testing.T) {
			h, p := klPrimary(t)
			w := klWin(t, h, p, c.tpot, ptrTo(int32(1)), false)
			w.keptAtTheWin()
			w.waitsToItsPoint()
			h.clock.Advance(10 * time.Second)
			h.Step()
			h.at(hoT(9, 30, 20))
			w.resolved(c.outranks)
		})
	}
}

// toPDrain steps to 09:50:30, ten seconds after P's drain start: a minute at a time to 09:49:30, then every 10 s.
func (w *klHedge) toPDrain() {
	w.h.t.Helper()
	hoStepTo(w.h.rig, hoT(9, 49, 30), time.Minute, nil)
	hoStepTo(w.h.rig, hoT(9, 50, 30), 10*time.Second, nil)
}

// resolved asserts the loser's ranking at 09:30:20 and what follows, to P's drain start.
func (w *klHedge) resolved(outranks bool) {
	t, h := w.h.t, w.h
	t.Helper()
	l, win, p := h.mustInstance(w.r1), h.mustInstance(w.r2), h.mustInstance(w.p)
	if p.Annotations[annotationRankedOut] != "" || p.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("P is %s with ranked-out %q: it is never compared with a candidate", p.Status.Phase, p.Annotations[annotationRankedOut])
	}
	if !outranks {
		if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost || l.Status.EndTime == nil || !l.Status.EndTime.Time.Equal(hoT(9, 30, 20)) {
			t.Fatalf("R1 is %s (end %q at %v) at 09:30:20, want ended for HedgeLost at 09:30:20", l.Status.Phase, l.Status.EndReason, l.Status.EndTime)
		}
		if h.released(w.r1A) {
			t.Fatal("R1's Allocation was released: a qualified warm spare is kept")
		}
		if since, ok := h.idleSince(w.r1A); !ok || !since.Equal(hoT(9, 30, 20)) {
			t.Fatalf("R1's Allocation idle-since %v (%v), want 09:30:20", since, ok)
		}
		if o, ok := h.outcomeOf(w.r1UID); !ok || o.Result != v1alpha1.StartOutcomeReady || !o.Time.Time.Equal(hoT(9, 30, 20)) {
			t.Fatalf("R1's outcome %+v (%v), want a success at 09:30:20: it passed every check", o, ok)
		}
		if evs := h.hedgeLostEvents(w.r1); len(evs) != 1 || evs[0].RelatedName != w.r2 {
			t.Fatalf("HedgeLost events regarding R1: %+v, want one with %s related", evs, w.r2)
		}
		if win.Status.Phase != v1alpha1.InstanceReady || win.Annotations[annotationRankedOut] != "" {
			t.Fatalf("R2 is %s with ranked-out %q, want it ready and unmarked: it serves on", win.Status.Phase, win.Annotations[annotationRankedOut])
		}
		w.toPDrain()
		if ph := hoPhase(h.rig, w.p); ph != v1alpha1.InstanceDraining && ph != v1alpha1.InstanceEnded {
			t.Fatalf("P is %s at 09:50:30, want draining or ended", ph)
		}
		if in := h.mustInstance(w.r2); in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("R2 is %s at 09:50:30, want ready, serving in P's place", in.Status.Phase)
		}
		return
	}
	if l.Status.Phase != v1alpha1.InstanceReady || l.Status.ReadyTime == nil || !l.Status.ReadyTime.Time.Equal(hoT(9, 30, 20)) {
		t.Fatalf("R1 is %s (ready %v) at 09:30:20, want ready at 09:30:20", l.Status.Phase, l.Status.ReadyTime)
	}
	if win.Annotations[annotationRankedOut] != w.r1 || win.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("R2 is %s with ranked-out %q at 09:30:20, want ready, marked as outranked by %s", win.Status.Phase, win.Annotations[annotationRankedOut], w.r1)
	}
	h.clock.Advance(10 * time.Second)
	h.Step() // 09:30:30: the front's probe interval after R1 became healthy
	if ph := hoPhase(h.rig, w.r2); ph != v1alpha1.InstanceDraining && ph != v1alpha1.InstanceEnded {
		t.Fatalf("R2 is %s at 09:30:30, want draining", ph)
	}
	h.until(2*time.Minute, 10*time.Second, "R2 ended", func() bool { return ended(h.mustInstance(w.r2)) })
	if in := h.mustInstance(w.r2); in.Status.EndReason != v1alpha1.ReasonRankedOut {
		t.Fatalf("R2 ended for %q, want RankedOut", in.Status.EndReason)
	}
	if h.released(w.r2A) {
		t.Fatal("R2's Allocation was released: an outranked winner is kept under its policy")
	}
	if since, ok := h.idleSince(w.r2A); !ok || since.Before(hoT(9, 30, 30)) {
		t.Fatalf("R2's Allocation idle-since %v (%v), want it written when R2 ended, not before 09:30:30", since, ok)
	}
	var evs []RecordedEvent
	for _, e := range h.rec.Find(eventReplaced, v1alpha1.ReasonRankedOut) {
		if e.Name == w.r2 {
			evs = append(evs, e)
		}
	}
	if len(evs) != 1 || evs[0].RelatedName != w.r1 {
		t.Fatalf("RankedOut events regarding R2: %+v, want one with %s related", evs, w.r1)
	}
	if n := len(h.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost)); n != 0 {
		t.Fatalf("%d HedgeLost events, want none: R1 serves", n)
	}
	if o, ok := h.outcomeOf(w.r1UID); !ok || o.Result != v1alpha1.StartOutcomeReady || !o.Time.Time.Equal(hoT(9, 30, 20)) {
		t.Fatalf("R1's outcome %+v (%v), want a success at its ready time, 09:30:20", o, ok)
	}
	w.toPDrain()
	if ph := hoPhase(h.rig, w.p); ph != v1alpha1.InstanceDraining && ph != v1alpha1.InstanceEnded {
		t.Fatalf("P is %s at 09:50:30, want draining or ended: its handover goes on", ph)
	}
	if in := h.mustInstance(w.r1); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("R1 is %s at 09:50:30, want ready, serving in P's place", in.Status.Phase)
	}
	handedTo := false
	for _, e := range h.rec.Find(eventReplaced, v1alpha1.EventReasonHandedOver) {
		handedTo = handedTo || (e.Name == w.p && strings.Contains(e.Message, w.r1))
	}
	if !handedTo {
		t.Fatalf("no HandedOver event regarding P naming %s: P's handover must go on with A's member", w.r1)
	}
}

// TestAStartedLoserEndsAtTheWinWhenAnyOfItsConditionsFails: the same fixture as the keep row, with the one condition
// that keeps it falsified in each case, so each case shows its own condition: k = 0 (the row's R1 ends at 09:20:40
// with HedgeLost, as on main); a surplus Instance of the deployment already on A (a pending extra X, made with P at
// k = 1); a warm Allocation of the deployment already on A (a drained primary's, kept). R1 measures 17.9 ms in each:
// had it been kept it would have outranked R2.
func TestAStartedLoserEndsAtTheWinWhenAnyOfItsConditionsFails(t *testing.T) {
	for _, c := range []struct {
		name string
		what string
		kEnd *int32 // overAllocation from just before the win
	}{
		{"k is 0", "", ptrTo(int32(0))},
		{"a surplus Instance is on A", "extra", nil},
		{"a warm Allocation is on A", "warm", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, p := klWith(t, c.what)
			w := klWin(t, h, p, rk179, c.kEnd, false)
			w.endsAtTheWin()
			if win := h.mustInstance(w.r2); win.Status.Phase != v1alpha1.InstanceReady || win.Annotations[annotationRankedOut] != "" {
				t.Fatalf("R2 is %s with ranked-out %q: nothing outranks it, R1 having ended", win.Status.Phase, win.Annotations[annotationRankedOut])
			}
			h.run(15*time.Minute, time.Minute)
			if win := h.mustInstance(w.r2); win.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("R2 is %s ten minutes on, want ready", win.Status.Phase)
			}
		})
	}
}
