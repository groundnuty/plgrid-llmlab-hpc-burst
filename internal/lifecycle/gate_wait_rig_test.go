package lifecycle

// The gate judges an Instance's first qualification and its retry only (HBEP-7, The throughput gate; Test plan, Only the
// first qualification and its retry; #616, PR 2): once its verdict is set, a later warm-up, after any restart, only
// updates status.throughput. A restart never releases an Allocation for throughput. RecordOnly is as final as the
// rest, even when its cause later clears (ruled by team-lead, 16 Nov 2026, with no change to HBEP-7): re-judging would
// gate an Instance that has served for hours, a late release the owner's concern rules out.
//
// A ready Instance restarted for a malfunction is gate_rig_test.go's TestGateJudgesTheFirstQualificationOnly. The rows
// here are the Instances that have no ready time: a waiting candidate (an extra, X, that passed its checks before its
// peer was ready and waits at its ranking point), on the fixture of rank_replacement_test.go: no wait observed on A, so P
// has a live replacement R from 00:10 and stays chosen; X's job starts at 00:15 and X passes at 00:35 (a 19 min delay),
// before P is ready at 00:40. At 00:36 X's worker exits, and X restarts at rung 1; its worker comes back after 19 min
// again, and X passes a second time at about 00:56, with 41 ms, at which point P (ready, with R live) is no peer, and X
// is ranked out. 41 ms is over the entry's 40 ms limit.

import (
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rkEntries installs the profile's entries, which the pass reads afresh every time.
func rkEntries(h *rkRig, entries ...v1alpha1.ExpectedPerformance) {
	h.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := h.c.Get(h.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		h.t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = entries
	if err := h.c.Update(h.ctx, &prof); err != nil {
		h.t.Fatal(err)
	}
}

// rkWaitingCandidate runs the fixture to the point where X waits, passed, at 00:35 with the verdict it was judged with,
// installing the entries at the start unless early is false (then X is judged with no entry). It returns the rig and
// the names. X measures 20 ms; its restarted worker will measure 41 ms.
func rkWaitingCandidate(t *testing.T, entryAtStart bool) (h *rkRig, p, x string) {
	t.Helper()
	h = newRkRig(t, true)
	p, x = rkReplacementStart(h, rkXWait, pfake.Script{})
	if entryAtStart {
		rkEntries(h, gateEntryFor(h.mustInstance(p).Spec.DeploymentID.Display()))
	}
	h.engineOf(p, rk200, 0)
	h.engineOf(x, rk200, 19*time.Minute) // 08:15:20 + 19:00 + 40 s = 08:35:00
	h.stepTo(rkAt(0, 35, 10), 10*time.Second, nil)
	rkCalibration(t, h, p, x)
	xi := h.mustInstance(x)
	if !passedChecks(xi) || xi.Status.Phase == v1alpha1.InstanceReady || ended(xi) || xi.Status.Gate == nil || xi.Status.Gate.Verdict == "" {
		t.Fatalf("X at 00:35:10: phase %s, passed %v, gate %+v, want it waiting, passed, with a verdict", xi.Status.Phase, passedChecks(xi), xi.Status.Gate)
	}
	return h, p, x
}

// rkRestartedMeasuresOver restarts X's worker (its part exits at 00:36) and steps until X has passed a second time and
// ended, with the worker measuring 41 ms now.
func rkRestartedMeasuresOver(h *rkRig, x string) *v1alpha1.Instance {
	h.t.Helper()
	h.tpot[h.alloc(x)] = 41 * time.Millisecond
	h.agentOf(h.alloc(x)).agent.CrashWorker("worker-0", 1)
	h.until(45*time.Minute, 10*time.Second, "X passed again, or ended", func() bool {
		xi := h.mustInstance(x)
		return ended(xi) || (xi.Status.Throughput != nil && xi.Status.Throughput.TimePerOutputToken == "41ms")
	})
	// A candidate whose worker came back waits for its tunnel as it did at its start, and is then ranked: it ends
	// RankedOut against no peer (P has a live replacement), or, were the gate to judge it again, QualificationFailed.
	h.until(30*time.Minute, 10*time.Second, "X ended", func() bool { return ended(h.mustInstance(x)) })
	return h.mustInstance(x)
}

// A waiting candidate that passed its checks, judged Passed against the entry, whose part exits and which is restarted
// and measures 41 ms: it is not gated again. Its verdict stays Passed, it is not restarted a second time and not
// failed, its status.throughput records the 41 ms, and it is ranked out against no peer when it passes again.
func TestGateDoesNotJudgeAWaitingCandidateAgainAfterItRestarts(t *testing.T) {
	h, _, x := rkWaitingCandidate(t, true)
	if g := h.mustInstance(x).Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || len(g.Misses) != 0 {
		t.Fatalf("X's gate %+v at 00:35, want Passed with no miss", g)
	}
	xi := rkRestartedMeasuresOver(h, x)
	if sig := gateRestartSignals(xi); len(sig) != 1 || sig[0] != "worker-0/PartExited" {
		t.Errorf("X's restarts %v, want the one rung-1 restart of its part's exit: the gate restarted nothing", sig)
	}
	if g := xi.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || len(g.Misses) != 0 {
		t.Errorf("X's gate %+v after 41 ms, want the Passed verdict kept with no miss recorded", g)
	}
	if xi.Status.EndReason != v1alpha1.ReasonRankedOut || xi.Status.Throughput.TimePerOutputToken != "41ms" {
		t.Errorf("X ended for %q with throughput %+v, want RankedOut and its 41 ms recorded: not QualificationFailed", xi.Status.EndReason, xi.Status.Throughput)
	}
}

// An Instance judged RecordOnly with no entry stays RecordOnly (a RecordOnly verdict is final): X passes at 00:35 with
// no entry, then an entry it would miss is added at 00:36, the entry's date 10 days before the clock, then its worker
// exits and it measures 41 ms after the rung-1 restart. No restart for throughput, no failure, and the verdict stays
// RecordOnly: the entry is not read again.
func TestGateRecordOnlyIsFinalForAWaitingCandidate(t *testing.T) {
	h, p, x := rkWaitingCandidate(t, false)
	if g := h.mustInstance(x).Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly {
		t.Fatalf("X's gate %+v at 00:35 with no entry, want RecordOnly", g)
	}
	rkEntries(h, gateEntryFor(h.mustInstance(p).Spec.DeploymentID.Display()))
	xi := rkRestartedMeasuresOver(h, x)
	if sig := gateRestartSignals(xi); len(sig) != 1 || sig[0] != "worker-0/PartExited" {
		t.Errorf("X's restarts %v, want the one rung-1 restart of its part's exit: the entry added later gates nothing", sig)
	}
	if g := xi.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || len(g.Misses) != 0 {
		t.Errorf("X's gate %+v after 41 ms, want RecordOnly kept with no miss", g)
	}
	if xi.Status.EndReason != v1alpha1.ReasonRankedOut {
		t.Errorf("X ended for %q, want RankedOut: not QualificationFailed", xi.Status.EndReason)
	}
}

// The same row on a ready Instance: judged RecordOnly with no entry and ready; an entry it misses is added; a rung-1
// restart's warm-up measures 41 ms. No restart for throughput, no failure, and the verdict stays RecordOnly.
func TestGateRecordOnlyIsFinalForAReadyInstance(t *testing.T) {
	k := &gateKnob{tpot: 20 * time.Millisecond, ttft: 300 * time.Millisecond}
	rg := newRig(t)
	rg.engine = gateEngineOf(k)
	name, id := gateEntries(rg, gateMD(), func(string) []v1alpha1.ExpectedPerformance { return nil })
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	if g := rg.instance(name).Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly {
		t.Fatalf("status.gate %+v, want RecordOnly with no entry", g)
	}
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = []v1alpha1.ExpectedPerformance{gateEntryFor(id)}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	k.tpot = 41 * time.Millisecond // what the restarted worker's warm-up measures
	rg.agentOf(rg.instance(name).Spec.AllocationRef.Name).agent.CrashWorker("worker-0", 1)
	startUntil(rg, "md", 10*time.Minute, "the restarted Instance ready with 41 ms", func() bool {
		in := rg.instance(name)
		return len(gateRestarts(rg, name)) >= 1 && in.Status.Phase == v1alpha1.InstanceReady && in.Status.Throughput != nil && in.Status.Throughput.TimePerOutputToken == "41ms"
	})
	got := rg.instance(name)
	if sig := gateRestartSignals(got); len(sig) != 1 || sig[0] != "worker-0/PartExited" {
		t.Errorf("restarts %v, want the one rung-1 restart of the part's exit", sig)
	}
	if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || len(g.Misses) != 0 || got.Status.EndReason != "" {
		t.Errorf("status.gate %+v, end reason %q, want RecordOnly kept with no miss and no end", g, got.Status.EndReason)
	}
}
