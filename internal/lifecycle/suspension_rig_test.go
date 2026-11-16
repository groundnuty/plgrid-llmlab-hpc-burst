package lifecycle

// A suspect entry (HBEP-7, Over-allocation and qualification (beta), The throughput gate: a suspect reference, At most
// two releases for one reference; Test plan, A suspect entry and Parallel second misses; #616, PR 2). Two Allocations in
// a row that miss an entry suspend it for the deployment, in the pass that records the second; the third records only;
// the listing is held in status, so it outlives the class's kept outcomes and a restarted controller; and a pass counts
// the second misses it has failed, so three Instances on their retry in one pass release two Allocations. The fixture is
// gate_rig_test.go's: one entry for the Instance's ID, 20 ms per output token and 400 ms to first token (limits 40 ms and
// 800 ms), dated 2026-10-23, and an engine that measures 41 ms every time, so each Instance misses twice. Every value
// is a literal worked out by hand; the history rows seed the class's outcomes as the literals they say.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// suspectKey is the fixture entry's key for the Instance's ID: max-num-seqs 16, 8 sessions, contexts of 1,024 and
// outputs of 128 tokens.
func suspectKey(id string) v1alpha1.ReferenceKey {
	k := gateKey()
	k.DeploymentID = id
	return k
}

// gateClass is the rig Provider's accelerator class, the outcomes' key.
var gateClass = v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}

// gateFailures is how many QualificationFailed events the deployment's Instances have raised.
func gateFailures(rg *rig) int {
	return len(rg.rec.Find("failing", v1alpha1.ReasonQualificationFailed))
}

// suspectEvents are the ReferenceSuspect events raised so far.
func suspectEvents(rg *rig) []RecordedEvent {
	return rg.rec.Find("degraded", v1alpha1.ReasonReferenceSuspect)
}

// seedOutcomes writes the class's outcomes into the deployment's status, as a controller that ran before would have
// left them.
func seedOutcomes(rg *rig, outcomes ...v1alpha1.StartOutcome) {
	rg.t.Helper()
	md := rg.md("md")
	md.Status.StartOutcomes = []v1alpha1.ClassStartOutcomes{{AcceleratorClass: gateClass, Outcomes: outcomes}}
	if err := rg.c.Status().Update(rg.ctx, md); err != nil {
		rg.t.Fatal(err)
	}
}

// seeded is a start-up outcome of an Allocation that is no longer there: a Failed one for QualificationFailed, or a
// Ready one, with the gate's verdict and the entry's date.
func seeded(uid string, minute int, failed bool, gate, date string) v1alpha1.StartOutcome {
	o := v1alpha1.StartOutcome{AllocationUID: types.UID(uid), Result: v1alpha1.StartOutcomeReady, Time: metav1.NewTime(hoT(7, minute, 0)), Gate: gate, ReferenceDate: date}
	if failed {
		o.Result, o.Reason = v1alpha1.StartOutcomeFailed, v1alpha1.ReasonQualificationFailed
	}
	return o
}

// suspectRig runs the fixture's deployment to the pass of the second failure, the third Instance not yet judged.
func suspectRig(t *testing.T, edit ...func(*v1alpha1.ModelDeployment)) (*rig, *causeLog, string) {
	t.Helper()
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	_, id := gateEntries(rg, gateMD(edit...), oneEntry())
	startUntil(rg, "md", time.Hour, "two Instances failed their retry", func() bool { return gateFailures(rg) == 2 })
	return rg, log, id
}

// Two Instances on two Allocations measure 41 ms twice and fail, each Allocation released after its cause. The entry is
// listed in suspendedReferences in the pass of the second failure, with its key and date, and one ReferenceSuspect event
// names it; there is no RestartLoop. The third Instance, at 41 ms, passes recording only: Suspended, ready, never
// restarted, and its outcome carries Suspended with the date. No later pass raises the event again.
func TestGateSuspendsAnEntryThatTwoAllocationsInARowMissed(t *testing.T) {
	rg, log, id := suspectRig(t)
	md := rg.md("md")
	if got := md.Status.SuspendedReferences; len(got) != 1 || got[0].Date != "2026-10-23" || !sameKey(got[0].ReferenceKey, suspectKey(id)) || got[0].SuspendedTime.IsZero() {
		t.Fatalf("suspendedReferences %+v in the pass of the second failure, want the entry dated 2026-10-23 with its key", got)
	}
	if got := md.Status.SuspendedReferences[0].ReferenceKey; got.DeploymentID != id || got.Sessions != 8 || got.ContextTokens != 1024 || got.OutputTokens != 128 || got.CapacityFlags["max-num-seqs"] != "16" {
		t.Errorf("the listed key %+v, want the entry's: %s, max-num-seqs 16, 8 sessions, 1024 and 128 tokens", got, id)
	}
	evs := suspectEvents(rg)
	if len(evs) != 1 || evs[0].Name != "md" || !strings.Contains(evs[0].Message, "2026-10-23") || !strings.Contains(evs[0].Message, "max-num-seqs=16") || !strings.Contains(evs[0].Message, "8 sessions") {
		t.Fatalf("ReferenceSuspect events %+v, want one regarding md, naming the entry's date, flags and sessions", evs)
	}
	if n := len(rg.rec.Find("degraded", v1alpha1.ReasonRestartLoop)); n != 0 {
		t.Errorf("%d RestartLoop events after two failures, want none: two stop short of three", n)
	}
	for _, in := range rg.instances("md") {
		if in.Status.EndReason == v1alpha1.ReasonQualificationFailed {
			startUntil(rg, "md", 5*time.Minute, "the failed Allocation deleted", func() bool { _, ok := log.deletedWith(in.Spec.AllocationRef.Name); return ok })
			if cause, _ := log.deletedWith(in.Spec.AllocationRef.Name); cause != "Instance "+in.Name+" failed: QualificationFailed" {
				t.Errorf("Instance %s's Allocation was deleted with the cause %q", in.Name, cause)
			}
		}
	}

	startUntil(rg, "md", time.Hour, "the third Instance ready", func() bool { return rg.readyCount("md") == 1 })
	rg.run(30*time.Minute, 10*time.Second)
	var third v1alpha1.Instance
	for _, in := range rg.instances("md") {
		if in.Status.Phase == v1alpha1.InstanceReady {
			third = in
		}
	}
	if g := third.Status.Gate; g == nil || g.Verdict != v1alpha1.GateSuspended || g.ReferenceDate != "2026-10-23" || len(g.Misses) != 0 || len(third.Status.Restarts) != 0 {
		t.Errorf("third Instance: gate %+v, restarts %v, want Suspended against 2026-10-23, no miss and no restart", g, gateRestartSignals(&third))
	}
	if msg := conditionMessage(&third, v1alpha1.ConditionQualified); !strings.Contains(msg, "throughput gate: records only: the entry dated 2026-10-23 is suspended for this deployment") {
		t.Errorf("third Instance's Qualified message %q, want the suspension named", msg)
	}
	if n := gateFailures(rg); n != 2 {
		t.Errorf("%d QualificationFailed events, want the two: a suspended entry releases nothing more", n)
	}
	if n := len(suspectEvents(rg)); n != 1 {
		t.Errorf("%d ReferenceSuspect events after 30 more minutes of passes, want the one raised when it was written", n)
	}
	outs := rg.md("md").Status.StartOutcomes
	if len(outs) != 1 || len(outs[0].Outcomes) != 3 {
		t.Fatalf("outcomes %+v, want the class's three", outs)
	}
	for i, want := range []struct{ result, gate string }{{"Failed", "Missed"}, {"Failed", "Missed"}, {"Ready", "Suspended"}} {
		if o := outs[0].Outcomes[i]; o.Result != want.result || o.Gate != want.gate || o.ReferenceDate != "2026-10-23" {
			t.Errorf("outcome %d is %+v, want %s with gate %s against 2026-10-23", i, o, want.result, want.gate)
		}
	}
}

// The listing is the record, not the outcomes: with twenty more outcomes in the class (none a miss) the two misses are
// out of startOutcomes, and the next Instance still records Suspended while the date stays. A changed date removes the
// listing in the next pass, and the Instance after that is gated again: its first miss restarts the worker.
func TestGateASuspensionOutlivesTheKeptOutcomesAndEndsWithTheDate(t *testing.T) {
	rg, _, id := suspectRig(t)
	startUntil(rg, "md", time.Hour, "the third Instance ready", func() bool { return rg.readyCount("md") == 1 })
	var outs []v1alpha1.StartOutcome
	for i := range 20 {
		outs = append(outs, seeded("filler-"+string(rune('a'+i)), i, false, "Suspended", "2026-10-23"))
	}
	seedOutcomes(rg, outs...)
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	startUntil(rg, "md", time.Hour, "a fourth Instance ready", func() bool { return rg.readyCount("md") == 2 })
	for _, in := range rg.instances("md") {
		if in.Status.Phase == v1alpha1.InstanceReady && (in.Status.Gate == nil || in.Status.Gate.Verdict != v1alpha1.GateSuspended) {
			t.Errorf("Instance %s: gate %+v, want Suspended: both misses are out of the 20 kept outcomes, the listing stays", in.Name, in.Status.Gate)
		}
	}
	for _, o := range rg.md("md").Status.StartOutcomes[0].Outcomes {
		if o.Gate == v1alpha1.GateMissed {
			t.Fatalf("outcome %+v: a miss is still among the kept outcomes, so the row shows nothing", o)
		}
	}
	if got := rg.md("md").Status.SuspendedReferences; len(got) != 1 || !sameKey(got[0].ReferenceKey, suspectKey(id)) {
		t.Fatalf("suspendedReferences %+v, want the listing to stay", got)
	}

	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = []v1alpha1.ExpectedPerformance{gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-10-24" })}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if got := rg.md("md").Status.SuspendedReferences; len(got) != 0 {
		t.Fatalf("suspendedReferences %+v after the entry's date changed, want the listing removed", got)
	}
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	have := len(gateRestarts0(rg))
	startUntil(rg, "md", time.Hour, "the next Instance's first miss", func() bool { return len(rg.rec.Find("failing", "WorkerRestart")) > have })
	var gated *v1alpha1.Instance
	for _, in := range rg.instances("md") {
		if in.Status.Gate != nil && len(in.Status.Gate.Misses) == 1 {
			gated = &in
		}
	}
	if gated == nil || gated.Status.Gate.ReferenceDate != "2026-10-24" || gateRestartSignals(gated)[0] != "worker-0/ThroughputMissed" {
		t.Errorf("the next Instance: %+v, want it gated against the entry dated 2026-10-24: its first miss restarts the worker", gated)
	}
}

// gateRestarts0 is every WorkerRestart event raised so far.
func gateRestarts0(rg *rig) []RecordedEvent { return rg.rec.Find("failing", "WorkerRestart") }

// A controller restarted after the second failure still records only: it reads suspendedReferences from status, with
// every memory of the failures lost.
func TestGateASuspensionSurvivesAControllerRestart(t *testing.T) {
	rg, _, _ := suspectRig(t)
	rg.restartController()
	startUntil(rg, "md", time.Hour, "the third Instance ready", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")
	var third v1alpha1.Instance
	for _, i := range in {
		if i.Status.Phase == v1alpha1.InstanceReady {
			third = i
		}
	}
	if g := third.Status.Gate; g == nil || g.Verdict != v1alpha1.GateSuspended || len(third.Status.Restarts) != 0 {
		t.Errorf("after the restart: gate %+v, restarts %v, want Suspended and no restart", g, gateRestartSignals(&third))
	}
	if n := gateFailures(rg); n != 2 {
		t.Errorf("%d QualificationFailed events, want the two before the restart", n)
	}
}

// A failure, a pass, then a failure suspend nothing: the two most recent verdicts against the entry are Passed and
// Missed. The history is seeded (Missed, then Passed, both against 2026-10-23); the Instance then fails twice against
// the entry like any, and is released: no listing, no ReferenceSuspect, and the Instance is not Suspended.
func TestGateAFailureThenAPassThenAFailureSuspendNothing(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	name, _ := gateEntries(rg, gateMD(), oneEntry())
	seedOutcomes(rg, seeded("seed-1", 1, true, "Missed", "2026-10-23"), seeded("seed-2", 2, false, "Passed", "2026-10-23"))
	startUntil(rg, "md", time.Hour, "the Instance failed its retry", func() bool { return ended(rg.instance(name)) })
	in := rg.instance(name)
	if in.Status.EndReason != v1alpha1.ReasonQualificationFailed || in.Status.Gate == nil || in.Status.Gate.Verdict != v1alpha1.GateMissed {
		t.Fatalf("the Instance ended for %q with gate %+v, want QualificationFailed, Missed: a pass between them suspends nothing", in.Status.EndReason, in.Status.Gate)
	}
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if got := rg.md("md").Status.SuspendedReferences; len(got) != 0 || len(suspectEvents(rg)) != 0 {
		t.Errorf("suspendedReferences %+v, %d events: a failure, a pass and a failure suspend nothing", got, len(suspectEvents(rg)))
	}
}

// A failure, an Instance judged RecordOnly with no entry, then a failure against a new entry (another date) suspend
// nothing: the new entry has one miss.
func TestGateAFailureARecordOnlyAndAFailureAgainstANewEntrySuspendNothing(t *testing.T) {
	rg := newRig(t)
	rg.engine = gateSequence(41 * time.Millisecond)
	name, _ := gateEntries(rg, gateMD(), oneEntry(func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-10-24" }))
	seedOutcomes(rg, seeded("seed-1", 1, true, "Missed", "2026-10-23"), seeded("seed-2", 2, false, "RecordOnly", ""))
	startUntil(rg, "md", time.Hour, "the Instance failed its retry", func() bool { return ended(rg.instance(name)) })
	in := rg.instance(name)
	if in.Status.EndReason != v1alpha1.ReasonQualificationFailed || in.Status.Gate == nil || in.Status.Gate.Verdict != v1alpha1.GateMissed || in.Status.Gate.ReferenceDate != "2026-10-24" {
		t.Fatalf("the Instance ended for %q with gate %+v, want QualificationFailed, Missed against 2026-10-24", in.Status.EndReason, in.Status.Gate)
	}
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if got := rg.md("md").Status.SuspendedReferences; len(got) != 0 || len(suspectEvents(rg)) != 0 {
		t.Errorf("suspendedReferences %+v, %d events: a new entry has one miss", got, len(suspectEvents(rg)))
	}
}

// Parallel second misses: three Instances (instances 3, the fixture's limit not binding) measure 41 ms, are restarted in
// one pass and are on their retry in one pass at 41 ms again. The pass counts the second misses it has failed: exactly
// two fail and are released, each Allocation with its cause, the third records Suspended and passes, and
// suspendedReferences is written in that pass. Counting per pass only, all three would fail.
func TestGateThreeSecondMissesInOnePassReleaseTwo(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	rg.create(gateMD(func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 3 }))
	startUntil(rg, "md", 2*time.Minute, "three Instances", func() bool { return len(rg.instances("md")) == 3 })
	id := rg.instances("md")[0].Spec.DeploymentID.Display()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = []v1alpha1.ExpectedPerformance{gateEntryFor(id)}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	failed := map[string]string{} // Instance -> its Allocation, at the pass of the failures
	startUntil(rg, "md", time.Hour, "the second misses", func() bool {
		for _, in := range rg.instances("md") {
			if in.Status.EndReason == v1alpha1.ReasonQualificationFailed {
				failed[in.Name] = in.Spec.AllocationRef.Name
			}
		}
		return len(failed) > 0
	})
	if len(failed) != 2 {
		t.Fatalf("%d Instances failed in the pass of the second misses (%v), want exactly two", len(failed), failed)
	}
	md := rg.md("md")
	if got := md.Status.SuspendedReferences; len(got) != 1 || got[0].Date != "2026-10-23" || !sameKey(got[0].ReferenceKey, suspectKey(id)) {
		t.Errorf("suspendedReferences %+v, want the entry written in the pass of the second misses", got)
	}
	suspended := 0
	for _, in := range rg.instances("md") {
		if in.Status.Gate != nil && in.Status.Gate.Verdict == v1alpha1.GateSuspended {
			suspended++
		}
	}
	if suspended != 1 {
		t.Errorf("%d Instances recorded Suspended in that pass, want the third", suspended)
	}
	startUntil(rg, "md", time.Hour, "the three ready", func() bool { return rg.readyCount("md") == 3 })
	if n := gateFailures(rg); n != 2 {
		t.Errorf("%d QualificationFailed events in all, want two", n)
	}
	for in, alloc := range failed {
		startUntil(rg, "md", 5*time.Minute, "the failed Allocation deleted", func() bool { _, ok := log.deletedWith(alloc); return ok })
		if cause, _ := log.deletedWith(alloc); cause != "Instance "+in+" failed: QualificationFailed" {
			t.Errorf("Instance %s's Allocation was deleted with the cause %q", in, cause)
		}
	}
	if n := len(suspectEvents(rg)); n != 1 {
		t.Errorf("%d ReferenceSuspect events, want one", n)
	}
}

// The entry suspended meanwhile (HBEP-7, The throughput gate): an Instance whose first miss awaits its retry, and the
// entry listed as suspended before the retry is judged, records Suspended and passes. The listing is the one a second
// Allocation's miss would have written; its key and date are the entry's, written here as the literals they are. The
// retry measures 41 ms again, which would fail the Instance were the entry not suspended.
func TestGateAnEntrySuspendedMeanwhileRecordsOnlyOnTheRetry(t *testing.T) {
	rg := newRig(t)
	rg.engine = gateSequence(41 * time.Millisecond)
	name, id := gateEntries(rg, gateMD(), oneEntry())
	startUntil(rg, "md", 10*time.Minute, "the first miss's restart", func() bool { return len(gateRestarts(rg, name)) == 1 })
	md := rg.md("md")
	md.Status.SuspendedReferences = []v1alpha1.SuspendedReference{{ReferenceKey: suspectKey(id), Date: "2026-10-23", SuspendedTime: metav1.NewTime(rg.clock.Now())}}
	if err := rg.c.Status().Update(rg.ctx, md); err != nil {
		t.Fatal(err)
	}
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	got := rg.instance(name)
	if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GateSuspended || g.ReferenceDate != "2026-10-23" || len(g.Misses) != 1 {
		t.Errorf("status.gate %+v, want Suspended against 2026-10-23 with the first miss kept", g)
	}
	if n := gateFailures(rg); n != 0 || len(gateRestartSignals(got)) != 1 || got.Status.EndReason != "" {
		t.Errorf("%d QualificationFailed events, restarts %v, end reason %q, want the one restart and no failure", n, gateRestartSignals(got), got.Status.EndReason)
	}
}
