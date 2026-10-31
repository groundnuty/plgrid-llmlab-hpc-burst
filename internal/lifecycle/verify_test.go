package lifecycle

// Scenario tests for verification, its records and qualification (HBEP-7, Test
// plan: Verification records, Verification, Qualification; Instance start).

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// verifRig makes a rig with one deployment "md" and an engine double per agent,
// and steps until its first Instance exists. It returns the rig and that Instance.
func verifRig(t *testing.T, mk func() afake.Engine, opts ...rigOption) (*rig, v1alpha1.Instance) {
	rg := newRig(t, opts...)
	if mk != nil {
		rg.engine = func(string) afake.Engine { return mk() }
	}
	rg.create(mdSpec("md"))
	startUntil(rg, "md", 2*time.Minute, "an Instance", func() bool { return len(rg.instances("md")) >= 1 })
	return rg, rg.instances("md")[0]
}

// verifRecordName is ver-<instance UID>-<generation>, written out here.
func verifRecordName(in v1alpha1.Instance, n int) string { return fmt.Sprintf("ver-%s-%d", in.UID, n) }

// verifRecord returns a record's stored bytes and verdict; ok is false when it
// does not exist.
func verifRecord(rg *rig, name string) (raw []byte, verdict string, ok bool) {
	rg.t.Helper()
	var rec appsv1.ControllerRevision
	err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &rec)
	if apierrors.IsNotFound(err) {
		return nil, "", false
	}
	if err != nil {
		rg.t.Fatal(err)
	}
	var d struct {
		Verdict string `json:"verdict"`
	}
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		rg.t.Fatalf("record %s: %v", name, err)
	}
	return rec.Data.Raw, d.Verdict, true
}

// verifRunUntil steps until cond holds, failing after limit, with the ready
// check at every Step; it reports whether any Instance was ever ready.
func verifRunUntil(rg *rig, limit time.Duration, what string, cond func() bool) (everReady bool) {
	rg.t.Helper()
	for end := rg.clock.Now().Add(limit); !cond(); {
		if !rg.clock.Now().Before(end) {
			rg.t.Fatalf("%s: not reached within %s (now %s)", what, limit, rg.clock.Now().Format(time.RFC3339))
		}
		everReady = startStep(rg, "md", startTick) > 0 || everReady
	}
	return everReady
}

// verifEnded steps until the Instance has ended and returns its status.
func verifEnded(rg *rig, name string) (st v1alpha1.InstanceStatus, everReady bool) {
	everReady = verifRunUntil(rg, 20*time.Minute, "Instance "+name+" ended", func() bool { return rg.instance(name).Status.Phase == v1alpha1.InstanceEnded })
	return rg.instance(name).Status, everReady
}

// verifVerifyRequests lists the verification requests an agent saw, as
// (verification generation, number of files to rehash), in order.
func verifVerifyRequests(ra *rigAgent) [][2]int {
	var out [][2]int
	for _, d := range ra.agent.Seen() {
		if d.Instance != nil && d.Instance.Checks != nil && d.Instance.Checks.Verify != nil {
			out = append(out, [2]int{int(d.Instance.Checks.VerificationGeneration), len(d.Instance.Checks.Verify.Files)})
		}
	}
	return out
}

// TestVerifyRecordsPerRestart covers "Verification records": a worker restart
// and then a whole-Instance restart each write a new record, ver-<uid>-2 then
// -3; the Instance's status names the latest; the earlier records' bytes do not
// change. attemptsPerRung is 1 so the second crash climbs to the Instance rung.
// The worker restart does not rehash (no files in the request); the
// whole-Instance restart does (both of the checkpoint's files).
func TestVerifyRecordsPerRestart(t *testing.T) {
	rg, in := verifRig(t, nil, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1))}
	}))
	alloc := in.Spec.AllocationRef.Name
	ready := func(n int) func() bool {
		return func() bool {
			st := rg.instance(in.Name).Status
			return st.Phase == v1alpha1.InstanceReady && st.VerificationRecord == verifRecordName(in, n)
		}
	}
	var stored [][]byte
	var wants []string
	check := func(n int) {
		t.Helper()
		verifRunUntil(rg, 10*time.Minute, "ready with record "+verifRecordName(in, n), ready(n))
		raw, verdict, ok := verifRecord(rg, verifRecordName(in, n))
		if !ok || verdict != "pass" {
			t.Fatalf("record %s: exists %v verdict %q", verifRecordName(in, n), ok, verdict)
		}
		for i, b := range stored {
			if cur, _, _ := verifRecord(rg, verifRecordName(in, i+1)); string(cur) != string(b) {
				t.Errorf("record %s changed after record %d was written", verifRecordName(in, i+1), n)
			}
		}
		stored, wants = append(stored, raw), append(wants, verifRecordName(in, n))
	}
	check(1)
	for n, code := range []string{"WorkerRestart", "InstanceRestart"} {
		rg.agentOf(alloc).agent.CrashWorker(agentapi.WorkerName(0), 1)
		verifRunUntil(rg, 5*time.Minute, code+" event", func() bool { return len(rg.rec.Find("failing", code)) == 1 })
		check(n + 2)
	}
	if _, _, ok := verifRecord(rg, verifRecordName(in, 4)); ok {
		t.Errorf("a fourth record exists after two restarts")
	}
	if got := verifVerifyRequests(rg.agentOf(alloc)); fmt.Sprint(got) != "[[1 2] [2 0] [3 2]]" {
		t.Errorf("verification requests (generation, files) = %v, want [[1 2] [2 0] [3 2]]: rehash at start and at the Instance restart only", got)
	}
}

// TestVerifyChangedFileRestagedOnceThenFails covers "a changed checkpoint file
// fails the rehash, is re-staged once (a whole-Instance restart: restart-instance
// annotation 1), then fails the Instance (VerificationFailed)", with its control:
// an engine that mismatches only the first time recovers after the one restage.
func TestVerifyChangedFileRestagedOnceThenFails(t *testing.T) {
	rg, in := verifRig(t, func() afake.Engine {
		return afake.Engine{Verify: afake.VerifyChangedFile("model.safetensors")}
	})
	st, everReady := verifEnded(rg, in.Name)
	if everReady || st.EndReason != "VerificationFailed" {
		t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	got := rg.instance(in.Name)
	if got.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance)] != "1" || got.Annotations[v1alpha1.RestartGenerationAnnotation("worker-0")] != "" {
		t.Errorf("restart annotations = %v, want restart-instance 1 only", got.Annotations)
	}
	if n := len(rg.rec.Find("failing", "InstanceRestart")); n != 1 {
		t.Errorf("failing/InstanceRestart events = %d, want one restage", n)
	}
	for n := 1; n <= 2; n++ {
		if _, v, ok := verifRecord(rg, verifRecordName(in, n)); !ok || v != "fail" {
			t.Errorf("record %d: exists %v verdict %q, want a failed rehash", n, ok, v)
		}
	}
	if _, _, ok := verifRecord(rg, verifRecordName(in, 3)); ok {
		t.Error("a third verification ran; the second failure must end the Instance")
	}
	if got := verifVerifyRequests(rg.agentOf(in.Spec.AllocationRef.Name)); fmt.Sprint(got) != "[[1 2] [2 2]]" {
		t.Errorf("verification requests = %v, want a rehash of both files twice", got)
	}
	verifRunUntil(rg, 2*time.Minute, "the Allocation released", func() bool { _, alive := rg.allocation(in.Spec.AllocationRef.Name); return !alive })

	t.Run("recovers-when-the-restage-cures-it", func(t *testing.T) {
		calls := 0
		rg, in := verifRig(t, func() afake.Engine {
			return afake.Engine{Verify: func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
				if calls++; calls == 1 {
					return afake.VerifyChangedFile("model.safetensors")(i, r)
				}
				return afake.DefaultVerify(i, r)
			}}
		})
		verifRunUntil(rg, 10*time.Minute, "ready", func() bool { return rg.readyCount("md") == 1 })
		_, v1, _ := verifRecord(rg, verifRecordName(in, 1))
		_, v2, _ := verifRecord(rg, verifRecordName(in, 2))
		if v1 != "fail" || v2 != "pass" || len(rg.rec.Find("failing", "InstanceRestart")) != 1 || len(rg.rec.Find("failing", "VerificationFailed")) != 0 {
			t.Errorf("records %q, %q; want fail then pass after one restage and no VerificationFailed", v1, v2)
		}
	})
}

// TestVerifyMaxModelLenContradictionStopsClass covers "an engine that reports
// another max-model-len fails at once and stops provisioning in that class
// (Degraded VerificationFailed, no further Allocation after the failed one, and
// it survives a controller restart)". The stop holds for the spec's ID, so a
// changed serving spec (max-model-len 16384, which the engine reports) lifts it.
func TestVerifyMaxModelLenContradictionStopsClass(t *testing.T) {
	rg, in := verifRig(t, func() afake.Engine { return afake.Engine{Verify: afake.VerifyMaxModelLen("16384")} })
	st, everReady := verifEnded(rg, in.Name)
	if everReady || st.EndReason != "VerificationFailed" {
		t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	if _, v, ok := verifRecord(rg, verifRecordName(in, 1)); !ok || v != "fail" {
		t.Errorf("record 1: exists %v verdict %q, want fail", ok, v)
	}
	if n := len(rg.rec.Find("failing", "InstanceRestart")) + len(rg.rec.Find("failing", "WorkerRestart")); n != 0 {
		t.Errorf("%d restarts; a contradiction fails the Instance at once", n)
	}
	names := map[string]bool{in.Spec.AllocationRef.Name: true} // the failed one, then any that follow
	hold := func(d time.Duration) {
		t.Helper()
		for end := rg.clock.Now().Add(d); rg.clock.Now().Before(end); {
			startStep(rg, "md", startTick)
			for _, a := range rg.allocations("md") {
				names[a.Name] = true
			}
		}
	}
	hold(30 * time.Minute)
	if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Status != "True" || c.Reason != v1alpha1.ReasonVerificationFailed {
		t.Errorf("Degraded = %s/%s (%s), want True/VerificationFailed", c.Status, c.Reason, c.Message)
	}
	rg.restartController()
	hold(30 * time.Minute)
	if len(names) != 1 || rg.submissions() != 1 {
		t.Errorf("Allocations seen %v, submissions %d; want only the failed one %s, and nothing after a controller restart", names, rg.submissions(), in.Spec.AllocationRef.Name)
	}
	if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Status != "True" || c.Reason != v1alpha1.ReasonVerificationFailed {
		t.Errorf("after the restart Degraded = %s/%s, want True/VerificationFailed", c.Status, c.Reason)
	}
	before := rg.submissions()
	rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) {
		md.Spec.Source.Served.Flags = []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "16384"}}
	})
	verifRunUntil(rg, 10*time.Minute, "ready after the spec changed", func() bool { return rg.readyCount("md") == 1 })
	if rg.submissions() != before+1 {
		t.Errorf("submissions = %d, want %d: a changed spec lifts the stop and provisions once", rg.submissions(), before+1)
	}
}

// TestVerifyEngineReportsNothingFails covers "an engine that reports nothing
// fails, because the version, the image digest and the context length are
// required".
func TestVerifyEngineReportsNothingFails(t *testing.T) {
	rg, in := verifRig(t, func() afake.Engine { return afake.Engine{Verify: afake.VerifyReportsNothing} })
	st, everReady := verifEnded(rg, in.Name)
	if everReady || st.EndReason != "VerificationFailed" {
		t.Errorf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	if _, v, ok := verifRecord(rg, verifRecordName(in, 1)); !ok || v != "fail" {
		t.Errorf("record 1: exists %v verdict %q, want fail", ok, v)
	}
}

// TestVerifyQualificationChecks covers the Test plan's Qualification item. Hand
// calculation of the KV rule: targetSessions 4 x contextTokens.typical 4096 =
// 16384 tokens, so 16383 fails and 16384 passes. The corruption check needs the
// first 32 tokens shared: a divergence at index 10 or 31 fails, at 32 or 40
// passes. A failing Instance ends QualificationFailed after a passed
// verification record, never becomes ready, and its Allocation is deleted.
func TestVerifyQualificationChecks(t *testing.T) {
	kv := func(n int64) afake.Engine { return afake.Engine{Qualify: afake.QualifyKVCapacity(n)} }
	greedy := func(f func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32) afake.Engine {
		return afake.Engine{GreedyOutputs: f}
	}
	cases := []struct {
		name string
		eng  afake.Engine
		fail bool
	}{
		{"kv-capacity-16383", kv(16383), true},
		{"kv-capacity-16384", kv(16384), false},
		{"greedy-diverges-at-10", greedy(afake.GreedyDivergingAt(10)), true},
		{"greedy-diverges-at-31", greedy(afake.GreedyDivergingAt(31)), true},
		{"greedy-diverges-at-32", greedy(afake.GreedyDivergingAt(32)), false},
		{"greedy-diverges-at-40", greedy(afake.GreedyDivergingAt(40)), false},
		{"greedy-single-repeated-token", greedy(afake.GreedySingleRepeatedToken), true},
		{"loop-at-long-context", afake.Engine{Qualify: afake.QualifyLoopAtLongContext}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := verifRig(t, func() afake.Engine { return tc.eng })
			alloc := in.Spec.AllocationRef.Name
			if !tc.fail {
				verifRunUntil(rg, 10*time.Minute, "ready", func() bool { return rg.readyCount("md") == 1 })
				if _, ok := rg.allocation(alloc); !ok || len(rg.rec.Find("failing", "")) != 0 {
					t.Errorf("a passing qualification must keep its Allocation and raise no failing event")
				}
				return
			}
			st, everReady := verifEnded(rg, in.Name)
			if everReady || st.EndReason != v1alpha1.ReasonQualificationFailed {
				t.Fatalf("ever ready %v, end reason %q, want never ready and QualificationFailed", everReady, st.EndReason)
			}
			if _, v, ok := verifRecord(rg, verifRecordName(in, 1)); !ok || v != "pass" {
				t.Errorf("record 1: exists %v verdict %q; verification passed, qualification failed", ok, v)
			}
			verifRunUntil(rg, 2*time.Minute, "the Allocation deleted", func() bool { _, alive := rg.allocation(alloc); return !alive })
			if n := len(rg.rec.Find("failing", v1alpha1.ReasonQualificationFailed)); n < 1 {
				t.Errorf("no failing/QualificationFailed event")
			}
		})
	}
}

// TestVerifyNoInstanceReadyBeforeChecksPass covers "No instance gets traffic
// before it is ready": at every Step of a start, with a slow first warm-up
// round, and of a worker restart, an Instance is ready only with the three
// conditions true and a passed latest record. Non-vacuity: the Steps include
// ones with the Instance not ready (so the check could have caught an early
// ready) and ones with it ready.
func TestVerifyNoInstanceReadyBeforeChecksPass(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{WarmUpRound: afake.WarmUpSlowFirstRound} }
	rg.create(mdSpec("md"))
	var notReady, ready int
	for i := 0; i < 120 && rg.readyCount("md") == 0; i++ {
		startStep(rg, "md", startTick)
		if ins := rg.instances("md"); len(ins) == 1 && ins[0].Status.Phase != v1alpha1.InstanceReady {
			notReady++
		}
	}
	in, alloc := startReady(rg)
	rg.agentOf(alloc).agent.CrashWorker(agentapi.WorkerName(0), 1)
	for i := 0; i < 60 && rg.instance(in.Name).Status.Phase == v1alpha1.InstanceReady; i++ {
		startStep(rg, "md", startTick) // a restart leaves ready for verifying
	}
	for i := 0; i < 60; i++ {
		if p := rg.instance(in.Name).Status.Phase; p == v1alpha1.InstanceVerifying || p == v1alpha1.InstanceStarting {
			notReady++
		}
		ready += startStep(rg, "md", startTick)
	}
	if notReady < 4 || ready == 0 {
		t.Fatalf("saw %d Steps not ready and %d ready; the check proved nothing", notReady, ready)
	}
	if got := len(rg.rec.Find("failing", "WorkerRestart")); got != 1 {
		t.Fatalf("worker restarts = %d, want 1", got)
	}
}
