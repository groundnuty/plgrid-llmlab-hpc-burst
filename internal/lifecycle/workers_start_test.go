package lifecycle

// An Instance of two workers behind the router starts (HBEP-7, Several workers; the science agent's
// rulings on #439): each worker is verified, qualified and warmed up on its own checks, with its own
// record ver-<uid>-<worker>-<generation> and its entry in status.workers, the warm-up at its share of the
// target sessions; the Instance enters ready only once every worker has passed, the router's probe
// passes and the router holds every worker healthy. A worker that exits before the first ready restarts
// at its own rung, alone. The rig is the lifecycle controller against the fake agent and its fake router.

import (
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// twoWorkersRig is the rig with the router flag and a deployment of two workers, run until its agent is
// up; f, when set, reshapes the fake router's view before the agent first reports.
func twoWorkersRig(t *testing.T, f func(*agentapi.RouterReport)) (*rig, *afake.Agent) {
	t.Helper()
	return twoWorkersRigWith(t, afake.Engine{}, f)
}

// twoWorkersRigWith is twoWorkersRig with the fake agent's engine.
func twoWorkersRigWith(t *testing.T, e afake.Engine, f func(*agentapi.RouterReport)) (*rig, *afake.Agent) {
	t.Helper()
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return e }
	rg.onAgent = func(ra *rigAgent) { ra.agent.RouterView = f }
	rg.create(withWorkers(mdSpec("md"), 2))
	startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
	var ag *afake.Agent
	for _, ra := range rg.agents {
		ag = ra.agent
	}
	return rg, ag
}

func TestTwoWorkersReachReadyEachVerifiedOnItsOwn(t *testing.T) {
	rg, ag := twoWorkersRig(t, nil)
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")[0]

	// Each worker's entry names its own record, of its first verification, which passed, and its warm-up.
	if len(in.Status.Workers) != 2 {
		t.Fatalf("status.workers %+v, want two entries", in.Status.Workers)
	}
	for i, e := range in.Status.Workers {
		w := v1alpha1.WorkerName(i)
		want := "ver-" + string(in.UID) + "-" + w + "-1"
		if e.Name != w || e.Record != want || e.Verdict != string(v1alpha1.VerdictPass) || !e.WarmedUp || e.VerifiedGeneration == nil || *e.VerifiedGeneration != 0 {
			t.Errorf("%s's entry %+v, want record %s passed, warmed up, verified at generation 0", w, e, want)
		}
		var rec appsv1.ControllerRevision
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: want}, &rec); err != nil {
			t.Errorf("%s's record %s: %v", w, want, err)
		} else if rec.Labels[v1alpha1.LabelInstanceUID] != string(in.UID) || rec.Revision != 1 {
			t.Errorf("%s's record labels %v, revision %d", w, rec.Labels, rec.Revision)
		}
	}
	if h := in.Status.HealthyWorkers; h == nil || *h != 2 {
		t.Errorf("healthyWorkers %s once ready, want 2", countOf(h))
	}

	// The desired states asked each worker's checks on the worker, never on the Instance, each warm-up at
	// ceil(targetSessions / 2); and once ready, every worker carries its verified generation.
	md := rg.md("md")
	want := (resolveShapes(md).targetSessions + 1) / 2
	warmUps := 0
	for _, d := range ag.Seen() {
		if d.Instance == nil {
			continue
		}
		if d.Instance.Checks != nil {
			t.Fatalf("a desired state asked the Instance's own checks: %+v", d.Instance.Checks)
		}
		for _, dw := range d.Instance.Workers {
			if dw.Checks != nil && dw.Checks.WarmUp != nil {
				warmUps++
				if dw.Checks.WarmUp.TargetSessions != want {
					t.Errorf("%s's warm-up at %d sessions, want ceil(%d / 2) = %d", dw.Name, dw.Checks.WarmUp.TargetSessions, resolveShapes(md).targetSessions, want)
				}
			}
		}
	}
	if warmUps == 0 {
		t.Error("no desired state asked a worker's warm-up")
	}
	seen := ag.Seen()
	last := seen[len(seen)-1].Instance
	for _, dw := range last.Workers {
		if dw.VerifiedGeneration == nil || *dw.VerifiedGeneration != dw.Generation || dw.Checks != nil {
			t.Errorf("once ready, %s is desired with verifiedGeneration %v and checks %+v; want its generation and none", dw.Name, dw.VerifiedGeneration, dw.Checks)
		}
	}
}

// While the router holds worker-1 failing, the Instance waits, verifying, with both workers passed; it
// enters ready once the router holds both healthy. The router holding worker-0 alone is not enough.
func TestTheFirstReadyWaitsForTheRouterToHoldEveryWorker(t *testing.T) {
	hold := true
	rg, _ := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if !hold {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck = false, true
			}
		}
	})
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up, the router holding worker-0", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && workerWarmed(&ins[0], "worker-0") && workerWarmed(&ins[0], "worker-1")
	})
	for range 6 {
		startStep(rg, "md", startTick)
	}
	if in := rg.instances("md")[0]; in.Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("ready while the router holds worker-1 failing")
	}
	hold = false
	startUntil(rg, "md", 5*time.Minute, "ready once the router holds both", func() bool { return rg.readyCount("md") == 1 })
}

// worker-1 exits before the first ready: its own rung restarts it, alone. worker-0's generation and
// entry stay as they are, worker-1 verifies again under ver-<uid>-worker-1-2, and the Instance reaches
// ready.
func TestAWorkerExitBeforeTheFirstReadyRestartsThatWorkerAlone(t *testing.T) {
	hold := true
	rg, ag := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if hold && rr.Probe != nil {
			rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 0 // held, as holdBeforeReady holds it
		}
	})
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up and the Instance's conditions True, held before ready", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && workerWarmed(&ins[0], "worker-0") && workerWarmed(&ins[0], "worker-1") &&
			isTrue(ins[0].Status.Conditions, v1alpha1.ConditionWarmedUp)
	})
	name := rg.instances("md")[0].Name
	before := rg.instance(name)
	entry0 := workerEntry(before, "worker-0")
	ag.CrashWorker("worker-1", 1)
	startUntil(rg, "md", 5*time.Minute, "worker-1's restart", func() bool {
		_, ok := lastRestart(rg, name)
		return ok
	})
	got := rg.instance(name)
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || r.Signal != v1alpha1.ReasonPartExited {
		t.Fatalf("the restart is %+v, want worker-1's for PartExited", r)
	}
	if g := restartGeneration(got, "worker-0"); g != 0 {
		t.Errorf("worker-0's restart generation is %d, want 0: only worker-1 restarts", g)
	}
	if e := workerEntry(got, "worker-0"); e.Record != entry0.Record || !e.WarmedUp {
		t.Errorf("worker-0's entry after worker-1's restart: %+v, want %+v", e, entry0)
	}
	if e := workerEntry(got, "worker-1"); e.WarmedUp || workerWarmed(got, "worker-1") {
		t.Errorf("worker-1's entry right after its restart: %+v, want its warm-up to run again", e)
	}
	// A worker's restart leaves the Instance's own conditions True (HBEP-7, Several workers).
	for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
		if !isTrue(got.Status.Conditions, c) {
			t.Errorf("the Instance's %s after worker-1's restart: %s, want True", c, conditionReason(got, c))
		}
	}
	hold = false
	startUntil(rg, "md", 10*time.Minute, "ready after worker-1's restart", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instance(name)
	if e := workerEntry(in, "worker-1"); e.Record != "ver-"+string(in.UID)+"-worker-1-2" || e.VerifiedGeneration == nil || *e.VerifiedGeneration != 1 {
		t.Errorf("worker-1's entry once ready: %+v, want record ver-<uid>-worker-1-2, verified at generation 1", e)
	}
	if parts := restartParts(in); !slices.Equal(parts, []string{"worker-1"}) {
		t.Errorf("restarts %v, want worker-1's alone", parts)
	}
	// worker-1's second record says its own rung restarted it; worker-0's first, a start.
	for rec, want := range map[string]string{"ver-" + string(in.UID) + "-worker-1-2": "worker", "ver-" + string(in.UID) + "-worker-0-1": "start"} {
		d, ok, err := rg.r.readVerification(rg.ctx, rigNS, rec)
		if err != nil || !ok || d.Restart != want {
			t.Errorf("record %s: restart %q (found %v, %v), want %q", rec, d.Restart, ok, err, want)
		}
	}
	// worker-1 warmed up again at its new verification generation, 2.
	if !askedWarmUp(ag, "worker-1", 2) {
		t.Error("no desired state asked worker-1's warm-up at verification generation 2")
	}
}

// askedWarmUp reports whether a desired state asked worker w's warm-up at verification generation gen.
func askedWarmUp(ag *afake.Agent, w string, gen int64) bool {
	for _, d := range ag.Seen() {
		if d.Instance == nil {
			continue
		}
		for _, dw := range d.Instance.Workers {
			if dw.Name == w && dw.Checks != nil && dw.Checks.VerificationGeneration == gen && dw.Checks.WarmUp != nil {
				return true
			}
		}
	}
	return false
}

// running reports whether the agent's last report runs worker w at restart generation gen.
func runningAt(ag *afake.Agent, w string, gen int64) bool {
	rep, ok := ag.LastReport()
	if !ok || rep.Instance == nil {
		return false
	}
	r := reportOf(rep.Instance, w)
	return r != nil && r.Generation == gen && r.Phase == agentapi.PartRunning
}

// With attemptsPerRung 1, each worker counts its own attempts: worker-0's restart spends worker-0's rung
// only, so worker-1's first exit restarts worker-1, and its second climbs to the whole-Instance rung.
// That restart verifies and warms up every worker again: right after it no worker is taken as verified,
// and each is asked its warm-up at its new verification generation.
func TestEachWorkerCountsItsOwnAttemptsThenTheWholeInstanceRung(t *testing.T) {
	hold := true
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1)), Window: dur(4 * time.Hour)}
	}))
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return afake.Engine{} }
	rg.onAgent = func(ra *rigAgent) {
		ra.agent.RouterView = func(rr *agentapi.RouterReport) {
			if hold && rr.Probe != nil {
				rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 0
			}
		}
	}
	rg.create(withWorkers(mdSpec("md"), 2))
	startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
	var ag *afake.Agent
	for _, ra := range rg.agents {
		ag = ra.agent
	}
	startUntil(rg, "md", 10*time.Minute, "both workers running", func() bool { return runningAt(ag, "worker-0", 0) && runningAt(ag, "worker-1", 0) })
	name := rg.instances("md")[0].Name
	crashUntil := func(w string, gen int64, want []string) {
		t.Helper()
		startUntil(rg, "md", 10*time.Minute, w+" running at its generation", func() bool { return runningAt(ag, w, gen) })
		ag.CrashWorker(w, 1)
		startUntil(rg, "md", 10*time.Minute, "the restart after "+w+"'s exit", func() bool { return len(rg.instance(name).Status.Restarts) == len(want) })
		if got := restartParts(rg.instance(name)); !slices.Equal(got, want) {
			t.Fatalf("restarts %v, want %v", got, want)
		}
	}
	crashUntil("worker-0", 0, []string{"worker-0"})
	crashUntil("worker-1", 0, []string{"worker-0", "worker-1"})
	// Both workers verify and warm up again, so their entries hold something for the whole-Instance
	// restart to clear.
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up again", func() bool {
		in := rg.instance(name)
		return workerWarmed(in, "worker-0") && workerWarmed(in, "worker-1")
	})
	crashUntil("worker-1", 1, []string{"worker-0", "worker-1", v1alpha1.PartInstance})
	got := rg.instance(name)
	for _, e := range got.Status.Workers {
		if e.WarmedUp || e.VerifiedGeneration != nil {
			t.Errorf("%s's entry right after the whole-Instance restart: %+v, want neither warmed up nor verified", e.Name, e)
		}
	}
	hold = false
	startUntil(rg, "md", 10*time.Minute, "ready after the whole-Instance restart", func() bool { return rg.readyCount("md") == 1 })
	// Verification generations: worker-0 is 1 + 1 + 1 = 3, worker-1 is 1 + 1 + 1 = 3.
	for _, w := range []string{"worker-0", "worker-1"} {
		if !askedWarmUp(ag, w, 3) {
			t.Errorf("no desired state asked %s's warm-up at verification generation 3", w)
		}
	}
}

func restartParts(in *v1alpha1.Instance) []string {
	var out []string
	for _, r := range in.Status.Restarts {
		out = append(out, r.Part)
	}
	return out
}

// A checkpoint file that changed fails the rehash on the workers' first verification: the whole Instance
// restages once, and the next failure ends it, VerificationFailed, never ready. The earlier failure is
// any worker's, since the workers share the staged checkpoint.
func TestTwoWorkersRehashRestagesOnceThenFails(t *testing.T) {
	rg, _ := twoWorkersRigWith(t, afake.Engine{Verify: afake.VerifyChangedFile("model.safetensors")}, nil)
	name := rg.instances("md")[0].Name
	st, everReady := verifEnded(rg, name)
	if everReady || st.EndReason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	if parts := restartParts(rg.instance(name)); !slices.Equal(parts, []string{v1alpha1.PartInstance}) {
		t.Errorf("restarts %v, want one restage, the whole Instance's", parts)
	}
}

// worker-1's qualification fails while worker-0's passes: the Instance fails, QualificationFailed, the
// condition naming worker-1, and it is never ready. Qualification is judged on every worker's results.
func TestOneWorkersFailedQualificationFailsTheInstance(t *testing.T) {
	var calls int
	e := afake.Engine{Qualify: func(i agentapi.DesiredInstance, r agentapi.QualifyRequest) agentapi.QualifyResult {
		q := afake.DefaultQualify(i, r)
		// The fake answers each worker's checks in the workers' order: the second call is worker-1's.
		if calls++; calls == 2 {
			for n := range q.Checks {
				if q.Checks[n].Name == agentapi.CheckHealth {
					q.Checks[n].Passed = false
				}
			}
		}
		return q
	}}
	rg, _ := twoWorkersRigWith(t, e, nil)
	name := rg.instances("md")[0].Name
	st, everReady := verifEnded(rg, name)
	if everReady || st.EndReason != v1alpha1.ReasonQualificationFailed {
		t.Fatalf("ever ready %v, end reason %q, want never ready and QualificationFailed", everReady, st.EndReason)
	}
	if c := conditionMessage(rg.instance(name), v1alpha1.ConditionQualified); !strings.HasPrefix(c, "worker-1: ") {
		t.Errorf("the Qualified condition says %q, want worker-1's failure", c)
	}
	if calls != 2 {
		t.Errorf("%d qualifications, want one for each worker", calls)
	}
}

func conditionMessage(in *v1alpha1.Instance, typ string) string {
	for _, c := range in.Status.Conditions {
		if c.Type == typ {
			return c.Message
		}
	}
	return ""
}

// holdWorkerOne makes the fake router hold worker-1 failing its active check while *hold.
func holdWorkerOne(hold *bool) func(*agentapi.RouterReport) {
	return func(rr *agentapi.RouterReport) {
		if !*hold {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck = false, true
			}
		}
	}
}

// After a restart, a start past health.startTimeout judges the worker that holds it back (HBEP-7:469):
// with both workers warmed up and the router holding worker-1 failing, the timeout restarts worker-1, not
// the healthy worker-0 (#498's review). The earlier restart, worker-0's, is what lets the timeout
// restart rather than fail the Instance, as a first start's does.
func TestAStartTimeoutRestartsTheWorkerTheRouterHoldsFailing(t *testing.T) {
	hold := true
	rg, ag := twoWorkersRig(t, holdWorkerOne(&hold))
	startUntil(rg, "md", 10*time.Minute, "both workers running", func() bool { return runningAt(ag, "worker-0", 0) && runningAt(ag, "worker-1", 0) })
	name := rg.instances("md")[0].Name
	ag.CrashWorker("worker-0", 1)
	startUntil(rg, "md", 10*time.Minute, "worker-0's restart", func() bool { return len(rg.instance(name).Status.Restarts) == 1 })
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up", func() bool {
		in := rg.instance(name)
		return workerWarmed(in, "worker-0") && workerWarmed(in, "worker-1")
	})
	// status.restarts keeps the restarts within restart.window, so worker-0's may have left it by then:
	// the timeout's restart is the one whose signal is StartTimeout.
	startUntil(rg, "md", 4*time.Hour, "the start timeout's restart, or the Instance's end", func() bool {
		in := rg.instance(name)
		r, _ := lastRestart(rg, name)
		return r.Signal == v1alpha1.ReasonStartTimeout || in.Status.Phase == v1alpha1.InstanceEnded || in.Status.Phase == v1alpha1.InstanceReady
	})
	if in := rg.instance(name); in.Status.Phase != v1alpha1.InstanceVerifying {
		t.Fatalf("phase %s (end reason %q, %s), restarts %v: want verifying, restarted by the timeout", in.Status.Phase, in.Status.EndReason, conditionMessage(in, v1alpha1.ConditionReady), restartParts(in))
	}
	r, _ := lastRestart(rg, name)
	if r.Part != "worker-1" || r.Signal != v1alpha1.ReasonStartTimeout {
		t.Errorf("the timeout's restart is %+v, want worker-1's for StartTimeout: the router holds worker-1 failing", r)
	}
}

// worker-1's verification fails after its restart, a settings contradiction: the Instance's Verified,
// which the restart left True, is set False with worker-1's message before the Instance ends with
// VerificationFailed, as with one worker (#498's review).
func TestAWorkersFailedVerdictSetsTheInstancesVerifiedFalse(t *testing.T) {
	var calls int
	e := afake.Engine{Verify: func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
		// The fake answers each worker in order: calls 1 and 2 are the first verifications, 3 worker-1's
		// after its restart.
		if calls++; calls >= 3 {
			return afake.VerifySettings(map[string]string{"max-model-len": "16384"})(i, r)
		}
		return afake.DefaultVerify(i, r)
	}}
	hold := true
	rg, ag := twoWorkersRigWith(t, e, func(rr *agentapi.RouterReport) {
		if hold && rr.Probe != nil {
			rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 0
		}
	})
	startUntil(rg, "md", 10*time.Minute, "the Instance verified, held before ready", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && isTrue(ins[0].Status.Conditions, v1alpha1.ConditionVerified) && runningAt(ag, "worker-1", 0)
	})
	name := rg.instances("md")[0].Name
	ag.CrashWorker("worker-1", 1)
	st, everReady := verifEnded(rg, name)
	if everReady || st.EndReason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	got := rg.instance(name)
	if isTrue(got.Status.Conditions, v1alpha1.ConditionVerified) || conditionReason(got, v1alpha1.ConditionVerified) != v1alpha1.ReasonFailed ||
		!strings.HasPrefix(conditionMessage(got, v1alpha1.ConditionVerified), "worker-1: ") {
		t.Errorf("the ended Instance's Verified: %s, %q; want False, Failed, naming worker-1", conditionReason(got, v1alpha1.ConditionVerified), conditionMessage(got, v1alpha1.ConditionVerified))
	}
}

// worker-1's first verification fails its rehash while worker-0's passes: the whole Instance restages
// once. At the next verification worker-0's rehash fails too, and it is worker-1's earlier record that
// makes it the second failure: the Instance ends VerificationFailed, with no second restage. The
// workers share the staged checkpoint (#498's review: the earlier record is any worker's).
func TestWorkerOnesEarlierRehashFailureDecides(t *testing.T) {
	var calls int
	e := afake.Engine{Verify: func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
		if calls++; calls >= 2 {
			return afake.VerifyChangedFile("model.safetensors")(i, r)
		}
		return afake.DefaultVerify(i, r)
	}}
	rg, _ := twoWorkersRigWith(t, e, nil)
	name := rg.instances("md")[0].Name
	st, everReady := verifEnded(rg, name)
	if everReady || st.EndReason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	in := rg.instance(name)
	if parts := restartParts(in); !slices.Equal(parts, []string{v1alpha1.PartInstance}) {
		t.Errorf("restarts %v, want one restage", parts)
	}
	for rec, want := range map[string]string{
		"ver-" + string(in.UID) + "-worker-0-1": string(v1alpha1.VerdictPass),
		"ver-" + string(in.UID) + "-worker-1-1": string(v1alpha1.VerdictFail),
		"ver-" + string(in.UID) + "-worker-0-2": string(v1alpha1.VerdictFail),
	} {
		if d, ok, err := rg.r.readVerification(rg.ctx, rigNS, rec); err != nil || !ok || string(d.Verdict) != want {
			t.Errorf("record %s: verdict %q (found %v, %v), want %q", rec, d.Verdict, ok, err, want)
		}
	}
}
