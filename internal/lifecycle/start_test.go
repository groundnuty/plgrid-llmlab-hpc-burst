package lifecycle

// Scenario tests for Instance start (HBEP-7, Test plan: Timeouts, Warm-up, "No
// instance gets traffic before it is ready", and The agent).

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

const startTick = 10 * time.Second

// startCheckReady asserts, after a Step, that every Instance of the deployment
// that is ready has Verified, Qualified and WarmedUp true and a latest
// verification record that exists and passed (Instance start, ready). It
// returns how many Instances were ready, so a test can show it looked at one.
func startCheckReady(rg *rig, md string) int {
	rg.t.Helper()
	n := 0
	for _, in := range rg.instances(md) {
		if in.Status.Phase != v1alpha1.InstanceReady {
			continue
		}
		n++
		for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
			if !apimeta.IsStatusConditionTrue(in.Status.Conditions, c) {
				rg.t.Fatalf("%s: Instance %s is ready while %s is not true (conditions %+v)", rg.clock.Now().Format(time.TimeOnly), in.Name, c, in.Status.Conditions)
			}
		}
		var rec appsv1.ControllerRevision
		if in.Status.VerificationRecord == "" {
			rg.t.Fatalf("Instance %s is ready with no verification record", in.Name)
		}
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: in.Status.VerificationRecord}, &rec); err != nil {
			rg.t.Fatalf("Instance %s is ready, but its record %s: %v", in.Name, in.Status.VerificationRecord, err)
		}
		var d struct {
			Verdict string `json:"verdict"`
		}
		if err := json.Unmarshal(rec.Data.Raw, &d); err != nil || d.Verdict != "pass" {
			rg.t.Fatalf("Instance %s is ready, but its record %s has verdict %q (%v)", in.Name, rec.Name, d.Verdict, err)
		}
	}
	return n
}

// startStep advances the clock by tick, runs one Step and checks that no
// Instance is ready too early.
func startStep(rg *rig, md string, tick time.Duration) int {
	rg.t.Helper()
	rg.clock.Advance(tick)
	rg.Step()
	return startCheckReady(rg, md)
}

// startUntil steps until cond holds, failing after limit, with the ready check
// at every Step.
func startUntil(rg *rig, md string, limit time.Duration, what string, cond func() bool) {
	rg.t.Helper()
	for end := rg.clock.Now().Add(limit); !cond(); startStep(rg, md, startTick) {
		if !rg.clock.Now().Before(end) {
			rg.t.Fatalf("%s: not reached within %s (now %s)", what, limit, rg.clock.Now().Format(time.RFC3339))
		}
	}
}

// startReady brings one Instance of "md" to ready and returns it and its
// Allocation's name.
func startReady(rg *rig) (v1alpha1.Instance, string) {
	rg.t.Helper()
	startUntil(rg, "md", 10*time.Minute, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")[0]
	return in, in.Spec.AllocationRef.Name
}

// startRounds is the number of warm-up rounds in the agent's last report.
func startRounds(ra *rigAgent) int {
	rep, ok := ra.agent.LastReport()
	if !ok || rep.Instance == nil || rep.Instance.Checks == nil || rep.Instance.Checks.WarmUp == nil {
		return 0
	}
	return len(rep.Instance.Checks.WarmUp.Rounds)
}

// TestStartWarmUpRounds covers the Test plan's Warm-up item: an engine slow on
// the first request of each shape settles in two rounds (judged on the Step that
// reports the second round, ready on the next, which reports the warm-up done,
// with no third round); one whose latency never
// settles fails after five (QualificationFailed on the Step that reports round
// 5, not at round 4).
func TestStartWarmUpRounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		round  func(int32, agentapi.WarmUpRequest) agentapi.WarmUpRound
		rounds int
		ready  bool
	}{
		{"slow-first-round-settles-in-two", afake.WarmUpSlowFirstRound, 2, true},
		{"never-settles-fails-after-five", afake.WarmUpNeverSettles, 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			rg.engine = func(string) afake.Engine { return afake.Engine{WarmUpRound: tc.round} }
			rg.create(mdSpec("md"))
			startUntil(rg, "md", 10*time.Minute, "an Instance", func() bool { return len(rg.instances("md")) == 1 })
			name, alloc := rg.instances("md")[0].Name, rg.instances("md")[0].Spec.AllocationRef.Name
			for i := 0; i < 120; i++ {
				startStep(rg, "md", startTick)
				in := rg.instance(name)
				done := in.Status.Phase == v1alpha1.InstanceReady || in.Status.Phase == v1alpha1.InstanceEnded
				rounds := startRounds(rg.agents[alloc])
				if rounds < tc.rounds && done {
					t.Fatalf("Instance %s is %s after %d warm-up rounds, want %d", name, in.Status.Phase, rounds, tc.rounds)
				}
				if rounds >= tc.rounds {
					if tc.ready && !done && rounds == tc.rounds {
						// Warm-up gates ready: on the Step that reports the
						// settling round the controller has judged it (WarmedUp
						// true) and stops asking for rounds; the Instance is ready
						// on the next Step, which reports the warm-up done, with
						// no further round run (InstanceStatus.ReadyTime).
						if c := startStatus(in.Status.Conditions, v1alpha1.ConditionWarmedUp); c != "True" {
							t.Fatalf("after the settling round %d WarmedUp is %q, want True", rounds, c)
						}
						startStep(rg, "md", startTick)
						in = rg.instance(name)
						done = in.Status.Phase == v1alpha1.InstanceReady || in.Status.Phase == v1alpha1.InstanceEnded
						rounds = startRounds(rg.agents[alloc])
					}
					if !done || rounds != tc.rounds {
						t.Fatalf("after %d rounds the Instance is %s with %d rounds reported", tc.rounds, in.Status.Phase, rounds)
					}
					if tc.ready != (in.Status.Phase == v1alpha1.InstanceReady) || (!tc.ready && in.Status.EndReason != v1alpha1.ReasonQualificationFailed) {
						t.Fatalf("phase %s end reason %q, want ready=%v and QualificationFailed otherwise", in.Status.Phase, in.Status.EndReason, tc.ready)
					}
					if tc.ready && (in.Status.Throughput == nil || in.Status.Throughput.TargetSessions != 4) {
						t.Errorf("the settled round's throughput is not recorded: %+v", in.Status.Throughput)
					}
					return
				}
			}
			t.Fatal("warm-up never finished")
		})
	}
}

// startApplyShim makes the rig's fake client take a server-side apply to a
// Provider's status as the API server does. The fake client, unlike the API
// server, wants a resourceVersion on an apply to the status subresource
// (pkg/provider/fake/controller_status_test.go says the same), and the
// controller's DialHomeUnreachable apply (setDialHome) carries none, so without
// this the registration timeout's pass fails with a conflict. Reported to the
// parent as a rig gap.
func startApplyShim() rigOption {
	return withInterceptors(interceptor.Funcs{SubResourceApply: func(ctx context.Context, c client.Client, sub string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
		if u, ok := obj.(interface {
			GetName() string
			SetResourceVersion(string)
		}); ok {
			var p v1alpha1.Provider
			if err := c.Get(ctx, client.ObjectKey{Name: u.GetName()}, &p); err != nil {
				return err
			}
			u.SetResourceVersion(p.ResourceVersion)
		}
		return c.SubResource(sub).Apply(ctx, obj, opts...)
	}})
}

// TestStartRegistrationTimeout covers "An agent that never registers fails its
// Instance after health.registrationTimeout" (15 min) from the job's start, not
// before, anchored on the fake Slurm's own start time: not ended at the last
// Step before the deadline, ended at the first Step at or after it, with
// RegistrationTimeout, a failing event, the Allocation released, a new one
// provisioned and the Provider's DialHomeUnreachable True (kept on later Steps).
func TestStartRegistrationTimeout(t *testing.T) {
	rg := newRig(t, startApplyShim())
	rg.noAgents = true
	rg.create(mdSpec("md"))
	startUntil(rg, "md", 5*time.Minute, "a running Allocation", func() bool {
		as := rg.allocations("md")
		return len(as) == 1 && as[0].Status.AtProvider.StartTime != nil && as[0].Annotations[v1alpha1.AnnotationJobID] != ""
	})
	first, a0 := rg.instances("md")[0].Name, rg.allocations("md")[0]
	job, ok := rg.batch.Job(a0.Annotations[v1alpha1.AnnotationJobID])
	if !ok || job.StartTime == nil {
		t.Fatalf("the fake Slurm has no started job %q", a0.Annotations[v1alpha1.AnnotationJobID])
	}
	deadline := job.StartTime.Add(15 * time.Minute)
	for {
		startStep(rg, "md", startTick)
		ended := rg.instance(first).Status.Phase == v1alpha1.InstanceEnded
		if rg.clock.Now().Before(deadline) {
			if ended {
				t.Fatalf("Instance ended at %s, before the deadline %s (job start %s + 15m)", rg.clock.Now(), deadline, job.StartTime)
			}
			continue
		}
		if !ended {
			t.Fatalf("Instance not ended at %s, the first Step at or after the deadline %s", rg.clock.Now(), deadline)
		}
		break
	}
	if got := rg.instance(first).Status.EndReason; got != v1alpha1.ReasonRegistrationTimeout {
		t.Errorf("EndReason = %q, want RegistrationTimeout", got)
	}
	if n := len(rg.rec.Find("failing", v1alpha1.ReasonRegistrationTimeout)); n != 1 {
		t.Errorf("failing/RegistrationTimeout events = %d, want 1", n)
	}
	dialHome := func() string {
		var p v1alpha1.Provider
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &p); err != nil {
			t.Fatal(err)
		}
		return startStatus(p.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	}
	for i := 0; i < 3; i++ {
		if got := dialHome(); got != "True" {
			t.Fatalf("Provider DialHomeUnreachable = %q %d Steps after the failure, want True", got, i)
		}
		startStep(rg, "md", startTick)
	}
	startUntil(rg, "md", 2*time.Minute, "the failed Allocation released and a new one", func() bool {
		_, alive := rg.allocation(a0.Name)
		return !alive && len(rg.allocations("md")) == 1
	})
	if rg.allocations("md")[0].Name == a0.Name || rg.submissions() != 2 {
		t.Errorf("want one new Allocation and 2 submissions, got %q and %d", rg.allocations("md")[0].Name, rg.submissions())
	}
	// The path comes back: the next agent on the Provider registers, and the
	// condition is cleared, False, by the same server-side apply.
	if got := dialHome(); got != "True" {
		t.Fatalf("Provider DialHomeUnreachable = %q before any agent registered, want True", got)
	}
	rg.noAgents = false
	startUntil(rg, "md", 5*time.Minute, "DialHomeUnreachable cleared by the next registration", func() bool { return dialHome() == "False" })
	if in := rg.instances("md"); in[len(in)-1].Status.Agent == nil {
		t.Errorf("the condition was cleared with no registration accepted on the new Instance")
	}
}

// TestStartWarmAllocationGetsFullStartTimeout covers "An Instance placed on a
// warm Allocation that started more than health.startTimeout ago gets the full
// timeout from its own start instruction". startTimeout is 20 min; the first
// Instance ends (scale 1 -> 0, retention keep) and the Allocation stays; the
// deployment scales back to 1 only after the first start instruction + 20 min.
// A timeout counted from the old start (or the Allocation's) would fail the new
// Instance at once. Control: with the agent silenced, the new Instance fails at
// its own start instruction + 20 min, not before.
func TestStartWarmAllocationGetsFullStartTimeout(t *testing.T) {
	setup := func(t *testing.T) (*rig, string, string) {
		rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
			p.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(20 * time.Minute)}
		}))
		rg.create(mdSpec("md"))
		first, alloc := startReady(rg)
		oldDeadline := first.Status.StartInstructionTime.Add(20 * time.Minute)
		rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 0 })
		startUntil(rg, "md", 10*time.Minute, "the first Instance ended", func() bool { return rg.instance(first.Name).Status.Phase == v1alpha1.InstanceEnded })
		for !rg.clock.Now().After(oldDeadline.Add(time.Minute)) {
			startStep(rg, "md", startTick)
		}
		if _, ok := rg.allocation(alloc); !ok {
			t.Fatal("the Allocation was released; retention keep must hold it as warm capacity")
		}
		rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 })
		return rg, first.Name, alloc
	}
	t.Run("full-timeout-from-own-start", func(t *testing.T) {
		rg, first, alloc := setup(t)
		startUntil(rg, "md", time.Minute, "a second Instance", func() bool { return len(rg.instances("md")) == 2 })
		second := rg.instances("md")[1]
		if second.Name == first || second.Spec.AllocationRef.Name != alloc {
			t.Fatalf("the new Instance %s must run on the warm Allocation %s, not on %s", second.Name, alloc, second.Spec.AllocationRef.Name)
		}
		startUntil(rg, "md", 5*time.Minute, "the new Instance ready", func() bool { return rg.readyCount("md") == 1 })
		if st := rg.instance(second.Name).Status; st.Phase != v1alpha1.InstanceReady || rg.submissions() != 1 {
			t.Fatalf("new Instance %s (%s), submissions %d; want ready on the warm Allocation, no new job", st.Phase, st.EndReason, rg.submissions())
		}
	})
	t.Run("own-timeout-fires", func(t *testing.T) {
		rg, _, alloc := setup(t)
		startUntil(rg, "md", time.Minute, "a second Instance", func() bool { return len(rg.instances("md")) == 2 })
		second := rg.instances("md")[1].Name
		rg.agentOf(alloc).agent.Silence(true) // it never gets the start instruction's desired state
		startUntil(rg, "md", time.Minute, "the new Instance starting", func() bool { return rg.instance(second).Status.Phase == v1alpha1.InstanceStarting })
		start := rg.clock.Now()
		if got := rg.instance(second).Status.StartInstructionTime; got == nil || !got.Time.Equal(start) {
			t.Fatalf("StartInstructionTime = %v, want the Step it started at, %s", got, start)
		}
		deadline := start.Add(20 * time.Minute)
		for {
			startStep(rg, "md", startTick)
			ended := rg.instance(second).Status.Phase == v1alpha1.InstanceEnded
			if rg.clock.Now().Before(deadline) && ended {
				t.Fatalf("ended at %s, before its own start instruction %s + 20m", rg.clock.Now(), start)
			}
			if !rg.clock.Now().Before(deadline) {
				if !ended || rg.instance(second).Status.EndReason != v1alpha1.ReasonStartTimeout {
					t.Fatalf("at %s: phase %s reason %q, want ended/StartTimeout", rg.clock.Now(), rg.instance(second).Status.Phase, rg.instance(second).Status.EndReason)
				}
				return
			}
		}
	})
}

// startProbe returns an agent that presents the given credential, tag and job
// ID to the rig's gate.
func startProbe(rg *rig, cred, tag, job string) *afake.Agent {
	return afake.NewAgent(rg.gate.As(cred, tag, job), agentapi.Caller{}, rg.clock.Now, afake.Engine{})
}

// TestStartAgentRefusedWithWrongIdentity covers "a registration with a wrong
// credential, or with the right credential and another job ID, is refused": the
// gate records the codes, the controller registers nothing, and the real
// Instance stays ready (a second registration of the job would fail it as a
// requeue). Control: before the probes the real agent has been refused nothing.
func TestStartAgentRefusedWithWrongIdentity(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	if r := rg.gate.Refusals(); len(r) != 0 {
		t.Fatalf("the real agent was refused: %+v", r)
	}
	a, _ := rg.allocation(alloc)
	tag, job := a.Annotations[v1alpha1.AnnotationExternalName], a.Annotations[v1alpha1.AnnotationJobID]
	cred, err := startCredential(rg, a)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		who  *afake.Agent
		code agentapi.Code
	}{
		{"wrong-credential", startProbe(rg, cred+"x", tag, job), agentapi.CodeUnauthenticated},
		{"other-job-id", startProbe(rg, cred, tag, job+"-other"), agentapi.CodeJobMismatch},
	} {
		var ae *agentapi.Error
		if err := tc.who.Step(context.Background()); !errors.As(err, &ae) || ae.Code != tc.code {
			t.Errorf("%s: Step returned %v, want an agentapi error with code %s", tc.name, err, tc.code)
		}
	}
	refusals := rg.gate.Refusals()
	if len(refusals) != 2 || refusals[0].Code != agentapi.CodeUnauthenticated || refusals[1].Code != agentapi.CodeJobMismatch ||
		refusals[0].Method != afake.MethodRegister || refusals[1].Method != afake.MethodRegister || refusals[0].Tag != tag {
		t.Errorf("refusals = %+v, want a refused register for each probe", refusals)
	}
	if n := len(rg.r.Backend.registrations(a.UID)); n != 1 {
		t.Errorf("%d registrations, want only the real agent's", n)
	}
	for i := 0; i < 6; i++ {
		startStep(rg, "md", startTick)
	}
	if got := rg.instance(in.Name).Status; got.Phase != v1alpha1.InstanceReady {
		t.Errorf("the real Instance is %s (%s) after the refused registrations, want ready", got.Phase, got.EndReason)
	}
}

// startCredential reads an Allocation's credential file from the rig's directory.
func startCredential(rg *rig, a *v1alpha1.Allocation) (string, error) {
	b, err := os.ReadFile(agentapi.AllocationCredentialPath(rg.creds, string(a.UID)))
	return string(b), err
}

// startStatus returns the status of a condition, "" when it is absent.
func startStatus(conds []metav1.Condition, typ string) string {
	if c := apimeta.FindStatusCondition(conds, typ); c != nil {
		return string(c.Status)
	}
	return ""
}

// TestStartAgentUnreachableRestartsNothing covers "An agent that stops reporting
// makes its Instance unreachable, and nothing is restarted": AgentUnreachable
// turns True at the last report + health.agentTimeout (2 min), not before, and
// the Instance stays ready with no restart, although its engine has stalled
// past noProgressTimeout (5 min here) meanwhile; reports resuming clear it.
// Control: the same stall restarts the worker once the reports are heard again.
func TestStartAgentUnreachableRestartsNothing(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Health = &v1alpha1.HealthThresholds{NoProgressTimeout: dur(5 * time.Minute)}
	}))
	rg.engine = func(string) afake.Engine { return afake.Engine{Health: afake.HealthStalled} }
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	ag := rg.agentOf(alloc).agent
	last, _ := ag.LastReport()
	ag.Silence(true)
	unreachable := func() string {
		return startStatus(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable)
	}
	for silent := time.Duration(0); silent < 8*time.Minute; {
		startStep(rg, "md", startTick)
		silent = rg.clock.Now().Sub(last.Time)
		want := "False"
		if silent >= 2*time.Minute {
			want = "True"
		}
		if got := unreachable(); got != want {
			t.Fatalf("%s after the last report: AgentUnreachable = %q, want %q", silent, got, want)
		}
		if st := rg.instance(in.Name); st.Status.Phase != v1alpha1.InstanceReady || len(rg.rec.Find("failing", "")) != 0 || len(st.Annotations) != 0 {
			t.Fatalf("%s silent: phase %s, %d failing events, annotations %v; nothing may restart", silent, st.Status.Phase, len(rg.rec.Find("failing", "")), st.Annotations)
		}
	}
	ag.Silence(false)
	startStep(rg, "md", startTick)
	if got := unreachable(); got != "False" {
		t.Errorf("AgentUnreachable = %q after the reports resumed, want False", got)
	}
	if n := len(rg.rec.Find("failing", "WorkerRestart")); n != 1 {
		t.Errorf("worker restarts = %d once reports resumed, want 1 (the stall was overdue, so the rule was armed all along)", n)
	}
}
