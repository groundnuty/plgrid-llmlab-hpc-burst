package lifecycle

// Tests for the PR 88 review's blocking findings on a recreated deployment,
// warm fit, the ladder's bound, a deleted deployment's maxIdle,
// DialHomeUnreachable and the reachability grace (Astra 3-5, 8-10).

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestReviewRecreatedDeploymentDrainsThePreviousInstances (Astra 3): the
// ModelDeployment is deleted and recreated under the same name before its
// Instance has drained (the fake client has no garbage collector, so the old
// Instance stays, owned by the old UID). The new deployment drains and ends
// the old Instance, does not count it, and never places its own Instance on the
// old one's Allocation. Without the fix the old Instance serves on forever.
func TestReviewRecreatedDeploymentDrainsThePreviousInstances(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	old := rg.instances(hoMD)[0]
	if err := rg.c.Delete(rg.ctx, rg.md(hoMD)); err != nil {
		t.Fatal(err)
	}
	rg.create(mdSpec(hoMD)) // a new UID, the same name
	rg.until(5*time.Minute, 10*time.Second, "the old Instance ended", func() bool {
		return rg.instance(old.Name).Status.Phase == v1alpha1.InstanceEnded
	})
	var fresh *v1alpha1.Instance
	rg.until(5*time.Minute, 10*time.Second, "the new deployment's Instance ready", func() bool {
		for _, in := range rg.liveInstances(hoMD) {
			if in.Name != old.Name && in.Status.Phase == v1alpha1.InstanceReady {
				in := in
				fresh = &in
				return true
			}
		}
		return false
	})
	if fresh.Spec.AllocationRef.Name == old.Spec.AllocationRef.Name {
		t.Fatalf("the new Instance was placed on the old one's Allocation %s while the old one served", old.Spec.AllocationRef.Name)
	}
	if f := rg.instance(old.Name).Finalizers; len(f) != 0 {
		t.Fatalf("the old Instance still holds finalizers %v", f)
	}
	md := rg.md(hoMD)
	if md.Status.Instances != 1 || md.Status.ReadyInstances != 1 {
		t.Fatalf("the new deployment counts %d Instances (%d ready), want only its own one", md.Status.Instances, md.Status.ReadyInstances)
	}
}

// TestReviewWarmReuseChecksFit (Astra 4): a one-GPU deployment scaled to zero
// keeps its started Allocation (retention keep); tensor parallelism then rises
// to 2 and it scales up. The two-GPU launch is not placed on the one-GPU
// Allocation: a new Allocation with gpus 2 is submitted. Without the fix the
// warm Allocation is reused and no second job is submitted.
func TestReviewWarmReuseChecksFit(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	warm := rg.allocations(hoMD)[0]
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(2*time.Minute, 10*time.Second, "scaled to zero", func() bool { return len(rg.liveInstances(hoMD)) == 0 })
	if allocReleased(rg, warm.Name) {
		t.Fatal("the Allocation was not kept as warm capacity; the test proves nothing")
	}
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
		m.Spec.Instances = 1
		m.Spec.Source.Served.Flags = append(m.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "2"})
	})
	rg.run(time.Minute, 10*time.Second)
	live := rg.liveInstances(hoMD)
	if len(live) != 1 {
		t.Fatalf("%d live Instances, want 1", len(live))
	}
	if live[0].Spec.AllocationRef.Name == warm.Name {
		t.Fatalf("the two-GPU Instance was placed on the one-GPU warm Allocation %s", warm.Name)
	}
	a, ok := rg.allocation(live[0].Spec.AllocationRef.Name)
	if !ok || a.Spec.ForProvider.Batch.GPUs != 2 || rg.submissions() != 2 {
		t.Fatalf("the new Allocation (found %v) has gpus %v and %d sbatch calls, want 2 and 2", ok, a, rg.submissions())
	}
}

// TestReviewLadderBound (Astra 5): status.restarts keeps 16 restarts, so
// attemptsPerRung above 8 cannot climb; such a profile is refused
// (ProviderUnavailable naming the field) and nothing is provisioned. At the
// boundary, 8, the ladder still climbs across controller restarts: 8 worker
// restarts, 8 whole-Instance restarts, then RestartLoop. Without the fix 9 is
// accepted.
func TestReviewLadderBound(t *testing.T) {
	t.Run("9 is refused", func(t *testing.T) {
		rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
			p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(9))}
		}))
		rg.create(mdSpec(hoMD))
		rg.run(time.Minute, 10*time.Second)
		c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
		if c.Reason != v1alpha1.ReasonProviderUnavailable || !strings.Contains(c.Message, "attemptsPerRung 9") || rg.submissions() != 0 {
			t.Fatalf("Degraded %s %q, %d submissions; want ProviderUnavailable naming attemptsPerRung 9, and none", c.Reason, c.Message, rg.submissions())
		}
	})
	t.Run("8 climbs across controller restarts", func(t *testing.T) {
		rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
			p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(8)), Window: dur(4 * time.Hour)}
		}))
		rg.create(mdSpec(hoMD))
		rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
		in := rg.instances(hoMD)[0]
		ag := rg.agentOf(in.Spec.AllocationRef.Name).agent
		for n := 1; n <= 17; n++ {
			rg.until(10*time.Minute, 10*time.Second, "the worker runs", func() bool {
				cur := rg.instance(in.Name)
				if cur.Status.Phase == v1alpha1.InstanceEnded {
					return true
				}
				rep, ok := ag.LastReport()
				return ok && rep.Instance != nil && worker0(rep.Instance) != nil && worker0(rep.Instance).Phase == agentapi.PartRunning
			})
			if rg.instance(in.Name).Status.Phase == v1alpha1.InstanceEnded {
				break
			}
			rg.run(40*time.Second, 10*time.Second) // past the 30 s backoff
			ag.CrashWorker(agentapi.WorkerName(0), 1)
			rg.run(10*time.Second, 10*time.Second)
			rg.restartController()
		}
		got := rg.instance(in.Name)
		if got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonRestartLoop {
			t.Fatalf("after the ladder the Instance is %s/%s, want ended/RestartLoop", got.Status.Phase, got.Status.EndReason)
		}
		if w, i := restartGeneration(got, "worker-0"), restartGeneration(got, v1alpha1.PartInstance); w != 8 || i != 8 {
			t.Fatalf("restart generations worker-0 %d, instance %d; want 8 and 8", w, i)
		}
	})
}

// TestReviewDeletedDeploymentKeepsMaxIdle (Astra 8): the deployment is deleted
// with its Allocation kept until tomorrow (keep, until), on a profile with
// maxIdle 30 min. The profile is still read for the deleted deployment's
// Allocation, so it is released 30 min after its Instance ended. Without the
// fix it is kept until tomorrow.
func TestReviewDeletedDeploymentKeepsMaxIdle(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
	md := mdSpec(hoMD)
	tomorrow := metav1.NewTime(rigStart.Add(24 * time.Hour))
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &tomorrow}
	rg.create(md)
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	in := rg.instances(hoMD)[0]
	if err := rg.c.Delete(rg.ctx, rg.md(hoMD)); err != nil {
		t.Fatal(err)
	}
	if err := rg.c.Delete(rg.ctx, rg.instance(in.Name)); err != nil { // as garbage collection would
		t.Fatal(err)
	}
	var end time.Time
	rg.until(2*time.Minute, 10*time.Second, "the Instance ended", func() bool {
		a, ok := rg.allocation(in.Spec.AllocationRef.Name)
		if !ok {
			return true
		}
		t0, set, _ := v1alpha1.IdleSince(a)
		end = t0
		return set
	})
	if end.IsZero() {
		t.Fatal("no idle anchor was written for the deleted deployment's Allocation")
	}
	retentionReleaseAt(t, rg, in.Spec.AllocationRef.Name, end.Add(30*time.Minute), time.Minute)
}

// TestReviewDialHomeClearedByAnyRegistration (Astra 9): DialHomeUnreachable is
// True on the Provider (an earlier registration timeout). A registration
// accepted afterwards clears it, whatever the phase of the Instance on that
// Allocation: here a serving one, whose agent registers again after a
// controller restart. The first clear fails; a later pass retries it. Without
// the fix only a pending Instance's registration clears it.
func TestReviewDialHomeClearedByAnyRegistration(t *testing.T) {
	failApply := 1
	rg := newRig(t, withInterceptors(interceptor.Funcs{SubResourceApply: func(ctx context.Context, c client.Client, sub string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
		if u, ok := obj.(interface {
			GetName() string
			SetResourceVersion(string)
		}); ok {
			var p v1alpha1.Provider
			if err := c.Get(ctx, client.ObjectKey{Name: u.GetName()}, &p); err != nil {
				return err
			}
			if p.Status.LastSuccessfulStatusRead != nil && failApply > 0 && isDialHomeTrue(&p) {
				failApply--
				return errInjected
			}
			u.SetResourceVersion(p.ResourceVersion)
		}
		return c.SubResource(sub).Apply(ctx, obj, opts...)
	}}))
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	// An earlier timeout left the condition True.
	if err := rg.r.setDialHome(rg.ctx, rigProvider, true, rg.clock.Now(), "an earlier Instance could not register", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !isDialHomeTrue(rg.provider()) {
		t.Fatal("the condition was not set; the test proves nothing")
	}
	rg.run(30*time.Second, 10*time.Second)
	if !isDialHomeTrue(rg.provider()) {
		t.Fatal("a registration made before the condition turned True cleared it")
	}
	rg.restartController() // the serving Instance's agent registers again
	for i := 0; i < 6; i++ {
		fixStep(rg, 10*time.Second)
	}
	if failApply != 0 {
		t.Fatal("the clearing apply was never attempted")
	}
	if isDialHomeTrue(rg.provider()) {
		t.Fatal("DialHomeUnreachable is still True after a registration on a serving Allocation and a failed first clear")
	}
}

func isDialHomeTrue(p *v1alpha1.Provider) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == v1alpha1.ConditionDialHomeUnreachable {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

func (rg *rig) provider() *v1alpha1.Provider {
	rg.t.Helper()
	var p v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &p); err != nil {
		rg.t.Fatal(err)
	}
	return &p
}

// TestReviewReachabilityGraceAfterARestart (Astra 10): with health.agentTimeout
// 20 s, below a long poll and a report interval (45 s), a restarted controller
// judges no Instance unreachable within 45 s of its start, at any step, even
// with the agent silent; after the grace it does. Without the fix the Instance
// turns unreachable 20 s after the restart.
func TestReviewReachabilityGraceAfterARestart(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Health = &v1alpha1.HealthThresholds{AgentTimeout: dur(20 * time.Second)}
	}))
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	in := rg.instances(hoMD)[0]
	rg.agentOf(in.Spec.AllocationRef.Name).agent.Silence(true)
	rg.restartController()
	restart := rg.clock.Now()
	for rg.clock.Now().Before(restart.Add(agentGrace)) {
		rg.Step()
		if c := startStatus(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable); c == "True" {
			t.Fatalf("AgentUnreachable True at %s, inside the grace of %s after the restart at %s", rg.clock.Now().Format(time.TimeOnly), agentGrace, restart.Format(time.TimeOnly))
		}
		rg.clock.Advance(5 * time.Second)
	}
	rg.run(30*time.Second, 5*time.Second)
	if c := startStatus(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable); c != "True" {
		t.Fatalf("AgentUnreachable %q after the grace with the agent silent, want True", c)
	}
}
