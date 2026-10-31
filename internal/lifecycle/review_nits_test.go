package lifecycle

// Tests for the PR 88 review's nits that change behaviour: failures item by
// item, the secrets sweep, a failed staging, a one-off verification read
// error, a replaced agent build, a deleted deployment's last pass, and warm
// capacity during the restart grace.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestReviewOneInstancesFailureDoesNotHoldTheOthers: the status writes of one
// Instance fail every time; the deployment's other Instance still becomes
// ready. Without the fix the pass stops at the first failing Instance.
func TestReviewOneInstancesFailureDoesNotHoldTheOthers(t *testing.T) {
	f := &faults{}
	rg := newRig(t, withInterceptors(f.funcs()))
	md := mdSpec(hoMD)
	md.Spec.Instances = 2
	rg.create(md)
	fixStep(rg, 10*time.Second)
	insts := rg.instances(hoMD)
	if len(insts) != 2 {
		t.Fatalf("%d Instances, want 2", len(insts))
	}
	stuck := insts[0].Name
	f.set(func(op string, obj client.Object) bool {
		in, ok := obj.(*v1alpha1.Instance)
		return ok && op == "status" && in.Name == stuck
	})
	for i := 0; i < 12; i++ {
		fixStep(rg, 10*time.Second)
	}
	if rg.instance(insts[1].Name).Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the healthy Instance is %s while the other's writes fail, want ready", rg.instance(insts[1].Name).Status.Phase)
	}
}

// TestReviewSecretsSweep: the credential file of an Allocation and the key
// file of an Instance that no longer exist are removed, old enough; a live
// object's file and a young orphan stay. Without the fix, files of objects
// gone before a controller restart, or released by another deployment's pass,
// were never removed.
func TestReviewSecretsSweep(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	in := rg.instances(hoMD)[0]
	a, _ := rg.allocation(in.Spec.AllocationRef.Name)
	key, _ := agentapi.NewKey()
	orphanCred := agentapi.AllocationCredentialPath(rg.creds, "11111111-2222-3333-4444-555555555555")
	orphanKey := agentapi.InstanceKeyPath(rg.creds, "66666666-7777-8888-9999-000000000000")
	young := agentapi.InstanceKeyPath(rg.creds, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	for _, p := range []string{orphanCred, orphanKey, young} {
		if err := agentapi.WriteKeyFile(p, key); err != nil {
			t.Fatal(err)
		}
	}
	old := rg.clock.Now().Add(-time.Hour)
	for _, p := range []string{orphanCred, orphanKey} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	// Written 1 min 30 s into the run: under 2 min old at every sweep of it.
	youngAt := rg.clock.Now().Add(90 * time.Second)
	if err := os.Chtimes(young, youngAt, youngAt); err != nil {
		t.Fatal(err)
	}
	rg.run(2*time.Minute, 10*time.Second) // past one sweep interval
	for _, p := range []string{orphanCred, orphanKey} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("orphan %s is still there (%v)", filepath.Base(p), err)
		}
	}
	if _, err := os.Stat(young); err != nil {
		t.Errorf("a young file was swept: %v", err)
	}
	if _, err := os.Stat(agentapi.AllocationCredentialPath(rg.creds, string(a.UID))); err != nil {
		t.Errorf("the live Allocation's credential was swept: %v", err)
	}
}

// TestReviewFailedStagingTakesTheInstanceRung: a staging that fails is retried
// at once at the whole-Instance rung (the agent retries staging only on a
// raised Instance generation), not left to health.startTimeout (1 h).
func TestReviewFailedStagingTakesTheInstanceRung(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{StagingDuration: 10 * time.Second, StagingError: "no space left on the staging directory"}
	}
	rg.create(mdSpec(hoMD))
	rg.run(3*time.Minute, 10*time.Second)
	in := rg.instances(hoMD)[0]
	if n := len(rg.rec.Find(eventFailing, v1alpha1.ReasonInstanceRestart)); n == 0 || restartGeneration(rg.instance(in.Name), v1alpha1.PartInstance) == 0 {
		t.Fatalf("no whole-Instance restart within 3 min of a failed staging (%d events)", n)
	}
	got := rg.instance(in.Name)
	if len(got.Status.Restarts) == 0 || got.Status.Restarts[0].Signal != signalStaging {
		t.Fatalf("restarts %+v, want the first with signal %s", got.Status.Restarts, signalStaging)
	}
}

// TestReviewOneOffVerifyReadErrorRetries: an agent that could not complete one
// verification (an engine read failed) gets a worker restart and verifies
// again, rather than losing the Allocation.
func TestReviewOneOffVerifyReadErrorRetries(t *testing.T) {
	rg := newRig(t)
	calls := 0
	rg.engine = func(string) afake.Engine {
		return afake.Engine{Verify: func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			calls++
			v := afake.DefaultVerify(i, r)
			if calls == 1 {
				v.Error = "the metrics read timed out"
			}
			return v
		}}
	}
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready after the retry", func() bool { return rg.readyCount(hoMD) == 1 })
	in := rg.instances(hoMD)[0]
	if len(rg.instances(hoMD)) != 1 || rg.submissions() != 1 {
		t.Fatalf("%d Instances and %d submissions, want the first Instance kept", len(rg.instances(hoMD)), rg.submissions())
	}
	if restartGeneration(rg.instance(in.Name), "worker-0") != 1 {
		t.Fatalf("restart-worker-0 %d, want 1", restartGeneration(rg.instance(in.Name), "worker-0"))
	}
}

// TestReviewReplacedAgentBuildIsReadAgain: the digest of an agent build is
// read again when the file changes, without a controller restart.
func TestReviewReplacedAgentBuildIsReadAgain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, agentapi.AgentBuildName("aarch64"))
	if err := os.WriteFile(path, []byte("build one"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := &agentBuilds{dir: dir}
	d1, err := b.digest("aarch64")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("build two, longer"), 0o600); err != nil {
		t.Fatal(err)
	}
	d2, err := b.digest("aarch64")
	if err != nil {
		t.Fatal(err)
	}
	if d1 == d2 {
		t.Fatal("the replaced build kept its old digest")
	}
}

// TestReviewDeletedDeploymentStopsRequeuing: a deleted deployment with no
// Instance or Allocation left gets no further pass.
func TestReviewDeletedDeploymentStopsRequeuing(t *testing.T) {
	rg := newRig(t)
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "gone"}})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("Reconcile of a deployment with nothing left: %+v, %v; want no requeue", res, err)
	}
}

// TestReviewWarmCapacityWaitsForItsAgentAfterARestart: after a controller
// restart, a warm Allocation whose agent has not registered again yet is
// waited for during the grace, not passed over for a new submission; once its
// agent registers, the Instance is placed on it.
func TestReviewWarmCapacityWaitsForItsAgentAfterARestart(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec(hoMD))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	warm := rg.allocations(hoMD)[0]
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(2*time.Minute, 10*time.Second, "scaled to zero", func() bool { return len(rg.liveInstances(hoMD)) == 0 })
	ag := rg.agentOf(warm.Name).agent
	ag.Silence(true)
	rg.restartController()
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	before := rg.submissions()
	rg.run(30*time.Second, 10*time.Second) // inside the 45 s grace
	if rg.submissions() != before {
		t.Fatalf("a new Allocation was submitted during the grace while warm capacity waits for its agent")
	}
	ag.Silence(false)
	rg.run(30*time.Second, 10*time.Second)
	live := rg.liveInstances(hoMD)
	if len(live) != 1 || live[0].Spec.AllocationRef.Name != warm.Name || rg.submissions() != before {
		t.Fatalf("live %d on %v, %d submissions; want one Instance on the warm Allocation %s and no new job", len(live), live, rg.submissions(), warm.Name)
	}
}
