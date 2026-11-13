package lifecycle

// Release (HBEP-7, Release; Test plan: "Scale-down, shutdown at the end time and
// deletion" and "An operation interrupted on one of several providers reports
// each provider's outcome"). In every test "no replacement is submitted" is
// checked as an unchanged count of sbatch calls: a count of Allocations would
// not show a second submission for one Allocation.

import (
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// releaseServingAndPending creates the deployment with two desired Instances, of
// which one Allocation serves and the other waits ten hours in the queue. The two
// submissions take the two scripts in the Provider's list order, which the
// random names leave open, so the pending one is found by its phase.
func releaseServingAndPending(t *testing.T, rg *rig, md *v1alpha1.ModelDeployment) (serving, pending v1alpha1.Allocation, inst v1alpha1.Instance) {
	t.Helper()
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	md.Spec.Instances = 2
	rg.create(md)
	rg.until(10*time.Minute, 10*time.Second, "one serving Allocation and one pending", func() bool {
		var s, p *v1alpha1.Allocation
		for _, a := range rg.allocations(md.Name) {
			switch a.Status.AtProvider.Phase {
			case v1alpha1.AllocationRunning:
				s = a.DeepCopy()
			case v1alpha1.AllocationPending:
				p = a.DeepCopy()
			}
		}
		if s == nil || p == nil || rg.readyCount(md.Name) != 1 {
			return false
		}
		serving, pending = *s, *p
		return true
	})
	for _, in := range rg.liveInstances(md.Name) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			inst = in
		}
	}
	return serving, pending, inst
}

// TestReleaseScaleDownCancelsPendingAllocationFirst covers scale-down (instances
// 2 to 1): "Delete pending Allocations first". The pending Allocation is
// released and its job cancelled; the serving Instance is not drained.
func TestReleaseScaleDownCancelsPendingAllocationFirst(t *testing.T) {
	rg := newRig(t)
	serving, pending, inst := releaseServingAndPending(t, rg, mdSpec("md"))
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.until(2*time.Minute, 10*time.Second, "pending Allocation released", func() bool { return allocReleased(rg, pending.Name) })
	rg.run(time.Hour, time.Minute)
	if n := allocCount(rg.batch, "scancel", allocJobID(&pending)); n < 1 {
		t.Errorf("scancel calls for the pending job = %d, want at least 1", n)
	}
	if got := rg.submissions(); got != 2 {
		t.Errorf("sbatch calls = %d, want the 2 from before the scale-down", got)
	}
	if p := rg.instance(inst.Name).Status.Phase; p != v1alpha1.InstanceReady || allocReleased(rg, serving.Name) {
		t.Errorf("serving Instance %q, Allocation released %v: a pending Allocation goes first", p, allocReleased(rg, serving.Name))
	}
}

// TestReleaseScaleDownDrainsNearestHardEnd covers scale-down with both Instances
// ready: "drain the ready primaries with the nearest hard end". The first job
// started 10 min before the second, so its hard end is nearer. Its Instance
// drains and ends with no replacement, and its Allocation, now empty, follows
// the default policy: kept (the deployment has no end time) while its lease lasts.
func TestReleaseScaleDownDrainsNearestHardEnd(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	first := rg.liveInstances("md")[0]
	rg.run(10*time.Minute, 10*time.Second)
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	rg.until(10*time.Minute, 10*time.Second, "two ready Instances", func() bool { return rg.readyCount("md") == 2 })
	var second v1alpha1.Instance
	for _, in := range rg.liveInstances("md") {
		if in.Name != first.Name {
			second = in
		}
	}
	if f, s := rg.instance(first.Name).Status.LeaseEnd, second.Status.LeaseEnd; f == nil || s == nil || !f.Before(s) {
		t.Fatalf("lease ends %v and %v: the first Instance must have the nearer hard end", f, s)
	}
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	ended := allocUntilEnded(rg, first.Name, 5*time.Minute)
	if ended.Status.EndReason != v1alpha1.ReasonDrained {
		t.Errorf("EndReason = %q, want %q", ended.Status.EndReason, v1alpha1.ReasonDrained)
	}
	rg.run(time.Hour, time.Minute)
	if p := rg.instance(second.Name).Status.Phase; p != v1alpha1.InstanceReady {
		t.Errorf("the second Instance is %q, want it to go on serving", p)
	}
	if allocReleased(rg, first.Spec.AllocationRef.Name) {
		t.Error("the drained Instance's Allocation was released: the default policy keeps it")
	}
	if got := rg.submissions(); got != 2 {
		t.Errorf("sbatch calls = %d, want 2: a scale-down submits nothing", got)
	}
}

// TestReleaseEndTimeDrainsAndCancelsPending covers "Shutdown at the deployment's
// end time": the serving Instance drains and ends, the pending Allocation is
// cancelled, and the started Allocation follows its retention policy. The
// default policy keeps until the deployment's end time, which has come, so it is
// released too. Nothing is submitted.
func TestReleaseEndTimeDrainsAndCancelsPending(t *testing.T) {
	rg := newRig(t)
	end := metav1.NewTime(rigStart.Add(30 * time.Minute))
	md := mdSpec("md")
	md.Spec.EndTime = &end
	serving, pending, inst := releaseServingAndPending(t, rg, md)
	allocRunTo(rg, end.Add(-10*time.Second), 10*time.Second)
	if allocReleased(rg, pending.Name) || allocReleased(rg, serving.Name) || rg.instance(inst.Name).Status.Phase != v1alpha1.InstanceReady {
		t.Fatal("before the end time the deployment must stand as it was")
	}
	ended := allocUntilEnded(rg, inst.Name, 2*time.Minute)
	if ended.Status.EndReason != v1alpha1.ReasonDrained {
		t.Errorf("EndReason = %q, want %q", ended.Status.EndReason, v1alpha1.ReasonDrained)
	}
	rg.until(2*time.Minute, 10*time.Second, "both Allocations released", func() bool {
		return allocReleased(rg, pending.Name) && allocReleased(rg, serving.Name)
	})
	if n := allocCount(rg.batch, "scancel", allocJobID(&pending)); n < 1 {
		t.Errorf("scancel calls for the pending job = %d, want at least 1", n)
	}
	rg.run(30*time.Minute, time.Minute)
	if got := rg.submissions(); got != 2 {
		t.Errorf("sbatch calls = %d, want 2: a shutdown submits nothing", got)
	}
}

// TestReleaseDeletionDrainsInstancesThenReleasesWhateverTheRetention covers "Deletion of the ModelDeployment: as shutdown, then the
// objects are removed", and HBEP-7's rule that a deployment's deletion releases its empty Allocations at once, whatever their policy
// (issue #503). The fake client has no garbage collector, so the test deletes the Instances as it would; they keep the drain finalizer
// until they have ended. Once they have, the started Allocation is released under the default policy, under keep until a date an hour
// away, and under release-when-idle with a limit of 20 minutes: none waits for its date or its limit, which a deployment that still
// exists would (TestAnEmptyAllocationOfADeletedDeploymentGoesAtOnceButAnExistingOnesKeepsItsPolicy).
func TestReleaseDeletionDrainsInstancesThenReleasesWhateverTheRetention(t *testing.T) {
	until := metav1.NewTime(rigStart.Add(time.Hour))
	for _, tc := range []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
	}{
		{"the default policy", nil},
		{"keep until a date", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &until}},
		{"release when idle, 20 minutes", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(20 * time.Minute)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			md := mdSpec("md")
			md.Spec.RetentionPolicy = tc.policy
			serving, pending, inst := releaseServingAndPending(t, rg, md)
			if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
				t.Fatal(err)
			}
			for _, in := range rg.instances("md") {
				if err := rg.c.Delete(rg.ctx, &in); err != nil {
					t.Fatal(err)
				}
			}
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			got := rg.instance(inst.Name)
			if got.DeletionTimestamp.IsZero() || !slices.Contains(got.Finalizers, v1alpha1.FinalizerDrain) || got.Status.Phase != v1alpha1.InstanceDraining {
				t.Errorf("serving Instance after one round: phase %q, deleting %v, finalizers %v; want draining with its drain finalizer",
					got.Status.Phase, !got.DeletionTimestamp.IsZero(), got.Finalizers)
			}
			rg.until(2*time.Minute, 10*time.Second, "the Instances gone", func() bool { return len(rg.instances("md")) == 0 })
			if n := allocCount(rg.batch, "scancel", allocJobID(&pending)); n < 1 || !allocReleased(rg, pending.Name) {
				t.Errorf("pending Allocation released %v, scancel calls %d: it must be cancelled", allocReleased(rg, pending.Name), n)
			}
			// The started Allocation goes as soon as its Instance has ended: well inside the 20 minute limit and the hour's date.
			rg.until(2*time.Minute, 10*time.Second, "the serving Allocation released", func() bool { return allocReleased(rg, serving.Name) })
			rg.run(time.Minute, 10*time.Second)
			if n := allocCount(rg.batch, "scancel", allocJobID(&serving)); n < 1 {
				t.Errorf("scancel calls for the serving job = %d, want at least 1: the release's finalizer cancels it", n)
			}
			rg.run(10*time.Minute, time.Minute)
			if got := rg.submissions(); got != 2 {
				t.Errorf("sbatch calls = %d, want 2: a deletion submits nothing", got)
			}
		})
	}
}

// releaseSecond is the name of the second batch Provider.
const releaseSecond = "helios-gpu-2"

// releaseTwo is the rig with a second batch Provider on a Slurm of its own. The
// rig's step cannot serve it: its agents look jobs up in one Batch, and both
// Batches number their jobs from 998. This step picks the Batch by the
// Allocation's provider label.
type releaseTwo struct {
	*rig
	batch2  *pfake.Batch
	prov2   *pfake.Controller
	batchOf map[string]*pfake.Batch // by Allocation name
}

func newReleaseTwo(t *testing.T) *releaseTwo {
	rg := newRig(t)
	rg.create(&v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: releaseSecond, UID: newUID(), Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: "helios"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
	})
	b2 := pfake.NewBatch(rg.clock, pfake.Config{Partitions: []pfake.Partition{{Name: rigPartition, GPUModel: rigGPU, GPUsPerNode: 4, MaxTime: 48 * time.Hour}}})
	return &releaseTwo{rig: rg, batch2: b2, batchOf: map[string]*pfake.Batch{},
		prov2: &pfake.Controller{Client: rg.c, Provider: releaseSecond, Batch: b2, Clock: rg.clock, HoldRelease: true}}
}

// wait advances by 10 s and steps both Providers until cond holds.
func (r *releaseTwo) wait(limit time.Duration, what string, cond func() bool) {
	r.t.Helper()
	for end := r.clock.Now().Add(limit); !cond(); {
		if !r.clock.Now().Before(end) {
			r.t.Fatalf("%s: not reached within %s", what, limit)
		}
		r.clock.Advance(10 * time.Second)
		for _, p := range []*pfake.Controller{r.prov, r.prov2} {
			if err := p.Sync(r.ctx); err != nil {
				r.t.Fatalf("provider sync: %v", err)
			}
		}
		r.syncAgents()
		r.reconcileAll()
	}
}

func (r *releaseTwo) syncAgents() {
	var l v1alpha1.AllocationList
	if err := r.c.List(r.ctx, &l); err != nil {
		r.t.Fatal(err)
	}
	for i := range l.Items {
		a := &l.Items[i]
		b := r.batch
		if a.Labels[v1alpha1.LabelProvider] == releaseSecond {
			b = r.batch2
		}
		tag, jobID := a.Annotations[v1alpha1.AnnotationExternalName], a.Annotations[v1alpha1.AnnotationJobID]
		cred, err := os.ReadFile(agentapi.AllocationCredentialPath(r.creds, string(a.UID)))
		if tag == "" || err != nil {
			continue
		}
		r.gate.Add(tag, afake.GateAllocation{Namespace: a.Namespace, Name: a.Name, UID: string(a.UID), Credential: string(cred), JobID: jobID})
		if _, ok := r.agents[a.Name]; ok || jobID == "" {
			continue
		}
		if job, found := b.Job(jobID); found && job.StartTime != nil && job.EndTime == nil {
			r.agents[a.Name] = &rigAgent{alloc: a.Name, jobID: jobID,
				agent: afake.NewAgent(r.gate.As(string(cred), tag, jobID), agentapi.Caller{}, r.clock.Now, afake.Engine{})}
			r.batchOf[a.Name] = b
		}
	}
	for name, ra := range r.agents {
		if job, ok := r.batchOf[name].Job(ra.jobID); ra.stopped || !ok || job.EndTime != nil {
			ra.stopped = true
			continue
		}
		if err := ra.agent.Step(r.ctx); err != nil {
			r.errs = append(r.errs, err)
		}
	}
	// The tunnel follows the agent, as in the rig's own syncAgents.
	if !r.noTunnels {
		r.syncTunnels()
	}
}

// TestReleaseInterruptedProviderReportsEachOutcome covers "An operation
// interrupted on one of several providers reports each provider's outcome".
// Two batch Providers, one Instance on each: the fake has no capacity limit, so
// the second primary would go to the first Provider in order; it is placed on
// the second by listing that one first when the second Instance is added. The
// deployment then ends while the second Provider's reads fail. The lifecycle
// controller writes no per-provider condition for a release, so each outcome is
// read from the Allocation: the healthy Provider's is released and its job
// cancelled; the failing one's keeps its finalizer, with the state last read,
// and no job of it is cancelled until the Provider answers.
func TestReleaseInterruptedProviderReportsEachOutcome(t *testing.T) {
	rt := newReleaseTwo(t)
	md := mdSpec("md")
	md.Spec.Providers.Names = []string{rigProvider, releaseSecond}
	rt.create(md)
	rt.wait(10*time.Minute, "one ready Instance", func() bool { return rt.readyCount("md") == 1 })
	rt.mutateMD("md", func(m *v1alpha1.ModelDeployment) {
		m.Spec.Instances, m.Spec.Providers.Names = 2, []string{releaseSecond, rigProvider}
	})
	rt.wait(10*time.Minute, "two ready Instances", func() bool { return rt.readyCount("md") == 2 })
	byProvider := map[string]v1alpha1.Allocation{}
	for _, a := range rt.allocations("md") {
		byProvider[a.Labels[v1alpha1.LabelProvider]] = a
	}
	healthy, failing := byProvider[rigProvider], byProvider[releaseSecond]
	if len(byProvider) != 2 {
		t.Fatalf("Allocations by Provider: %v, want one on each", byProvider)
	}

	rt.batch2.FailReads(errors.New("the platform of " + releaseSecond + " does not answer"))
	rt.mutateMD("md", func(m *v1alpha1.ModelDeployment) { now := metav1.NewTime(rt.clock.Now()); m.Spec.EndTime = &now })
	rt.wait(5*time.Minute, "both Instances ended", func() bool { return len(rt.liveInstances("md")) == 0 })
	rt.wait(5*time.Minute, "the healthy Provider's Allocation gone", func() bool { _, there := rt.allocation(healthy.Name); return !there })
	later := rt.clock.Now().Add(10 * time.Minute) // the failing Provider stays stuck while time passes
	rt.wait(11*time.Minute, "ten minutes to pass", func() bool { return !rt.clock.Now().Before(later) })
	if n := allocCount(rt.batch, "scancel", allocJobID(&healthy)); n < 1 {
		t.Errorf("scancel calls on the healthy Provider = %d, want at least 1", n)
	}
	stuck, there := rt.allocation(failing.Name)
	if !there || stuck.DeletionTimestamp.IsZero() || !slices.Contains(stuck.Finalizers, v1alpha1.FinalizerCancelAll) || stuck.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the failing Provider's Allocation: %+v; want it deleting with its finalizer, in the state last read (Running)", stuck)
	}
	if n := allocCount(rt.batch2, "scancel", allocJobID(&failing)); n != 0 {
		t.Errorf("scancel calls on the failing Provider = %d while its reads fail, want 0", n)
	}

	rt.batch2.FailReads(nil)
	rt.wait(5*time.Minute, "the failing Provider's Allocation gone", func() bool { _, there := rt.allocation(failing.Name); return !there })
	if n := allocCount(rt.batch2, "scancel", allocJobID(&failing)); n < 1 {
		t.Errorf("scancel calls on the second Provider after it answered = %d, want at least 1", n)
	}
}

// TestAnEmptyAllocationOfADeletedDeploymentGoesAtOnceButAnExistingOnesKeepsItsPolicy is the reproduction of issue #503, turned into the
// behaviour HBEP-7's Release now says (the science agent's ruling on #503, 13 Nov 2026). Row A's live run timed out at 12 minutes
// waiting for its job to end after the deployment was deleted: the deployment carried the example's release-when-idle with 30 minutes,
// and a deletion was not consulted in that branch, so the idle Allocation kept its job for the whole limit. Now an empty Allocation of a
// deployment that is deleted is released at once, whatever its policy, and one of a deployment that still exists keeps its policy:
//
//	policy                      deployment deleted         deployment exists (scaled to zero)
//	release when idle, 20 min   released within a minute   kept 10 minutes in, released at the limit
//	keep until a date (1 h)     released within a minute   kept until the date, then released
//
// Each Allocation is made empty the same way (retentionEmpty: one Instance served, then spec.instances 0 and stepped until it ended), so
// the two columns differ in the deployment's deletion alone.
func TestAnEmptyAllocationOfADeletedDeploymentGoesAtOnceButAnExistingOnesKeepsItsPolicy(t *testing.T) {
	until := metav1.NewTime(rigStart.Add(time.Hour))
	for _, tc := range []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
		// release is when a deployment that exists releases the Allocation: its emptiness plus the idle limit, or the date.
		release func(emptyAt time.Time) time.Time
	}{
		{"release when idle, 20 minutes", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(20 * time.Minute)},
			func(emptyAt time.Time) time.Time { return emptyAt.Add(20 * time.Minute) }},
		{"keep until a date", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &until},
			func(time.Time) time.Time { return until.Time }},
	} {
		t.Run(tc.name+"/the deployment is deleted", func(t *testing.T) {
			rg := newRig(t)
			a, _ := retentionEmpty(t, rg, tc.policy)
			if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
				t.Fatal(err)
			}
			rg.until(time.Minute, 10*time.Second, "the empty Allocation of the deleted deployment released", func() bool { return allocReleased(rg, a.Name) })
			rg.run(time.Minute, 10*time.Second)
			if n := allocCount(rg.batch, "scancel", allocJobID(&a)); n < 1 {
				t.Errorf("scancel calls for the released job = %d, want at least 1", n)
			}
		})
		// A deployment that a finalizer holds (a foreground delete puts one) is still there, with its deletion timestamp set: it is being
		// deleted, and the Allocation goes just the same.
		t.Run(tc.name+"/the deployment is being deleted", func(t *testing.T) {
			rg := newRig(t)
			a, _ := retentionEmpty(t, rg, tc.policy)
			rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Finalizers = append(m.Finalizers, "test.example/hold") })
			if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
				t.Fatal(err)
			}
			if got := rg.md("md"); got == nil || got.DeletionTimestamp.IsZero() {
				t.Fatalf("the deployment must be held by its finalizer with its deletion timestamp set: %+v", got)
			}
			rg.until(time.Minute, 10*time.Second, "the empty Allocation of the deployment being deleted released", func() bool { return allocReleased(rg, a.Name) })
		})
		t.Run(tc.name+"/the deployment exists", func(t *testing.T) {
			rg := newRig(t)
			a, emptyAt := retentionEmpty(t, rg, tc.policy)
			release := tc.release(emptyAt)
			// Ten minutes in, nothing has released it, and nothing has cancelled its job.
			for rg.clock.Now().Before(emptyAt.Add(10 * time.Minute)) {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			if allocReleased(rg, a.Name) || allocCount(rg.batch, "scancel", allocJobID(&a)) != 0 {
				t.Fatalf("the Allocation of a deployment that exists was released 10 minutes after it emptied (scancel calls %d): its policy no longer holds",
					allocCount(rg.batch, "scancel", allocJobID(&a)))
			}
			retentionReleaseAt(t, rg, a.Name, release, 10*time.Second)
		})
	}
}
