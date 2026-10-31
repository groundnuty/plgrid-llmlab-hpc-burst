package lifecycle

// Tests for the PR 88 review's blocking findings on draining, scale-down,
// start-to-ready, a failed release and the idle anchor (Opus 1-3, Astra 1, 2,
// 6, 7). Each fails without its fix.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

var errInjected = errors.New("injected failure")

// fixStep is rg.Step that tolerates the controller's errors, for tests that
// inject failures into its writes; it returns them.
func fixStep(rg *rig, tick time.Duration) []error {
	rg.t.Helper()
	rg.clock.Advance(tick)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	var errs []error
	var mds v1alpha1.ModelDeploymentList
	if err := rg.c.List(rg.ctx, &mds); err != nil {
		rg.t.Fatal(err)
	}
	keys := map[string]bool{hoMD: true}
	for _, md := range mds.Items {
		keys[md.Name] = true
	}
	for k := range keys {
		if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: k}}); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// faults is a switchable set of injected write failures.
type faults struct {
	mu   sync.Mutex
	fail func(op string, obj client.Object) bool
}

func (f *faults) set(fn func(op string, obj client.Object) bool) {
	f.mu.Lock()
	f.fail = fn
	f.mu.Unlock()
}

func (f *faults) check(op string, obj client.Object) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil && f.fail(op, obj) {
		return errInjected
	}
	return nil
}

// funcs wraps the controller's writes with the faults.
func (f *faults) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := f.check("update", obj); err != nil {
				return err
			}
			return c.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := f.check("delete", obj); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := f.check("status", obj); err != nil {
				return err
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
}

// TestReviewDrainEndsOnlyOnAReportAfterTheDrainBegan (Opus 1): an idle primary
// whose last report showed no running request does not end in the pass that
// makes it draining; that report predates requests the front admitted until
// the drain start. It ends on the agent's first report after the drain began.
// Without the fix it goes ready -> ended in one pass at 09:50:20.
func TestReviewDrainEndsOnlyOnAReportAfterTheDrainBegan(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	p := hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 50, 10), time.Minute, nil) // the last report, 09:50:10, shows nothing running
	// The drain start, 09:50:20 (12:00:20 - 2h10m): one controller pass with
	// no agent step before it, so the latest report is from 09:50:10.
	rg.clock.Set(hoT(9, 50, 20))
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	rg.reconcileAll()
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("after the pass at the drain start the primary is %s, want draining (its latest report is from before the drain)", ph)
	}
	if _, ok := drainBegan(rg.instance(p.Name)); !ok {
		t.Fatal("the drain start is not persisted on the Instance")
	}
	// A controller restart keeps the rule: the persisted drain start, not memory.
	rg.restartController()
	rg.reconcileAll()
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("after a controller restart the primary is %s, want still draining", ph)
	}
	rg.run(10*time.Second, 10*time.Second) // the agent reports at 09:50:30, after the drain began
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("at 09:50:30 the primary is %s/%s, want ended/Drained on the fresh report", got.Status.Phase, got.Status.EndReason)
	}
}

// TestReviewNoRequestReadAfter (Opus 1): the drain ends only on a report that
// the controller received no earlier than the cutoff, the drain start plus a
// second. For a running worker the read-time check alone refuses an older
// report, but a report whose workers are all starting, stopped or exited has no
// read to date, so only the receipt time refuses it: it can predate requests
// the front admitted until the drain began. Each refusal has its control, the
// same report received at the cutoff, which counts. Without the receipt-time
// check a starting worker's old report ends the drain.
func TestReviewNoRequestReadAfter(t *testing.T) {
	cutoff := hoT(10, 0, 0)
	probe := defaultProbeInterval
	// report builds a view whose one worker is in phase with the given gauge
	// (nil: no Health). The report is received at at; a worker with Health read
	// its metrics at the same moment, so the sample is fresh.
	report := func(at time.Time, phase agentapi.PartPhase, running *int64) agentView {
		w := agentapi.WorkerReport{Name: agentapi.WorkerName(0), Phase: phase}
		if running != nil || phase == agentapi.PartRunning {
			read := at
			w.Health = &agentapi.Health{Time: at, ProbeOK: true, MetricsReadAt: &read, RunningRequests: running}
		}
		return agentView{
			report: &agentapi.InstanceReport{Workers: []agentapi.WorkerReport{w}}, reportAt: at, reportTime: at,
			now: at.Add(time.Second), probeInterval: probe,
		}
	}
	zero, one := int64(0), int64(1)
	before, after := cutoff.Add(-10*time.Second), cutoff.Add(5*time.Second)

	for _, phase := range []agentapi.PartPhase{agentapi.PartStarting, agentapi.PartStopped, agentapi.PartExited} {
		t.Run(string(phase)+" worker, received before the cutoff", func(t *testing.T) {
			if report(before, phase, nil).noRequestReadAfter(cutoff) {
				t.Fatalf("a report received before the cutoff, whose only worker is %s, ended the drain", phase)
			}
		})
		t.Run(string(phase)+" worker, received at the cutoff", func(t *testing.T) {
			if !report(cutoff, phase, nil).noRequestReadAfter(cutoff) {
				t.Fatalf("a report received at the cutoff, whose only worker is %s, holds a request", phase)
			}
		})
	}
	for _, tc := range []struct {
		name string
		av   agentView
		want bool
	}{
		{"running, zero known, read after the cutoff", report(after, agentapi.PartRunning, &zero), true},
		{"running, zero known, read at the cutoff", report(cutoff, agentapi.PartRunning, &zero), true},
		{"running, zero known, read before the cutoff", report(before, agentapi.PartRunning, &zero), false},
		{"running, gauge unknown (nil), read after the cutoff", report(after, agentapi.PartRunning, nil), false},
		{"running, one request, read after the cutoff", report(after, agentapi.PartRunning, &one), false},
		{"stopping, zero known, read after the cutoff", report(after, agentapi.PartStopping, &zero), true},
		{"stopping, one request, read after the cutoff", report(after, agentapi.PartStopping, &one), false},
		{"no report", agentView{reportAt: after, now: after, probeInterval: probe}, false},
		{"a report with no worker", agentView{report: &agentapi.InstanceReport{}, reportAt: after, reportTime: after, now: after, probeInterval: probe}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.av.noRequestReadAfter(cutoff); got != tc.want {
				t.Fatalf("noRequestReadAfter = %v, want %v", got, tc.want)
			}
		})
	}
	t.Run("running, zero known, fresh metrics but a stale receipt", func(t *testing.T) {
		av := report(after, agentapi.PartRunning, &zero)
		av.now = after.Add(10 * time.Minute) // the report arrived long ago and no newer came
		if av.noRequestReadAfter(cutoff) {
			t.Fatal("a zero gauge from a report received ten minutes ago counted as fresh")
		}
	})
}

// TestReviewScaleDownDrainsAReadyReplacement (Astra 2, Opus 2): with two
// primaries and their ready replacements, scaling 2 -> 1 drains the surplus
// primary and its replacement; neither is cut, and the replacement's started
// Allocation follows retention (kept), not released. Without the fix the
// replacement ends at once and its Allocation is deleted.
func TestReviewScaleDownDrainsAReadyReplacement(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	md := mdSpec(hoMD)
	md.Spec.Instances = 2
	rg.create(md)
	// With no queue wait observed, each replacement is created at its primary's start.
	for i := 0; i < 30 && rg.readyCount(hoMD) < 4; i++ {
		rg.clock.Advance(10 * time.Second)
		hoStepNoWaits(rg)
	}
	if n := rg.readyCount(hoMD); n != 4 {
		t.Fatalf("%d ready, want two primaries and two ready replacements", n)
	}
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.clock.Advance(10 * time.Second)
	hoStepNoWaits(rg)
	var drainedRepl *v1alpha1.Instance
	for _, in := range rg.instances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] != "" && in.Status.Phase != v1alpha1.InstanceReady {
			in := in
			drainedRepl = &in
		}
	}
	if drainedRepl == nil {
		t.Fatal("no replacement was taken out with its surplus predecessor")
	}
	if drainedRepl.Status.Phase != v1alpha1.InstanceDraining {
		t.Fatalf("the surplus predecessor's ready replacement %s is %s/%s, want draining, not cut", drainedRepl.Name, drainedRepl.Status.Phase, drainedRepl.Status.EndReason)
	}
	rg.run(time.Minute, 10*time.Second)
	got := rg.instance(drainedRepl.Name)
	if got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("the drained replacement is %s/%s, want ended/Drained", got.Status.Phase, got.Status.EndReason)
	}
	if allocReleased(rg, got.Spec.AllocationRef.Name) {
		t.Fatal("the drained replacement's started Allocation was released; it must follow its retention policy (keep)")
	}
	if n := rg.readyCount(hoMD); n < 1 {
		t.Fatalf("%d ready after the scale-down, want at least the kept primary", n)
	}
}

// TestReviewScaleDownReleasesOnlyAnAllocationThatNeverStarted (Astra 2, and the
// Opus review fix): on scale-down a surplus Instance that is pending or still
// starting ends at once, but its Allocation is released only if its job has
// not started (HBEP-7, Release). Three Instances are wanted: one serves, one
// is on a job that started and whose engine stays in phase starting, and one
// waits in the queue. When instances drops from 3 to 1 the last two are
// surplus. The queued one's Allocation is released and its job cancelled
// (control); the started one's follows its retention policy (keep) and stays,
// since its model is loading and a later scale-up can reuse it. Without the
// rule the started Allocation is deleted with its job.
func TestReviewScaleDownReleasesOnlyAnAllocationThatNeverStarted(t *testing.T) {
	rg := newRig(t)
	agents := 0
	rg.engine = func(string) afake.Engine {
		agents++
		if agents == 1 {
			return afake.Engine{} // the first job to start serves
		}
		return afake.Engine{StartDelay: 30 * time.Minute} // the next stays starting
	}
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 2 * time.Minute}, pfake.Script{QueueWait: 10 * time.Hour})
	md := mdSpec(hoMD)
	md.Spec.Instances = 3
	rg.create(md)
	var serving, starting, waiting *v1alpha1.Instance
	rg.until(10*time.Minute, 10*time.Second, "one Instance ready, one starting on a started job, one waiting", func() bool {
		serving, starting, waiting = nil, nil, nil
		for _, in := range rg.liveInstances(hoMD) {
			in := in
			a, ok := rg.allocation(in.Spec.AllocationRef.Name)
			switch {
			case !ok:
			case in.Status.Phase == v1alpha1.InstanceReady:
				serving = &in
			case a.Status.AtProvider.StartTime != nil && in.Status.Phase == v1alpha1.InstanceStarting:
				starting = &in
			case a.Status.AtProvider.StartTime == nil && in.Status.Phase == v1alpha1.InstancePending:
				waiting = &in
			}
		}
		return serving != nil && starting != nil && waiting != nil
	})
	// The scenario is the one the rule is about: a job that started and an
	// Instance on it that does not serve, and a job still in the queue.
	startedAlloc, _ := rg.allocation(starting.Spec.AllocationRef.Name)
	if startedAlloc.Status.AtProvider.StartTime == nil || startedAlloc.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the starting Instance's Allocation has not started its job: %+v", startedAlloc.Status.AtProvider)
	}
	waitingAlloc, _ := rg.allocation(waiting.Spec.AllocationRef.Name)
	if waitingAlloc.Status.AtProvider.StartTime != nil {
		t.Fatalf("the waiting Instance's Allocation has started: %+v", waitingAlloc.Status.AtProvider)
	}

	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.until(2*time.Minute, 10*time.Second, "both surplus Instances ended", func() bool {
		return hoPhase(rg, starting.Name) == v1alpha1.InstanceEnded && hoPhase(rg, waiting.Name) == v1alpha1.InstanceEnded
	})
	for _, in := range []*v1alpha1.Instance{starting, waiting} {
		if got := rg.instance(in.Name); got.Status.EndReason != v1alpha1.ReasonDrained {
			t.Fatalf("surplus Instance %s ended with %q, want %q", in.Name, got.Status.EndReason, v1alpha1.ReasonDrained)
		}
	}
	rg.run(5*time.Minute, 10*time.Second)
	if !allocReleased(rg, waiting.Spec.AllocationRef.Name) {
		t.Error("the Allocation of the surplus Instance whose job never started was not released")
	}
	if n := allocCount(rg.batch, "scancel", allocJobID(waitingAlloc)); n < 1 {
		t.Errorf("scancel calls for the queued job = %d, want at least 1", n)
	}
	if allocReleased(rg, starting.Spec.AllocationRef.Name) {
		t.Error("the Allocation of the surplus starting Instance was released although its job had started; it must follow its retention policy (keep)")
	}
	if p := hoPhase(rg, serving.Name); p != v1alpha1.InstanceReady || allocReleased(rg, serving.Spec.AllocationRef.Name) {
		t.Errorf("the serving Instance is %s (Allocation released %v), want ready and kept", p, allocReleased(rg, serving.Spec.AllocationRef.Name))
	}
}

// TestReviewStartToReadyIsRecoveredAfterALostWrite (Astra 6, Opus 3): the
// ModelDeployment's status write fails in the passes around the first
// Instance becoming ready; a later pass still writes the class's
// start-to-ready time, from the Instance's ReadyTime (08:01:10) minus its
// Allocation's start (08:00:20) = 50 s. Without the fix it stays empty.
func TestReviewStartToReadyIsRecoveredAfterALostWrite(t *testing.T) {
	f := &faults{}
	rg := newRig(t, withInterceptors(f.funcs()))
	rg.create(mdSpec(hoMD))
	f.set(func(op string, obj client.Object) bool {
		_, isMD := obj.(*v1alpha1.ModelDeployment)
		return op == "status" && isMD && !rg.clock.Now().Before(hoT(8, 1, 0)) && rg.clock.Now().Before(hoT(8, 1, 30))
	})
	for i := 0; i < 12; i++ { // to 08:02:00
		fixStep(rg, 10*time.Second)
	}
	in := rg.instances(hoMD)[0]
	if in.Status.ReadyTime == nil || !in.Status.ReadyTime.Time.Equal(hoT(8, 1, 10)) {
		t.Fatalf("ready time %v, want 08:01:10 (inside the failing window)", in.Status.ReadyTime)
	}
	s2r := rg.md(hoMD).Status.StartToReady
	if len(s2r) != 1 || s2r[0].Duration != "50s" || s2r[0].Instance != in.Name {
		t.Fatalf("startToReady %+v, want one entry of 50s measured on %s", s2r, in.Name)
	}
}

// failedEngine makes qualification fail on a too small KV capacity, so the
// Instance fails while its job runs and its Allocation must be released.
func failedEngine(string) afake.Engine { return afake.Engine{Qualify: afake.QualifyKVCapacity(1)} }

// TestReviewFailedReleaseIsResumed (Astra 1): a failed Instance's Allocation
// is released even when the Delete fails, or the controller stops between the
// end and the Delete. The release is read again from the persisted end reason
// and made before the drain finalizer goes. Without the fix the Allocation of
// the failed Instance is kept.
func TestReviewFailedReleaseIsResumed(t *testing.T) {
	t.Run("a failed Delete", func(t *testing.T) {
		f := &faults{}
		rg := newRig(t, withInterceptors(f.funcs()))
		rg.engine = failedEngine
		failOnce := true
		f.set(func(op string, obj client.Object) bool {
			if _, ok := obj.(*v1alpha1.Allocation); ok && op == "delete" && failOnce {
				failOnce = false
				return true
			}
			return false
		})
		rg.create(mdSpec(hoMD))
		var failed *v1alpha1.Instance
		for i := 0; i < 30 && failed == nil; i++ {
			fixStep(rg, 10*time.Second)
			for _, in := range rg.instances(hoMD) {
				if in.Status.EndReason == v1alpha1.ReasonQualificationFailed {
					in := in
					failed = &in
				}
			}
		}
		if failed == nil || failOnce {
			t.Fatalf("no Instance failed qualification with the Delete failing (failed %v, fault used %v)", failed != nil, !failOnce)
		}
		for i := 0; i < 3; i++ {
			fixStep(rg, 10*time.Second)
		}
		if !allocReleased(rg, failed.Spec.AllocationRef.Name) {
			t.Fatalf("the failed Instance's Allocation %s is not released after the Delete failed once", failed.Spec.AllocationRef.Name)
		}
	})
	t.Run("an interrupted pass", func(t *testing.T) {
		k := &hoKiller{}
		rg := newRig(t, withInterceptors(k.funcs()))
		rg.engine = failedEngine
		k.arm(func(op string, obj client.Object) bool {
			in, ok := obj.(*v1alpha1.Instance)
			return ok && op == "status" && in.Status.EndReason == v1alpha1.ReasonQualificationFailed
		})
		rg.create(mdSpec(hoMD))
		for i := 0; i < 30 && !k.isDead(); i++ {
			rg.clock.Advance(10 * time.Second)
			k.step(rg)
		}
		if !k.isDead() {
			t.Fatal("the controller never died after the failed end")
		}
		failed := rg.instances(hoMD)[0]
		if allocReleased(rg, failed.Spec.AllocationRef.Name) {
			t.Fatal("the Allocation was released before the kill; the test proves nothing")
		}
		k.revive(rg)
		rg.run(30*time.Second, 10*time.Second)
		if !allocReleased(rg, failed.Spec.AllocationRef.Name) {
			t.Fatalf("after the restart the failed Instance's Allocation %s is still kept", failed.Spec.AllocationRef.Name)
		}
	})
}

// TestReviewIdleAnchorSurvivesTheInstancesDisappearance (Astra 7): an Instance
// deleted while it serves (as garbage collection deletes it) drains and ends
// at 09:00:10; the idle anchor write fails twice. The anchor is written before
// the drain finalizer goes, so the Instance cannot vanish first; maxIdle (30
// min) then counts from its end, not from the job's start (08:00:20), which
// would release the Allocation at once. Without the fix it is released at the
// first pass after the end. The write fails twice, not once: with the
// finalizer removed first, one failure is spent on the write in end(), and the
// retention step of the same pass, which still sees the ended Instance, writes
// the anchor itself; the second failure takes that write too, and the next
// pass finds no Instance to anchor from.
func TestReviewIdleAnchorSurvivesTheInstancesDisappearance(t *testing.T) {
	f := &faults{}
	rg := newRig(t, withInterceptors(f.funcs()), withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
	md := mdSpec(hoMD)
	md.Spec.Instances = 1
	rg.create(md)
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount(hoMD) == 1 })
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 }) // nothing replaces it
	rg.clock.Set(hoT(9, 0, 0))
	in := rg.instances(hoMD)[0]
	failLeft := 2
	f.set(func(op string, obj client.Object) bool {
		a, ok := obj.(*v1alpha1.Allocation)
		if ok && op == "update" && failLeft > 0 {
			if _, has := a.Annotations[v1alpha1.AnnotationIdleSince]; has {
				failLeft--
				return true
			}
		}
		return false
	})
	if err := rg.c.Delete(rg.ctx, rg.instance(in.Name)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		fixStep(rg, 10*time.Second)
	}
	if failLeft != 0 {
		t.Fatalf("the anchor write failed %d times, want 2; the test proves nothing", 2-failLeft)
	}
	a, ok := rg.allocation(in.Spec.AllocationRef.Name)
	if !ok || !a.DeletionTimestamp.IsZero() {
		t.Fatal("the Allocation was released right after the Instance ended; maxIdle must count from the end")
	}
	since, set, err := v1alpha1.IdleSince(a)
	if err != nil || !set || since.Before(hoT(9, 0, 0)) {
		t.Fatalf("idle-since %s (set %v, %v), want the Instance's end, after 09:00:00", since, set, err)
	}
	retentionReleaseAt(t, rg, a.Name, since.Add(30*time.Minute), time.Minute)
}
