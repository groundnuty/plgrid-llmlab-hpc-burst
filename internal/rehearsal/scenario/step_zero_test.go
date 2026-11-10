package scenario_test

// Step (g) of issue #146: scale to 0, then hpcburst down. Asserted from the
// watch, the API, the fake cluster's job snapshots, frps's proxy list and the
// launcher's own status, never from log lines.

import (
	"fmt"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// stepScaleToZero is step (g): scale to 0. Every Instance ends Drained, each
// ready one through draining (HBEP-7, Release; shutdownInstance in
// internal/lifecycle/instances.go); the Allocations end, none of them failed,
// each with its acct-<UID> record; every job's processes are gone; frps lists
// no online proxy for an ended Instance, and no TunnelOrphaned event fired in
// steps; and hpcburst down stops every process of the stack within its grace.
func (r *rig) stepScaleToZero(steps string) {
	r.t.Helper()
	uids := map[types.UID]string{}
	for _, a := range r.allocations() {
		uids[a.UID] = a.Name
	}
	insts := r.watch.UIDs()
	for _, in := range r.instances() {
		if !slices.Contains(insts, in.UID) {
			insts = append(insts, in.UID)
		}
	}
	patchAt := r.scaleToZero()
	r.waitAllEnded(insts)
	r.checkShutdown(insts, patchAt)
	r.checkAllocations(uids, nil)
	r.waitRecords(uids)
	r.waitJobsGone()
	r.checkProxiesOffline(r.watch.UIDs())
	r.assertNoOrphans(steps)
	r.downAndCheck()
}

// scaleToZero sets the deployment's spec.instances to 0 and returns the time
// just before the patch: every observation received before it shows a state
// the controller had not yet acted on for the scale.
func (r *rig) scaleToZero() time.Time {
	r.t.Helper()
	var md v1alpha1.ModelDeployment
	if !r.get(&md, stack.MDName, stack.Namespace) {
		r.t.Fatal("step (g): no ModelDeployment")
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.Instances = 0
	at := time.Now()
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Patch(ctx, &md, patch); err != nil {
		r.t.Fatalf("scale to 0: %v", err)
	}
	return at
}

// waitAllEnded waits until the watch has seen every Instance of the run ended
// and the API lists only Ended Allocations; one that failed fails the step at
// once.
func (r *rig) waitAllEnded(insts []types.UID) {
	r.t.Helper()
	last := time.Now()
	stack.WaitFor(r.t, stack.WaitDeadline, "every Instance to end and every Allocation to end or go", func() bool {
		if time.Since(last) > 10*time.Second {
			r.t.Logf("step (g) waiting:%s", r.describe())
			last = time.Now()
		}
		for _, uid := range insts {
			if _, ok := r.watch.First(uid, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceEnded }); !ok {
				return false
			}
		}
		for _, a := range r.allocations() {
			switch a.Status.AtProvider.Phase {
			case v1alpha1.AllocationEnded:
			case v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
				r.t.Fatalf("step (g): Allocation %s is %s at the scale to 0, not Ended: %s", a.Name, a.Status.AtProvider.Phase, r.describe())
			default:
				return false
			}
		}
		return true
	})
}

// checkShutdown holds every Instance of the run to Release's shutdown case,
// from the watch: one that was ready when the scale to 0 applied is seen
// draining, then ended Drained; any other that had not ended by then ends
// Drained too (the deployment is shutting down). One that had ended before is
// left out: step (c) holds the primary's end.
func (r *rig) checkShutdown(insts []types.UID, patchAt time.Time) {
	r.t.Helper()
	ready, other := 0, 0
	for _, uid := range insts {
		obs := r.watch.Of(uid)
		var at stack.Observation // the last version seen before the scale
		for _, o := range obs {
			if o.At.Before(patchAt) {
				at = o
			}
		}
		if at.Phase == v1alpha1.InstanceEnded {
			continue
		}
		after := slices.DeleteFunc(slices.Clone(obs), func(o stack.Observation) bool { return o.At.Before(patchAt) })
		end := slices.IndexFunc(after, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceEnded })
		switch {
		case end < 0:
			r.t.Errorf("step (g): Instance %s (%s) was never seen ended after the scale to 0", at.Name, uid)
			continue
		case after[end].EndReason != v1alpha1.ReasonDrained:
			r.t.Errorf("step (g): Instance %s ended %q after the scale to 0, not %s", after[end].Name, after[end].EndReason, v1alpha1.ReasonDrained)
		}
		if at.Phase != v1alpha1.InstanceReady {
			other++
			continue
		}
		ready++
		if !slices.ContainsFunc(after[:end], func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceDraining }) {
			r.t.Errorf("step (g): Instance %s was ready at the scale to 0 and ended with no draining version: it was not drained", at.Name)
		}
	}
	r.t.Logf("step (g): %d Instances were ready at the scale to 0, %d others had not ended; each held to the drain above", ready, other)
	if ready == 0 {
		r.t.Errorf("step (g): no Instance was ready at the scale to 0: the drain was not exercised")
	}
}

// checkAllocations holds every Allocation of the run to its end, from the
// watch, deleted ones included: none was ever seen Failed, Lost or Refused,
// and each of those listed at the scale to 0 was last seen Ended or deleted. A
// normal stop ends a job CANCELLED or COMPLETED, which is Ended
// (pkg/provider/phase.go); a released Allocation is deleted
// (internal/lifecycle/retention.go), in the rehearsal while still Running.
// failedOK names the Allocations a step failed on purpose (#193's requeue).
// The API's LIST that waitAllEnded ends on leads the watch, so the check first
// makes the watch catch up (syncWatch, issue #281).
func (r *rig) checkAllocations(atZero map[types.UID]string, failedOK map[types.UID]bool) {
	r.t.Helper()
	res := r.syncWatch(stack.KindAllocations, "step (g): the Allocations")
	checkAllocationEvents(r.t, r.watch.Allocations, res, atZero, failedOK)
}

// waitRecords waits for the acct-<UID> record of every Allocation.
func (r *rig) waitRecords(uids map[types.UID]string) {
	r.t.Helper()
	stack.WaitFor(r.t, stack.WaitDeadline, "an acct- record for every Allocation", func() bool {
		var l appsv1.ControllerRevisionList
		ctx, cancel := stack.CallCtx(r.t)
		defer cancel()
		if err := r.c.List(ctx, &l); err != nil {
			return false
		}
		have := map[string]bool{}
		for _, cr := range l.Items {
			have[cr.Name] = true
		}
		for uid := range uids {
			name, err := v1alpha1.AccountingRecordName(uid)
			if err != nil || !have[name] {
				return false
			}
		}
		return true
	})
}

// waitJobsGone waits until no job of the fake cluster has a process left.
func (r *rig) waitJobsGone() {
	r.t.Helper()
	for _, j := range r.cl.Plat.Jobs() {
		id := j.ID
		stack.WaitFor(r.t, stack.WaitDeadline, fmt.Sprintf("job %d's processes to be gone", id), func() bool {
			pids, err := r.cl.Plat.Processes(id)
			return err == nil && len(pids) == 0
		})
	}
}

// checkProxiesOffline: frps lists no Instance of the run online, and each
// one's proxy end is recorded for the TunnelOrphaned check.
func (r *rig) checkProxiesOffline(insts []types.UID) {
	r.t.Helper()
	checked := time.Now()
	online := r.frpsProxies()
	for _, uid := range insts {
		if online[string(uid)] {
			r.t.Errorf("step (g): frps still lists the ended Instance %s online", uid)
		} else {
			r.markProxyEnd(uid, checked)
		}
	}
}

// stackProcesses lists every process hpcburst status reports for the host
// (stack.Host.Processes, issue #251's one implementation): the recorded
// processes it judges ours (k3s), the supervisor, and each supervised entry
// that runs (the controller, the front, frps).
func (r *rig) stackProcesses() []stack.Proc {
	r.t.Helper()
	procs, err := r.h.Processes()
	switch {
	case err != nil && procs == nil:
		r.t.Fatalf("step (g): %v", err)
	case err != nil:
		r.t.Errorf("step (g): %v", err)
	}
	var names []string
	for _, p := range procs {
		names = append(names, fmt.Sprintf("%s=%d", p.Name, p.PID))
	}
	// The whole set, as stack names it for every check of a stop (the alpha stand-ins' too).
	for _, m := range stack.MissingComponents(procs) {
		r.t.Errorf("step (g): hpcburst status reports no %s before down (it reports %v)", m, names)
	}
	r.t.Logf("step (g): the stack before down: %v", names)
	return procs
}

// downAndCheck runs hpcburst down, then checks that every process of the
// stack recorded before it is gone and that nothing listens on its ports.
// down removes its own record (internal/launcher/down.go, clearRecord), so
// hpcburst status after it reports nothing whatever survived: the processes
// are read before.
func (r *rig) downAndCheck() {
	r.t.Helper()
	procs := r.stackProcesses()
	start := time.Now()
	r.h.Down()
	if d := time.Since(start); d > 40*time.Second {
		r.t.Errorf("step (g): hpcburst down took %s, above its default grace of 40 s", d)
	}
	stack.WaitState(r.t, 30*time.Second, "every process of the stack to be gone after down", func() (bool, string) {
		var left []string
		for _, p := range procs {
			if !p.Gone() {
				left = append(left, fmt.Sprintf("%s=%d", p.Name, p.PID))
			}
		}
		return len(left) == 0, fmt.Sprintf("still running: %v", left)
	})
	if l := stack.Listening(r.t, r.ports.All()); len(l) > 0 {
		r.t.Errorf("step (g): ports %v still listen after down", l)
	}
	r.t.Logf("step (g): down stopped %d processes of the stack in %s", len(procs), time.Since(start).Round(100*time.Millisecond))
}
