package scenario_test

// Step (g) of issue #146: scale to 0, then hpcburst down. Asserted from the
// watch, the API, the fake cluster's job snapshots, frps's proxy list and the
// launcher's own status, never from log lines.

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
	insts := r.watch.uids()
	for _, in := range r.instances() {
		if !slices.Contains(insts, in.UID) {
			insts = append(insts, in.UID)
		}
	}
	patchAt := r.scaleToZero()
	r.waitAllEnded(insts)
	r.checkShutdown(insts, patchAt)
	r.checkAllocations(uids)
	r.waitRecords(uids)
	r.waitJobsGone()
	r.checkProxiesOffline(r.watch.uids())
	r.assertNoOrphans(steps)
	r.downAndCheck()
}

// scaleToZero sets the deployment's spec.instances to 0 and returns the time
// just before the patch: every observation received before it shows a state
// the controller had not yet acted on for the scale.
func (r *rig) scaleToZero() time.Time {
	r.t.Helper()
	var md v1alpha1.ModelDeployment
	if !r.get(&md, mdName, namespace) {
		r.t.Fatal("step (g): no ModelDeployment")
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.Instances = 0
	at := time.Now()
	ctx, cancel := callCtx(r.t)
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
	waitFor(r.t, waitDeadline, "every Instance to end and every Allocation to end or go", func() bool {
		if time.Since(last) > 10*time.Second {
			r.t.Logf("step (g) waiting:%s", r.describe())
			last = time.Now()
		}
		for _, uid := range insts {
			if _, ok := r.watch.first(uid, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded }); !ok {
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
		obs := r.watch.of(uid)
		var at observation // the last version seen before the scale
		for _, o := range obs {
			if o.at.Before(patchAt) {
				at = o
			}
		}
		if at.phase == v1alpha1.InstanceEnded {
			continue
		}
		after := slices.DeleteFunc(slices.Clone(obs), func(o observation) bool { return o.at.Before(patchAt) })
		end := slices.IndexFunc(after, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded })
		switch {
		case end < 0:
			r.t.Errorf("step (g): Instance %s (%s) was never seen ended after the scale to 0", at.name, uid)
			continue
		case after[end].endReason != v1alpha1.ReasonDrained:
			r.t.Errorf("step (g): Instance %s ended %q after the scale to 0, not %s", after[end].name, after[end].endReason, v1alpha1.ReasonDrained)
		}
		if at.phase != v1alpha1.InstanceReady {
			other++
			continue
		}
		ready++
		if !slices.ContainsFunc(after[:end], func(o observation) bool { return o.phase == v1alpha1.InstanceDraining }) {
			r.t.Errorf("step (g): Instance %s was ready at the scale to 0 and ended with no draining version: it was not drained", at.name)
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
func (r *rig) checkAllocations(atZero map[types.UID]string) {
	r.t.Helper()
	type seen struct {
		name    string
		phases  []v1alpha1.AllocationPhase
		deleted bool
	}
	all := map[types.UID]*seen{}
	var order []types.UID
	for _, a := range r.watch.allocations() {
		s := all[a.uid]
		if s == nil {
			s = &seen{name: a.name}
			all[a.uid] = s
			order = append(order, a.uid)
		}
		if n := len(s.phases); n == 0 || s.phases[n-1] != a.phase {
			s.phases = append(s.phases, a.phase)
		}
		s.deleted = s.deleted || a.deleted
		switch a.phase {
		case v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
			r.t.Errorf("Allocation %s was seen %s at %s", a.name, a.phase, a.at.Format(time.RFC3339Nano))
		}
	}
	for uid, name := range atZero {
		s := all[uid]
		switch {
		case s == nil:
			r.t.Errorf("step (g): the watch never saw Allocation %s", name)
		case !s.deleted && s.phases[len(s.phases)-1] != v1alpha1.AllocationEnded:
			r.t.Errorf("step (g): Allocation %s was last seen %s, neither Ended nor deleted", name, s.phases[len(s.phases)-1])
		}
	}
	for _, uid := range order {
		r.t.Logf("Allocation %s: phases %v, deleted %v", all[uid].name, all[uid].phases, all[uid].deleted)
	}
}

// waitRecords waits for the acct-<UID> record of every Allocation.
func (r *rig) waitRecords(uids map[types.UID]string) {
	r.t.Helper()
	waitFor(r.t, waitDeadline, "an acct- record for every Allocation", func() bool {
		var l appsv1.ControllerRevisionList
		ctx, cancel := callCtx(r.t)
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
	for _, j := range r.cl.plat.Jobs() {
		id := j.ID
		waitFor(r.t, waitDeadline, fmt.Sprintf("job %d's processes to be gone", id), func() bool {
			pids, err := r.cl.plat.Processes(id)
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

// procID is a process by its PID and its start time (/proc/<pid>/stat field
// 22, clock ticks since boot), so that a PID the kernel has since given to
// another process does not count as the same one.
type procID struct {
	name  string
	pid   int
	start uint64
}

// procStart is a live process's start time, false when there is none.
func procStart(pid int) (uint64, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(string(b[i+1:])) // f[0] is field 3, the state
	if len(f) < 20 {
		return 0, false
	}
	n, err := strconv.ParseUint(f[19], 10, 64)
	return n, err == nil
}

// gone reports whether the process has ended: no such PID, another process
// under it, or a zombie, which holds nothing.
func (p procID) gone() bool {
	st, ok := procStart(p.pid)
	return !ok || st != p.start || !alive(p.pid)
}

// stackProcesses lists every process hpcburst status reports for the host:
// the recorded processes it judges ours (k3s), the supervisor, and each
// supervised entry that runs (the controller, the front, frps).
func (r *rig) stackProcesses() []procID {
	r.t.Helper()
	out, err := run(r.t, within(r.t, statusTimeout), r.h.env, r.b.hpcburst, "status", "--state-dir", r.h.state, "--json", "--no-api")
	var st struct {
		Processes []struct {
			Name    string `json:"name"`
			PID     int    `json:"pid"`
			Verdict string `json:"verdict"`
		} `json:"processes"`
		Supervisor *struct {
			Running bool `json:"running"`
			PID     int  `json:"pid"`
		} `json:"supervisor"`
		Controllers []struct {
			Name    string `json:"name"`
			Running bool   `json:"running"`
			PID     int    `json:"pid"`
		} `json:"controllers"`
	}
	if jerr := json.Unmarshal([]byte(out), &st); jerr != nil {
		r.t.Fatalf("hpcburst status --json (%v): %v: %.500s", err, jerr, out)
	}
	var procs []procID
	add := func(name string, pid int) {
		if slices.ContainsFunc(procs, func(p procID) bool { return p.pid == pid }) {
			return // the supervisor is both a recorded process and the supervisor
		}
		start, ok := procStart(pid)
		if !ok {
			r.t.Errorf("hpcburst status reports %s as pid %d, which does not run", name, pid)
			return
		}
		procs = append(procs, procID{name: name, pid: pid, start: start})
	}
	recorded := 0
	for _, p := range st.Processes {
		if p.Verdict == "ours" && p.PID > 0 {
			add(p.Name, p.PID)
			recorded++
		}
	}
	if st.Supervisor != nil && st.Supervisor.Running && st.Supervisor.PID > 0 {
		add("supervisor", st.Supervisor.PID)
	}
	for _, c := range st.Controllers {
		if c.Running && c.PID > 0 {
			add(c.Name, c.PID)
		}
	}
	var names []string
	for _, p := range procs {
		names = append(names, fmt.Sprintf("%s=%d", p.name, p.pid))
	}
	for _, want := range []string{"supervisor", "controller", "front", "frps"} {
		if !slices.ContainsFunc(procs, func(p procID) bool { return p.name == want }) {
			r.t.Errorf("step (g): hpcburst status reports no running %s before down (it reports %v)", want, names)
		}
	}
	if recorded == 0 {
		r.t.Errorf("step (g): hpcburst status reports no recorded process of ours (k3s) before down: %.500s", out)
	}
	r.t.Logf("step (g): the stack before down: %v", names)
	return procs
}

// stackPorts are the stack's listeners: the front's client and dial-home
// listeners and up's block from portBase (13 ports).
func stackPorts() []int {
	ports := []int{29500, 29501}
	for p := portBase; p <= portBase+12; p++ {
		ports = append(ports, p)
	}
	return ports
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
	r.h.down()
	if d := time.Since(start); d > 40*time.Second {
		r.t.Errorf("step (g): hpcburst down took %s, above its default grace of 40 s", d)
	}
	waitState(r.t, 30*time.Second, "every process of the stack to be gone after down", func() (bool, string) {
		var left []string
		for _, p := range procs {
			if !p.gone() {
				left = append(left, fmt.Sprintf("%s=%d", p.name, p.pid))
			}
		}
		return len(left) == 0, fmt.Sprintf("still running: %v", left)
	})
	if l := listening(r.t, stackPorts()); len(l) > 0 {
		r.t.Errorf("step (g): ports %v still listen after down", l)
	}
	r.t.Logf("step (g): down stopped %d processes of the stack in %s", len(procs), time.Since(start).Round(100*time.Millisecond))
}
