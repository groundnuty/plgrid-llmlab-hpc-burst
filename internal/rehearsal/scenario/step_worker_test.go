package scenario_test

// Step (d) of issue #146: a worker-rung restart. The engine double's process
// in the primary's job is killed; the requests during the outage each end
// with a definite status and a usage record; the front pauses the Instance
// after HBEP-5's three Instance-emitted 5xx in a row, and the other Instance
// serves; and the lifecycle controller restarts the worker (HBEP-7, Health and
// restarts), so the Instance serves again on the same Allocation
// (science-agent rulings, 1 Nov 2026).

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// pauseAfterFailures is the front config's pauseAfterFailures (rehearsalFrontTuning):
// HBEP-5's three Instance-emitted 5xx in a row.
const pauseAfterFailures = 3

// engineProcesses lists the job's engine double processes: those whose
// environment holds HB_TEST_ROLE=engine, which the fake Apptainer sets on the
// engine it starts (the engine double's doc, The executables). Never by name.
func (r *rig) engineProcesses(jobID int) []int {
	r.t.Helper()
	pids, err := r.cl.Plat.Processes(jobID)
	if err != nil {
		r.t.Fatalf("job %d's processes: %v", jobID, err)
	}
	var out []int
	for _, p := range pids {
		env, err := os.ReadFile("/proc/" + strconv.Itoa(p) + "/environ")
		if err != nil {
			continue // gone since the listing
		}
		for _, kv := range bytes.Split(env, []byte{0}) {
			if string(kv) == "HB_TEST_ROLE=engine" {
				out = append(out, p)
			}
		}
	}
	return out
}

// outcome is one request sent during the outage, its reply and its record.
type outcome struct {
	rp  stack.Reply
	row stack.UsageRow
}

func (r *rig) allocationNames() map[string]bool {
	out := map[string]bool{}
	for _, a := range r.allocations() {
		out[a.Name] = true
	}
	return out
}

func countReason(evs []string, reason string) int {
	n := 0
	for _, e := range evs {
		if e == reason {
			n++
		}
	}
	return n
}

func (r *rig) eventReasons(name string) []string {
	var out []string
	for _, e := range stack.EventsFor(r.t, r.c, name) {
		out = append(out, e.Reason)
	}
	return out
}

// stepWorkerRestart is step (d). The front's pause is proved apart from the
// two other things that can take an Instance out of routing: the controller's
// move to verifying when it learns the worker exited, and the front's probe,
// which unregisters a transport after probeFailures (2) failed probes 2 s
// apart (HBEP-6, The front's half). The agent learns of a worker's exit only
// from its leader process (internal/agent/workers.go, leaderExited), which in
// the rehearsal is the fake Apptainer, the engine's parent. So the leader is
// stopped (SIGSTOP) before the engine is killed: requests then meet the dead
// engine and get the agent's 5xx, while the controller sees a running worker
// and keeps the Instance ready. pauseAfterFailures+1 requests go back to back;
// the first pauseAfterFailures must be the primary's 5xx and the next must be
// served by the other Instance, with the pause counter up, the primary still
// ready on the watch and still registered at the front: only the pause can
// have moved it. Then the leader continues, reaps the engine and exits, and
// the controller restarts the worker; requests go one at a time until the
// restarted primary serves the key again.
func (r *rig) stepWorkerRestart(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	defer r.stepTimer("d")()
	r.beforeDrain("d", in)
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady {
		r.t.Fatalf("step (d) needs a ready Instance %s: %+v", in.Name, before.Status)
	}
	jobID, _ := strconv.Atoi(before.Status.JobID)
	job, _ := r.cl.Plat.Job(jobID)
	allocs := r.allocationNames()
	restartEvents := countReason(r.eventReasons(in.Name), v1alpha1.ReasonWorkerRestart)
	engines := r.engineProcesses(jobID)
	if len(engines) != 1 {
		r.t.Fatalf("step (d): job %d has %d engine processes %v, want 1", jobID, len(engines), engines)
	}
	leader := r.workerLeader(jobID, engines[0])
	pauses, ok := r.frontCounter("hpcburst_front_pauses_total")
	if !ok {
		r.t.Fatal("step (d): the front's /metrics cannot be read")
	}
	if err := syscall.Kill(leader, syscall.SIGSTOP); err != nil {
		r.t.Fatalf("step (d): stop the worker's leader %d: %v", leader, err)
	}
	stopped := true
	resume := func() {
		if stopped {
			stopped = false
			if err := syscall.Kill(leader, syscall.SIGCONT); err != nil {
				r.t.Errorf("step (d): continue the worker's leader %d: %v", leader, err)
			}
		}
	}
	defer resume()
	if err := syscall.Kill(engines[0], syscall.SIGKILL); err != nil {
		r.t.Fatalf("step (d): kill the engine %d: %v", engines[0], err)
	}
	killAt := time.Now()
	sent := 0
	send := func() stack.Reply {
		rp := r.fc.SendCtx(context.Background(), "/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key)
		if rp.Err != nil {
			r.t.Fatalf("step (d): request %d after the engine's kill got no answer within %s: %v (a hang)", sent, r.fc.RequestTimeout(), rp.Err)
		}
		sent++
		return rp
	}
	// Back to back, records read after.
	var burst []stack.Reply
	for i := 0; i <= pauseAfterFailures; i++ {
		burst = append(burst, send())
	}
	pausesAfter, _ := r.frontCounter("hpcburst_front_pauses_total")
	reg, known := stack.Registered(r.ports, in)
	still, _ := r.instance(in.Name)
	r.syncWatch(stack.KindInstances, "step (d): the Instance's phases after the kill")
	moved, left := r.watch.First(in.UID, killAt, func(o stack.Observation) bool { return o.Phase != v1alpha1.InstanceReady })
	resume()
	var outs []outcome
	for _, rp := range burst {
		outs = append(outs, outcome{rp: rp, row: r.fc.RowOf(rp.ID)})
	}
	r.checkPause(in, outs, pauses, pausesAfter)
	switch {
	case !known || !reg:
		r.t.Errorf("step (d): after the burst the front's probe no longer held Instance %s registered (known %v): the probe, not the pause, may have moved its key", in.Name, known)
	case still.Status.Phase != v1alpha1.InstanceReady || left && !moved.At.After(parseRecordTime(outs[pauseAfterFailures].row.StartTime)):
		r.t.Errorf("step (d): Instance %s left ready (%s; first seen %s at %s) before the request after the 5xx: the controller, not the pause, may have moved its key",
			in.Name, still.Status.Phase, moved.Phase, moved.At.Format(time.RFC3339Nano))
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "the primary to serve again after a worker restart", func() bool {
		rp := send()
		o := outcome{rp: rp, row: r.fc.RowOf(rp.ID)}
		outs = append(outs, o)
		now, ok := r.instance(in.Name)
		return ok && now.Status.Phase == v1alpha1.InstanceReady && len(now.Status.Restarts) > len(before.Status.Restarts) &&
			o.rp.Status == http.StatusOK && o.row.InstanceUID == string(in.UID)
	})
	r.t.Logf("step (d): %d requests; the primary served again %s after the engine's kill", len(outs), time.Since(killAt).Round(100*time.Millisecond))
	r.checkOutage(in, outs)
	r.checkWorkerRestart(in, before, job.PID, allocs, restartEvents, engines[0], killAt)
}

// workerLeader is the engine's parent, the fake Apptainer (this test binary
// under the name apptainer), a process of the job.
func (r *rig) workerLeader(jobID, engine int) int {
	r.t.Helper()
	leader := parentOf(engine)
	pids, err := r.cl.Plat.Processes(jobID)
	self, serr := os.Executable()
	if err != nil || serr != nil || leader == 0 || !slices.Contains(pids, leader) || stack.ProcessExe(leader) != self {
		r.t.Fatalf("step (d): the engine %d's parent %d (%s) is not the fake Apptainer of job %d (%v, %v)", engine, leader, stack.ProcessExe(leader), jobID, err, serr)
	}
	return leader
}

// checkPause holds the burst to HBEP-5's pause: the first pauseAfterFailures
// requests are the primary's own 5xx, the next is served by another Instance,
// and the front counted a pause.
func (r *rig) checkPause(in *v1alpha1.Instance, burst []outcome, pausesBefore, pausesAfter float64) {
	r.t.Helper()
	for i, o := range burst[:pauseAfterFailures] {
		if o.row.Emitter != "instance" || o.row.InstanceUID != string(in.UID) || o.row.Status < 500 {
			r.t.Errorf("step (d) burst request %d: record %+v, want the primary's own 5xx: %.300s", i, o.row, o.rp.Body)
		}
	}
	next := burst[pauseAfterFailures]
	if next.rp.Status != http.StatusOK || next.row.Emitter != "instance" || next.row.InstanceUID == "" || next.row.InstanceUID == string(in.UID) {
		r.t.Errorf("step (d): the request after %d of the primary's 5xx in a row: %d, record %+v; want another Instance's 200 (HBEP-5, Pauses): %.300s",
			pauseAfterFailures, next.rp.Status, next.row, next.rp.Body)
	}
	if pausesAfter <= pausesBefore {
		r.t.Errorf("step (d): hpcburst_front_pauses_total stayed %v across %d of the primary's 5xx in a row", pausesAfter, pauseAfterFailures)
	}
	r.t.Logf("step (d): the burst: %d of the primary's 5xx, then %s served it; pauses %v -> %v", pauseAfterFailures, next.row.Instance, pausesBefore, pausesAfter)
}

// checkOutage holds every request of the outage to HBEP-5 and the ruling of
// 1 Nov 2026 (13:25Z, with a second routable Instance): each has a record
// with its emitter; at most pauseAfterFailures of the primary's own 5xx come
// in a row; every other is served, by the other Instance or by the primary
// after its restart; and none is the front's own error, since the front's
// no_ready_target applies only when the paused Instance is the only one.
func (r *rig) checkOutage(in *v1alpha1.Instance, outs []outcome) {
	r.t.Helper()
	run, longest, fivexx, other, own := 0, 0, 0, 0, 0
	for i, o := range outs {
		row := o.row
		switch {
		case row.Emitter == "":
			r.t.Errorf("step (d) request %d: its record has no emitter: %+v", i, row)
		case row.Emitter == "instance" && row.InstanceUID == string(in.UID) && row.Status >= 500:
			fivexx++
			run++
			longest = max(longest, run)
			continue
		case row.Emitter == "instance" && row.Status == http.StatusOK && row.InstanceUID == string(in.UID):
			own++
		case row.Emitter == "instance" && row.Status == http.StatusOK:
			other++
		default:
			r.t.Errorf("step (d) request %d: record %+v, want the primary's 5xx or an Instance's 200 (a front row is no_ready_target, for a sole Instance): %.300s", i, row, o.rp.Body)
		}
		run = 0
	}
	r.t.Logf("step (d): %d of the primary's 5xx (longest run %d), %d served by another Instance, %d by the restarted primary", fivexx, longest, other, own)
	if longest > pauseAfterFailures {
		r.t.Errorf("step (d): %d of the primary's 5xx came in a row; the front pauses after %d (HBEP-5, Pauses)", longest, pauseAfterFailures)
	}
	r.checkNoGap(r.fc.UsageRows())
}

// checkWorkerRestart: the controller raised WorkerRestart (its event and a
// status.restarts entry for worker-0 on the PartExited signal), the Instance
// left ready and came back, a new engine runs, and the Allocation and its job
// are the same.
func (r *rig) checkWorkerRestart(in *v1alpha1.Instance, before v1alpha1.Instance, jobPID int, allocs map[string]bool, events, killed int, killAt time.Time) {
	r.t.Helper()
	now, _ := r.instance(in.Name)
	if len(now.Status.Restarts) <= len(before.Status.Restarts) {
		r.t.Fatalf("step (d): status.restarts %+v gained nothing over %+v", now.Status.Restarts, before.Status.Restarts)
	}
	added := now.Status.Restarts[len(before.Status.Restarts):]
	if added[0].Part != v1alpha1.WorkerName(0) || added[0].Signal != v1alpha1.ReasonPartExited {
		r.t.Errorf("step (d): the restart recorded is %+v, want part %s on signal %s", added[0], v1alpha1.WorkerName(0), v1alpha1.ReasonPartExited)
	}
	if n := countReason(r.eventReasons(in.Name), v1alpha1.ReasonWorkerRestart); n <= events {
		r.t.Errorf("step (d): %d WorkerRestart events on Instance %s, %d before the kill", n, in.Name, events)
	}
	r.syncWatch(stack.KindInstances, "step (d): the Instance's phases after the restart")
	if _, ok := r.watch.First(in.UID, killAt, func(o stack.Observation) bool { return o.Phase != v1alpha1.InstanceReady }); !ok {
		r.t.Errorf("step (d): Instance %s was never seen out of ready after the kill: no reverification", in.Name)
	}
	if now.Spec.AllocationRef.Name != before.Spec.AllocationRef.Name {
		r.t.Errorf("step (d): the Instance's Allocation changed from %s to %s", before.Spec.AllocationRef.Name, now.Spec.AllocationRef.Name)
	}
	if after := r.allocationNames(); !maps.Equal(after, allocs) {
		r.t.Errorf("step (d): Allocations %v after the restart, %v before: a new Allocation", after, allocs)
	}
	jobID, _ := strconv.Atoi(now.Status.JobID)
	if j, _ := r.cl.Plat.Job(jobID); j.PID != jobPID || j.State != "RUNNING" {
		r.t.Errorf("step (d): job %d is %s with PID %d, was RUNNING with %d", jobID, j.State, j.PID, jobPID)
	}
	if e := r.engineProcesses(jobID); len(e) != 1 || e[0] == killed {
		r.t.Errorf("step (d): the job's engine processes are %v after the restart; the killed one was %d", e, killed)
	}
}
