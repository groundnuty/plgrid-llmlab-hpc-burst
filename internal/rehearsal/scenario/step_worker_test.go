package scenario_test

// Step (d) of issue #146: a worker-rung restart. The engine double's process
// in the primary's job is killed; the requests during the outage each end
// with a definite status and a usage record; the front pauses the Instance
// after HBEP-5's three Instance-emitted 5xx in a row; and the lifecycle
// controller restarts the worker (HBEP-7, Health and restarts), so the
// Instance serves again on the same Allocation (science-agent ruling, 1 Nov
// 2026).

import (
	"bytes"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// pauseAfterFailures is the front config's pauseAfterFailures (startHost):
// HBEP-5's three Instance-emitted 5xx in a row.
const pauseAfterFailures = 3

// engineProcesses lists the job's engine double processes: those whose
// environment holds HB_TEST_ROLE=engine, which the fake Apptainer sets on the
// engine it starts (the engine double's doc, The executables). Never by name.
func (r *rig) engineProcesses(jobID int) []int {
	r.t.Helper()
	pids, err := r.cl.plat.Processes(jobID)
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
	rp  reply
	row usageRow
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
	for _, e := range eventsFor(r.t, r.c, name) {
		out = append(out, e.Reason)
	}
	return out
}

// stepWorkerRestart is step (d).
func (r *rig) stepWorkerRestart(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	defer r.stepTimer("d")()
	r.beforeDrain("d", in)
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady {
		r.t.Fatalf("step (d) needs a ready Instance %s: %+v", in.Name, before.Status)
	}
	jobID, _ := strconv.Atoi(before.Status.JobID)
	job, _ := r.cl.plat.Job(jobID)
	allocs := r.allocationNames()
	restartEvents := countReason(r.eventReasons(in.Name), v1alpha1.ReasonWorkerRestart)
	engines := r.engineProcesses(jobID)
	if len(engines) != 1 {
		r.t.Fatalf("step (d): job %d has %d engine processes %v, want 1", jobID, len(engines), engines)
	}
	pauses, ok := frontCounter("hpcburst_front_pauses_total")
	if !ok {
		r.t.Fatal("step (d): the front's /metrics cannot be read")
	}
	if err := syscall.Kill(engines[0], syscall.SIGKILL); err != nil {
		r.t.Fatalf("step (d): kill the engine %d: %v", engines[0], err)
	}
	killAt := time.Now()
	var outs []outcome
	ask := func() outcome {
		rp := r.send("/v1/chat/completions", chatBody(4, 0, false), key)
		if rp.err != nil {
			r.t.Fatalf("step (d): request %d after the engine's kill got no answer within %s: %v (a hang)", len(outs), requestTimeout, rp.err)
		}
		return outcome{rp: rp, row: r.rowOf(rp.id)}
	}
	// Back to back first, so that the front's pause rule, not its probe,
	// meets the failures; then one at a time until the primary serves the key
	// again after the controller restarted its worker.
	for i := 0; i <= pauseAfterFailures; i++ {
		outs = append(outs, ask())
	}
	waitFor(r.t, waitDeadline, "the primary to serve again after a worker restart", func() bool {
		o := ask()
		outs = append(outs, o)
		now, ok := r.instance(in.Name)
		return ok && now.Status.Phase == v1alpha1.InstanceReady && len(now.Status.Restarts) > len(before.Status.Restarts) &&
			o.rp.status == http.StatusOK && o.row.InstanceUID == string(in.UID)
	})
	r.t.Logf("step (d): %d requests; the primary served again %s after the engine's kill", len(outs), time.Since(killAt).Round(100*time.Millisecond))
	r.checkOutage(in, outs, pauses)
	r.checkWorkerRestart(in, before, job.PID, allocs, restartEvents, engines[0], killAt)
}

// checkOutage holds every request of the outage to HBEP-5 and the ruling:
// each has a record with its emitter; at most pauseAfterFailures
// Instance-emitted 5xx come in a row, and such a run pauses the Instance;
// the rest are served by another ready Instance (the replacement, routable
// too) or are the front's own model_inactive/no_ready_target with
// Retry-After.
func (r *rig) checkOutage(in *v1alpha1.Instance, outs []outcome, pausesBefore float64) {
	r.t.Helper()
	run, longest, fivexx, other, front := 0, 0, 0, 0, 0
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
		case row.Emitter == "instance" && row.Status == http.StatusOK:
			if row.InstanceUID != string(in.UID) {
				other++
			}
		case row.Emitter == "front":
			front++
			e := errorOf(o.rp.body)
			if row.Status != http.StatusServiceUnavailable || e.Code != "model_inactive" || e.Detail == nil || *e.Detail != "no_ready_target" || o.rp.header.Get("Retry-After") == "" {
				r.t.Errorf("step (d) request %d: the front's row is %d %+v (Retry-After %q), want 503 model_inactive/no_ready_target with Retry-After: %.300s",
					i, row.Status, e, o.rp.header.Get("Retry-After"), o.rp.body)
			}
		default:
			r.t.Errorf("step (d) request %d: unexpected record %+v: %.300s", i, row, o.rp.body)
		}
		run = 0
	}
	r.t.Logf("step (d): %d Instance-emitted 5xx (longest run %d), %d served by another Instance, %d front rows", fivexx, longest, other, front)
	if fivexx == 0 {
		r.t.Errorf("step (d): no request met the dead engine: the outage never reached a client")
	}
	if longest > pauseAfterFailures {
		r.t.Errorf("step (d): %d Instance-emitted 5xx came in a row; the front pauses after %d (HBEP-5, Pauses)", longest, pauseAfterFailures)
	}
	if other+front == 0 {
		r.t.Errorf("step (d): every request went to the dead primary; the pause never made it ineligible")
	}
	if longest == pauseAfterFailures {
		if after, _ := frontCounter("hpcburst_front_pauses_total"); after <= pausesBefore {
			r.t.Errorf("step (d): %d failures in a row and hpcburst_front_pauses_total stayed %v", longest, after)
		}
	}
	r.checkNoGap(r.usageRows())
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
	if _, ok := r.watch.first(in.UID, killAt, func(o observation) bool { return o.phase != v1alpha1.InstanceReady }); !ok {
		r.t.Errorf("step (d): Instance %s was never seen out of ready after the kill: no reverification", in.Name)
	}
	if now.Spec.AllocationRef.Name != before.Spec.AllocationRef.Name {
		r.t.Errorf("step (d): the Instance's Allocation changed from %s to %s", before.Spec.AllocationRef.Name, now.Spec.AllocationRef.Name)
	}
	if after := r.allocationNames(); len(after) != len(allocs) {
		r.t.Errorf("step (d): Allocations %v after the restart, %v before: a new Allocation", after, allocs)
	}
	jobID, _ := strconv.Atoi(now.Status.JobID)
	if j, _ := r.cl.plat.Job(jobID); j.PID != jobPID || j.State != "RUNNING" {
		r.t.Errorf("step (d): job %d is %s with PID %d, was RUNNING with %d", jobID, j.State, j.PID, jobPID)
	}
	if e := r.engineProcesses(jobID); len(e) != 1 || e[0] == killed {
		r.t.Errorf("step (d): the job's engine processes are %v after the restart; the killed one was %d", e, killed)
	}
}
