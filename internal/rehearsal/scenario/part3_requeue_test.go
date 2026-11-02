package scenario_test

// Item 3 of issue #193, HBEP-3: each Instance carries its deployment ID under
// its Allocation's tag, and a requeue of the Allocation's job (the fake's
// RequeueHold, a requeued job that returns to a user hold) fails its Instance:
// the hold is not lifted again, the Allocation is deleted, its finalizer
// cancels the job, and the replacement is a new Allocation (HBEP-3, open
// question 13 and Test plan). Also the queued path's successor before the
// drain start (HBEP-7, No-overlap mode).

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// hbep3Tag is an Allocation's job tag computed from HBEP-3 (Allocation and
// job identity), here and not with the contracts' helper: hb-<n>-<h>, <n> the
// name cut to its first 40 characters, <h> the first 12 hex characters of the
// SHA-256 of "<namespace>/<name>/<uid>".
func hbep3Tag(ns, name string, uid types.UID) string {
	sum := sha256.Sum256([]byte(ns + "/" + name + "/" + string(uid)))
	n := name
	if len(n) > 40 {
		n = n[:40]
	}
	return "hb-" + n + "-" + hex.EncodeToString(sum[:])[:12]
}

var fullID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// checkIdentity: the Instance's Allocation runs its job under the tag HBEP-3
// computes, which is also the Allocation's external name and observed tag;
// the Instance carries a full deployment ID, its display form in its label,
// and the identity record it names exists for that ID; and every Instance of
// the deployment so far carries the same ID (one accelerator class).
func (p3 *part3) checkIdentity(d *depRun, in *v1alpha1.Instance) {
	r := p3.rig
	r.t.Helper()
	var a v1alpha1.Allocation
	if !r.get(&a, in.Spec.AllocationRef.Name, namespace) {
		r.t.Fatalf("%s: Instance %s's Allocation %s is not in the API", d.name, in.Name, in.Spec.AllocationRef.Name)
	}
	tag := hbep3Tag(a.Namespace, a.Name, a.UID)
	id, err := strconv.Atoi(a.Status.AtProvider.JobID)
	if err != nil {
		r.t.Fatalf("%s: Allocation %s's job ID %q: %v", d.name, a.Name, a.Status.AtProvider.JobID, err)
	}
	j, ok := r.cl.plat.Job(id)
	switch {
	case !ok:
		r.t.Errorf("%s: the fake cluster has no job %d for Allocation %s", d.name, id, a.Name)
	case j.Name != tag:
		r.t.Errorf("%s: job %d of Allocation %s runs as %q, not under its tag %q (HBEP-3)", d.name, id, a.Name, j.Name, tag)
	}
	if a.Annotations[v1alpha1.AnnotationExternalName] != tag || a.Status.AtProvider.Tag != tag {
		r.t.Errorf("%s: Allocation %s's external name %q and observed tag %q, want %q", d.name, a.Name, a.Annotations[v1alpha1.AnnotationExternalName], a.Status.AtProvider.Tag, tag)
	}
	full := string(in.Spec.DeploymentID)
	if !fullID.MatchString(full) || in.Labels[v1alpha1.LabelDeploymentID] != full[:min(12, len(full))] || in.Spec.IdentityRecord != "id-"+full[:min(12, len(full))] {
		r.t.Errorf("%s: Instance %s carries deployment ID %q, label %q, identity record %q", d.name, in.Name, full, in.Labels[v1alpha1.LabelDeploymentID], in.Spec.IdentityRecord)
		return
	}
	var rec appsv1.ControllerRevision
	if !r.get(&rec, in.Spec.IdentityRecord, namespace) || rec.Labels[v1alpha1.LabelDeploymentID] != full[:12] || rec.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordIdentity {
		r.t.Errorf("%s: the identity record %s of Instance %s is missing or not for %s: labels %v", d.name, in.Spec.IdentityRecord, in.Name, full[:12], rec.Labels)
	}
	if d.primary != nil && d.primary.Spec.DeploymentID != in.Spec.DeploymentID {
		r.t.Errorf("%s: Instance %s carries deployment ID %s, the primary %s carried %s: one class has one ID (HBEP-3)", d.name, in.Name, full, d.primary.Name, d.primary.Spec.DeploymentID)
	}
	r.t.Logf("%s: Instance %s carries deployment ID %s under Allocation %s's tag %s (job %d)", d.name, in.Name, full[:12], a.Name, tag, id)
}

// checkQueuedSuccessor: on the queued path the successor is created at its
// planned time, before the drain start, marked as a queued successor; its job
// is accepted and pends, not held, while the predecessor's job runs: in the
// fake, whose queued partition has no slot limit, only the GPU cap keeps it
// pending (HBEP-7, No-overlap mode).
func (p3 *part3) checkQueuedSuccessor(d *depRun) {
	r := p3.rig
	r.t.Helper()
	var succ v1alpha1.Instance
	var jobID int
	waitState(r.t, waitDeadline, "the queued successor's job to be accepted and released", func() (bool, string) {
		for _, in := range p3.instancesOf(d.name) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] != d.primary.Name {
				continue
			}
			var a v1alpha1.Allocation
			if !r.get(&a, in.Spec.AllocationRef.Name, namespace) || a.Status.AtProvider.JobID == "" {
				return false, "the successor " + in.Name + "'s Allocation has no job yet"
			}
			jobID, _ = strconv.Atoi(a.Status.AtProvider.JobID)
			j, ok := r.cl.plat.Job(jobID)
			if !ok || j.Held {
				return false, fmt.Sprintf("the successor's job %d is held or unknown", jobID)
			}
			succ = in
			return true, ""
		}
		return false, "no successor of " + d.primary.Name + " yet: " + r.describe()
	})
	pj, _ := r.cl.plat.Job(d.primaryJob)
	sj, _ := r.cl.plat.Job(jobID)
	if pj.State != "RUNNING" || sj.State != "PENDING" || sj.Held || !sj.Start.IsZero() {
		r.t.Errorf("%s: the predecessor's job %d is %s and the successor's job %d is %s (held %v, started %s); want RUNNING and pending on the GPU cap",
			d.name, d.primaryJob, pj.State, jobID, sj.State, sj.Held, ts(sj.Start))
	}
	if time.Until(d.ds) <= 0 {
		r.t.Errorf("%s: the successor was seen pending only after the drain start", d.name)
	}
	var a v1alpha1.Allocation
	if !r.get(&a, succ.Spec.AllocationRef.Name, namespace) || a.Annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		r.t.Errorf("%s: the queued successor's Allocation %s carries no queued-successor mark", d.name, succ.Spec.AllocationRef.Name)
	}
	p3.checkIdentity(d, &succ)
	r.t.Logf("%s: the queued successor %s's job %d is accepted and pends on the GPU cap beside the predecessor's running job %d, %s before the drain start",
		d.name, succ.Name, jobID, d.primaryJob, time.Until(d.ds).Round(time.Second))
}

// requeueResult names what the requeue failed.
type requeueResult struct {
	alloc   string
	uid     types.UID
	inst    string
	instUID types.UID
	job     int
}

// requeue is item 3: the fake requeues the queued successor's job, which then
// returns to a user hold. The Instance ends Requeued; no scontrol release of
// the job follows; the Allocation is deleted and its finalizer cancels the
// job; and a new Allocation hosts the replacement, which carries the same
// deployment ID under its own tag.
func (p3 *part3) requeue(d *depRun) {
	r := p3.rig
	r.t.Helper()
	in, ok := r.instance(d.succ.Name)
	if !ok || in.Status.Phase != v1alpha1.InstanceReady {
		r.t.Fatalf("%s: the requeue needs the ready successor %s: %+v", d.name, d.succ.Name, in.Status)
	}
	var a v1alpha1.Allocation
	if !r.get(&a, in.Spec.AllocationRef.Name, namespace) {
		r.t.Fatalf("%s: the successor's Allocation %s is gone", d.name, in.Spec.AllocationRef.Name)
	}
	p3.requeued = requeueResult{alloc: a.Name, uid: a.UID, inst: in.Name, instUID: in.UID, job: d.succJob}
	known := map[types.UID]bool{}
	for _, o := range r.watch.allocations() {
		known[o.uid] = true
	}
	at := time.Now()
	if err := r.cl.plat.RequeueHold(d.succJob); err != nil {
		r.t.Fatalf("%s: requeue job %d: %v", d.name, d.succJob, err)
	}
	var repl v1alpha1.Allocation
	waitState(r.t, waitDeadline, "the requeued Instance to fail, its Allocation to go and a new one to take its place", func() (bool, string) {
		o, ended := r.watch.first(in.UID, at, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded })
		gone := slices.ContainsFunc(r.watch.allocations(), func(o allocObservation) bool { return o.uid == a.UID && o.deleted })
		j, _ := r.cl.plat.Job(d.succJob)
		var fresh *v1alpha1.Allocation
		for _, x := range p3.allocationsOf(d.name) {
			if !known[x.UID] && x.Status.AtProvider.JobID != "" {
				x := x
				fresh = &x
			}
		}
		state := fmt.Sprintf("Instance %s ended %v (%q); Allocation %s deleted %v; job %d %s; new Allocation %v", in.Name, ended, o.endReason, a.Name, gone, d.succJob, j.State, fresh != nil)
		if fresh != nil {
			repl = *fresh
		}
		return ended && gone && j.State == "CANCELLED" && fresh != nil, state
	})
	o, _ := r.watch.first(in.UID, at, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded })
	if o.endReason != v1alpha1.ReasonRequeued {
		r.t.Errorf("%s: the requeued Instance %s ended %q, want %s (HBEP-3, open question 13)", d.name, in.Name, o.endReason, v1alpha1.ReasonRequeued)
	}
	j, _ := r.cl.plat.Job(d.succJob)
	if j.RestartCount < 1 || j.Executions < 2 {
		r.t.Errorf("%s: job %d has restart count %d and %d executions after the requeue", d.name, d.succJob, j.RestartCount, j.Executions)
	}
	want := strconv.Itoa(d.succJob)
	cancelled := false
	for _, c := range r.cl.plat.Calls() {
		if c.Time.Before(at) || len(c.Words) == 0 || !slices.Contains(c.Words, want) {
			continue
		}
		switch {
		case c.Words[0] == "scontrol" && slices.Contains(c.Words, "release"):
			r.t.Errorf("%s: the requeued job %d's hold was lifted again at %s (HBEP-3: never lifted again)", d.name, d.succJob, ts(c.Time))
		case c.Words[0] == "scancel":
			cancelled = true
		}
	}
	if !cancelled {
		r.t.Errorf("%s: no scancel of the requeued job %d after the requeue: the finalizer did not cancel it", d.name, d.succJob)
	}
	var ri v1alpha1.Instance
	waitFor(r.t, waitDeadline, "the replacement's Instance on the new Allocation "+repl.Name, func() bool {
		for _, x := range p3.instancesOf(d.name) {
			if x.Spec.AllocationRef.Name == repl.Name {
				ri = x
				return true
			}
		}
		return false
	})
	p3.checkIdentity(d, &ri)
	r.t.Logf("%s: the requeue of job %d failed Instance %s (%s); Allocation %s deleted and its job cancelled; replacement %s on the new Allocation %s",
		d.name, d.succJob, in.Name, o.endReason, a.Name, ri.Name, repl.Name)
}
