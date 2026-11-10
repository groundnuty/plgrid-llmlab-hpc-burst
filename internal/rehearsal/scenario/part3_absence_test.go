package scenario_test

// Part 3's forbidden events, proven absent on the lifecycle controller's
// raised-events counter by HBEP-8's four conditions (absence_test.go; #256):
// no ReplacementLate (HBEP-7 sets none in no-overlap mode) and no HandedOver
// (no replacement is ready at a primary's drain start). The first read is
// taken as soon as the controller restarted in longStreamAcrossRestart serves
// (part3.base), before the no-overlap handovers; the second after a reconcile
// pass of every ModelDeployment that began at or after the drain starts.
//
// The first read is armed only once every served deployment's status records
// its primary's class's start-to-ready time (issue #321, armFirstRead). An
// Instance's ready write and its deployment's status write, which records the
// class's start-to-ready time, are two writes of one pass (internal/lifecycle,
// verify.go: becomeReady; status.go: writeStatus), so the primary can be seen
// ready before the time is recorded, and step (f)'s kill can come between
// them. The restarted controller then records it in its first pass, after the
// first read: with no entry at the first read, ReplacementLate's condition 1
// counts back the 30-minute default and the check is inconclusive.

import (
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// defaultStartToReady is the controller's start-to-ready time for a class
// with no measurement (internal/lifecycle, settings.go; HBEP-7, Handover).
const defaultStartToReady = 30 * time.Minute

// classStartToReady is the deployment's measured start-to-ready time per
// accelerator class, as its status records them (one entry per class, the
// newest measurement).
func classStartToReady(md *v1alpha1.ModelDeployment) map[v1alpha1.AcceleratorClass]time.Duration {
	out := map[v1alpha1.AcceleratorClass]time.Duration{}
	for _, s := range md.Status.StartToReady {
		if d, err := s.Duration.AsDuration(); err == nil {
			out[s.AcceleratorClass] = d
		}
	}
	return out
}

// lateStartToReady is the start-to-ready time ReplacementLate's due time
// counts back from the drain start by, for condition 1: the controller uses
// the primary's class's entry, or defaultStartToReady when there is none
// (internal/lifecycle, plan.go: startToReady). A later measurement overwrites
// the class's entry, so the value read after the handovers can be shorter
// than the one the controller judged by at the first read; the larger of the
// two read (the first read's snapshot and the final status), each with the
// default where it has no entry, is never later than the earliest due time
// they give. In between the first read and the handovers no Instance becomes
// ready, so no measurement changes it.
func lateStartToReady(class v1alpha1.AcceleratorClass, atFirst, final map[v1alpha1.AcceleratorClass]time.Duration) time.Duration {
	of := func(m map[v1alpha1.AcceleratorClass]time.Duration) time.Duration {
		if d, ok := m[class]; ok {
			return d
		}
		return defaultStartToReady
	}
	return max(of(atFirst), of(final))
}

// primaryClass is the accelerator class the controller judges the primary
// by: its Provider's (the Instance's provider label) first allowed partition
// in the Provider's PlatformProfile (internal/lifecycle, topology.go:
// allowedPartition); ok is false when one of them cannot be read.
func (r *rig) primaryClass(in *v1alpha1.Instance) (v1alpha1.AcceleratorClass, string, bool) {
	name := in.Labels[v1alpha1.LabelProvider]
	var p v1alpha1.Provider
	if name == "" || !r.get(&p, name, "") {
		return v1alpha1.AcceleratorClass{}, fmt.Sprintf("the primary %s's Provider %q cannot be read", in.Name, name), false
	}
	var prof v1alpha1.PlatformProfile
	if !r.get(&prof, p.Spec.PlatformProfileRef.Name, "") {
		return v1alpha1.AcceleratorClass{}, fmt.Sprintf("Provider %s's PlatformProfile %q cannot be read", p.Name, p.Spec.PlatformProfileRef.Name), false
	}
	if len(p.Spec.AllowedPartitions) == 0 {
		return v1alpha1.AcceleratorClass{}, "Provider " + p.Name + " allows no partition", false
	}
	for _, part := range prof.Spec.Partitions {
		if part.Name == p.Spec.AllowedPartitions[0] {
			return part.AcceleratorClass, "", true
		}
	}
	return v1alpha1.AcceleratorClass{}, fmt.Sprintf("PlatformProfile %s describes no partition %s", prof.Name, p.Spec.AllowedPartitions[0]), false
}

// ownedBy reports whether the ModelDeployment named md is the Instance's
// controller owner.
func ownedBy(in *v1alpha1.Instance, md string) bool {
	for _, ref := range in.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.Kind == "ModelDeployment" && ref.Name == md {
			return true
		}
	}
	return false
}

// lateChange is the earliest moment ReplacementLate could be raised for the
// deployment, condition 1's change: its primary's drain start counted back by
// lateStartToReady, from the first read's snapshot (atFirst) and md's status
// now. ok is false, with why, when the primary's class cannot be read.
func (r *rig) lateChange(d *depRun, atFirst map[v1alpha1.AcceleratorClass]time.Duration, md *v1alpha1.ModelDeployment) (time.Time, string, bool) {
	r.t.Helper()
	class, why, ok := r.primaryClass(d.primary)
	if !ok {
		return time.Time{}, fmt.Sprintf("condition 1: %s: %s, so the class whose start-to-ready time ReplacementLate counts by is unknown", d.Name, why), false
	}
	now := classStartToReady(md)
	s2r := lateStartToReady(class, atFirst, now)
	r.t.Logf("%s: ReplacementLate counts back %s from the drain start %s (class %v; at the first read %v, now %v)",
		d.Name, s2r, ts(d.ds), class, atFirst, now)
	return d.ds.Add(-s2r), "", true
}

// longStreamLead is how long before the drain start the 7,000-token stream
// across step (f)'s restart must start (longStreamAcrossRestart).
const longStreamLead = 2*time.Minute + streamLead

// s2rPoll is how often unrecordedStartToReady reads the deployments again.
const s2rPoll = 200 * time.Millisecond

// unrecordedStartToReady reads the deployments until each one's status
// records the start-to-ready time of its primary's class, or until the bound
// passes (or WaitCap, when that comes first). It returns nil once all are
// recorded; at the bound, one problem per deployment still without its class,
// naming the deployment, its primary and the class. miss, when set, is called
// after each read that found one missing.
func (r *rig) unrecordedStartToReady(deps []*depRun, until time.Time, miss func([]string)) []string {
	r.t.Helper()
	if c, ok := stack.WaitCap(r.t); ok && c.Before(until) {
		until = c
	}
	for {
		snap := r.startToReadySnapshot("the start-to-ready times the first read waits for")
		var missing []string
		for _, d := range deps {
			class, why, ok := r.primaryClass(d.primary)
			if !ok {
				missing = append(missing, d.Name+": "+why)
			} else if _, ok := snap[d.Name][class]; !ok {
				missing = append(missing, fmt.Sprintf("%s: its status records no start-to-ready time for class %v, its primary %s's", d.Name, class, d.primary.Name))
			}
		}
		if len(missing) == 0 {
			return nil
		}
		if miss != nil {
			miss(missing)
		}
		if !time.Now().Before(until) {
			return missing
		}
		time.Sleep(s2rPoll)
	}
}

// armFirstRead arms the forbidden events' first read (r.onLed, taken as soon
// as the controller step (f) restarts serves) once every served deployment's
// status records its primary's class's start-to-ready time: the value
// ReplacementLate's condition 1 counts back by (issue #321, the science
// agent's ruling of 10 Nov 2026). The bound is the row's own: the earliest
// drain start minus longStreamLead, past which the stream across the restart
// cannot start in time either. A class still unrecorded then is a FAIL, never
// NOT RUN and never a pass: the primary is ready (waitPrimary), so the
// measurement HBEP-7 records at an Allocation's first ready Instance is
// missing, and the check could only be inconclusive. The error names each
// deployment and class; nothing is armed then.
func (p3 *part3) armFirstRead() error {
	r := p3.rig
	r.t.Helper()
	var first *depRun
	for _, d := range p3.deps {
		if first == nil || d.ds.Before(first.ds) {
			first = d
		}
	}
	if first == nil {
		return fmt.Errorf("FAIL (#321): no served deployment to wait for")
	}
	until := first.ds.Add(-longStreamLead)
	began, misses := time.Now(), 0
	missing := r.unrecordedStartToReady(p3.deps, until, func(m []string) {
		misses++
		if p3.s2rMiss != nil {
			p3.s2rMiss(m)
		}
	})
	if len(missing) > 0 {
		return fmt.Errorf("FAIL (#321): by %s, %s before %s's drain start %s (the stream across the restart needs %s), %s; with no start-to-ready time "+
			"at the first read ReplacementLate's condition 1 counts back %s, so its absence check could only be inconclusive",
			ts(time.Now()), time.Until(first.ds).Round(time.Second), first.Name, ts(first.ds), longStreamLead, strings.Join(missing, "; "), defaultStartToReady)
	}
	r.t.Logf("HEADROOM start-to-ready: every served deployment's status records its primary's class %s after the wait began (%d reads without), %s before %s's drain start",
		time.Since(began).Round(100*time.Millisecond), misses, time.Until(first.ds).Round(time.Second), first.Name)
	r.onLed = func() {
		p3.base = r.baseline("as part 3's restarted controller serves", keyTunnelOrphaned, keyReplacementLate, keyHandedOver)
		r.orphanBase = p3.base
	}
	return nil
}

// checkForbidden proves the two absences, each with one barrier per
// ModelDeployment: a pass of its reconcile, serialized per deployment, whose
// status.lastReconcileTime (the pass's start, stored to the second, so never
// later than it) is at or after the last drain start of its Instances that has
// passed, after which a pass has judged every drain start reached.
func (p3 *part3) checkForbidden() {
	r := p3.rig
	r.t.Helper()
	base := p3.base
	if base == nil {
		r.t.Fatal("the forbidden events' check has no first read from the restarted controller: it would be inconclusive")
	}
	var unmetLate []string
	var changeLate, changeHanded time.Time
	dues := map[string]time.Time{}
	for _, d := range p3.deps {
		var md v1alpha1.ModelDeployment
		if !r.get(&md, d.Name, stack.Namespace) {
			r.t.Fatalf("no ModelDeployment %s", d.Name)
		}
		if changeHanded.IsZero() || d.ds.Before(changeHanded) {
			changeHanded = d.ds
		}
		if late, why, ok := r.lateChange(d, base.startToReady[d.Name], &md); !ok {
			unmetLate = append(unmetLate, why)
		} else if changeLate.IsZero() || late.Before(changeLate) {
			changeLate = late
		}
		due := d.ds
		for _, in := range r.instances() {
			if ds, ok := drainStartOf(&in); ok && ownedBy(&in, d.Name) && ds.Before(time.Now()) && ds.After(due) {
				due = ds
			}
		}
		dues[d.Name] = due
	}
	// The managed deployment's passes take the managed path, which raises
	// neither event; its barrier needs only to follow both changes.
	dues[managedMD] = changeHanded
	if changeLate.After(changeHanded) {
		dues[managedMD] = changeLate
	}
	var all v1alpha1.ModelDeploymentList
	ctx, cancel := stack.CallCtx(r.t)
	err := r.c.List(ctx, &all)
	cancel()
	if err != nil {
		r.t.Fatalf("list ModelDeployments: %v", err)
	}
	quiet := "ReplacementLate and HandedOver are raised only by a ModelDeployment's reconcile pass, and every ModelDeployment has a barrier pass"
	for _, md := range all.Items {
		if _, ok := dues[md.Name]; !ok || md.Namespace != stack.Namespace {
			quiet = "" // a deployment with no barrier: its passes are not shown quiet
			r.t.Logf("ModelDeployment %s/%s has no barrier", md.Namespace, md.Name)
		}
	}
	var barriers []eventcount.Barrier
	for name, due := range dues {
		var b eventcount.Barrier
		stack.WaitState(r.t, stack.WaitDeadline, "a reconcile pass of "+name+" that began at or after "+ts(due), func() (bool, string) {
			var md v1alpha1.ModelDeployment
			if !r.get(&md, name, stack.Namespace) || md.Status.LastReconcileTime == nil {
				return false, "no lastReconcileTime"
			}
			at := md.Status.LastReconcileTime.Time
			if at.Before(due) {
				return false, "lastReconcileTime " + ts(at)
			}
			b = eventcount.Barrier{What: "a reconcile pass of " + name, PassStart: at, Observed: time.Now(), Due: due}
			return true, ""
		})
		barriers = append(barriers, b)
	}
	r.t.Logf("forbidden events: ReplacementLate could first be raised at %s, HandedOver at %s; the first read was at %s",
		ts(changeLate), ts(changeHanded), ts(base.reads[keyHandedOver].Answered))
	r.judgeAbsence(base, keyReplacementLate, changeLate, unmetLate, barriers, quiet)
	r.judgeAbsence(base, keyHandedOver, changeHanded, nil, barriers, quiet)
}
