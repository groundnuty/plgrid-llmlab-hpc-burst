package scenario_test

// Part 3's forbidden events, proven absent on the lifecycle controller's
// raised-events counter by HBEP-8's four conditions (absence_test.go; #256):
// no ReplacementLate (HBEP-7 sets none in no-overlap mode) and no HandedOver
// (no replacement is ready at a primary's drain start). The first read is
// taken as soon as the controller restarted in longStreamAcrossRestart serves
// (part3.base), before the no-overlap handovers; the second after a reconcile
// pass of every ModelDeployment that began at or after the drain starts.

import (
	"fmt"
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
		if !r.get(&md, d.name, namespace) {
			r.t.Fatalf("no ModelDeployment %s", d.name)
		}
		if changeHanded.IsZero() || d.ds.Before(changeHanded) {
			changeHanded = d.ds
		}
		if class, why, ok := r.primaryClass(d.primary); !ok {
			unmetLate = append(unmetLate, fmt.Sprintf("condition 1: %s: %s, so the class whose start-to-ready time ReplacementLate counts by is unknown", d.name, why))
		} else {
			s2r := lateStartToReady(class, base.startToReady[d.name], classStartToReady(&md))
			r.t.Logf("%s: ReplacementLate counts back %s from the drain start %s (class %v; at the first read %v, now %v)",
				d.name, s2r, ts(d.ds), class, base.startToReady[d.name], classStartToReady(&md))
			if late := d.ds.Add(-s2r); changeLate.IsZero() || late.Before(changeLate) {
				changeLate = late
			}
		}
		due := d.ds
		for _, in := range r.instances() {
			if ds, ok := drainStartOf(&in); ok && ownedBy(&in, d.name) && ds.Before(time.Now()) && ds.After(due) {
				due = ds
			}
		}
		dues[d.name] = due
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
		if _, ok := dues[md.Name]; !ok || md.Namespace != namespace {
			quiet = "" // a deployment with no barrier: its passes are not shown quiet
			r.t.Logf("ModelDeployment %s/%s has no barrier", md.Namespace, md.Name)
		}
	}
	var barriers []eventcount.Barrier
	for name, due := range dues {
		var b eventcount.Barrier
		stack.WaitState(r.t, stack.WaitDeadline, "a reconcile pass of "+name+" that began at or after "+ts(due), func() (bool, string) {
			var md v1alpha1.ModelDeployment
			if !r.get(&md, name, namespace) || md.Status.LastReconcileTime == nil {
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
