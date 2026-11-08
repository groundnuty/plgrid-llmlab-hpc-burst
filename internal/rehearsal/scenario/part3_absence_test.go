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
)

// startToReadyOf is the longest start-to-ready time the deployment's status
// records, the time HBEP-7 counts a replacement's start back from the drain
// start by (ReplacementLate's due time: the drain start minus the class's
// start-to-ready time); ok is false when it records none.
func startToReadyOf(md *v1alpha1.ModelDeployment) (time.Duration, bool) {
	var longest time.Duration
	ok := false
	for _, s := range md.Status.StartToReady {
		d, err := s.Duration.AsDuration()
		if err != nil {
			continue
		}
		if d > longest {
			longest = d
		}
		ok = true
	}
	return longest, ok
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
		if s2r, ok := startToReadyOf(&md); !ok {
			unmetLate = append(unmetLate, fmt.Sprintf("condition 1: %s records no start-to-ready time, so when ReplacementLate could be raised is unknown", d.name))
		} else if late := d.ds.Add(-s2r); changeLate.IsZero() || late.Before(changeLate) {
			changeLate = late
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
	ctx, cancel := callCtx(r.t)
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
		waitState(r.t, waitDeadline, "a reconcile pass of "+name+" that began at or after "+ts(due), func() (bool, string) {
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
