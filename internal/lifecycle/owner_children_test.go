package lifecycle

// What the lifecycle controller stamps on the Allocations and Instances it creates (owner.go; HBEP-10, Owner and
// purpose; issue #662). Each check reads the object as the Create call carried it (ownerLog), made by a real pass
// through createAllocation and createInstance, and compares it with a map written out by hand.

import (
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// wantChild checks one created object against the hand-written maps. The Instance's deployment-ID label is the
// class's ID, not something owner.go sets, so it is left out of the comparison.
func wantChild(t *testing.T, r ownerRecord, md string, annotations, wantLabels map[string]string) {
	t.Helper()
	lb := without(r.labels, v1alpha1.LabelDeploymentID)
	full := map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: md}
	for k, v := range wantLabels {
		full[k] = v
	}
	wantMap(t, r.kind+" "+r.name+" annotations", r.annotations, annotations)
	wantMap(t, r.kind+" "+r.name+" labels", lb, full)
}

// TestChildrenCarryTheirDeploymentsOwnerAndPurpose: two owners' deployments run side by side. Every Allocation and
// Instance the controller creates for each carries its deployment's owner and purpose and its owner's label, and the
// selector owner-hash=u-77d125ed9ec1 selects A's objects and A's deployment, never B's.
func TestChildrenCarryTheirDeploymentsOwnerAndPurpose(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.create(ownedMD("md-a", ownerA, "example purpose of a"))
	rg.create(ownedMD("md-b", ownerB, "example purpose of b"))
	rg.until(5*time.Minute, 10*time.Second, "both deployments have a ready Instance", func() bool {
		return rg.readyCount("md-a") >= 1 && rg.readyCount("md-b") >= 1
	})

	want := map[string]struct{ owner, purpose, hash string }{
		"md-a": {ownerA, "example purpose of a", hashA},
		"md-b": {ownerB, "example purpose of b", hashB},
	}
	for md, w := range want {
		allocs, insts := log.createdOf("Allocation", md), log.createdOf("Instance", md)
		if len(allocs) == 0 || len(insts) == 0 {
			t.Fatalf("%s: %d Allocations and %d Instances created, want some of each", md, len(allocs), len(insts))
		}
		ann := map[string]string{keyOwner: w.owner, keyPurpose: w.purpose}
		for _, r := range append(slices.Clone(allocs), insts...) {
			wantChild(t, r, md, ann, map[string]string{keyOwnerHash: w.hash})
		}
	}

	// The selection: a label selector, as the CLI's --owner filter will write it.
	sel := labels.SelectorFromSet(labels.Set{keyOwnerHash: hashA})
	if parsed, err := labels.Parse(keyOwnerHash + "=" + hashA); err != nil || parsed.String() != sel.String() {
		t.Fatalf("the selector parses as %v, %v", parsed, err)
	}
	var al v1alpha1.AllocationList
	if err := rg.c.List(rg.ctx, &al, client.InNamespace(rigNS), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		t.Fatal(err)
	}
	var gotAllocs []string
	for _, a := range al.Items {
		gotAllocs = append(gotAllocs, a.Name)
	}
	var wantAllocs []string
	for _, r := range log.createdOf("Allocation", "md-a") {
		wantAllocs = append(wantAllocs, r.name)
	}
	slices.Sort(gotAllocs)
	slices.Sort(wantAllocs)
	if !slices.Equal(gotAllocs, wantAllocs) {
		t.Errorf("the selector owner-hash=%s selects Allocations %v, want A's %v", hashA, gotAllocs, wantAllocs)
	}
	var il v1alpha1.InstanceList
	if err := rg.c.List(rg.ctx, &il, client.InNamespace(rigNS), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		t.Fatal(err)
	}
	if len(il.Items) == 0 || len(il.Items) != len(rg.instances("md-a")) {
		t.Errorf("the selector selects %d Instances, want A's %d (at least one)", len(il.Items), len(rg.instances("md-a")))
	}
	for _, in := range il.Items {
		if in.Labels[v1alpha1.LabelModelDeployment] != "md-a" {
			t.Errorf("the selector selects Instance %s of %s", in.Name, in.Labels[v1alpha1.LabelModelDeployment])
		}
	}
	// The deployments themselves: A's alone (Q4, the controller sets the label on the deployment too).
	var ml v1alpha1.ModelDeploymentList
	if err := rg.c.List(rg.ctx, &ml, client.InNamespace(rigNS), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		t.Fatal(err)
	}
	if len(ml.Items) != 1 || ml.Items[0].Name != "md-a" {
		t.Errorf("the selector selects ModelDeployments %v, want md-a alone", ml.Items)
	}
}

// TestADeploymentWithNoOwnerStampsNoOwnerAndNoLabel (S5): a deployment with no owner annotation (none at all, an empty
// one, or a purpose alone) has children with no owner annotation and no owner-hash label. A purpose is still copied:
// the purpose-only deployment's children carry it, and nothing else.
func TestADeploymentWithNoOwnerStampsNoOwnerAndNoLabel(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.create(ownedMD("md-none", "", ""))
	rg.create(ownedMD("md-purpose", "", "example purpose alone"))
	empty := ownedMD("md-empty", "", "")
	empty.Annotations = map[string]string{keyOwner: ""}
	rg.create(empty)
	rg.until(5*time.Minute, 10*time.Second, "every deployment has a ready Instance", func() bool {
		return rg.readyCount("md-none") >= 1 && rg.readyCount("md-purpose") >= 1 && rg.readyCount("md-empty") >= 1
	})
	for md, ann := range map[string]map[string]string{
		"md-none":    nil,
		"md-purpose": {keyPurpose: "example purpose alone"},
		"md-empty":   nil,
	} {
		allocs, insts := log.createdOf("Allocation", md), log.createdOf("Instance", md)
		if len(allocs) == 0 || len(insts) == 0 {
			t.Fatalf("%s: %d Allocations and %d Instances created, want some of each", md, len(allocs), len(insts))
		}
		for _, r := range append(slices.Clone(allocs), insts...) {
			wantChild(t, r, md, ann, nil)
		}
		if lb, _ := rg.mdMeta(md); len(lb) != 0 {
			t.Errorf("%s has the labels %v, want none", md, lb)
		}
	}
	if n := log.mdWrites(); n != 0 {
		t.Errorf("%d patches of a ModelDeployment, want none: no deployment here has an owner or a label", n)
	}
}

// TestAFirstPrimarysAllocationAndInstanceCarryTheOwner is the hand-written case of one deployment, with the exact
// annotation and label maps of its first Allocation and Instance: the first provision.
func TestAFirstPrimarysAllocationAndInstanceCarryTheOwner(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.create(ownedMD("md-a", ownerA, "example purpose of a"))
	rg.until(5*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount("md-a") == 1 })
	in := rg.instances("md-a")[0]
	a := log.createdOf("Allocation", "md-a")[0]
	i := log.record(t, "Instance", in.Name)
	if a.name != in.Spec.AllocationRef.Name {
		t.Fatalf("the first Allocation is %s, the Instance's is %s", a.name, in.Spec.AllocationRef.Name)
	}
	ann := map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"}
	wantChild(t, a, "md-a", ann, map[string]string{keyOwnerHash: hashA})
	wantChild(t, i, "md-a", ann, map[string]string{keyOwnerHash: hashA})
	// And the stored objects keep them.
	stored, _ := rg.allocation(a.name)
	wantMap(t, "the stored Allocation's owner-hash", map[string]string{keyOwnerHash: stored.Labels[keyOwnerHash]}, map[string]string{keyOwnerHash: hashA})
	wantMap(t, "the stored Instance's owner-hash", map[string]string{keyOwnerHash: in.Labels[keyOwnerHash]}, map[string]string{keyOwnerHash: hashA})
}

// TestAHandoverReplacementCarriesTheOwnerBesideItsPredecessorMark: the replacement made for a primary at its planned
// time, 09:19:30 on the handover timeline (handover_test.go), is created with the predecessor annotation, which the
// create sets by assigning a whole map; the owner and purpose are beside it, on the Instance and on its Allocation.
func TestAHandoverReplacementCarriesTheOwnerBesideItsPredecessorMark(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withMaxTime(4*time.Hour), withInterceptors(log.funcs()))
	rg.create(ownedMD(hoMD, ownerA, "example purpose of a"))
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	primary := rg.instances(hoMD)[0]
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	hoStepUntil(rg, hoT(9, 30, 0), 10*time.Second, func() bool { return len(hoRepl(rg, primary.Name)) == 1 }, nil)
	repl := hoRepl(rg, primary.Name)[0]

	ann := map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"}
	hash := map[string]string{keyOwnerHash: hashA}
	i := log.record(t, "Instance", repl.Name)
	wantChild(t, i, hoMD, map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a", keyPredecessor: primary.Name}, hash)
	a := log.record(t, "Allocation", repl.Spec.AllocationRef.Name)
	wantChild(t, a, hoMD, ann, hash)
}

// TestAHedgeMemberCarriesTheOwnerBesideItsHedgeGroup: the second member of a hedge, made by the hedge's growth in a
// real pass (hedge_grow.go; A's queue waits of 10 h put the missing primary past its deadline, so B's member comes in
// the first pass), is created with the hedge-group annotation, which the create sets by assigning a whole map; the
// owner and purpose are beside it, on the Instance and on its Allocation, on Provider B.
func TestAHedgeMemberCarriesTheOwnerBesideItsHedgeGroup(t *testing.T) {
	log := &ownerLog{}
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	first := missingPrimary(t, h, 10*time.Hour, func(md *v1alpha1.ModelDeployment) {
		md.Annotations = ownedMD(hoMD, ownerA, "example purpose of a").Annotations
	})
	members := h.membersOn(hoMD, first, hedgeB)
	if len(members) != 1 {
		t.Fatalf("%d members of %s's hedge on %s after the first pass, want the one growth makes", len(members), first, hedgeB)
	}
	second := members[0].Name

	hash := map[string]string{keyOwnerHash: hashA}
	i := log.record(t, "Instance", second)
	if i.labels[v1alpha1.LabelProvider] != hedgeB {
		t.Fatalf("the member %s was made on %s, want %s", second, i.labels[v1alpha1.LabelProvider], hedgeB)
	}
	// The hedge's own members are on B: compare the Provider label on its own line.
	wantMap(t, "member "+second+" annotations", i.annotations, map[string]string{keyHedgeGroup: first, keyOwner: ownerA, keyPurpose: "example purpose of a"})
	wantMap(t, "member "+second+" labels", without(i.labels, v1alpha1.LabelDeploymentID), map[string]string{
		v1alpha1.LabelProvider: hedgeB, v1alpha1.LabelModelDeployment: hoMD, keyOwnerHash: hashA,
	})
	a := log.record(t, "Allocation", h.mustInstance(second).Spec.AllocationRef.Name)
	wantMap(t, "member allocation annotations", a.annotations, map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"})
	wantMap(t, "member allocation labels", a.labels, map[string]string{
		v1alpha1.LabelProvider: hedgeB, v1alpha1.LabelModelDeployment: hoMD, keyOwnerHash: hashA,
	})
	// The first member, made by the pass itself, carries them too.
	wantChild(t, log.record(t, "Instance", first), hoMD, map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"}, hash)
}

// TestAQueuedSuccessorsAllocationCarriesTheOwnerBesideItsMark: on the queued path (no-overlap mode on Helios) the
// successor's Allocation is created with the queued-successor mark, which the create sets by assigning a whole map, and
// its Instance with the predecessor and no-overlap marks; the owner and purpose are beside every one of them.
func TestAQueuedSuccessorsAllocationCarriesTheOwnerBesideItsMark(t *testing.T) {
	log := &ownerLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	rg.create(releaseWhenIdle5m(ownedMD(hoMD, ownerA, "example purpose of a")))
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	primary := rg.instances(hoMD)[0]
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	hoStepUntil(rg, hoT(9, 30, 0), 10*time.Second, func() bool { return len(hoRepl(rg, primary.Name)) == 1 }, nil)
	succ := hoRepl(rg, primary.Name)[0]

	i := log.record(t, "Instance", succ.Name)
	mark := i.annotations[annotationNoOverlapSuccessor]
	if mark == "" {
		t.Fatalf("the successor's Instance %v carries no no-overlap mark: this is not the no-overlap path", i.annotations)
	}
	wantMap(t, "successor Instance annotations", i.annotations, map[string]string{
		keyPredecessor: primary.Name, annotationNoOverlapSuccessor: mark, keyOwner: ownerA, keyPurpose: "example purpose of a",
	})
	a := log.record(t, "Allocation", succ.Spec.AllocationRef.Name)
	queued := a.annotations[v1alpha1.AnnotationQueuedSuccessor]
	if queued == "" {
		t.Fatalf("the successor's Allocation %v carries no queued-successor mark: this is not the queued path", a.annotations)
	}
	wantMap(t, "successor Allocation annotations", a.annotations, map[string]string{
		"hpcburst.groundnuty.github.io/queued-successor": queued, keyOwner: ownerA, keyPurpose: "example purpose of a",
	})
	wantMap(t, "successor Allocation labels", a.labels, map[string]string{
		v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: hoMD, keyOwnerHash: hashA,
	})
}

// TestNothingIsBackfilled (Q5): an Allocation and an Instance that exist before the deployment's owner is read are not
// patched. The deployment starts with no owner annotation and runs; the annotation then appears (a person's write: the
// fake client has no owner pin) and the deployment grows to two Instances. The new Allocation and Instance carry the
// owner, which shows the annotation was in force, and the first pair, made before it, still carry none.
func TestNothingIsBackfilled(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.create(ownedMD("md-a", "", ""))
	rg.until(5*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount("md-a") == 1 })
	oldIn := rg.instances("md-a")[0]
	oldAlloc := oldIn.Spec.AllocationRef.Name
	patchesBefore := len(log.childPatches)

	rg.mutateMD("md-a", func(md *v1alpha1.ModelDeployment) {
		md.Annotations = map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"}
		md.Spec.Instances = 2
	})
	rg.until(5*time.Minute, 10*time.Second, "two ready Instances", func() bool { return rg.readyCount("md-a") == 2 })

	var newIn string
	for _, in := range rg.instances("md-a") {
		if in.Name != oldIn.Name {
			newIn = in.Name
		}
	}
	ann := map[string]string{keyOwner: ownerA, keyPurpose: "example purpose of a"}
	hash := map[string]string{keyOwnerHash: hashA}
	n := rg.instance(newIn)
	wantChild(t, log.record(t, "Instance", newIn), "md-a", ann, hash)
	wantChild(t, log.record(t, "Allocation", n.Spec.AllocationRef.Name), "md-a", ann, hash)

	// The old pair, stored now: no owner annotation, no purpose, no label.
	oi := rg.instance(oldIn.Name)
	wantMap(t, "the old Instance's annotations", oi.Annotations, nil)
	wantMap(t, "the old Instance's labels", without(oi.Labels, v1alpha1.LabelDeploymentID), map[string]string{
		v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: "md-a",
	})
	oa, ok := rg.allocation(oldAlloc)
	if !ok {
		t.Fatalf("the old Allocation %s is gone", oldAlloc)
	}
	for _, k := range []string{keyOwner, keyPurpose} {
		if _, has := oa.Annotations[k]; has {
			t.Errorf("the old Allocation has the annotation %s, a backfill", k)
		}
	}
	wantMap(t, "the old Allocation's labels", oa.Labels, map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: "md-a"})
	for _, body := range log.childPatches[patchesBefore:] {
		if strings.Contains(body, "hpcburst.groundnuty.github.io/owner") || strings.Contains(body, "hpcburst.groundnuty.github.io/purpose") {
			t.Errorf("an Allocation or Instance was patched with the owner or purpose after the owner appeared: %s", body)
		}
	}
}
