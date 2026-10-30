package e2e

import (
	"bytes"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const gcTimeout = 90 * time.Second

// Deleting a ModelDeployment drains its Instances through their finalizer, and
// leaves the identity and verification records in place (HBEP-11, Ownership,
// records and deletion; Test plan). It needs a garbage collector, so it runs on
// a host with a controller manager and proves the collector works first.
//
// Three controls make "the records remain" mean something:
//   - a ConfigMap owned by the ModelDeployment must be collected: the collector runs;
//   - a ControllerRevision with the record's shape but an owner reference to the
//     ModelDeployment must be collected: records survive because they are not owned,
//     not because ControllerRevisions escape collection;
//   - the Instance has a deletion timestamp and stays until its finalizer is removed.
func TestDeletingAModelDeploymentLeavesTheRecords(t *testing.T) {
	ns := newNamespace(t)

	md := newModelDeployment(ns, "owner")
	must(t, c.Create(bg(), md), "create the ModelDeployment")

	in := newInstance(ns, "inst-1")
	in.OwnerReferences = []metav1.OwnerReference{ownerRef(md, "ModelDeployment")}
	in.Finalizers = []string{v1alpha1.FinalizerDrain}
	must(t, c.Create(bg(), in), "create the owned Instance")

	// A started Allocation names the deployment but is not owned by it.
	al := newAllocation(ns, "alloc-1")
	al.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: md.Name}
	must(t, c.Create(bg(), al), "create the Allocation")

	identityData := []byte(`{"scheme":"v1","hash":"` + fullID + `","kind":"served"}`)
	verifyData := []byte(`{"verdict":"pass","gpu":"GH200"}`)
	identity, err := v1alpha1.NewIdentityRecord(ns, md.Name, fullID, identityData)
	must(t, err, "build the identity record")
	must(t, c.Create(bg(), identity), "create the identity record")
	verification, err := v1alpha1.NewVerificationRecord(ns, md.Name, fullID, in.UID, verifyData)
	must(t, err, "build the verification record")
	must(t, c.Create(bg(), verification), "create the verification record")
	for _, r := range []*appsv1.ControllerRevision{identity, verification} {
		if len(r.OwnerReferences) != 0 {
			t.Fatalf("record %s has owner references %v", r.Name, r.OwnerReferences)
		}
	}

	ownedConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owned-control", Namespace: ns,
		OwnerReferences: []metav1.OwnerReference{ownerRef(md, "ModelDeployment")}}}
	must(t, c.Create(bg(), ownedConfigMap), "create the owned ConfigMap")
	ownedRecord, err := v1alpha1.NewIdentityRecord(ns, md.Name, fullID, identityData)
	must(t, err, "build the owned control record")
	ownedRecord.Name = "id-owned-control"
	ownedRecord.OwnerReferences = []metav1.OwnerReference{ownerRef(md, "ModelDeployment")}
	must(t, c.Create(bg(), ownedRecord), "create the owned control record")

	identityUID, verificationUID := identity.UID, verification.UID
	mdUID, inUID, alUID := md.UID, in.UID, al.UID

	must(t, c.Delete(bg(), md), "delete the ModelDeployment")

	// The collector works: the controls it must remove are gone, by UID.
	eventually(t, gcTimeout, "the owned ConfigMap to be collected", func() (bool, error) { return goneByUID(ownedConfigMap, ownedConfigMap.UID) })
	eventually(t, gcTimeout, "the owned record-shaped ControllerRevision to be collected", func() (bool, error) { return goneByUID(ownedRecord, ownedRecord.UID) })
	eventually(t, gcTimeout, "the ModelDeployment to be gone", func() (bool, error) { return goneByUID(md, mdUID) })

	// The Instance is marked for deletion by the collector and held by its drain finalizer.
	eventually(t, gcTimeout, "the Instance to be marked for deletion", func() (bool, error) {
		got := &v1alpha1.Instance{}
		if err := c.Get(bg(), objKey(in), got); err != nil {
			return false, err
		}
		return got.DeletionTimestamp != nil, nil
	})
	consistently(t, 3*time.Second, "the Instance staying until its drain finalizer is removed", func() error {
		got := &v1alpha1.Instance{}
		if err := c.Get(bg(), objKey(in), got); err != nil {
			return err
		}
		if got.UID != inUID || got.DeletionTimestamp == nil {
			t.Fatalf("Instance changed identity or lost its deletion timestamp")
		}
		return nil
	})
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get the draining Instance")
	got.Finalizers = nil // the lifecycle controller does this once the Instance is ended
	must(t, c.Update(bg(), got), "remove the drain finalizer")
	eventually(t, 30*time.Second, "the Instance to go once drained", func() (bool, error) { return goneByUID(in, inUID) })

	// Only now, with the Instances gone, read the records by label.
	var idList appsv1.ControllerRevisionList
	must(t, c.List(bg(), &idList, client.InNamespace(ns), client.MatchingLabels{
		v1alpha1.LabelRecord: v1alpha1.RecordIdentity, v1alpha1.LabelDeploymentID: fullID[:12]}), "list identity records")
	if len(idList.Items) != 1 || idList.Items[0].UID != identityUID || idList.Items[0].Name != "id-"+fullID[:12] {
		t.Fatalf("identity records after the cascade: %d, want the one created (%s)", len(idList.Items), identityUID)
	}
	var verList appsv1.ControllerRevisionList
	must(t, c.List(bg(), &verList, client.InNamespace(ns), client.MatchingLabels{
		v1alpha1.LabelRecord: v1alpha1.RecordVerification, v1alpha1.LabelInstanceUID: string(inUID)}), "list verification records")
	if len(verList.Items) != 1 || verList.Items[0].UID != verificationUID {
		t.Fatalf("verification records after the cascade: %d, want the one created (%s)", len(verList.Items), verificationUID)
	}
	for _, tc := range []struct {
		rec  appsv1.ControllerRevision
		want []byte
	}{{idList.Items[0], identityData}, {verList.Items[0], verifyData}} {
		if !bytes.Equal(tc.rec.Data.Raw, tc.want) {
			t.Errorf("record %s data changed: %s, want %s", tc.rec.Name, tc.rec.Data.Raw, tc.want)
		}
		if len(tc.rec.OwnerReferences) != 0 {
			t.Errorf("record %s gained owner references %v", tc.rec.Name, tc.rec.OwnerReferences)
		}
	}

	// No ModelDeployment owns an Allocation: it outlives the deployment.
	gotAl := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(al), gotAl), "get the Allocation")
	if gotAl.UID != alUID || gotAl.DeletionTimestamp != nil {
		t.Fatalf("the Allocation was removed or marked for deletion with its deployment")
	}
}

// The Allocation's finalizer holds the object until the provider's jobs are
// cancelled. The controllers do the cancelling; here the finalizer is removed by hand.
func TestAllocationFinalizerHoldsTheObject(t *testing.T) {
	ns := newNamespace(t)
	al := newAllocation(ns, "held")
	al.Finalizers = []string{v1alpha1.FinalizerCancelAll}
	must(t, c.Create(bg(), al), "create")
	uid := al.UID

	must(t, c.Delete(bg(), al), "delete")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(al), got), "get after delete")
	if got.DeletionTimestamp == nil {
		t.Fatal("the Allocation has no deletion timestamp after a delete")
	}
	consistently(t, 2*time.Second, "the Allocation staying while its finalizer is set", func() error {
		g := &v1alpha1.Allocation{}
		return c.Get(bg(), objKey(al), g)
	})
	got.Finalizers = nil
	must(t, c.Update(bg(), got), "remove the finalizer")
	eventually(t, 30*time.Second, "the Allocation to go", func() (bool, error) { return goneByUID(al, uid) })
}

// The API server refuses any change to a ControllerRevision's data (Kubernetes'
// own update validation), which is what keeps identity records immutable.
func TestRecordDataCannotBeEdited(t *testing.T) {
	ns := newNamespace(t)
	rec, err := v1alpha1.NewIdentityRecord(ns, "md", fullID, []byte(`{"hash":"`+fullID+`"}`))
	must(t, err, "build")
	must(t, c.Create(bg(), rec), "create")
	got := &appsv1.ControllerRevision{}
	must(t, c.Get(bg(), objKey(rec), got), "get")
	got.Data.Raw = []byte(`{"hash":"tampered"}`)
	requireRefused(t, c.Update(bg(), got), "immutable")
}
