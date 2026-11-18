package main

// The Provider's identity and an Operation's start estimates (#661; HBEP-4, Estimate; the science agent's S4 ruling of
// 17 Nov 2026, and its bound on the grant: the Provider identity writes ONLY status.estimates, by server-side apply under
// its own field manager). Issue #660's roles grant exactly what each controller calls, so the estimate controller's calls
// (internal/providerctl/estimate.go) are in the hpcburst-provider role or the estimates never appear: its cache's list and
// watch and its API reader's get of operations, and the apply, a patch of operations/status. This file holds what makes
// that a proof under the real identity:
//   - the estimate controller (providerctl.Setup, as setupBatch calls it) runs on the PROVIDER identity's kubeconfig, in a
//     manager of its own, and the Operation gets its entry, written under hpcburst-provider-<provider>; the API server's
//     audit log says what the identity asked, answered 200, and that it never wrote an Operation's main resource;
//   - each granted verb is made by that identity as a request of its own (get, list, watch, and the apply), so a role
//     without one is refused (403) at that row, and the authorizer's own answer is asked for each verb the roles name,
//     granted and not granted (a SelfSubjectAccessReview);
//   - the verbs outside the bound are refused: an update of an Operation, a write to its spec, a status update, a create,
//     a delete. A status PATCH is the one write the role allows, and RBAC cannot narrow it to one field: the field is the
//     code's, which the audit rows show (the only write is the apply, to status).
// It does not run the lifecycle controller: that one would plan a Release of an Allocation of a ModelDeployment that does not
// exist and Refuse the Operation, which the estimate controller does not write to. The binary's own Provider manager is proved
// to start under this role by TestEachManagerOfTheBinaryRunsUnderItsOwnIdentity, whose end-of-test check fails on a refused
// list or watch of operations.

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// estimateRowOperation is a Release Operation of one Allocation, with the plan the lifecycle controller would write: Planned,
// observed now, one costed target on the Provider. The ModelDeployment it names does not exist, which no one but the
// lifecycle controller reads.
func estimateRowOperation(ns, name string, alloc *v1alpha1.Allocation, prov string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.OperationSpec{
			Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "example-md"},
			Release: &v1alpha1.ReleaseParameters{Allocations: []string{alloc.Name}}, Owner: "example-owner",
		},
		Status: v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: &v1alpha1.OperationPlan{
			ID: "0123456789abcdef", ObservedAt: metav1.Now(),
			ModelDeployment: v1alpha1.PlannedDeployment{Name: "example-md", UID: "00000000-0000-0000-0000-0000000000b1", Generation: 1},
			Targets: []v1alpha1.PlanTarget{{Kind: v1alpha1.TargetKindAllocation, Name: alloc.Name, UID: alloc.UID, Generation: 1,
				Provider: prov, Effect: v1alpha1.PlanEffectReleaseKept, Cost: &v1alpha1.ReleaseCost{}}},
		}},
	}
}

func TestTheProviderIdentityEstimatesAnOperationAndWritesNothingElseOfIt(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	_, providerKC := api.controllerIdentities(t, t.TempDir())
	providerSA := controllerSA + "hpcburst-provider"

	// A batch Provider on the fake Slurm, whose profile does not say how to read the estimate (no startEstimateTime), so the
	// estimate is unknown with that reason and no sbatch runs: the row needs no --test-only answer.
	profile := batchProfile("estimate-slurm")
	prov := batchProvider("estimate-batch", profile.Name, partX86, sshCredentials(t))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "estimate-rows"}}
	for _, o := range []client.Object{profile, prov, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	alloc := batchAllocation("estimate-alloc", ns.Name, prov.Name, partX86, strings.Repeat("a", 64))
	if err := c.Create(ctx, alloc); err != nil {
		t.Fatal(err)
	}

	// The verbs, one request each, under the Provider's identity, on an Operation the estimate controller leaves alone (no plan).
	asProvider, err := client.NewWithWatch(restFor(t, providerKC, negativeRowAgent), client.Options{Scheme: c.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	rows := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "estimate-verbs", Namespace: ns.Name}, Spec: v1alpha1.OperationSpec{
		Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "example-md"},
		Release: &v1alpha1.ReleaseParameters{Allocations: []string{alloc.Name}}, Owner: "example-owner"}}
	if err := c.Create(ctx, rows); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(rows)
	if err := asProvider.Get(ctx, key, &v1alpha1.Operation{}); err != nil {
		t.Errorf("GET of an Operation as hpcburst-provider (freshPlan's API reader): %v", err)
	}
	if err := asProvider.List(ctx, &v1alpha1.OperationList{}); err != nil {
		t.Errorf("LIST of Operations, cluster-wide, as hpcburst-provider (the cache's informer): %v", err)
	}
	if w, err := asProvider.Watch(ctx, &v1alpha1.OperationList{}); err != nil {
		t.Errorf("WATCH of Operations, cluster-wide, as hpcburst-provider (the cache's informer): %v", err)
	} else {
		w.Stop()
	}
	mine := v1alpha1.FieldManagerProviderEstimate(prov.Name)
	apply := func(c client.Client, op *v1alpha1.Operation) error {
		t.Helper()
		start := metav1.Now()
		u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, prov.Name, []v1alpha1.OperationEstimate{{
			Provider: prov.Name, PlanID: "0123456789abcdef", Shape: "aaaaaaaaaaaaaaaa", Targets: []string{string(alloc.UID)},
			Start: v1alpha1.TimeFigure{Time: &start, ObservedAt: &start},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(mine), client.ForceOwnership)
	}
	if err := apply(asProvider, rows); err != nil {
		t.Errorf("the estimates' server-side apply to operations/status as hpcburst-provider (PATCH of the status): %v", err)
	}
	var got v1alpha1.Operation
	if err := c.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Estimates) != 1 || got.Status.Estimates[0].Provider != prov.Name {
		t.Errorf("the identity's apply left %+v, want its one entry", got.Status.Estimates)
	}

	// What is not granted, by the real request and by the authorizer's own answer.
	forbidden := func(what string, err error) {
		t.Helper()
		switch {
		case err == nil:
			t.Errorf("%s was ACCEPTED for hpcburst-provider: its role grants more than the estimates", what)
		case !apierrors.IsForbidden(err):
			t.Errorf("%s was refused otherwise than by RBAC: %v", what, err)
		case !strings.Contains(err.Error(), "hpcburst-provider"):
			t.Errorf("%s: the refusal does not name the identity: %v", what, err)
		}
	}
	upd := got.DeepCopy()
	upd.Annotations = map[string]string{"example.org/row": "x"}
	forbidden("an UPDATE of an Operation", asProvider.Update(ctx, upd))
	forbidden("a write to an Operation's SPEC (a merge patch of spec.confirm)", asProvider.Patch(ctx, got.DeepCopy(),
		client.RawPatch(types.MergePatchType, []byte(`{"spec":{"confirm":{"planID":"0123456789abcdef"}}}`))))
	spec := got.DeepCopy()
	spec.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: "0123456789abcdef"}
	forbidden("an UPDATE of an Operation's spec", asProvider.Update(ctx, spec))
	forbidden("an UPDATE of an Operation's status (only the patch is granted)", asProvider.Status().Update(ctx, got.DeepCopy()))
	forbidden("a CREATE of an Operation", asProvider.Create(ctx, &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "estimate-created", Namespace: ns.Name}, Spec: rows.Spec}))
	forbidden("a DELETE of an Operation", asProvider.Delete(ctx, got.DeepCopy()))
	if err := c.Get(ctx, key, &got); err != nil || got.Spec.Confirm != nil || len(got.Annotations) != 0 {
		t.Errorf("after the refused writes the Operation is %v (confirm %v, annotations %v), want it as it was", err, got.Spec.Confirm, got.Annotations)
	}
	for _, v := range []struct {
		verb, resource string
		want           bool
	}{
		{"get", "operations", true}, {"list", "operations", true}, {"watch", "operations", true}, {"patch", "operations/status", true},
		{"update", "operations", false}, {"patch", "operations", false}, {"create", "operations", false}, {"delete", "operations", false},
		{"deletecollection", "operations", false}, {"update", "operations/status", false}, {"get", "operations/status", false},
	} {
		resource, sub, _ := strings.Cut(v.resource, "/")
		if got := canI(t, providerKC, v.verb, v1alpha1.GroupVersion.Group, resource, sub); got != v.want {
			t.Errorf("hpcburst-provider may %s %s: the API server says %v, want %v", v.verb, v.resource, got, v.want)
		}
	}

	// The estimate controller itself, as the binary's Provider manager runs it (providerctl.Setup), on the Provider's
	// identity and in a manager of its own, against an Operation in Planned with a costed target on this Provider.
	scheme, err := providerScheme()
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := providerctl.NewProviderManager(restFor(t, providerKC, ""), scheme, prov.Name, providerctl.ManagerOptions{
		LeaseDuration: 8 * time.Second, RenewDeadline: 6 * time.Second, RetryPeriod: time.Second, GracefulShutdownTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	sl, session := newFakeSlurm()
	var served sync.Map
	if err := providerctl.Setup(mgr, providerctl.Options{
		Provider: prov.Name, Profiles: &slurm.ProfileReader{Reader: mgr.GetClient()}, NewPlatform: recordingPlatforms(session, &served),
	}); err != nil {
		t.Fatal(err)
	}
	mctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the Provider manager stopped with %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Error("the Provider manager did not stop within 60 s")
		}
	})

	op := estimateRowOperation(ns.Name, "estimate-op", alloc, prov.Name)
	status := op.Status
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	op.Status = status
	if err := c.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	opKey := client.ObjectKeyFromObject(op)
	waitUntil(t, 90*time.Second, "the Provider's estimate controller to apply its entry under the Provider identity", func() bool {
		return c.Get(ctx, opKey, op) == nil && len(op.Status.Estimates) == 1
	})
	est := op.Status.Estimates[0]
	if est.Provider != prov.Name || est.PlanID != "0123456789abcdef" || !slices.Equal(est.Targets, []string{string(alloc.UID)}) {
		t.Errorf("the entry is %+v, want the Provider's, the plan's and the Allocation's UID", est)
	}
	if est.Start.Time != nil || est.Start.Unknown != provider.EstimateUndecided || !est.Start.Advisory {
		t.Errorf("the start is %+v, want unknown (the profile does not say how to read it), advisory", est.Start)
	}
	owners := map[string]string{}
	for _, mf := range op.ManagedFields {
		if mf.FieldsV1 != nil && strings.Contains(string(mf.FieldsV1.Raw), "f:estimates") {
			owners[mf.Manager] = mf.Subresource
		}
	}
	if len(owners) != 1 || owners[mine] != "status" {
		t.Errorf("the managers that own status.estimates are %v, want only %s on the status subresource", owners, mine)
	}
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls for an undecided platform, want none", n)
	}

	// The audit log: what the identity asked of Operations, answered, and the only write it made. The log is written a moment
	// after the answer, so the rows wait for the two requests the entry came from before they read it whole.
	estimator := func() (gets, patchesOfStatus int, outside []string) {
		for _, e := range readAuditEvents(t, api.auditLog) {
			if e.User.Username != providerSA || e.ObjectRef.Resource != "operations" || e.UserAgent == negativeRowAgent {
				continue
			}
			r := e.ObjectRef.Subresource
			switch {
			case e.ResponseStatus.Code == 403:
				outside = append(outside, "REFUSED (403): "+e.String())
			case e.Verb == "get" && r == "" && e.ObjectRef.Name == op.Name:
				gets++
			case e.Verb == "patch" && r == "status" && e.ObjectRef.Name == op.Name && e.ResponseStatus.Code == 200:
				patchesOfStatus++
			case slices.Contains([]string{"get", "list", "watch"}, e.Verb) && r == "":
			case e.Verb == "patch" && r == "status":
			default:
				outside = append(outside, "outside its grant: "+e.String())
			}
		}
		return gets, patchesOfStatus, outside
	}
	waitUntil(t, 30*time.Second, "the audit log to hold the estimate controller's GET and its PATCH of the status", func() bool {
		g, p, _ := estimator()
		return g > 0 && p > 0
	})
	gets, patchesOfStatus, outside := estimator()
	for _, o := range outside {
		t.Errorf("the estimate controller's request, %s", o)
	}
	t.Logf("the estimate controller made %d GETs of the Operation and %d PATCHes of its status, and nothing else of an Operation's main resource", gets, patchesOfStatus)
}
