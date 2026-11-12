package main

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
)

// The front's routing table without a controller: the objects the front's
// own inventory.Watch lists and watches, served by client-go's fake dynamic
// client, as internal/front/tunnel_run_test.go and keys_test.go run the front.
// One batch PlatformProfile and Provider, and per tunnel one served
// ModelDeployment (its own model name, so a request names its tunnel) with
// one ready Instance whose UID is the tunnel's proxy name. The names are
// examples of the product's shapes.

// fixtureObjects are the configuration's objects, as the API server would
// return them.
func fixtureObjects(sp Spec, now time.Time) ([]runtime.Object, error) {
	var out []runtime.Object
	add := func(kind string, obj any) error {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return err
		}
		u := &unstructured.Unstructured{Object: m}
		u.SetAPIVersion(v1alpha1.GroupVersion.String())
		u.SetKind(kind)
		out = append(out, u)
		return nil
	}
	prof := v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: sp.ProfileName, ResourceVersion: "1"},
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch}}
	if err := add("PlatformProfile", &prof); err != nil {
		return nil, err
	}
	prov := v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: sp.ProviderName, ResourceVersion: "1",
		Labels: map[string]string{v1alpha1.LabelPlatform: "example", v1alpha1.LabelOrganization: "example"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: sp.ProfileName}}}
	if err := add("Provider", &prov); err != nil {
		return nil, err
	}
	leaseEnd := metav1.NewTime(now.Add(48 * time.Hour))
	ctrl := true
	for _, t := range sp.Tunnels {
		mdName := fmt.Sprintf("md-%04d", t.Index)
		mdUID := types.UID(fmt.Sprintf("20000000-0000-4000-8000-%012d", t.Index+1))
		md := v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: sp.DeploymentsNS, Name: mdName, UID: mdUID, ResourceVersion: "1"},
			Spec: v1alpha1.ModelDeploymentSpec{Model: t.Model, Instances: 1, Providers: v1alpha1.ProviderSelection{Names: []string{sp.ProviderName}},
				Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{}}}}
		if err := add("ModelDeployment", &md); err != nil {
			return nil, err
		}
		inst := v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: sp.DeploymentsNS, Name: mdName + "-0", UID: types.UID(t.UID), ResourceVersion: "1",
				Labels:          map[string]string{v1alpha1.LabelProvider: sp.ProviderName, v1alpha1.LabelModelDeployment: mdName},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: mdName, UID: mdUID, Controller: &ctrl}}},
			Spec:   v1alpha1.InstanceSpec{DeploymentID: v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0x5160000+t.Index)), Launch: v1alpha1.Launch{MaxNumSeqs: 256}},
			Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady, LeaseEnd: &leaseEnd},
		}
		if err := add("Instance", &inst); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// fixtureClient is the fake API the front watches.
func fixtureClient(objs []runtime.Object) *dynfake.FakeDynamicClient {
	return dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, objs...)
}
