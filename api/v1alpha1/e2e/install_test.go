package e2e

import (
	"reflect"
	"strings"
	"testing"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The k3s run of this suite (HBEP-11, single-user host) needs the launcher's
// k3s. Until it is wired, this test reports why the k3s variant did not run.
func TestRunsOnK3s(t *testing.T) {
	if host != hostK3s {
		t.Skip("k3s variant not run: it needs the user-level k3s that internal/launcher (#41) starts. " +
			"Run with HPCBURST_TEST_HOST=k3s HPCBURST_KUBECONFIG=<its kubeconfig>")
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	must(t, err, "discovery client")
	v, err := dc.ServerVersion()
	must(t, err, "server version")
	// A pass here means the whole suite ran against k3s, not against some other server.
	if !strings.Contains(v.GitVersion, "k3s") {
		t.Fatalf("HPCBURST_TEST_HOST=k3s, but the server reports version %q", v.GitVersion)
	}
}

// The five kinds are served in the group and version, with the scopes of
// HBEP-3: this asks the API server's discovery, not the Go types.
func TestKindsAreServedWithTheirScopes(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	must(t, err, "discovery client")
	list, err := dc.ServerResourcesForGroupVersion(v1alpha1.GroupVersion.String())
	must(t, err, "discovery of "+v1alpha1.GroupVersion.String())
	want := map[string]bool{ // kind -> namespaced
		"Provider": false, "PlatformProfile": false, "ModelDeployment": true, "Allocation": true, "Instance": true,
	}
	got := map[string]bool{}
	for _, r := range list.APIResources {
		if !strings.Contains(r.Name, "/") {
			got[r.Kind] = r.Namespaced
			// Every kind is in the group's own category and no other. In
			// particular an Allocation is not in crossplane's "managed" category:
			// "kubectl delete managed --all" on a cluster that also runs
			// Crossplane must not reach Allocations and cancel their jobs.
			if len(r.Categories) != 1 || r.Categories[0] != "hpcburst" {
				t.Errorf("kind %s is in categories %v, want exactly [hpcburst]", r.Kind, r.Categories)
			}
		}
	}
	for kind, namespaced := range want {
		g, ok := got[kind]
		if !ok {
			t.Errorf("kind %s is not served", kind)
		} else if g != namespaced {
			t.Errorf("kind %s: namespaced=%v, want %v", kind, g, namespaced)
		}
	}
	if len(got) != len(want) {
		t.Errorf("served kinds %v, want exactly %d", got, len(want))
	}
}

// Objects of every kind are created, read back with their defaults, and their
// status subresource accepts a write. Listing the CRDs would prove none of it.
func TestObjectsOfEveryKindRoundTripWithDefaults(t *testing.T) {
	ns := newNamespace(t)

	prof := newProfile(uniq("prof"))
	must(t, c.Create(bg(), prof), "create PlatformProfile")
	cleanup(t, prof)
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(prof), got), "get PlatformProfile")
	if got.Spec.HoldRelease {
		t.Errorf("hold_release defaulted to true; an unverified platform must run fallback mode")
	}
	if got.Spec.LostGrace == nil || *got.Spec.LostGrace != "2m" {
		t.Errorf("lost_grace default = %v, want 2m (HBEP-11)", got.Spec.LostGrace)
	}
	if got.Spec.UnknownBound == nil || *got.Spec.UnknownBound != "10m" {
		t.Errorf("unknown_bound default = %v, want 10m (HBEP-11)", got.Spec.UnknownBound)
	}
	if got.Spec.InflightWindow != nil {
		t.Errorf("inflight_window has a default %v; HBEP-11 gives none", *got.Spec.InflightWindow)
	}
	got.Status.LastChecked = &metav1.Time{Time: metav1.Now().Time}
	must(t, c.Status().Update(bg(), got), "write PlatformProfile status")

	prov := newProvider(uniq("prov"))
	must(t, c.Create(bg(), prov), "create Provider")
	cleanup(t, prov)

	md := newModelDeployment(ns, "md")
	must(t, c.Create(bg(), md), "create ModelDeployment")
	if md.Spec.RetentionPolicy != nil {
		t.Errorf("a ModelDeployment without a retention policy got %v", md.Spec.RetentionPolicy)
	}
	md.Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{
		AcceleratorClass: &v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}, ID: fullID}}
	md.Status.ReadyInstances, md.Status.DesiredInstances = 1, 2
	must(t, c.Status().Update(bg(), md), "write ModelDeployment status")

	al := newAllocation(ns, "alloc")
	must(t, c.Create(bg(), al), "create Allocation")
	// S4's set, not crossplane's "*", which includes Update (TestAllocationManagementPolicies).
	if want := []xpv2.ManagementAction{"Observe", "Create", "Delete", "LateInitialize"}; !reflect.DeepEqual([]xpv2.ManagementAction(al.Spec.ManagementPolicies), want) {
		t.Errorf("managementPolicies default = %v, want %v", al.Spec.ManagementPolicies, want)
	}

	in := newInstance(ns, "inst")
	must(t, c.Create(bg(), in), "create Instance")
	in.Status.Phase = v1alpha1.InstancePending
	must(t, c.Status().Update(bg(), in), "write Instance status")
}

// Selecting Providers by the platform label returns every provider on that
// platform, two grants included (HBEP-11, Test plan).
func TestProvidersAreSelectedByTheirPlatformLabel(t *testing.T) {
	plat := uniq("plat")
	var want []string
	for i, platform := range []string{plat, plat, "other-" + plat} {
		p := newProvider(uniq("prov"))
		p.Labels[v1alpha1.LabelPlatform] = platform
		p.Labels[v1alpha1.LabelAccount] = []string{"grant-a", "grant-b", "grant-c"}[i]
		must(t, c.Create(bg(), p), "create Provider")
		cleanup(t, p)
		if platform == plat {
			want = append(want, p.Name)
		}
	}
	var list v1alpha1.ProviderList
	must(t, c.List(bg(), &list, client.MatchingLabels{v1alpha1.LabelPlatform: plat}), "list by platform label")
	var got []string
	for _, p := range list.Items {
		got = append(got, p.Name)
	}
	if len(got) != 2 || !(got[0] == want[0] && got[1] == want[1] || got[0] == want[1] && got[1] == want[0]) {
		t.Fatalf("providers on platform %s = %v, want %v", plat, got, want)
	}
}

func TestSchemeKnowsEveryKind(t *testing.T) {
	for _, obj := range []client.Object{&v1alpha1.Provider{}, &v1alpha1.PlatformProfile{}, &v1alpha1.ModelDeployment{},
		&v1alpha1.Allocation{}, &v1alpha1.Instance{}} {
		gvk, err := c.GroupVersionKindFor(obj)
		must(t, err, "GVK")
		if gvk.Group != v1alpha1.Group || gvk.Version != v1alpha1.Version {
			t.Errorf("%T is %v", obj, gvk)
		}
		if _, err := c.IsObjectNamespaced(obj); err != nil {
			t.Error(err)
		}
	}
}
