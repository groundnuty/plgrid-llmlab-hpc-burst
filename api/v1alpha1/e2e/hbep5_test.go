package e2e

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The fields HBEP-5 adds (PR #63) and the owner's rulings on the contracts'
// open points. Expected values are HBEP-5's and HBEP-4's tables, written out.

// An absent routing comes back with both load bounds: c = 1.25 while the
// desired Instances are ready, and 2.0 below that (HBEP-5, Configuration). The
// cap and the request deadline have no schema default: absent, the launch's
// maxNumSeqs and the front's maxRequestDuration apply.
func TestRoutingDefaults(t *testing.T) {
	ns := newNamespace(t)
	for name, routing := range map[string]*v1alpha1.Routing{
		"no routing":                nil,
		"a routing with a deadline": {MaxRequestDuration: ptr.To(v1alpha1.Duration("30m"))},
	} {
		t.Run(name, func(t *testing.T) {
			md := newModelDeployment(ns, uniq("md"))
			md.Spec.Routing = routing
			must(t, c.Create(bg(), md), "create")
			got := &v1alpha1.ModelDeployment{}
			must(t, c.Get(bg(), objKey(md), got), "get")
			r := got.Spec.Routing
			if r == nil || r.LoadBound == nil || r.LoadBound.Tight == nil || *r.LoadBound.Tight != "1.25" ||
				r.LoadBound.Loose == nil || *r.LoadBound.Loose != "2.0" {
				t.Fatalf("routing = %+v, want loadBound tight 1.25 and loose 2.0", r)
			}
			if r.InstanceCap != nil {
				t.Errorf("instanceCap defaulted to %d", *r.InstanceCap)
			}
			if routing == nil && r.MaxRequestDuration != nil {
				t.Errorf("maxRequestDuration defaulted to %s", *r.MaxRequestDuration)
			}
			if routing != nil && (r.MaxRequestDuration == nil || *r.MaxRequestDuration != "30m") {
				t.Errorf("maxRequestDuration = %v, want 30m", r.MaxRequestDuration)
			}
		})
	}
}

// A load bound is a decimal of at least 1. These are the rules
// modeldeployments:$.spec.routing.loadBound.tight#0 and .loose#0; the decimal
// form and the cap's minimum are the schema's.
func TestRoutingLoadBoundIsAtLeastOne(t *testing.T) {
	const msg = "a load bound is at least 1.0"
	const orderMsg = "the tight load bound is at most the loose one"
	lf := func(s string) *v1alpha1.LoadFactor { return ptr.To(v1alpha1.LoadFactor(s)) }
	bound := func(tight, loose *v1alpha1.LoadFactor) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) {
			md.Spec.Routing = &v1alpha1.Routing{LoadBound: &v1alpha1.LoadBound{Tight: tight, Loose: loose}}
		}
	}
	routing := func(r v1alpha1.Routing) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &r }
	}
	runMDCases(t, []mdCase{
		{"a tight bound below 1", bound(lf("0.99"), nil), msg},
		{"a tight bound of 0", bound(lf("0"), nil), msg},
		{"a loose bound below 1", bound(nil, lf("0.5")), msg},
		{"a bound that is not a decimal", bound(lf("1.2.5"), nil), "routing.loadBound.tight"},
		{"a negative bound", bound(nil, lf("-2")), "routing.loadBound.loose"},
		{"a cap of 0", routing(v1alpha1.Routing{InstanceCap: ptr.To(int32(0))}), "routing.instanceCap"},
		{"a deadline that is not a duration", routing(v1alpha1.Routing{MaxRequestDuration: ptr.To(v1alpha1.Duration("30 minutes"))}), "routing.maxRequestDuration"},
		{"a tight bound above the loose one", bound(lf("2.5"), lf("2.0")), orderMsg},
		{"a tight bound above the default loose bound", bound(lf("3"), nil), orderMsg},
		{"a loose bound below the default tight bound", bound(nil, lf("1.1")), orderMsg},
	}, []mdCase{
		{"equal bounds", bound(lf("1.5"), lf("1.5")), ""},
		{"a tight bound of exactly 1", bound(lf("1"), nil), ""},
		{"bounds of 1.0 and 1.25", bound(lf("1.0"), lf("1.25")), ""},
		{"HBEP-5's defaults written out", bound(lf("1.25"), lf("2.0")), ""},
		{"a loose bound of 10.5", bound(nil, lf("10.5")), ""},
		{"a cap and a deadline", routing(v1alpha1.Routing{InstanceCap: ptr.To(int32(64)), MaxRequestDuration: ptr.To(v1alpha1.Duration("30m"))}), ""},
	})
}

// The drain margin and the per-partition image format (HBEP-4, Platform
// profile schema; owner ruling): sif on Helios GH200 and sandbox on Athena
// A100, measured by spike S1's probe 2. An absent format stays absent: the
// profile has not stated it.
func TestPlatformProfileImageFormat(t *testing.T) {
	p := newProfile(uniq("prof"))
	p.Spec.Partitions[0].ImageFormat = "squashfs"
	requireRefused(t, c.Create(bg(), p), "imageFormat")

	for _, f := range []v1alpha1.ImageFormat{v1alpha1.ImageFormatSIF, v1alpha1.ImageFormatSandbox} {
		p := newProfile(uniq("prof"))
		p.Spec.Partitions[0].ImageFormat = f
		must(t, c.Create(bg(), p), "create with imageFormat "+string(f))
		got := &v1alpha1.PlatformProfile{}
		must(t, c.Get(bg(), objKey(p), got), "get")
		if got.Spec.Partitions[0].ImageFormat != f || got.Spec.Partitions[1].ImageFormat != "" {
			t.Errorf("partitions = %+v, want %s on the first and none on the second", got.Spec.Partitions, f)
		}
	}
}

// A managed Provider's catalog lives in its status, one entry per name, with
// the managed ID in full form (HBEP-4, Managed provider in alpha).
func TestProviderCatalog(t *testing.T) {
	p := newProvider(uniq("prov"))
	must(t, c.Create(bg(), p), "create")
	cleanup(t, p)
	got := &v1alpha1.Provider{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	t0 := metav1.NewTime(time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC))
	got.Status.Catalog = []v1alpha1.CatalogEntryStatus{
		{Name: "example-model-27b", ManagedID: fullID, Listed: true, LastListedTime: &t0},
		{Name: "example-model-7b", Revision: ptr.To("r1"), ManagedID: v1alpha1.FullDeploymentID(strings.Repeat("e", 64)), Listed: false},
	}
	got.Status.LastCatalogRead = &t0
	must(t, c.Status().Update(bg(), got), "write the catalog")
	again := &v1alpha1.Provider{}
	must(t, c.Get(bg(), objKey(p), again), "get")
	if len(again.Status.Catalog) != 2 || !again.Status.Catalog[0].Listed || again.Status.Catalog[1].Listed ||
		again.Status.Catalog[1].Revision == nil || *again.Status.Catalog[1].Revision != "r1" || again.Status.LastCatalogRead == nil {
		t.Fatalf("catalog = %+v", again.Status)
	}

	short := again.DeepCopy()
	short.Status.Catalog[0].ManagedID = v1alpha1.FullDeploymentID(fullID[:12])
	requireRefused(t, c.Status().Update(bg(), short), "managedID")

	dup := again.DeepCopy()
	dup.Status.Catalog[1].Name = dup.Status.Catalog[0].Name
	requireRefused(t, c.Status().Update(bg(), dup), "catalog")
}

// Owner ruling: the lifecycle controller sets DialHomeUnreachable on a
// Provider by server-side apply, under its own field manager, and owns only
// that condition; the Provider's controller writes the rest of the status and
// neither removes the other's.
func TestDialHomeUnreachableByServerSideApply(t *testing.T) {
	p := newProvider(uniq("prov"))
	must(t, c.Create(bg(), p), "create")
	cleanup(t, p)
	t0 := metav1.NewTime(time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC))

	// The Provider's controller writes Ready.
	got := &v1alpha1.Provider{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	got.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: "Polled", Message: "read", LastTransitionTime: t0}}
	must(t, c.Status().Update(bg(), got, client.FieldOwner(v1alpha1.FieldManagerProvider)), "provider writes Ready")

	// The lifecycle controller applies its one condition.
	apply := func(status metav1.ConditionStatus, reason string) {
		t.Helper()
		u, err := v1alpha1.DialHomeUnreachableApply(p.Name, metav1.Condition{Status: status, Reason: reason, Message: "registration through the dial-home path", LastTransitionTime: t0})
		must(t, err, "build the apply")
		must(t, c.Status().Apply(bg(), client.ApplyConfigurationFromUnstructured(u),
			client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership), "lifecycle applies DialHomeUnreachable")
	}
	apply(metav1.ConditionTrue, "NoRegistration")

	// The Provider's controller rewrites its own condition from a fresh read.
	must(t, c.Get(bg(), objKey(p), got), "re-read")
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == v1alpha1.ConditionProviderReady {
			got.Status.Conditions[i].Status, got.Status.Conditions[i].Reason = metav1.ConditionFalse, "ReadFailed"
		}
	}
	must(t, c.Status().Update(bg(), got, client.FieldOwner(v1alpha1.FieldManagerProvider)), "provider rewrites Ready")

	// The lifecycle controller changes its condition; Ready stays as written.
	apply(metav1.ConditionFalse, "Registered")
	final := &v1alpha1.Provider{}
	must(t, c.Get(bg(), objKey(p), final), "get")
	want := map[string]string{v1alpha1.ConditionProviderReady: "False/ReadFailed", v1alpha1.ConditionDialHomeUnreachable: "False/Registered"}
	if len(final.Status.Conditions) != 2 {
		t.Fatalf("conditions = %+v, want exactly Ready and DialHomeUnreachable", final.Status.Conditions)
	}
	for _, cd := range final.Status.Conditions {
		if got := string(cd.Status) + "/" + cd.Reason; want[cd.Type] != got {
			t.Errorf("condition %s = %s, want %s", cd.Type, got, want[cd.Type])
		}
	}
	// The lifecycle controller's manager owns that one list entry and nothing
	// else of the status.
	for _, mf := range final.ManagedFields {
		if mf.Manager != v1alpha1.FieldManagerLifecycle {
			continue
		}
		owned := string(mf.FieldsV1.Raw)
		if !strings.Contains(owned, `"k:{\"type\":\"DialHomeUnreachable\"}"`) || strings.Contains(owned, `"k:{\"type\":\"Ready\"}"`) ||
			strings.Contains(owned, "queueWaits") || strings.Contains(owned, "lastSuccessfulStatusRead") {
			t.Errorf("the lifecycle controller owns %s; want only the DialHomeUnreachable condition", owned)
		}
	}
}

// The engine's image is pulled as <image>@<digest>, so a served spec without a
// digest is refused (owner ruling). The schema requires imageDigest; a CEL
// rule repeating that would be redundant and no test could kill it.
func TestEngineDigestIsRequired(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, uniq("md"))
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(md)
	must(t, err, "to unstructured")
	u := &unstructured.Unstructured{Object: raw}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("ModelDeployment")
	unstructured.RemoveNestedField(u.Object, "spec", "source", "served", "engine", "imageDigest")
	requireRefused(t, c.Create(bg(), u), "imageDigest", "Required value")

	ok := newModelDeployment(ns, uniq("md"))
	must(t, c.Create(bg(), ok), "a spec with an image and a digest")
}

// The launch records its --max-num-seqs, which every launch passes (HBEP-7,
// What the controller reads), and it is immutable with the rest of the spec.
// The lease end is not fixed: the controller copies a later hard end too.
func TestInstanceLaunchMaxNumSeqsAndLeaseEnd(t *testing.T) {
	ns := newNamespace(t)
	bad := newInstance(ns, uniq("inst"))
	bad.Spec.Launch.MaxNumSeqs = 0
	requireRefused(t, c.Create(bg(), bad), "launch.maxNumSeqs")

	in := newInstance(ns, uniq("inst"))
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Spec.Launch.MaxNumSeqs != 256 {
		t.Errorf("launch.maxNumSeqs = %d, want 256", got.Spec.Launch.MaxNumSeqs)
	}
	changed := got.DeepCopy()
	changed.Spec.Launch.MaxNumSeqs = 128
	requireRefused(t, c.Update(bg(), changed), "immutable")

	first := metav1.NewTime(time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC))
	later := metav1.NewTime(first.Add(6 * time.Hour))
	for _, end := range []metav1.Time{first, later} {
		must(t, c.Get(bg(), objKey(in), got), "get")
		got.Status.LeaseEnd = &end
		must(t, c.Status().Update(bg(), got), "write leaseEnd")
	}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Status.LeaseEnd == nil || !got.Status.LeaseEnd.Equal(&later) {
		t.Errorf("leaseEnd = %v, want the later hard end %v", got.Status.LeaseEnd, later)
	}
}
