package e2e

import (
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestAllocationNamesAProvider(t *testing.T) {
	ns := newNamespace(t)
	refs := []struct {
		name string
		ref  *xpv2.ProviderConfigReference
		want string // refusals only
	}{
		{"no reference (crossplane defaults to ClusterProviderConfig/default)", nil,
			"providerConfigRef must name a Provider: set kind to 'Provider'"},
		{"crossplane's ProviderConfig kind", &xpv2.ProviderConfigReference{Kind: "ProviderConfig", Name: "helios-a"},
			"providerConfigRef must name a Provider: set kind to 'Provider'"},
		{"the ClusterProviderConfig kind", &xpv2.ProviderConfigReference{Kind: "ClusterProviderConfig", Name: "helios-a"},
			"providerConfigRef must name a Provider: set kind to 'Provider'"},
		{"a name with a dot", &xpv2.ProviderConfigReference{Kind: "Provider", Name: "helios.a"},
			"providerConfigRef.name must be a Provider name, a DNS-1123 label"},
		{"a name with uppercase letters", &xpv2.ProviderConfigReference{Kind: "Provider", Name: "Helios-a"},
			"providerConfigRef.name must be a Provider name, a DNS-1123 label"},
		{"a name of 64 characters", &xpv2.ProviderConfigReference{Kind: "Provider", Name: strings.Repeat("a", 64)},
			"providerConfigRef.name must be a Provider name, a DNS-1123 label"},
		{"a Provider by a valid name", &xpv2.ProviderConfigReference{Kind: "Provider", Name: "helios-a"}, ""},
		{"a Provider with a 63-character name", &xpv2.ProviderConfigReference{Kind: "Provider", Name: strings.Repeat("a", 63)}, ""},
	}
	for _, tc := range refs {
		t.Run(tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			a.Spec.ProviderConfigReference = tc.ref
			err := c.Create(bg(), a)
			if tc.want == "" {
				must(t, err, "create Allocation")
				return
			}
			requireRefused(t, err, tc.want)
		})
	}
}

func TestAllocationSubmissionParameters(t *testing.T) {
	ns := newNamespace(t)
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.Allocation)
	}{
		{"no partition", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Partition = "" }},
		{"no time limit", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.TimeLimit = "" }},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			tc.mutate(a)
			requireRefused(t, c.Create(bg(), a), "forProvider")
		})
	}
}

// An Allocation's submission parameters are immutable: a submitted job is never
// edited. The provider it names is immutable too (TestAllocationProviderIsImmutable).
// The rest may change, a repurpose included.
func TestAllocationSubmissionParametersAreImmutable(t *testing.T) {
	const msg = "an Allocation's submission parameters are immutable"
	ns := newNamespace(t)
	a := newAllocation(ns, "immutable")
	a.Spec.ForProvider.Account = "example-account"
	a.Spec.ForProvider.Wrap = "sleep 60"
	must(t, c.Create(bg(), a), "create")

	refused := map[string]func(*v1alpha1.Allocation){
		"the partition":        func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Partition = "other" },
		"the time limit":       func(a *v1alpha1.Allocation) { a.Spec.ForProvider.TimeLimit = "02:00:00" },
		"the account":          func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Account = "other-account" },
		"the command":          func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Wrap = "sleep 61" },
		"removing the command": func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Wrap = "" },
	}
	for what, edit := range refused {
		t.Run("refuse changing "+what, func(t *testing.T) {
			got := &v1alpha1.Allocation{}
			must(t, c.Get(bg(), objKey(a), got), "get")
			edit(got)
			requireRefused(t, c.Update(bg(), got), msg)
		})
	}

	accepted := map[string]func(*v1alpha1.Allocation){
		"a label":                 func(a *v1alpha1.Allocation) { a.Labels = map[string]string{v1alpha1.LabelProvider: "helios-a"} },
		"an annotation":           func(a *v1alpha1.Allocation) { a.Annotations = map[string]string{v1alpha1.AnnotationJobID: "12345"} },
		"the assigned deployment": func(a *v1alpha1.Allocation) { a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "md-a"} },
		"a repurpose":             func(a *v1alpha1.Allocation) { a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "md-b"} },
		"the retention policy": func(a *v1alpha1.Allocation) {
			a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}
		},
		"a finalizer": func(a *v1alpha1.Allocation) { a.Finalizers = []string{v1alpha1.FinalizerCancelAll} },
		"the management policies": func(a *v1alpha1.Allocation) {
			a.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete}
		},
	}
	for what, edit := range accepted {
		t.Run("accept changing "+what, func(t *testing.T) {
			got := &v1alpha1.Allocation{}
			must(t, c.Get(bg(), objKey(a), got), "get")
			edit(got)
			must(t, c.Update(bg(), got), "update")
		})
	}
	// The accepted edits left the submission parameters as created.
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	if got.Spec.ForProvider != a.Spec.ForProvider {
		t.Fatalf("submission parameters changed: %+v -> %+v", a.Spec.ForProvider, got.Spec.ForProvider)
	}
	// An update that leaves the parameters alone, writing the same object back, is accepted.
	must(t, c.Update(bg(), got), "write the object back unchanged")
}

// The provider that holds the job is part of the job's identity: moving a live
// Allocation to another provider would leave the job running on the first while
// the second's manager observes and cancels on the wrong platform (HBEP-3, an
// Allocation is one lease on a provider; HBEP-11, one manager per provider).
func TestAllocationProviderIsImmutable(t *testing.T) {
	const msg = "an Allocation's providerConfigRef is immutable"
	ns := newNamespace(t)
	a := newAllocation(ns, "pinned") // names helios-a
	a.Annotations = map[string]string{v1alpha1.AnnotationJobID: "12345"}
	must(t, c.Create(bg(), a), "create")

	get := func() *v1alpha1.Allocation {
		got := &v1alpha1.Allocation{}
		must(t, c.Get(bg(), objKey(a), got), "get")
		return got
	}
	refused := map[string]func(*v1alpha1.Allocation){
		"moving from helios-a to athena-a": func(a *v1alpha1.Allocation) { a.Spec.ProviderConfigReference.Name = "athena-a" },
		"moving to a name that differs by one letter": func(a *v1alpha1.Allocation) {
			a.Spec.ProviderConfigReference.Name = "helios-b"
		},
		"moving together with a valid repurpose": func(a *v1alpha1.Allocation) {
			a.Spec.ProviderConfigReference.Name = "athena-a"
			a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "md-a"}
		},
	}
	for what, edit := range refused {
		t.Run("refuse "+what, func(t *testing.T) {
			got := get()
			edit(got)
			requireRefused(t, c.Update(bg(), got), msg)
		})
	}
	if got := get().Spec.ProviderConfigReference; got == nil || got.Name != "helios-a" || got.Kind != v1alpha1.ProviderConfigKind {
		t.Fatalf("the refused moves changed the reference: %+v", got)
	}

	accepted := map[string]func(*v1alpha1.Allocation){
		"the reference written back unchanged": func(*v1alpha1.Allocation) {},
		"the same reference spelled out again": func(a *v1alpha1.Allocation) {
			a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "helios-a"}
		},
		"a repurpose, the reference unchanged": func(a *v1alpha1.Allocation) {
			a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "md-b"}
		},
		"a retention change, the reference unchanged": func(a *v1alpha1.Allocation) {
			a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}
		},
	}
	for what, edit := range accepted {
		t.Run("accept "+what, func(t *testing.T) {
			got := get()
			edit(got)
			must(t, c.Update(bg(), got), "update")
		})
	}
}

func TestAllocationRetentionPolicyRule(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle}
	requireRefused(t, c.Create(bg(), a), "idleLimit is required with mode ReleaseWhenIdle")
	a.Spec.RetentionPolicy.IdleLimit = ptr.To(v1alpha1.Duration("15m"))
	must(t, c.Create(bg(), a), "release when idle with a limit")
}

func TestAllocationStatusMirrorsTheRecord(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	must(t, c.Create(bg(), a), "create")
	a.Status.AtProvider = v1alpha1.AllocationObservation{
		Tag: "hb-helios-a-abcde-0123456789ab", JobID: "424242", Phase: "Running",
		Submission: &v1alpha1.SubmissionRecord{Attempt: 2, State: "Done", Token: "t", Writer: "controller"},
		Duplicates: []string{"424243"},
	}
	must(t, c.Status().Update(bg(), a), "write the status")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	if got.Status.AtProvider.JobID != "424242" || got.Status.AtProvider.Submission == nil || got.Status.AtProvider.Submission.Attempt != 2 {
		t.Fatalf("status.atProvider = %+v", got.Status.AtProvider)
	}
	// The main resource ignores status; only the subresource writes it.
	got.Status.AtProvider.JobID = "999"
	must(t, c.Update(bg(), got), "update with a changed status")
	again := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), again), "get")
	if again.Status.AtProvider.JobID != "424242" {
		t.Fatalf("the main resource wrote the status: jobID = %s", again.Status.AtProvider.JobID)
	}

	// On the wire the key is jobID, the Kubernetes spelling of an ID, as on an Instance.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Allocation"))
	must(t, c.Get(bg(), objKey(a), u), "get unstructured")
	obs, _, _ := unstructured.NestedMap(u.Object, "status", "atProvider")
	if obs["jobID"] != "424242" {
		t.Errorf("status.atProvider.jobID = %v; keys %v", obs["jobID"], obs)
	}
	if _, wrong := obs["jobId"]; wrong {
		t.Errorf("status.atProvider.jobId is written: %v", obs)
	}
}

// A lease runs from a start estimate, which may be unknown, to a hard end
// (HBEP-3, Vocabulary). The estimate is its own field: it is not the actual
// start, and "unknown" is the absence of the field.
func TestAllocationStatusKeepsTheStartEstimate(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	must(t, c.Create(bg(), a), "create")
	get := func() *v1alpha1.AllocationObservation {
		got := &v1alpha1.Allocation{}
		must(t, c.Get(bg(), objKey(a), got), "get")
		return &got.Status.AtProvider
	}
	if obs := get(); obs.StartEstimate != nil || obs.StartTime != nil {
		t.Fatalf("a new Allocation has an estimate or a start: %+v", obs)
	}

	// Unknown from the start, with the actual start unset: nothing is invented.
	a.Status.AtProvider = v1alpha1.AllocationObservation{Phase: "Pending"}
	must(t, c.Status().Update(bg(), a), "write a status without an estimate")
	if obs := get(); obs.StartEstimate != nil || obs.StartTime != nil || obs.Phase != "Pending" {
		t.Fatalf("an unknown estimate came back as %+v", obs)
	}

	// Known. Kubernetes keeps times to the second.
	est := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	a.Status.AtProvider.StartEstimate = &metav1.Time{Time: est}
	must(t, c.Status().Update(bg(), a), "write a known estimate")
	obs := get()
	if obs.StartEstimate == nil || !obs.StartEstimate.Time.Equal(est) {
		t.Fatalf("startEstimate = %v, want %v", obs.StartEstimate, est)
	}
	if obs.StartTime != nil {
		t.Fatalf("writing the estimate set startTime: %v", obs.StartTime)
	}

	// The estimate moves, and the actual start is a different time and a different field.
	moved, start := est.Add(3*time.Hour), est.Add(-time.Hour)
	a.Status.AtProvider.StartEstimate = &metav1.Time{Time: moved}
	a.Status.AtProvider.StartTime = &metav1.Time{Time: start}
	must(t, c.Status().Update(bg(), a), "move the estimate and set the start")
	obs = get()
	if obs.StartEstimate == nil || !obs.StartEstimate.Time.Equal(moved) || obs.StartTime == nil || !obs.StartTime.Time.Equal(start) {
		t.Fatalf("estimate %v, start %v; want %v and %v", obs.StartEstimate, obs.StartTime, moved, start)
	}

	// The estimate becomes unknown again (the scheduler went "unknown", HBEP-7):
	// it is cleared, and the start stays.
	a.Status.AtProvider.StartEstimate = nil
	must(t, c.Status().Update(bg(), a), "make the estimate unknown")
	obs = get()
	if obs.StartEstimate != nil {
		t.Fatalf("an unknown estimate kept its old value %v", obs.StartEstimate)
	}
	if obs.StartTime == nil || !obs.StartTime.Time.Equal(start) {
		t.Fatalf("clearing the estimate changed startTime: %v", obs.StartTime)
	}
}
