package e2e

import (
	"reflect"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
		// providerConfigRef is required and has no default: crossplane's own default,
		// ClusterProviderConfig/default, names no Provider.
		{"no reference", nil, "providerConfigRef: Required value"},
		{"crossplane's ProviderConfig kind", &xpv2.ProviderConfigReference{Kind: "ProviderConfig", Name: "helios-a"},
			"providerConfigRef must name a Provider: set kind to 'Provider'"},
		{"the ClusterProviderConfig kind", &xpv2.ProviderConfigReference{Kind: "ClusterProviderConfig", Name: "helios-a"},
			"providerConfigRef must name a Provider: set kind to 'Provider'"},
		{"crossplane's default reference", &xpv2.ProviderConfigReference{Kind: "ClusterProviderConfig", Name: "default"},
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
		{"no partition", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.Partition = "" }},
		{"no time limit", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.TimeLimit = "" }},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			tc.mutate(a)
			requireRefused(t, c.Create(bg(), a), "forProvider")
		})
	}
}

// An Allocation is a lease on a batch job or on a managed catalog entry, never
// both and never neither (review finding B9): a catalog entry has no partition
// and no time limit, and none is invented for it.
func TestAllocationIsABatchJobOrACatalogEntry(t *testing.T) {
	const msg = "forProvider names exactly one of batch and managed"
	ns := newNamespace(t)
	batch := &v1alpha1.BatchParameters{Partition: "example-gpu", TimeLimit: "01:00:00"}
	entry := &v1alpha1.ManagedAllocation{CatalogName: "example-model-27b"}
	for _, tc := range []struct {
		name string
		fp   v1alpha1.AllocationParameters
		want string // refusals only
	}{
		{"neither a batch job nor a catalog entry", v1alpha1.AllocationParameters{}, msg},
		{"both a batch job and a catalog entry", v1alpha1.AllocationParameters{Batch: batch, Managed: entry}, msg},
		{"a catalog entry without a name", v1alpha1.AllocationParameters{Managed: &v1alpha1.ManagedAllocation{}}, "catalogName"},
		{"a batch job without a partition", v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{TimeLimit: "01:00:00"}}, "partition"},
		{"a batch job without a time limit", v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{Partition: "p"}}, "timeLimit"},
		{"a batch job", v1alpha1.AllocationParameters{Batch: batch}, ""},
		{"a catalog entry, with no partition and no time limit", v1alpha1.AllocationParameters{Managed: entry}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			a.Spec.ForProvider = tc.fp
			err := c.Create(bg(), a)
			if tc.want == "" {
				must(t, err, "create")
				return
			}
			requireRefused(t, err, tc.want)
		})
	}
	// A catalog entry's stored form has neither of the batch fields.
	a := newAllocation(ns, "managed-stored")
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Managed: entry}
	must(t, c.Create(bg(), a), "create a catalog entry")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	if got.Spec.ForProvider.Batch != nil || got.Spec.ForProvider.Managed == nil || got.Spec.ForProvider.Managed.CatalogName != "example-model-27b" {
		t.Fatalf("forProvider = %+v", got.Spec.ForProvider)
	}
}

// What an Allocation is a lease on cannot change: a batch job cannot become a
// catalog entry, and the entry cannot be renamed.
func TestAllocationKindOfLeaseIsImmutable(t *testing.T) {
	const msg = "an Allocation's submission parameters are immutable"
	ns := newNamespace(t)
	a := newAllocation(ns, "lease-kind")
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Managed: &v1alpha1.ManagedAllocation{CatalogName: "example-model-27b"}}
	must(t, c.Create(bg(), a), "create a catalog entry")
	get := func() *v1alpha1.Allocation {
		got := &v1alpha1.Allocation{}
		must(t, c.Get(bg(), objKey(a), got), "get")
		return got
	}
	t.Run("refuse renaming the catalog entry", func(t *testing.T) {
		got := get()
		got.Spec.ForProvider.Managed.CatalogName = "another-model"
		requireRefused(t, c.Update(bg(), got), msg)
	})
	t.Run("refuse turning it into a batch job", func(t *testing.T) {
		got := get()
		got.Spec.ForProvider = v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{Partition: "p", TimeLimit: "01:00:00"}}
		requireRefused(t, c.Update(bg(), got), msg)
	})
	t.Run("accept writing it back unchanged", func(t *testing.T) {
		must(t, c.Update(bg(), get()), "update")
	})
}

// An Allocation's submission parameters are immutable: a submitted job is never
// edited. The provider it names is immutable too (TestAllocationProviderIsImmutable).
// The management policies are immutable too (TestAllocationManagementPolicies).
// The rest may change, a repurpose included.
func TestAllocationSubmissionParametersAreImmutable(t *testing.T) {
	const msg = "an Allocation's submission parameters are immutable"
	ns := newNamespace(t)
	a := newAllocation(ns, "immutable")
	a.Spec.ForProvider.Batch.Account = "example-account"
	a.Spec.ForProvider.Batch.Wrap = "sleep 60"
	must(t, c.Create(bg(), a), "create")

	refused := map[string]func(*v1alpha1.Allocation){
		"the partition":        func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.Partition = "other" },
		"the time limit":       func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.TimeLimit = "02:00:00" },
		"the account":          func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.Account = "other-account" },
		"the command":          func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.Wrap = "sleep 61" },
		"removing the command": func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.Wrap = "" },
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
	if !reflect.DeepEqual(got.Spec.ForProvider, a.Spec.ForProvider) {
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

	// A policy with no mode is Keep: an idle limit is then refused, as with Keep.
	unset := newAllocation(ns, uniq("alloc"))
	unset.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{IdleLimit: ptr.To(v1alpha1.Duration("15m"))}
	requireRefused(t, c.Create(bg(), unset), "idleLimit is required with mode ReleaseWhenIdle and refused with mode Keep")
}

// A kept Allocation is kept until a date; one released when idle is not. The
// two do not go together (review nit), and an unset mode is Keep.
func TestAllocationRetentionUntilGoesWithKeep(t *testing.T) {
	const msg = "until is refused with mode ReleaseWhenIdle"
	ns := newNamespace(t)
	until := &metav1.Time{Time: time.Now().Add(24 * time.Hour)}
	for _, tc := range []struct {
		name string
		p    v1alpha1.RetentionPolicy
		want string
	}{
		{"until with ReleaseWhenIdle", v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("15m")), Until: until}, msg},
		{"until with Keep", v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: until}, ""},
		{"until with no mode, which is Keep", v1alpha1.RetentionPolicy{Until: until}, ""},
		{"ReleaseWhenIdle without until", v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("15m"))}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			a.Spec.RetentionPolicy = &tc.p
			err := c.Create(bg(), a)
			if tc.want == "" {
				must(t, err, "create")
				return
			}
			requireRefused(t, err, tc.want)
		})
	}
}

// The mode has no schema default: the stored object says nothing, and the
// controller reads an unset mode as Keep. A later change of that default then
// leaves no stale values behind.
func TestAllocationRetentionModeHasNoDefault(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{}
	must(t, c.Create(bg(), a), "create with an empty retention policy")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	if got.Spec.RetentionPolicy == nil || got.Spec.RetentionPolicy.Mode != "" {
		t.Fatalf("an empty retention policy came back as %+v; the schema must not write a mode", got.Spec.RetentionPolicy)
	}
	if got.Spec.RetentionPolicy.EffectiveMode() != v1alpha1.RetentionKeep {
		t.Errorf("the effective mode of an unset one is %q, want Keep", got.Spec.RetentionPolicy.EffectiveMode())
	}
}

// An Allocation takes exactly Observe, Create, Delete and LateInitialize, and
// the set never changes (review finding B1). crossplane-runtime's embedded
// default, "*", includes Update; [Observe] orphans the job when the Allocation
// is deleted; [] pauses the reconciler so a deletion hangs.
func TestAllocationManagementPolicies(t *testing.T) {
	const (
		exact     = "managementPolicies must be exactly Observe, Create, Delete and LateInitialize"
		immutable = "an Allocation's managementPolicies are immutable"
	)
	ns := newNamespace(t)
	obs, cre, del, late, upd := xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize, xpv2.ManagementActionUpdate
	pol := func(p ...xpv2.ManagementAction) func(*v1alpha1.Allocation) {
		return func(a *v1alpha1.Allocation) { a.Spec.ManagementPolicies = p }
	}
	// asEmptyList writes managementPolicies as an explicit [], which a typed
	// client cannot do: an empty slice is omitted, and the omitted field takes the default.
	create := func(t *testing.T, a *v1alpha1.Allocation, explicit []any) error {
		t.Helper()
		if explicit == nil {
			return c.Create(bg(), a)
		}
		return c.Create(bg(), unstructuredWithPolicies(t, a, explicit))
	}
	refused := []struct {
		name     string
		mutate   func(*v1alpha1.Allocation)
		explicit []any
	}{
		{"[Update]", pol(upd), nil},
		{"[Observe]", pol(obs), nil},
		{"[*]", pol("*"), nil},
		{"[] written explicitly", nil, []any{}},
		{"the set without LateInitialize", pol(obs, cre, del), nil},
		{"the set with Update added", pol(obs, cre, del, late, upd), nil},
		{"four entries with a repeat and a gap", pol(obs, obs, cre, del), nil},
		{"Observe, Create, Update, LateInitialize", pol(obs, cre, upd, late), nil},
	}
	for _, tc := range refused {
		t.Run("refuse on create "+tc.name, func(t *testing.T) {
			a := newAllocation(ns, uniq("alloc"))
			if tc.mutate != nil {
				tc.mutate(a)
			}
			requireRefused(t, create(t, a, tc.explicit), exact)
		})
	}
	t.Run("accept an omitted value, which takes the default", func(t *testing.T) {
		a := newAllocation(ns, uniq("alloc"))
		must(t, c.Create(bg(), a), "create")
		got := &v1alpha1.Allocation{}
		must(t, c.Get(bg(), objKey(a), got), "get")
		if !reflect.DeepEqual(got.Spec.ManagementPolicies, v1alpha1.DefaultManagementPolicies()) {
			t.Fatalf("managementPolicies defaulted to %v, want %v", got.Spec.ManagementPolicies, v1alpha1.DefaultManagementPolicies())
		}
		// The order is S4's: the provider registers exactly this set.
		want := []xpv2.ManagementAction{"Observe", "Create", "Delete", "LateInitialize"}
		if !reflect.DeepEqual([]xpv2.ManagementAction(got.Spec.ManagementPolicies), want) {
			t.Fatalf("managementPolicies = %v, want %v", got.Spec.ManagementPolicies, want)
		}
	})
	t.Run("accept the set written out", func(t *testing.T) {
		a := newAllocation(ns, uniq("alloc"))
		pol(obs, cre, del, late)(a)
		must(t, c.Create(bg(), a), "create")
	})
	t.Run("accept the same set in another order", func(t *testing.T) {
		a := newAllocation(ns, uniq("alloc"))
		pol(late, del, cre, obs)(a)
		must(t, c.Create(bg(), a), "create")
	})

	// On update: the set never changes, nor its order.
	live := newAllocation(ns, "policies-live")
	must(t, c.Create(bg(), live), "create the live Allocation")
	get := func() *v1alpha1.Allocation {
		got := &v1alpha1.Allocation{}
		must(t, c.Get(bg(), objKey(live), got), "get")
		return got
	}
	updates := []struct {
		name string
		set  []xpv2.ManagementAction
		want string
	}{
		{"[Update]", []xpv2.ManagementAction{upd}, exact},
		{"[Observe], which would orphan the job on deletion", []xpv2.ManagementAction{obs}, exact},
		{"the set with Update added", []xpv2.ManagementAction{obs, cre, del, late, upd}, exact},
		{"[*]", []xpv2.ManagementAction{"*"}, exact},
		{"the same set in another order", []xpv2.ManagementAction{late, del, cre, obs}, immutable},
	}
	for _, tc := range updates {
		t.Run("refuse on update "+tc.name, func(t *testing.T) {
			got := get()
			got.Spec.ManagementPolicies = tc.set
			requireRefused(t, c.Update(bg(), got), tc.want)
		})
	}
	t.Run("refuse on update [] written explicitly, which would pause the reconciler", func(t *testing.T) {
		got := get()
		requireRefused(t, c.Update(bg(), unstructuredWithPolicies(t, got, []any{})), exact)
	})
	t.Run("accept on update the object written back unchanged", func(t *testing.T) {
		must(t, c.Update(bg(), get()), "update")
	})
	t.Run("accept on update a value left out, which takes the default", func(t *testing.T) {
		got := get()
		got.Spec.ManagementPolicies = nil
		must(t, c.Update(bg(), got), "update")
		if !reflect.DeepEqual(get().Spec.ManagementPolicies, v1alpha1.DefaultManagementPolicies()) {
			t.Fatalf("managementPolicies = %v after an update that left it out", get().Spec.ManagementPolicies)
		}
	})
	if !reflect.DeepEqual(get().Spec.ManagementPolicies, v1alpha1.DefaultManagementPolicies()) {
		t.Fatalf("the refused updates changed the policies: %v", get().Spec.ManagementPolicies)
	}
}

// unstructuredWithPolicies returns a with spec.managementPolicies set to the
// given list as written, [] included.
func unstructuredWithPolicies(t *testing.T, a *v1alpha1.Allocation, policies []any) *unstructured.Unstructured {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(a)
	must(t, err, "convert")
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Allocation"))
	must(t, unstructured.SetNestedSlice(u.Object, policies, "spec", "managementPolicies"), "set managementPolicies")
	return u
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
