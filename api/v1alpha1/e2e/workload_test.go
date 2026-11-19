package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The Workload kind and the fields around it (HBEP-764, PR 1; HBEP-3, Grammar; HBEP-7; the science agent's
// rulings of 18 Nov 2026 on #764, Q2, Q3, Q4 and Q12). Each rule test below names the rule it covers in
// hack/mutate-cel.sh. Every refusal asserts the rule's own message, so a test cannot pass because another
// rule or the schema refused first.

const (
	workloadNameMessage  = "a Workload name must be a DNS-1123 label"
	modelHashMessage     = "a model name never contains '#'"
	workloadsOnceMsg     = "spec.workloads lists each Workload once"
	managedFirstMsg      = "routeOrder ManagedFirst needs managedSessions"
	pinTTLMessage        = "pinTTL must be from 1h to 7 days (168h)"
	modelImmutableMsg    = "a Workload's model is immutable"
	affinityImmutableMsg = "a Workload's sessionAffinity is immutable"
)

// newWorkload returns a Workload that admission accepts: its required fields only.
func newWorkload(ns, name string) *v1alpha1.Workload {
	return &v1alpha1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.WorkloadSpec{
			Model: "example-27b",
			Shape: v1alpha1.WorkloadShape{
				ContextTokens: v1alpha1.ContextTokens{Typical: 8192},
				OutputTokens:  v1alpha1.OutputTokens{Typical: 1024},
			},
		},
	}
}

type workloadCase struct {
	name string
	edit func(*v1alpha1.Workload)
	want string // refusals only: the rule's message
}

func runWorkloadCases(t *testing.T, refused, accepted []workloadCase) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			w := newWorkload(ns, uniq("study"))
			tc.edit(w)
			requireRefused(t, c.Create(bg(), w), tc.want)
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			w := newWorkload(ns, uniq("study"))
			tc.edit(w)
			must(t, c.Create(bg(), w), "create Workload")
		})
	}
}

// Envtest applies the CRD's defaults, so the defaults are read back from the API server, never from a Go
// struct: routeOrder OwnFirst (HBEP-5, Workloads), and the shape's own (contextTokens.short 512 and
// outputTokens.longest 4096, which the shape shares with a ModelDeployment's workload block).
func TestWorkloadReadsBackItsDefaults(t *testing.T) {
	ns := newNamespace(t)
	w := newWorkload(ns, uniq("study"))
	if w.Spec.RouteOrder != "" {
		t.Fatalf("the fixture sets routeOrder %q: the test needs it absent", w.Spec.RouteOrder)
	}
	must(t, c.Create(bg(), w), "create a Workload without routeOrder")
	got := &v1alpha1.Workload{}
	must(t, c.Get(bg(), objKey(w), got), "get")
	if got.Spec.RouteOrder != v1alpha1.RouteOwnFirst {
		t.Errorf("routeOrder read back as %q, want %q", got.Spec.RouteOrder, v1alpha1.RouteOwnFirst)
	}
	if got.Spec.Shape.ContextTokens.Short == nil || *got.Spec.Shape.ContextTokens.Short != 512 {
		t.Errorf("shape.contextTokens.short read back as %v, want the default 512", got.Spec.Shape.ContextTokens.Short)
	}
	if got.Spec.Shape.OutputTokens.Longest == nil || *got.Spec.Shape.OutputTokens.Longest != 4096 {
		t.Errorf("shape.outputTokens.longest read back as %v, want the default 4096", got.Spec.Shape.OutputTokens.Longest)
	}
	if got.Spec.Shape.ContextTokens.Longest != nil {
		t.Errorf("shape.contextTokens.longest read back as %d, want it absent: it has no default", *got.Spec.Shape.ContextTokens.Longest)
	}
	if got.Spec.Sessions != nil || got.Spec.ManagedSessions != nil {
		t.Errorf("sessions %v and managedSessions %v read back, want both absent", got.Spec.Sessions, got.Spec.ManagedSessions)
	}
}

// The enum admits OwnFirst and ManagedFirst (HBEP-764, PR 5: the front routes both; Q12). An unknown order is
// refused as well, and so is a wrong-cased one. This is the schema's enum, not a CEL rule, so the cases that
// are refused carry managedSessions, which keeps the spec rule below out of their message.
func TestWorkloadRouteOrderAdmitsOwnFirstAndManagedFirst(t *testing.T) {
	order := func(o v1alpha1.RouteOrder) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) {
			w.Spec.RouteOrder = o
			w.Spec.ManagedSessions = ptr.To[int32](4)
		}
	}
	runWorkloadCases(t, []workloadCase{
		{"an unknown order", order("Elsewhere"), "Unsupported value: \"Elsewhere\""},
		{"another unknown order", order("Anywhere"), "Unsupported value: \"Anywhere\""},
		{"a lowercase order", order("ownfirst"), "Unsupported value: \"ownfirst\""},
		{"a lowercase ManagedFirst", order("managedfirst"), "Unsupported value: \"managedfirst\""},
	}, []workloadCase{
		{"OwnFirst", func(w *v1alpha1.Workload) { w.Spec.RouteOrder = v1alpha1.RouteOwnFirst }, ""},
		{"ManagedFirst with managedSessions", order(v1alpha1.RouteManagedFirst), ""},
	})
}

// ManagedFirst needs managedSessions (HBEP-5, Workloads, Route order), and managedSessions is allowed with
// OwnFirst too, where it bounds the overflow. This is the rule workloads:$.spec#0, whose mutant admits a
// ManagedFirst Workload with no managedSessions. The cases name no other invalid field, so the spec rule is the
// only refusal.
func TestWorkloadManagedFirstNeedsManagedSessions(t *testing.T) {
	runWorkloadCases(t, []workloadCase{
		{"ManagedFirst without managedSessions", func(w *v1alpha1.Workload) { w.Spec.RouteOrder = v1alpha1.RouteManagedFirst }, managedFirstMsg},
		{"ManagedFirst with only sessions", func(w *v1alpha1.Workload) {
			w.Spec.RouteOrder = v1alpha1.RouteManagedFirst
			w.Spec.Sessions = ptr.To[int32](16)
		}, managedFirstMsg},
	}, []workloadCase{
		{"ManagedFirst with managedSessions 4", func(w *v1alpha1.Workload) {
			w.Spec.RouteOrder = v1alpha1.RouteManagedFirst
			w.Spec.ManagedSessions = ptr.To[int32](4)
		}, ""},
		{"OwnFirst with managedSessions 2", func(w *v1alpha1.Workload) {
			w.Spec.RouteOrder = v1alpha1.RouteOwnFirst
			w.Spec.ManagedSessions = ptr.To[int32](2)
		}, ""},
		{"OwnFirst without managedSessions", func(w *v1alpha1.Workload) { w.Spec.RouteOrder = v1alpha1.RouteOwnFirst }, ""},
		{"the default order with managedSessions 8", func(w *v1alpha1.Workload) { w.Spec.ManagedSessions = ptr.To[int32](8) }, ""},
		{"no routeOrder and no managedSessions", func(*v1alpha1.Workload) {}, ""},
		{"sessions without managedSessions", func(w *v1alpha1.Workload) { w.Spec.Sessions = ptr.To[int32](16) }, ""},
	})
	// The bounds of the field itself, unchanged: 0 and 1025 are refused by the schema, with ManagedFirst and with OwnFirst.
	ns := newNamespace(t)
	for _, order := range []v1alpha1.RouteOrder{v1alpha1.RouteManagedFirst, v1alpha1.RouteOwnFirst} {
		for _, n := range []int32{0, 1025} {
			w := newWorkload(ns, uniq("study"))
			w.Spec.RouteOrder = order
			w.Spec.ManagedSessions = ptr.To(n)
			requireRefused(t, c.Create(bg(), w), "spec.managedSessions")
		}
	}
}

// sessions is optional and at most 1024 (Q2's bounds).
func TestWorkloadSessionsAreBetween1And1024(t *testing.T) {
	sessions := func(n int32) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) { w.Spec.Sessions = ptr.To(n) }
	}
	runWorkloadCases(t, []workloadCase{
		{"0 sessions", sessions(0), "spec.sessions"},
		{"1025 sessions", sessions(1025), "spec.sessions"},
		{"-1 sessions", sessions(-1), "spec.sessions"},
	}, []workloadCase{
		{"1 session", sessions(1), ""},
		{"1024 sessions", sessions(1024), ""},
	})
}

// '#' separates a Workload in a request's model name (HBEP-3, Grammar), so no model name contains it. This is
// the rule workloads:$.spec.model#0; the ModelDeployment's twin is below.
func TestWorkloadModelNeverContainsAHash(t *testing.T) {
	model := func(m string) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) { w.Spec.Model = m }
	}
	runWorkloadCases(t, []workloadCase{
		{"a hash inside the name", model("example-27b#study-a"), modelHashMessage},
		{"a hash at the start", model("#example-27b"), modelHashMessage},
		{"a hash alone", model("#"), modelHashMessage},
		{"two hashes", model("example#27b#study"), modelHashMessage},
		{"a hash after a slash", model("example-org/example-27b#study-a"), modelHashMessage},
		{"an empty name", model(""), "spec.model"},
		{"a name of 257 characters", model(strings.Repeat("m", 257)), "spec.model"},
	}, []workloadCase{
		{"a plain name", model("example-27b"), ""},
		{"a name with a slash, as LLMLab's have", model("example-org/example-27b"), ""},
		{"a name of 256 characters", model(strings.Repeat("m", 256)), ""},
	})
}

// The same rule on a ModelDeployment's spec.model: this is the rule modeldeployments:$.spec.model#0. A
// pre-existing value stays updatable, since the cluster's k3s ratchets a rule on a field an update does not
// change; no shipped example or LLMLab name has a '#'.
func TestModelDeploymentModelNeverContainsAHash(t *testing.T) {
	model := func(m string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Model = m }
	}
	runMDCases(t, []mdCase{
		{"a hash inside the name", model("example-27b#study-a"), modelHashMessage},
		{"a hash at the start", model("#example-27b"), modelHashMessage},
		{"a hash alone", model("#"), modelHashMessage},
		{"two hashes", model("example#27b#study"), modelHashMessage},
		{"a hash after a slash", model("example-org/example-27b#study-a"), modelHashMessage},
	}, []mdCase{
		{"a plain name", model("example-27b"), ""},
		{"a name with a slash", model("example-org/example-27b"), ""},
	})
}

// A Workload's name is a DNS-1123 label, since a request names it after '#' (HBEP-3, Grammar). The API
// server's own check already refuses uppercase letters, '_' and a leading or trailing '-', so only a name
// with '.' or one of 64 characters shows that the rule itself works. This is the rule workloads:$#0.
func TestWorkloadNameIsADNS1123Label(t *testing.T) {
	ns := newNamespace(t)
	refused := map[string]string{
		"dot":         "study.a",
		"dots":        "study.a.b",
		"64 chars":    strings.Repeat("a", 64),
		"64 digits":   strings.Repeat("1", 64),
		"label+dot63": strings.Repeat("a", 30) + "." + strings.Repeat("b", 30),
	}
	for what, name := range refused {
		t.Run("refuse "+what, func(t *testing.T) {
			requireRefused(t, c.Create(bg(), newWorkload(ns, name)), workloadNameMessage)
		})
	}
	// Names the API server refuses first must still be refused.
	for _, name := range []string{"Study-a", "study@a", "study/a", "study_a", "-study", "study-"} {
		t.Run("refuse "+name, func(t *testing.T) {
			if err := c.Create(bg(), newWorkload(ns, name)); err == nil {
				t.Fatalf("Workload %q was accepted", name)
			}
		})
	}
	accepted := map[string]string{
		"simple":     uniq("study-a"),
		"digits":     uniq("s1"),
		"63 chars":   "a" + strings.Repeat("b", 61) + "c",
		"single":     "x",
		"hyphenated": uniq("a-b-c"),
	}
	for what, name := range accepted {
		t.Run("accept "+what, func(t *testing.T) {
			must(t, c.Create(bg(), newWorkload(ns, name)), "create Workload "+name)
		})
	}
}

// A Workload's shape is required, with its context and output, each with its typical length (Q3): the
// charge reads the two typicals. These are the schema's "required"; Go zero values would reach the minimum
// instead, so the fields are removed from the object as it is sent.
func TestWorkloadShapeIsRequired(t *testing.T) {
	ns := newNamespace(t)
	without := func(path ...string) func(*unstructured.Unstructured) {
		return func(u *unstructured.Unstructured) { unstructured.RemoveNestedField(u.Object, path...) }
	}
	for _, tc := range []struct {
		name string
		edit func(*unstructured.Unstructured)
		want string
	}{
		{"no model", without("spec", "model"), "spec.model: Required value"},
		{"no shape", without("spec", "shape"), "spec.shape: Required value"},
		{"no context tokens", without("spec", "shape", "contextTokens"), "spec.shape.contextTokens: Required value"},
		{"no typical context", without("spec", "shape", "contextTokens", "typical"), "spec.shape.contextTokens.typical: Required value"},
		{"no output tokens", without("spec", "shape", "outputTokens"), "spec.shape.outputTokens: Required value"},
		{"no typical output", without("spec", "shape", "outputTokens", "typical"), "spec.shape.outputTokens.typical: Required value"},
		{"no spec", without("spec"), "spec: Required value"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newWorkload(ns, uniq("study")))
			must(t, err, "to unstructured")
			u := &unstructured.Unstructured{Object: m}
			u.SetAPIVersion(v1alpha1.GroupVersion.String())
			u.SetKind("Workload")
			tc.edit(u)
			requireRefused(t, c.Create(bg(), u), tc.want)
		})
	}
	// The typical lengths are at least 1, like a ModelDeployment's block (the same types).
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.Workload)
		want string
	}{
		{"a typical context of 0", func(w *v1alpha1.Workload) { w.Spec.Shape.ContextTokens.Typical = 0 }, "contextTokens.typical"},
		{"a typical output of 0", func(w *v1alpha1.Workload) { w.Spec.Shape.OutputTokens.Typical = 0 }, "outputTokens.typical"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			w := newWorkload(ns, uniq("study"))
			tc.edit(w)
			requireRefused(t, c.Create(bg(), w), tc.want)
		})
	}
}

// The shape reuses a ModelDeployment's contextTokens type, rule included: the typical context is at most the
// longest, when both are set. This is the rule workloads:$.spec.shape.contextTokens#0, the second use of the
// type's rule (the first is modeldeployments:$.spec.workload.contextTokens#0).
func TestWorkloadShapeTypicalContextIsAtMostLongest(t *testing.T) {
	const msg = "contextTokens.typical must not exceed contextTokens.longest"
	ctx := func(typical int32, longest *int32) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) {
			w.Spec.Shape.ContextTokens.Typical = typical
			w.Spec.Shape.ContextTokens.Longest = longest
		}
	}
	runWorkloadCases(t, []workloadCase{
		{"a typical context above the longest", ctx(9000, ptr.To[int32](8000)), msg},
		{"a typical context one above the longest", ctx(8193, ptr.To[int32](8192)), msg},
	}, []workloadCase{
		{"a typical context equal to the longest", ctx(8192, ptr.To[int32](8192)), ""},
		{"a typical context below the longest", ctx(8192, ptr.To[int32](30000)), ""},
		{"no longest context", ctx(8192, nil), ""},
	})
}

// spec.workloads names Workloads of the ModelDeployment's namespace (HBEP-7). The list is atomic (Q2): each
// name once by a CEL rule, and its order is kept. This is the rule modeldeployments:$.spec.workloads#0.
func TestModelDeploymentWorkloadsListsEachOnce(t *testing.T) {
	names := func(n ...string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Workloads = n }
	}
	runMDCases(t, []mdCase{
		{"a name listed twice in a row", names("study-a", "study-a"), workloadsOnceMsg},
		{"a name listed twice with another between", names("study-a", "study-b", "study-a"), workloadsOnceMsg},
		{"a repeated name among three", names("study-a", "study-b", "study-b"), workloadsOnceMsg},
		{"a name listed three times", names("study-a", "study-a", "study-a"), workloadsOnceMsg},
	}, []mdCase{
		{"no list", names(), ""},
		{"one name", names("study-a"), ""},
		{"two different names", names("study-a", "study-b"), ""},
	})

	// The order is kept: study-b is listed first and stays first (a list of type atomic is replaced whole).
	ns := newNamespace(t)
	for _, want := range [][]string{{"study-b", "study-a"}, {"study-c", "study-a", "study-b"}} {
		md := newModelDeployment(ns, uniq("md"))
		md.Spec.Workloads = want
		must(t, c.Create(bg(), md), "create")
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		if !slices.Equal(got.Spec.Workloads, want) {
			t.Errorf("spec.workloads = %v, want %v in that order", got.Spec.Workloads, want)
		}
	}
}

// The items of spec.workloads are DNS-1123 labels and the list has at most 64 of them (Q2's bounds). These are
// the schema's Pattern, MaxLength and MaxItems, not CEL rules.
func TestModelDeploymentWorkloadsItemsAreDNS1123LabelsAndAtMost64(t *testing.T) {
	names := func(n ...string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Workloads = n }
	}
	distinct := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("study-%d", i)
		}
		return out
	}
	runMDCases(t, []mdCase{
		{"an uppercase item", names("Study-A"), "spec.workloads[0]"},
		{"an item with a dot", names("study.a"), "spec.workloads[0]"},
		{"an item with an underscore", names("study_a"), "spec.workloads[0]"},
		{"an item that starts with a hyphen", names("-study"), "spec.workloads[0]"},
		{"an item that ends with a hyphen", names("study-"), "spec.workloads[0]"},
		{"an item with a hash", names("study#a"), "spec.workloads[0]"},
		{"an empty item", names(""), "spec.workloads[0]"},
		{"a second item that is not a label", names("study-a", "study b"), "spec.workloads[1]"},
		{"an item of 64 characters", names(strings.Repeat("a", 64)), "spec.workloads[0]"},
		{"65 distinct items", names(distinct(65)...), "spec.workloads"},
	}, []mdCase{
		{"an item of 63 characters", names("a" + strings.Repeat("b", 61) + "c"), ""},
		{"a single-character item", names("x"), ""},
		{"a hyphenated item with digits", names("study-1-a"), ""},
		{"64 distinct items", names(distinct(64)...), ""},
	})
}

// status.kvCacheTokens is the Instance's KV cache size per worker, in tokens (HBEP-7, The KV cache size; Q4):
// absent until a worker reports it, at least 1 once there (an engine that does not report it leaves it
// absent, never 0), and wider than 32 bits.
func TestInstanceKVCacheTokensRoundTripsAndIsAtLeastOne(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "kv-cache-tokens")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Status.KVCacheTokens != nil {
		t.Errorf("a new Instance has kvCacheTokens %d, want none", *got.Status.KVCacheTokens)
	}
	for _, n := range []int64{1, 131072, 5_000_000_000} {
		must(t, c.Get(bg(), objKey(in), got), "get")
		v := n
		got.Status.KVCacheTokens = &v
		must(t, c.Status().Update(bg(), got), "write kvCacheTokens")
		again := &v1alpha1.Instance{}
		must(t, c.Get(bg(), objKey(in), again), "get")
		if again.Status.KVCacheTokens == nil || *again.Status.KVCacheTokens != n {
			t.Errorf("kvCacheTokens read back as %v, want %d", again.Status.KVCacheTokens, n)
		}
	}
	// 0 and a negative size are refused, and the stored value stays.
	for _, n := range []int64{0, -1} {
		must(t, c.Get(bg(), objKey(in), got), "get")
		v := n
		got.Status.KVCacheTokens = &v
		requireRefused(t, c.Status().Update(bg(), got), "status.kvCacheTokens")
	}
	again := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), again), "get")
	if again.Status.KVCacheTokens == nil || *again.Status.KVCacheTokens != 5_000_000_000 {
		t.Errorf("kvCacheTokens after the refused writes is %v, want the last accepted 5000000000", again.Status.KVCacheTokens)
	}
	// It can be unset again: a worker's new generation unsets it until the worker reports (Q4).
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.KVCacheTokens = nil
	must(t, c.Status().Update(bg(), got), "unset kvCacheTokens")
	must(t, c.Get(bg(), objKey(in), again), "get")
	if again.Status.KVCacheTokens != nil {
		t.Errorf("kvCacheTokens after the unset is %d, want none", *again.Status.KVCacheTokens)
	}
}

// Sticky routes (HBEP-5, Workloads, Sticky routes; HBEP-11, the Workload row; HBEP-764, R9). Typed literals: the Go durations and counts below are
// the HBEP's own bounds, not read from the types.

// sessionAffinity is Route or absent: Route is admitted, anything else, a wrong-cased Route included, is refused by the schema's enum.
func TestWorkloadSessionAffinityAdmitsRouteAndRefusesOtherValues(t *testing.T) {
	affinity := func(a v1alpha1.SessionAffinity) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) { w.Spec.SessionAffinity = a }
	}
	runWorkloadCases(t, []workloadCase{
		{"None", affinity("None"), "Unsupported value: \"None\""},
		{"a lowercase route", affinity("route"), "Unsupported value: \"route\""},
		{"ClientIP", affinity("ClientIP"), "Unsupported value: \"ClientIP\""},
	}, []workloadCase{
		{"Route", affinity(v1alpha1.SessionAffinityRoute), ""},
		{"no sessionAffinity", func(*v1alpha1.Workload) {}, ""},
	})
}

// pinTTL is from 1h to 7 days, written 168h since CEL's duration has no day unit. This is the rule workloads:$.spec.pinTTL#0, whose mutant admits
// every value that the schema's pattern admits: 59m and 169h are refused by the rule alone.
func TestWorkloadPinTTLIsBetween1hAnd7Days(t *testing.T) {
	ttl := func(d v1alpha1.Duration) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) { w.Spec.PinTTL = &d }
	}
	runWorkloadCases(t, []workloadCase{
		{"59m", ttl("59m"), pinTTLMessage},
		{"59m59s", ttl("59m59s"), pinTTLMessage},
		{"169h", ttl("169h"), pinTTLMessage},
		{"168h1s", ttl("168h1s"), pinTTLMessage},
		{"1ns", ttl("1ns"), pinTTLMessage},
	}, []workloadCase{
		{"1h", ttl("1h"), ""},
		{"60m", ttl("60m"), ""},
		{"12h", ttl("12h"), ""},
		{"168h", ttl("168h"), ""},
	})
}

// maxPins is from 1 to 1,000,000: the schema's bounds.
func TestWorkloadMaxPinsIsBetween1And1000000(t *testing.T) {
	pins := func(n int32) func(*v1alpha1.Workload) {
		return func(w *v1alpha1.Workload) { w.Spec.MaxPins = ptr.To(n) }
	}
	runWorkloadCases(t, []workloadCase{
		{"0 pins", pins(0), "spec.maxPins"},
		{"-1 pins", pins(-1), "spec.maxPins"},
		{"1000001 pins", pins(1000001), "spec.maxPins"},
	}, []workloadCase{
		{"1 pin", pins(1), ""},
		{"1000000 pins", pins(1000000), ""},
	})
}

// The defaults are read back from the API server, never from a Go struct (envtest applies the CRD's defaults): pinTTL 12h and maxPins 100000 on
// every Workload, with or without sessionAffinity, which has no default.
func TestWorkloadReadsBackItsStickyDefaults(t *testing.T) {
	ns := newNamespace(t)
	for _, affinity := range []v1alpha1.SessionAffinity{"", v1alpha1.SessionAffinityRoute} {
		w := newWorkload(ns, uniq("study"))
		w.Spec.SessionAffinity = affinity
		if w.Spec.PinTTL != nil || w.Spec.MaxPins != nil {
			t.Fatalf("the fixture sets pinTTL %v and maxPins %v: the test needs both absent", w.Spec.PinTTL, w.Spec.MaxPins)
		}
		must(t, c.Create(bg(), w), "create a Workload without pinTTL and maxPins")
		got := &v1alpha1.Workload{}
		must(t, c.Get(bg(), objKey(w), got), "get")
		if got.Spec.PinTTL == nil || *got.Spec.PinTTL != "12h" {
			t.Errorf("sessionAffinity %q: pinTTL read back as %v, want the default 12h", affinity, got.Spec.PinTTL)
		}
		if got.Spec.MaxPins == nil || *got.Spec.MaxPins != 100000 {
			t.Errorf("sessionAffinity %q: maxPins read back as %v, want the default 100000", affinity, got.Spec.MaxPins)
		}
		if got.Spec.SessionAffinity != affinity {
			t.Errorf("sessionAffinity read back as %q, want %q: it has no default", got.Spec.SessionAffinity, affinity)
		}
	}
}

// A Workload's model is immutable (HBEP-11, Workload). This is the rule workloads:$.spec.model#1. The new model is a plain name, so the '#' rule
// is not what refuses it; pinTTL, shape and the other fields of the same object still update.
func TestWorkloadModelIsImmutable(t *testing.T) {
	ns := newNamespace(t)
	w := newWorkload(ns, uniq("study"))
	w.Spec.SessionAffinity = v1alpha1.SessionAffinityRoute
	must(t, c.Create(bg(), w), "create")
	got := &v1alpha1.Workload{}
	must(t, c.Get(bg(), objKey(w), got), "get")
	got.Spec.Model = "example-13b"
	requireRefused(t, c.Update(bg(), got), modelImmutableMsg)

	must(t, c.Get(bg(), objKey(w), got), "get")
	got.Spec.PinTTL = ptr.To(v1alpha1.Duration("24h"))
	got.Spec.Shape.ContextTokens.Typical = 4096
	must(t, c.Update(bg(), got), "update pinTTL and shape with the model unchanged")
	must(t, c.Get(bg(), objKey(w), got), "get")
	if got.Spec.PinTTL == nil || *got.Spec.PinTTL != "24h" || got.Spec.Shape.ContextTokens.Typical != 4096 || got.Spec.Model != "example-27b" {
		t.Errorf("after the update: pinTTL %v, typical context %d, model %q; want 24h, 4096, example-27b", got.Spec.PinTTL, got.Spec.Shape.ContextTokens.Typical, got.Spec.Model)
	}
}

// sessionAffinity is immutable, set or absent, from the Workload's creation (HBEP-11, Workload): adding Route to a Workload created without it is
// refused, and so is removing it from one created with it. A field-level rule on an optional field would miss both, which is why the rule is the
// spec's (workloads:$.spec#1). Each Workload's pinTTL and shape still update while it is in place.
func TestWorkloadSessionAffinityIsImmutable(t *testing.T) {
	ns := newNamespace(t)
	bare := newWorkload(ns, uniq("study"))
	must(t, c.Create(bg(), bare), "create a Workload without sessionAffinity")
	pinned := newWorkload(ns, uniq("study"))
	pinned.Spec.SessionAffinity = v1alpha1.SessionAffinityRoute
	must(t, c.Create(bg(), pinned), "create a Workload with sessionAffinity Route")

	got := &v1alpha1.Workload{}
	must(t, c.Get(bg(), objKey(bare), got), "get")
	got.Spec.SessionAffinity = v1alpha1.SessionAffinityRoute
	requireRefused(t, c.Update(bg(), got), affinityImmutableMsg)

	must(t, c.Get(bg(), objKey(pinned), got), "get")
	got.Spec.SessionAffinity = ""
	requireRefused(t, c.Update(bg(), got), affinityImmutableMsg)

	for _, w := range []*v1alpha1.Workload{bare, pinned} {
		must(t, c.Get(bg(), objKey(w), got), "get")
		got.Spec.PinTTL = ptr.To(v1alpha1.Duration("48h"))
		got.Spec.Shape.OutputTokens.Typical = 2048
		must(t, c.Update(bg(), got), "update pinTTL and shape with sessionAffinity unchanged")
		must(t, c.Get(bg(), objKey(w), got), "get")
		if got.Spec.SessionAffinity != w.Spec.SessionAffinity || got.Spec.PinTTL == nil || *got.Spec.PinTTL != "48h" || got.Spec.Shape.OutputTokens.Typical != 2048 {
			t.Errorf("after the update: sessionAffinity %q, pinTTL %v, typical output %d; want %q, 48h, 2048", got.Spec.SessionAffinity, got.Spec.PinTTL, got.Spec.Shape.OutputTokens.Typical, w.Spec.SessionAffinity)
		}
	}
}
