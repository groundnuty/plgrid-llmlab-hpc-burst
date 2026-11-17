package e2e

// An Operation's status.estimates, written by each Provider's controller by
// server-side apply under its own field manager (HBEP-10, The release cost;
// HBEP-4, Estimate; `@hpc-burst-science-agent`, 17 Nov 2026, #661). The rows are
// S4-4 of the plan: two Providers' entries on one Operation both survive each
// other's applies. The controls show what a shared manager, and one entry at a
// time, would do, so the survival rows can fail.

import (
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	estPlan    = "0123456789abcdef"
	estPlanNew = "fedcba9876543210"
	estShapeA  = "aaaaaaaaaaaaaaaa"
	estShapeB  = "bbbbbbbbbbbbbbbb"
)

var estObserved = metav1.NewTime(time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC))

func estEntry(provider, plan, shape string, target string, start *metav1.Time, unknown string) v1alpha1.OperationEstimate {
	return v1alpha1.OperationEstimate{
		Provider: provider, PlanID: plan, Shape: shape, Targets: []string{target},
		Start: v1alpha1.TimeFigure{Time: start, Unknown: unknown, ObservedAt: &estObserved},
	}
}

// applyEstimates applies one Provider's entries to the Operation's status under
// the manager of that Provider.
func applyEstimates(t *testing.T, op *v1alpha1.Operation, manager, provider string, entries ...v1alpha1.OperationEstimate) {
	t.Helper()
	u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, provider, entries)
	must(t, err, "build the estimate apply")
	must(t, c.Status().Apply(bg(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership),
		"apply estimates as "+manager)
}

// estKeys are the (provider, plan, shape) of each stored entry, sorted.
func estKeys(t *testing.T, op *v1alpha1.Operation) []string {
	t.Helper()
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get the Operation")
	var keys []string
	for _, e := range got.Status.Estimates {
		keys = append(keys, e.Provider+"/"+e.PlanID+"/"+e.Shape)
	}
	slices.Sort(keys)
	return keys
}

func wantKeys(t *testing.T, op *v1alpha1.Operation, what string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := estKeys(t, op); !slices.Equal(got, want) {
		t.Fatalf("%s: estimates are %v, want %v", what, got, want)
	}
}

func newEstimateOperation(t *testing.T) *v1alpha1.Operation {
	t.Helper()
	op := newOperation(newNamespace(t), uniq("op"))
	must(t, c.Create(bg(), op), "create the Operation")
	return op
}

// S4-4: Provider athena-a and Provider helios-a each apply their own entries.
// Each manager's later applies, unchanged or changed, leave the other's entries
// as they were, and each manager owns only its own.
func TestTwoProvidersEstimatesSurviveEachOthersApplies(t *testing.T) {
	op := newEstimateOperation(t)
	start := metav1.NewTime(time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC))
	athena := estEntry("athena-a", estPlan, estShapeA, "uid-1", &start, "")
	helios := estEntry("helios-a", estPlan, estShapeA, "uid-2", nil, "this platform's probe has not decided how to read the estimate (HBEP-4, Estimate)")
	athenaManager, heliosManager := v1alpha1.FieldManagerProviderEstimate("athena-a"), v1alpha1.FieldManagerProviderEstimate("helios-a")

	applyEstimates(t, op, athenaManager, "athena-a", athena)
	applyEstimates(t, op, heliosManager, "helios-a", helios)
	both := []string{"athena-a/" + estPlan + "/" + estShapeA, "helios-a/" + estPlan + "/" + estShapeA}
	wantKeys(t, op, "after both applied", both...)

	// Each applies again, with the same entries and then a changed one: the other's entry stays.
	applyEstimates(t, op, athenaManager, "athena-a", athena)
	wantKeys(t, op, "after athena-a applied again", both...)
	later := metav1.NewTime(start.Add(time.Hour))
	applyEstimates(t, op, athenaManager, "athena-a", estEntry("athena-a", estPlan, estShapeA, "uid-1", &later, ""))
	applyEstimates(t, op, heliosManager, "helios-a", helios)
	wantKeys(t, op, "after both applied again", both...)

	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get the Operation")
	byProvider := map[string]v1alpha1.OperationEstimate{}
	for _, e := range got.Status.Estimates {
		byProvider[e.Provider] = e
	}
	if a := byProvider["athena-a"].Start; a.Time == nil || !a.Time.Equal(&later) || !a.Advisory {
		t.Errorf("athena-a's entry = %+v, want its changed time %v, advisory", a, later)
	}
	if h := byProvider["helios-a"].Start; h.Time != nil || !strings.Contains(h.Unknown, "has not decided") || !h.Advisory {
		t.Errorf("helios-a's entry = %+v, want its unknown reason, advisory", h)
	}

	// Each manager owns only its own entry, and neither the shared Provider manager nor the lifecycle
	// controller's owns any.
	owned := map[string]string{}
	for _, mf := range got.ManagedFields {
		if mf.FieldsV1 != nil && strings.Contains(string(mf.FieldsV1.Raw), "estimates") {
			owned[mf.Manager] = string(mf.FieldsV1.Raw)
		}
	}
	if len(owned) != 2 {
		t.Fatalf("managers that own estimates: %v, want exactly %q and %q", owned, athenaManager, heliosManager)
	}
	if o := owned[athenaManager]; !strings.Contains(o, `athena-a`) || strings.Contains(o, `helios-a`) {
		t.Errorf("%s owns %s, want only athena-a's entry", athenaManager, o)
	}
	if o := owned[heliosManager]; !strings.Contains(o, `helios-a`) || strings.Contains(o, `athena-a`) {
		t.Errorf("%s owns %s, want only helios-a's entry", heliosManager, o)
	}
}

// The control of the survival row: with ONE manager for every Provider, as the
// shared v1alpha1.FieldManagerProvider is, the second Provider's apply removes
// the first's entry, because an apply is its manager's whole set. This is the
// hazard that FieldManagerProviderEstimate removes; if it did not happen here, the
// row above would prove nothing.
func TestOneSharedManagerWouldLetOneProviderEraseAnother(t *testing.T) {
	op := newEstimateOperation(t)
	athena := estEntry("athena-a", estPlan, estShapeA, "uid-1", nil, "a reason")
	helios := estEntry("helios-a", estPlan, estShapeA, "uid-2", nil, "a reason")
	applyEstimates(t, op, v1alpha1.FieldManagerProvider, "athena-a", athena)
	applyEstimates(t, op, v1alpha1.FieldManagerProvider, "helios-a", helios)
	wantKeys(t, op, "with one shared manager", "helios-a/"+estPlan+"/"+estShapeA)
}

// One Provider can have two shapes on one Operation. Its apply carries ALL its
// entries for the current plan: applying shape B alone, as one entry at a time
// would, drops shape A (the control), and applying both keeps both.
func TestOneProvidersTwoShapesSurviveWhenEveryApplyCarriesBoth(t *testing.T) {
	op := newEstimateOperation(t)
	manager := v1alpha1.FieldManagerProviderEstimate("athena-a")
	a := estEntry("athena-a", estPlan, estShapeA, "uid-1", nil, "a reason")
	b := estEntry("athena-a", estPlan, estShapeB, "uid-2", nil, "a reason")
	key := func(shape string) string { return "athena-a/" + estPlan + "/" + shape }

	applyEstimates(t, op, manager, "athena-a", a)
	wantKeys(t, op, "shape A estimated first", key(estShapeA))
	applyEstimates(t, op, manager, "athena-a", a, b)
	wantKeys(t, op, "shape B estimated after, with both carried", key(estShapeA), key(estShapeB))
	applyEstimates(t, op, manager, "athena-a", a, b)
	wantKeys(t, op, "the same apply again", key(estShapeA), key(estShapeB))

	// The control: shape B alone drops shape A.
	applyEstimates(t, op, manager, "athena-a", b)
	wantKeys(t, op, "control, one entry at a time", key(estShapeB))
}

// A new plan's entries replace the same manager's old plan's entries, and leave
// another Provider's alone until it applies.
func TestANewPlansApplyReplacesOnlyItsOwnProvidersOldEntries(t *testing.T) {
	op := newEstimateOperation(t)
	athenaManager, heliosManager := v1alpha1.FieldManagerProviderEstimate("athena-a"), v1alpha1.FieldManagerProviderEstimate("helios-a")
	applyEstimates(t, op, athenaManager, "athena-a", estEntry("athena-a", estPlan, estShapeA, "uid-1", nil, "a reason"))
	applyEstimates(t, op, heliosManager, "helios-a", estEntry("helios-a", estPlan, estShapeA, "uid-2", nil, "a reason"))

	applyEstimates(t, op, athenaManager, "athena-a", estEntry("athena-a", estPlanNew, estShapeA, "uid-1", nil, "a reason"))
	wantKeys(t, op, "after athena-a asked for the new plan",
		"athena-a/"+estPlanNew+"/"+estShapeA, "helios-a/"+estPlan+"/"+estShapeA)
	applyEstimates(t, op, heliosManager, "helios-a", estEntry("helios-a", estPlanNew, estShapeA, "uid-2", nil, "a reason"))
	wantKeys(t, op, "after helios-a asked for the new plan",
		"athena-a/"+estPlanNew+"/"+estShapeA, "helios-a/"+estPlanNew+"/"+estShapeA)
}

// The schema bounds: 32 entries in all, 256 targets of at most 64 characters per
// entry, and a list map keyed by Provider, plan and shape, so the same key twice
// in one apply is refused.
func TestEstimateSchemaBounds(t *testing.T) {
	op := newEstimateOperation(t)
	manager := v1alpha1.FieldManagerProviderEstimate("athena-a")

	var many []v1alpha1.OperationEstimate
	for i := range 33 {
		many = append(many, estEntry("athena-a", estPlan, "shape"+string(rune('a'+i%26))+string(rune('a'+i/26)), "uid-1", nil, "a reason"))
	}
	u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, "athena-a", many)
	must(t, err, "build")
	if err := c.Status().Apply(bg(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err == nil {
		t.Error("33 entries were accepted, want at most 32")
	}
	u, err = v1alpha1.EstimateApply(op.Namespace, op.Name, "athena-a", many[:32])
	must(t, err, "build")
	if err := c.Status().Apply(bg(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		t.Errorf("32 entries were refused: %v", err)
	}

	wide := estEntry("athena-a", estPlanNew, estShapeA, "uid-1", nil, "a reason")
	wide.Targets = make([]string, 257)
	for i := range wide.Targets {
		wide.Targets[i] = "uid-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+i/676))
	}
	long := estEntry("athena-a", estPlanNew, estShapeB, strings.Repeat("u", 65), nil, "a reason")
	for name, e := range map[string]v1alpha1.OperationEstimate{"257 targets": wide, "a 65-character target": long} {
		u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, "athena-a", []v1alpha1.OperationEstimate{e})
		must(t, err, "build")
		if err := c.Status().Apply(bg(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err == nil {
			t.Errorf("%s were accepted", name)
		}
	}
}
