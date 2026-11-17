package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// Field managers (docs/development.md, Single writer). Every write names its
// controller's field manager, so the objects' managedFields show who wrote
// what.
const (
	// FieldManagerLifecycle is the lifecycle controller's field manager, its
	// Backend's included. On a Provider it owns exactly one thing: the
	// condition of type ConditionDialHomeUnreachable, which it sets by
	// server-side apply (DialHomeUnreachableApply).
	FieldManagerLifecycle = "hpcburst-lifecycle"
	// FieldManagerProvider is the field manager of every Provider's
	// controller. It writes the Allocation annotations and status the
	// Provider's controller owns, and all of Provider status except
	// ConditionDialHomeUnreachable, which it keeps as it found it.
	FieldManagerProvider = "hpcburst-provider"
)

// FieldManagerProviderEstimate is the field manager under which one Provider's
// controller applies its entries of an Operation's status.estimates
// (EstimateApply): "hpcburst-provider-" plus the Provider's name, so at most 81
// characters. Server-side apply removes the entries that a manager owned and its
// next apply leaves out, so a manager shared by every Provider would let one
// Provider's apply erase another's entries; one manager per Provider keeps each
// Provider's entries its own (`@hpc-burst-science-agent`, 17 Nov 2026, #661).
// It is for this apply only: Provider status stays under FieldManagerProvider.
func FieldManagerProviderEstimate(provider string) string {
	return FieldManagerProvider + "-" + provider
}

// EstimateApply returns the server-side apply object with which a Provider's
// controller writes its estimates onto an Operation's status. It holds the
// Operation's name and namespace and the entries, and nothing else, so the apply
// owns only those list entries (status.estimates is a map list
// keyed by provider, planID and shape). Apply it to the status subresource
// under FieldManagerProviderEstimate(provider), with forced ownership:
//
//	c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u),
//		client.FieldOwner(FieldManagerProviderEstimate(provider)), client.ForceOwnership)
//
// An apply removes the entries its manager owns that it omits. So entries must
// be ALL of the Provider's entries for the Operation's current plan, every shape,
// finished or failed, never one entry at a time: applying shape B alone would
// drop shape A, and an older plan's entries go the same way, which is meant.
// Each entry must name provider, hold a plan ID, a shape and at least one
// target, and carry either a time or the reason it is unknown, never both and
// never neither; its figure is always advisory, whatever the caller set.
func EstimateApply(namespace, name, provider string, entries []OperationEstimate) (*unstructured.Unstructured, error) {
	switch {
	case namespace == "" || name == "":
		return nil, fmt.Errorf("estimate apply: no Operation name")
	case provider == "":
		return nil, fmt.Errorf("estimate apply: no Provider name")
	case len(entries) == 0:
		return nil, fmt.Errorf("estimate apply: no entries: an apply of none would remove this Provider's entries")
	}
	sorted := make([]OperationEstimate, len(entries))
	for i, e := range entries {
		switch {
		case e.Provider != provider:
			return nil, fmt.Errorf("estimate apply: entry %d names Provider %q, not %q", i, e.Provider, provider)
		case e.PlanID == "" || e.Shape == "":
			return nil, fmt.Errorf("estimate apply: entry %d has no plan ID or no shape", i)
		case len(e.Targets) == 0:
			return nil, fmt.Errorf("estimate apply: entry %d covers no target", i)
		case (e.Start.Time == nil) == (e.Start.Unknown == ""):
			return nil, fmt.Errorf("estimate apply: entry %d must carry a time or the reason it is unknown, and not both", i)
		}
		e.Start.Advisory = true
		sorted[i] = *e.DeepCopy()
	}
	slices.SortFunc(sorted, func(a, b OperationEstimate) int {
		return cmp.Or(strings.Compare(a.PlanID, b.PlanID), strings.Compare(a.Shape, b.Shape))
	})
	list := make([]any, len(sorted))
	for i := range sorted {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&sorted[i])
		if err != nil {
			return nil, fmt.Errorf("estimate apply: entry %d: %w", i, err)
		}
		list[i] = m
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": GroupVersion.String(),
		"kind":       "Operation",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"status":     map[string]any{"estimates": list},
	}}, nil
}

// DialHomeUnreachableApply returns the server-side apply object with which the
// lifecycle controller sets ConditionDialHomeUnreachable on a Provider's
// status. It holds the Provider's name and that one condition and nothing
// else, so the apply takes ownership of that list entry only (conditions is a
// map list keyed by type). Apply it to the status subresource under
// FieldManagerLifecycle, with forced ownership; for example, with
// controller-runtime:
//
//	c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u),
//		client.FieldOwner(FieldManagerLifecycle), client.ForceOwnership)
//
// cond.Type is set here. The caller keeps cond.LastTransitionTime from the
// stored condition while the status does not change, as
// meta.SetStatusCondition does. A message and a reason are required, as for
// every metav1.Condition.
func DialHomeUnreachableApply(provider string, cond metav1.Condition) (*unstructured.Unstructured, error) {
	switch {
	case provider == "":
		return nil, fmt.Errorf("dial-home condition: no Provider name")
	case cond.Status != metav1.ConditionTrue && cond.Status != metav1.ConditionFalse && cond.Status != metav1.ConditionUnknown:
		return nil, fmt.Errorf("dial-home condition: status %q is not True, False or Unknown", cond.Status)
	case cond.Reason == "":
		return nil, fmt.Errorf("dial-home condition: no reason")
	case cond.LastTransitionTime.IsZero():
		return nil, fmt.Errorf("dial-home condition: no lastTransitionTime")
	}
	c := map[string]any{
		"type":               ConditionDialHomeUnreachable,
		"status":             string(cond.Status),
		"reason":             cond.Reason,
		"message":            cond.Message,
		"lastTransitionTime": cond.LastTransitionTime.UTC().Format(time.RFC3339),
	}
	if cond.ObservedGeneration != 0 {
		c["observedGeneration"] = cond.ObservedGeneration
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": GroupVersion.String(),
		"kind":       "Provider",
		"metadata":   map[string]any{"name": provider},
		"status":     map[string]any{"conditions": []any{c}},
	}}
	return u, nil
}

// The drain window's defaults, the schema defaults of PlatformProfileSpec's
// drainWindow and drainMargin (HBEP-4, Platform profile schema; HBEP-5).
const (
	DefaultDrainWindow = 2*time.Hour + 10*time.Minute
	DefaultDrainMargin = 10 * time.Minute
)

// InstanceDrainWindow returns the drain window of an Instance of a
// ModelDeployment with routing on a platform with profile (HBEP-5, Draining;
// HBEP-7, Health and restarts, Draining): the profile's drainWindow, or the
// deployment's routing.maxRequestDuration plus the profile's drainMargin when
// that is shorter. A deployment never lengthens the window. The front and the
// lifecycle controller both call it, so both compute the same drain start
// (DrainStart). An absent drainWindow or drainMargin takes its schema default,
// so an object built in a test without the API server gives the same window.
func InstanceDrainWindow(profile PlatformProfileSpec, routing *Routing) (time.Duration, error) {
	window, err := durationOr(profile.DrainWindow, DefaultDrainWindow, "drainWindow")
	if err != nil {
		return 0, err
	}
	if routing == nil || routing.MaxRequestDuration == nil {
		return window, nil
	}
	margin, err := durationOr(profile.DrainMargin, DefaultDrainMargin, "drainMargin")
	if err != nil {
		return 0, err
	}
	maxReq, err := routing.MaxRequestDuration.AsDuration()
	if err != nil {
		return 0, fmt.Errorf("routing.maxRequestDuration %q: %w", *routing.MaxRequestDuration, err)
	}
	if short := maxReq + margin; short < window {
		return short, nil
	}
	return window, nil
}

// DrainStart returns when an Instance with lease end leaseEnd starts draining:
// leaseEnd minus its drain window (InstanceDrainWindow).
func DrainStart(leaseEnd time.Time, profile PlatformProfileSpec, routing *Routing) (time.Time, error) {
	w, err := InstanceDrainWindow(profile, routing)
	if err != nil {
		return time.Time{}, err
	}
	return leaseEnd.Add(-w), nil
}

func durationOr(d *Duration, def time.Duration, field string) (time.Duration, error) {
	if d == nil {
		return def, nil
	}
	v, err := d.AsDuration()
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", field, *d, err)
	}
	return v, nil
}
