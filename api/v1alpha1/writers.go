package v1alpha1

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
