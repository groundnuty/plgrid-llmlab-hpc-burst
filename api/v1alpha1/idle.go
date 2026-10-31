package v1alpha1

import (
	"fmt"
	"time"
)

// The idle anchor of an Allocation (HBEP-7, Retention). Retention's idleLimit
// and the profile's maxIdle count from the time an Allocation stopped hosting
// an Instance. That time must survive a controller restart and the deletion of
// the Instance, so it lives in the Allocation's annotation AnnotationIdleSince,
// which only the lifecycle controller writes, with a resourceVersion
// precondition:
//
//   - when the Allocation's last Instance ends, it sets it to the latest
//     InstanceStatus.EndTime of the Allocation's Instances;
//   - when the Allocation's job starts with no Instance placed on it, it sets
//     it to the job's start (status.atProvider.startTime);
//   - when it places an Instance on the Allocation, it removes it.
//
// An Allocation is idle exactly while it carries the annotation.

// IdleSince returns the time in an Allocation's idle-since annotation. ok is
// false when the annotation is absent. A value that is not an RFC 3339 time is
// an error, never a zero time: an unreadable anchor must not release an
// Allocation at once.
func IdleSince(a *Allocation) (t time.Time, ok bool, err error) {
	v, ok := a.Annotations[AnnotationIdleSince]
	if !ok {
		return time.Time{}, false, nil
	}
	t, err = time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("annotation %s %q is not an RFC 3339 time: %w", AnnotationIdleSince, v, err)
	}
	return t.UTC(), true, nil
}

// SetIdleSince sets an Allocation's idle-since annotation to t, in UTC and
// whole seconds, the form IdleSince reads. It changes a in place; the caller
// writes it with an Update.
func SetIdleSince(a *Allocation, t time.Time) {
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[AnnotationIdleSince] = t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// ClearIdleSince removes an Allocation's idle-since annotation, when an
// Instance is placed on it. It changes a in place.
func ClearIdleSince(a *Allocation) { delete(a.Annotations, AnnotationIdleSince) }
