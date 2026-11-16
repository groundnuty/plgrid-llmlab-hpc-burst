package providerctl

// Planned or not (HBEP-8, Allocation events (beta), Planned or not): a job that
// ends while its Instance drains is JobEnded, whatever Slurm's state, not
// AllocationEndedEarly. The Provider controller reads that Instance's phase from
// its manager's cache, by the Instance's spec.allocationRef: an index on that
// field, registered in Setup. The lifecycle controller's own rule
// (internal/lifecycle, allocationFailed) is the lifecycle's; this is the
// Provider's.

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// instanceAllocationIndex is the field index of Instances by the name of the
// Allocation they run in (spec.allocationRef.name), in each Provider manager's
// cache.
const instanceAllocationIndex = "spec.allocationRef.name"

// instanceAllocationName is the index function of instanceAllocationIndex.
func instanceAllocationName(o client.Object) []string {
	in, ok := o.(*v1alpha1.Instance)
	if !ok || in.Spec.AllocationRef.Name == "" {
		return nil
	}
	return []string{in.Spec.AllocationRef.Name}
}

// drainingFunc reports whether an Instance of the Allocation is Draining.
type drainingFunc func(ctx context.Context, a *v1alpha1.Allocation) (bool, error)

// drainingIn returns drainingFunc over r. With indexed set, r is a cache with
// the field index registered (Setup), and the list asks the index; otherwise
// (a controller built with New alone, whose reader is the API server or a test's
// client) the Allocation's namespace is listed and filtered here.
func drainingIn(r client.Reader, indexed bool) drainingFunc {
	return func(ctx context.Context, a *v1alpha1.Allocation) (bool, error) {
		opts := []client.ListOption{client.InNamespace(a.Namespace)}
		if indexed {
			opts = append(opts, client.MatchingFields{instanceAllocationIndex: a.Name})
		}
		var list v1alpha1.InstanceList
		if err := r.List(ctx, &list, opts...); err != nil {
			return false, err
		}
		for i := range list.Items {
			in := &list.Items[i]
			if in.Spec.AllocationRef.Name == a.Name && in.Status.Phase == v1alpha1.InstanceDraining {
				return true, nil
			}
		}
		return false, nil
	}
}
