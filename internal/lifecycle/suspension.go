package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A suspect reference (HBEP-7, Over-allocation and qualification (beta), The throughput gate: a suspect reference;
// #616, PR 2). The gate reads the ModelDeployment's status.suspendedReferences: an entry listed there with the
// same date gives the verdict Suspended, which records only.

// listedSuspended reports whether the listing holds the entry with this key and date.
func listedSuspended(list []v1alpha1.SuspendedReference, key v1alpha1.ReferenceKey, date string) bool {
	for _, r := range list {
		if r.Date == date && sameKey(r.ReferenceKey, key) {
			return true
		}
	}
	return false
}

// referenceSuspended reports whether the entry with this key and date is suspended for the deployment, as the gate
// reads it.
func (p *pass) referenceSuspended(pv *providerView, entry v1alpha1.ReferenceKey, date string) bool {
	return p.md != nil && listedSuspended(p.md.Status.SuspendedReferences, entry, date)
}
