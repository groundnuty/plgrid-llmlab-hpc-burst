package lifecycle

// The frozen hedge (HBEP-7, Hedging (beta), Interactions, Operations; #604's seam, #589 plan, Rulings): while an Operation
// is committed to act on a member of a hedge, the hedge gets no new member. A hedge's growth in the middle of an
// Operation would add a member the Operation never committed to, to a hedge it is taking apart.

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// frozenByOperation reports whether a hedge is frozen: an Operation of ops is Executing, or PartlyFailed (committed,
// not finished: its failed targets are retried), with a committed target whose UID is one of the hedge's members. It is
// pure. The Operations are the pass's own (p.ops), which the Operations step reads before step 7, and a hedge is frozen
// from the pass that commits the Operation, since the commit's status write is applied to the pass's own object.
func frozenByOperation(members []*v1alpha1.Instance, ops []*v1alpha1.Operation) bool {
	for _, op := range ops {
		if !isExecuting(op) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind != v1alpha1.TargetKindInstance {
				continue
			}
			for _, m := range members {
				if m.UID == t.UID {
					return true
				}
			}
		}
	}
	return false
}
