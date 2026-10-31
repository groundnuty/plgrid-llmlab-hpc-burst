package agentserver

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Allocation is what the server needs of one Allocation, found by its job tag.
type Allocation struct {
	Namespace string
	Name      string
	UID       string
	// JobID is the job ID the provider recorded, empty until it records one.
	JobID string
}

var (
	// ErrNotFound is what ByTag returns when no Allocation has the tag.
	ErrNotFound = errors.New("agentserver: no Allocation has that tag")

	// ErrAmbiguous is what ByTag returns when more than one Allocation has the
	// tag. The server answers it as it answers an unknown tag, and logs it.
	ErrAmbiguous = errors.New("agentserver: more than one Allocation has that tag")
)

// AllocationLookup finds an Allocation by its job tag.
type AllocationLookup interface {
	// ByTag returns the Allocation whose job tag is tag, ErrNotFound when none
	// has it, and an error that wraps ErrAmbiguous when several do. Any other
	// error means the lookup cannot answer now.
	ByTag(ctx context.Context, tag string) (Allocation, error)
}

// NewClientLookup returns a lookup over the Allocations that r lists, in every
// namespace: the one whose v1alpha1.AnnotationExternalName is the tag, with the
// job ID from v1alpha1.AnnotationJobID. It lists on each call, which is cheap
// on a cache-backed reader at alpha's scale (a few dozen Allocations).
func NewClientLookup(r client.Reader) AllocationLookup { return clientLookup{r: r} }

type clientLookup struct{ r client.Reader }

func (l clientLookup) ByTag(ctx context.Context, tag string) (Allocation, error) {
	// An Allocation without the annotation has the empty tag: it must never
	// match one that is empty only because the caller sent none.
	if tag == "" {
		return Allocation{}, ErrNotFound
	}
	var list v1alpha1.AllocationList
	if err := l.r.List(ctx, &list); err != nil {
		return Allocation{}, fmt.Errorf("listing Allocations: %w", err)
	}
	var found []Allocation
	for i := range list.Items {
		a := &list.Items[i]
		if a.Annotations[v1alpha1.AnnotationExternalName] != tag {
			continue
		}
		found = append(found, Allocation{
			Namespace: a.Namespace,
			Name:      a.Name,
			UID:       string(a.UID),
			JobID:     a.Annotations[v1alpha1.AnnotationJobID],
		})
	}
	switch len(found) {
	case 0:
		return Allocation{}, ErrNotFound
	case 1:
		return found[0], nil
	}
	return Allocation{}, fmt.Errorf("%w: %s/%s and %s/%s share a job tag (%d in all)",
		ErrAmbiguous, found[0].Namespace, found[0].Name, found[1].Namespace, found[1].Name, len(found))
}
