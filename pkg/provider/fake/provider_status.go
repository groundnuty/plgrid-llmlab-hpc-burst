package fake

import (
	"context"
	"fmt"
	"sort"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The fakes write as the Provider's controller does, under its field manager
// (docs/development.md, Single writer), so a client that records managed
// fields shows who wrote what.

// update writes obj's metadata and spec as the Provider's controller.
func update(ctx context.Context, cl client.Client, obj client.Object) error {
	return cl.Update(ctx, obj, client.FieldOwner(v1alpha1.FieldManagerProvider))
}

// updateStatus writes obj's status as the Provider's controller.
func updateStatus(ctx context.Context, cl client.Client, obj client.Object) error {
	return cl.Status().Update(ctx, obj, client.FieldOwner(v1alpha1.FieldManagerProvider))
}

// writeProviderStatus writes prov's status when it differs from old's. It sends
// the whole status, read from the store by the caller just before, so a
// condition the caller did not touch, such as DialHomeUnreachable, goes back as
// it was found.
func writeProviderStatus(ctx context.Context, cl client.Client, old, prov *v1alpha1.Provider) error {
	if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
		return nil
	}
	if err := updateStatus(ctx, cl, prov); err != nil {
		return fmt.Errorf("update Provider %s status: %w", prov.Name, err)
	}
	return nil
}

// syncProvider writes the Provider's status: the queue waits found in this
// pass, and, when no read failed, the time of the last successful read and the
// Ready condition.
func (c *Controller) syncProvider(ctx context.Context, p *pass) error {
	var prov v1alpha1.Provider
	if err := c.Client.Get(ctx, client.ObjectKey{Name: c.Provider}, &prov); err != nil {
		return fmt.Errorf("get Provider %s: %w", c.Provider, err)
	}
	old := prov.DeepCopy()
	// A list is ordered by name; the oldest wait is the earliest start.
	sort.SliceStable(p.waits, func(i, k int) bool { return p.waits[i].StartTime.Before(&p.waits[k].StartTime) })
	for _, w := range p.waits {
		prov.Status.QueueWaits = v1alpha1.AddQueueWait(prov.Status.QueueWaits, w)
	}
	prov.Status.ObservedGeneration = prov.Generation
	prov.Status.LastReconcileTime = mt(p.now)
	if !p.readFailed {
		prov.Status.LastSuccessfulStatusRead = mt(p.now)
		apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionProviderReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Polled",
			ObservedGeneration: prov.Generation,
			LastTransitionTime: *mt(p.now),
		})
	}
	return writeProviderStatus(ctx, c.Client, old, &prov)
}
