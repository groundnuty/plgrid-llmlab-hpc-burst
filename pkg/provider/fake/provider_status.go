package fake

import (
	"context"
	"fmt"
	"sort"
	"time"

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
// pass, and the readiness fields (setReadiness). A pass that made no Batch read
// polls the platform first, so Ready always rests on a real read.
func (c *Controller) syncProvider(ctx context.Context, p *pass) error {
	if !p.read && c.Batch.Poll(ctx) != nil {
		p.readFailed = true
	}
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
	c.setReadiness(&prov, p)
	return writeProviderStatus(ctx, c.Client, old, &prov)
}

// setReadiness is HBEP-4's Operational readiness 1, the rule of docs/
// development.md, Provider readiness. A pass with no failed read sets
// lastSuccessfulStatusRead to now and Ready True (reason Polled). A pass with a
// failed read leaves lastSuccessfulStatusRead alone and sets Ready False
// (reason ReadsFailing) only when there is no successful read, or the last is
// older than the staleness bound; until then Ready keeps its value.
func (c *Controller) setReadiness(prov *v1alpha1.Provider, p *pass) {
	cond := metav1.Condition{Type: v1alpha1.ConditionProviderReady, ObservedGeneration: prov.Generation, LastTransitionTime: *mt(p.now)}
	if !p.readFailed {
		prov.Status.LastSuccessfulStatusRead = mt(p.now)
		cond.Status, cond.Reason = metav1.ConditionTrue, v1alpha1.ReasonPolled
		apimeta.SetStatusCondition(&prov.Status.Conditions, cond)
		return
	}
	last := prov.Status.LastSuccessfulStatusRead
	switch {
	case last == nil:
		cond.Message = "reads are failing: no successful read yet"
	case p.now.Sub(last.Time) > c.readyStaleness():
		// The message names the time of the last success, not its age, so it is
		// the same at every pass and a failing Provider is not rewritten for it.
		cond.Message = fmt.Sprintf("reads are failing: the last successful read was at %s", last.UTC().Format(time.RFC3339))
	default:
		return
	}
	cond.Status, cond.Reason = metav1.ConditionFalse, v1alpha1.ReasonReadsFailing
	apimeta.SetStatusCondition(&prov.Status.Conditions, cond)
}
