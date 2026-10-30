package kit

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Canceller stands for the provider plugin that cancels a provider job (HBEP-4).
// A slot's deletion completes only after Cancel returned nil.
type Canceller interface {
	Cancel(ctx context.Context, slot *unstructured.Unstructured) error
}

// FleetReconciler keeps spec.replicas Workers per Fleet, each owned by it, and
// reports status. It has NO deletion logic for a Fleet's Workers when the Fleet
// goes: that is the garbage collector's job, which the cascade test checks.
type FleetReconciler struct{ client.Client }

func (r *FleetReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	fleet := New(FleetGVK)
	if err := r.Get(ctx, req.NamespacedName, fleet); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if fleet.GetLabels()[LabelController] == "off" || fleet.GetDeletionTimestamp() != nil {
		return reconcile.Result{}, nil
	}
	want, _, _ := unstructured.NestedInt64(fleet.Object, "spec", "replicas")

	workers := NewList(WorkerGVK)
	if err := r.List(ctx, workers, client.InNamespace(req.Namespace), client.MatchingLabels{LabelFleet: fleet.GetName()}); err != nil {
		return reconcile.Result{}, err
	}
	have := map[int64]*unstructured.Unstructured{}
	for i := range workers.Items {
		w := &workers.Items[i]
		if !metav1.IsControlledBy(w, fleet) {
			continue
		}
		idx, _, _ := unstructured.NestedInt64(w.Object, "spec", "index")
		have[idx] = w
	}
	for i := int64(0); i < want; i++ {
		if _, ok := have[i]; ok {
			continue
		}
		w := New(WorkerGVK)
		w.SetNamespace(fleet.GetNamespace())
		w.SetName(fmt.Sprintf("%s-%d", fleet.GetName(), i))
		w.SetLabels(map[string]string{LabelFleet: fleet.GetName()})
		_ = unstructured.SetNestedField(w.Object, fleet.GetName(), "spec", "fleet")
		_ = unstructured.SetNestedField(w.Object, i, "spec", "index")
		if err := controllerutil.SetControllerReference(fleet, w, r.Scheme()); err != nil {
			return reconcile.Result{}, err
		}
		if err := r.Create(ctx, w); err != nil && !apierrors.IsAlreadyExists(err) {
			return reconcile.Result{}, err
		}
	}
	var extra []int64
	for idx := range have {
		if idx >= want {
			extra = append(extra, idx)
		}
	}
	sort.Slice(extra, func(i, j int) bool { return extra[i] < extra[j] })
	for _, idx := range extra { // scale down
		if err := r.Delete(ctx, have[idx]); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
	}

	// Every index below want now exists: the ones found plus the ones created above.
	ready := want
	_ = unstructured.SetNestedField(fleet.Object, fleet.GetGeneration(), "status", "observedGeneration")
	_ = unstructured.SetNestedField(fleet.Object, ready, "status", "readyWorkers")
	_ = unstructured.SetNestedField(fleet.Object, time.Now().UTC().Format(time.RFC3339), "status", "lastReconcileTime")
	if err := r.Status().Update(ctx, fleet); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// SlotReconciler adds the cancel finalizer and, once the slot is being deleted,
// cancels the "provider job" and then releases the object.
type SlotReconciler struct {
	client.Client
	Canceller Canceller
}

func (r *SlotReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	slot := New(SlotGVK)
	if err := r.Get(ctx, req.NamespacedName, slot); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if slot.GetDeletionTimestamp() == nil {
		if controllerutil.AddFinalizer(slot, FinalizerCancel) {
			return reconcile.Result{}, r.Update(ctx, slot)
		}
		return reconcile.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(slot, FinalizerCancel) {
		return reconcile.Result{}, nil
	}
	if err := r.Canceller.Cancel(ctx, slot); err != nil {
		return reconcile.Result{RequeueAfter: 500 * time.Millisecond}, nil // keep the object; try again
	}
	controllerutil.RemoveFinalizer(slot, FinalizerCancel)
	if err := r.Update(ctx, slot); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// Manager builds a controller-runtime manager with both reconcilers. Metrics,
// health probes and leader election are off: the spike measures the host, not the manager.
func Manager(cfg *rest.Config, canceller Canceller) (ctrl.Manager, error) {
	scheme := Scheme()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		// The test restarts managers in one process; names must be reusable.
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		return nil, err
	}
	fleet, worker, slot := New(FleetGVK), New(WorkerGVK), New(SlotGVK)
	if err := ctrl.NewControllerManagedBy(mgr).Named("fleet").For(fleet).Owns(worker).
		Complete(&FleetReconciler{Client: mgr.GetClient()}); err != nil {
		return nil, err
	}
	if err := ctrl.NewControllerManagedBy(mgr).Named("slot").For(slot).
		Complete(&SlotReconciler{Client: mgr.GetClient(), Canceller: canceller}); err != nil {
		return nil, err
	}
	return mgr, nil
}
