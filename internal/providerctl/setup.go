package providerctl

import (
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// SupportedPolicies is the one management-policy set the reconciler accepts:
// exactly the CRD's fixed set, Observe, Create, Delete and LateInitialize.
// Update is not in it, because a submitted job is never edited (HBEP-4; HBEP-11).
func SupportedPolicies() []sets.Set[xpv2.ManagementAction] {
	return []sets.Set[xpv2.ManagementAction]{
		sets.New(xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize),
	}
}

// clientManager is the part of a manager that crossplane-runtime's
// NewReconciler reads: the client, here wrapped with the provider's field
// manager, and the scheme.
type clientManager struct {
	manager.Manager
	c      client.Client
	scheme *runtime.Scheme
}

func (m clientManager) GetClient() client.Client   { return m.c }
func (m clientManager) GetScheme() *runtime.Scheme { return m.scheme }

// Controller is one Provider's controller before it is registered: the managed
// reconciler and the Provider's status writer. Tests drive them directly.
type Controller struct {
	Reconciler *managed.Reconciler
	status     *statusLoop
	shared     *shared
}

// SyncStatus writes the Provider's status once, as the status loop does every
// poll interval.
func (c *Controller) SyncStatus(ctx context.Context) error { return c.status.sync(ctx) }

// Close releases the Provider's platform connection.
func (c *Controller) Close() { c.shared.h.close() }

// New builds one Provider's controller over kube (writes) and reader (reads
// from the API server). Every write names the field manager
// v1alpha1.FieldManagerProvider.
func New(kube client.Client, reader client.Reader, scheme *runtime.Scheme, o Options) (*Controller, error) {
	if err := o.defaults(); err != nil {
		return nil, err
	}
	kube = client.WithFieldOwner(kube, v1alpha1.FieldManagerProvider)
	s := &shared{o: o, kube: kube, reader: reader, waits: newWaitSink(), builds: &buildCache{}}
	s.h = &holder{o: &s.o}
	r := managed.NewReconciler(clientManager{c: kube, scheme: scheme}, resource.ManagedKind(v1alpha1.GroupVersion.WithKind("Allocation")),
		managed.WithExternalConnector(&connector{s: s}),
		managed.WithFinalizer(resource.NewAPIFinalizer(kube, v1alpha1.FinalizerCancelAll)),
		managed.WithInitializers(tagInitializer{kube: kube}),
		managed.WithCriticalAnnotationUpdater(criticalAnnotationUpdater{kube: kube, reader: reader}),
		managed.WithDeterministicExternalName(true),
		managed.WithManagementPolicies(),
		managed.WithReconcilerSupportedManagementPolicies(SupportedPolicies()),
		managed.WithPollInterval(o.PollInterval),
		managed.WithPollIntervalHook(pollHook(o)),
		managed.WithTimeout(o.Timeout),
		managed.WithLogger(o.Log),
		managed.WithRecorder(o.Recorder),
	)
	return &Controller{Reconciler: r, status: &statusLoop{s: s}, shared: s}, nil
}

// pollHook re-checks soon while an attempt is unresolved, a job is held or
// waited on, or the Allocation is being deleted; otherwise the normal poll
// interval holds. A waiting Observe therefore waits on a RequeueAfter, never in
// a hot loop (S4).
func pollHook(o Options) managed.PollIntervalHook {
	return func(mg resource.Managed, poll time.Duration) time.Duration {
		a, ok := mg.(*v1alpha1.Allocation)
		if !ok {
			return poll
		}
		rec, err := readRecord(a)
		busy := err != nil || needsResolution(rec) || meta.WasDeleted(a)
		switch a.Status.AtProvider.Phase {
		case v1alpha1.AllocationHeld, v1alpha1.AllocationSubmitting, v1alpha1.AllocationSubmissionUnknown, "":
			busy = true
		}
		if busy && o.BusyInterval < poll {
			return o.BusyInterval
		}
		return poll
	}
}

// Setup registers one Provider's controller with mgr: the managed reconciler
// over the Allocations whose providerConfigRef names the Provider, and the
// Provider's status writer.
func Setup(mgr manager.Manager, o Options) error {
	c, err := New(mgr.GetClient(), mgr.GetAPIReader(), mgr.GetScheme(), o)
	if err != nil {
		return err
	}
	name := c.shared.o.Provider
	skip := true
	err = builder.ControllerManagedBy(mgr).
		Named("allocation-"+name).
		For(&v1alpha1.Allocation{}, builder.WithPredicates(
			resource.DesiredStateChanged(),
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				a, ok := obj.(*v1alpha1.Allocation)
				return ok && a.Spec.ProviderConfigReference != nil && a.Spec.ProviderConfigReference.Name == name
			}),
		)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: c.shared.o.MaxConcurrentReconciles,
			// A Provider's manager is started again after a failure, in the
			// same process, under the same controller name.
			SkipNameValidation: &skip,
			RateLimiter:        workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, time.Minute),
		}).
		Complete(c.Reconciler)
	if err != nil {
		return err
	}
	return mgr.Add(c.status)
}
