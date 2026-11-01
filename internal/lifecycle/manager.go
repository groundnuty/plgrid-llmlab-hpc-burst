package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/util/workqueue"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// DefaultWorkers is the number of workers of the work queue (HBEP-7,
// Controllers: four by default).
const DefaultWorkers = 4

// Options configure the lifecycle controller's manager.
type Options struct {
	// Config reaches the control plane's API.
	Config *rest.Config
	// CredentialsDir is the controller's credentials directory, mode 700.
	CredentialsDir string
	// AgentBuildsDir holds the allocation agent's builds, hb-agent-<cpuArch>.
	AgentBuildsDir string
	// FrontConfigured: a front address and dial-home mode are configured.
	FrontConfigured bool
	// HealthAddr is where /healthz and /readyz listen; "0" or empty disables.
	HealthAddr string
	// MetricsAddr is where controller-runtime's metrics server serves
	// /metrics, on loopback at the launcher's port base + 12 (HBEP-8,
	// Metrics); "0" or empty disables it.
	MetricsAddr string
	// Registry is where the engine and accounting gauges' collectors
	// register while the manager runs; nil means controller-runtime's
	// registry, the one its metrics server serves.
	Registry prometheus.Registerer
	// LeaderElection takes the Lease v1alpha1.LeaseLifecycle in
	// v1alpha1.ControllerNamespace, a guard against a stale second copy.
	LeaderElection bool
	// Workers is the work queue's worker count; 0 means DefaultWorkers.
	Workers int
	// Checkpoints reads checkpoints; nil means the Hub, cached.
	Checkpoints CheckpointSource
	// Clock is the time source; nil means SystemClock.
	Clock Clock

	// newCache, when set, builds the manager's cache: a test holds the
	// caches' sync with it.
	newCache cache.NewCacheFunc
	// onLeaseRead, when set, runs after each read of the lifecycle Lease
	// that the start's ObservationGap counts: a test waits on it to know the
	// elector has read a Lease another copy holds.
	onLeaseRead func()
	// skipNameCheck lets a test build more than one manager in a process (a
	// restart, or a run with -count above 1): controller-runtime refuses a
	// second controller named lifecycle in a process unless its name check is
	// skipped. Production builds one manager and never sets it.
	skipNameCheck bool
}

// Scheme returns the scheme the controller uses: the built-in kinds and the
// framework's.
func Scheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// leaderRenewDeadline is controller-runtime's default renew deadline
// (v0.23.1, pkg/manager/manager.go, defaultRenewDeadline), which the lifecycle
// Lease's lock sizes its client timeout by.
const leaderRenewDeadline = 10 * time.Second

// NewManager builds the lifecycle controller: one controller-runtime manager
// whose work queue is keyed per ModelDeployment, and the agent Backend that the
// agent API server calls (HBEP-7, Controllers). With leader election it holds
// the lifecycle Lease through a gapLock, and raises the start's ObservationGap
// once it holds it (HBEP-8, Gaps after a restart). It judges the Providers'
// and the front's heartbeats on the leader (observer), and serves the engine
// gauges per Instance on /metrics when MetricsAddr is set (HBEP-8, Metrics).
func NewManager(o Options) (manager.Manager, *Backend, error) {
	if o.Config == nil {
		return nil, nil, errors.New("lifecycle: no API config")
	}
	if o.CredentialsDir == "" {
		return nil, nil, errors.New("lifecycle: no credentials directory")
	}
	scheme, err := Scheme()
	if err != nil {
		return nil, nil, err
	}
	health := o.HealthAddr
	if health == "" {
		health = "0"
	}
	metricsAddr := o.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = "0"
	}
	mopts := manager.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        health,
		LeaderElection:                o.LeaderElection,
		LeaderElectionID:              v1alpha1.LeaseLifecycle,
		LeaderElectionNamespace:       v1alpha1.ControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		NewCache:                      o.newCache,
	}
	// With leader election off there is no Lease to read or regard, so no
	// ObservationGap (`@hpc-burst-science-agent`, 31 Oct 2026, for #111).
	var lock *gapLock
	if o.LeaderElection {
		if lock, err = newGapLock(o.Config, leaderRenewDeadline); err != nil {
			return nil, nil, fmt.Errorf("lifecycle: the lifecycle Lease's lock: %w", err)
		}
		lock.onObserve = o.onLeaseRead
		mopts.LeaderElectionResourceLockInterface = lock
	}
	mgr, err := ctrl.NewManager(o.Config, mopts)
	if err != nil {
		return nil, nil, fmt.Errorf("lifecycle: manager: %w", err)
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return nil, nil, err
	}
	ready := &readiness{mgr: mgr}
	if err := mgr.Add(ready); err != nil {
		return nil, nil, err
	}
	if err := mgr.AddReadyzCheck("serving", func(*http.Request) error { return ready.check() }); err != nil {
		return nil, nil, err
	}
	clock := o.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	events := make(chan event.TypedGenericEvent[*v1alpha1.ModelDeployment], 1024)
	backend := &Backend{
		Reader: mgr.GetClient(), Clock: clock, CredentialsDir: o.CredentialsDir, Ready: ready.serving,
		Enqueue: func(k types.NamespacedName) {
			select {
			case events <- event.TypedGenericEvent[*v1alpha1.ModelDeployment]{Object: &v1alpha1.ModelDeployment{ObjectMeta: objectMeta(k)}}:
			default: // full: the key's 30 s requeue picks the change up
			}
		},
	}
	src := o.Checkpoints
	if src == nil {
		src = NewCachedSource(&HubSource{}, clock)
	}
	builds := &agentBuilds{dir: o.AgentBuildsDir}
	r := &Reconciler{
		Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Clock: clock, Backend: backend, Checkpoints: src,
		Recorder: KubeRecorder{R: mgr.GetEventRecorder(v1alpha1.FieldManagerLifecycle)},
		Config:   Config{CredentialsDir: o.CredentialsDir, FrontConfigured: o.FrontConfigured, AgentDigest: builds.digest},
	}
	if lock != nil {
		if err := mgr.Add(&gapEmitter{lock: lock, reader: mgr.GetAPIReader(), rec: r.Recorder}); err != nil {
			return nil, nil, err
		}
	}
	if err := mgr.Add(&observer{r: r, every: observeEvery}); err != nil {
		return nil, nil, err
	}
	reg := o.Registry
	if reg == nil {
		reg = ctrlmetrics.Registry
	}
	if err := mgr.Add(&collectorRegistration{reg: reg, cs: []prometheus.Collector{
		&instanceCollector{inv: r.inventory(), backend: backend, clock: clock},
		&accountingCollector{allocs: mgr.GetClient()},
	}}); err != nil {
		return nil, nil, err
	}
	workers := o.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}
	c := mgr.GetClient()
	err = ctrl.NewControllerManagedBy(mgr).
		Named("lifecycle").
		For(&v1alpha1.ModelDeployment{}).
		Watches(&v1alpha1.Allocation{}, handler.EnqueueRequestsFromMapFunc(allocationToDeployment)).
		Watches(&v1alpha1.Instance{}, handler.EnqueueRequestsFromMapFunc(instanceToDeployment)).
		Watches(&v1alpha1.Provider{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return deploymentsAllowing(ctx, c, map[string]bool{o.GetName(): true})
		})).
		Watches(&v1alpha1.PlatformProfile{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return deploymentsOnProfile(ctx, c, o.GetName())
		})).
		// An identity record that changes re-evaluates a collision with it
		// (HBEP-3): every deployment of its namespace is enqueued.
		Watches(&appsv1.ControllerRevision{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if o.GetLabels()[v1alpha1.LabelRecord] != v1alpha1.RecordIdentity {
				return nil
			}
			return deploymentsInNamespace(ctx, c, o.GetNamespace())
		})).
		WatchesRawSource(source.Channel(events, &handler.TypedEnqueueRequestForObject[*v1alpha1.ModelDeployment]{})).
		// A failing key retries with backoff capped at the requeue bound, not
		// controller-runtime's 1000 s, so one Instance's failing write does not
		// hold the deployment back for minutes.
		WithOptions(controller.Options{
			MaxConcurrentReconciles: workers,
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, maxRequeue),
			SkipNameValidation:      &o.skipNameCheck,
		}).
		Complete(r)
	if err != nil {
		return nil, nil, fmt.Errorf("lifecycle: controller: %w", err)
	}
	return mgr, backend, nil
}

func objectMeta(k types.NamespacedName) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: k.Namespace, Name: k.Name}
}

// allocationToDeployment maps an Allocation to the ModelDeployment it is
// assigned to (spec.modelDeploymentRef holds the full name; the label may be
// shortened).
func allocationToDeployment(_ context.Context, o client.Object) []reconcile.Request {
	a, ok := o.(*v1alpha1.Allocation)
	if !ok || a.Spec.ModelDeploymentRef == nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Spec.ModelDeploymentRef.Name}}}
}

// instanceToDeployment maps an Instance to its controller owner, also once the
// owner is gone, so its drain still runs.
func instanceToDeployment(_ context.Context, o client.Object) []reconcile.Request {
	for _, ref := range o.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller && ref.Kind == "ModelDeployment" {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: ref.Name}}}
		}
	}
	return nil
}

// deploymentsAllowing returns every ModelDeployment that allows one of the
// Providers, by name or by selector.
func deploymentsAllowing(ctx context.Context, c client.Client, providers map[string]bool) []reconcile.Request {
	var mds v1alpha1.ModelDeploymentList
	if err := c.List(ctx, &mds); err != nil {
		return nil
	}
	var provs v1alpha1.ProviderList
	if err := c.List(ctx, &provs); err != nil {
		return nil
	}
	byName := map[string]labels.Set{}
	for _, p := range provs.Items {
		byName[p.Name] = p.Labels
	}
	var out []reconcile.Request
	for _, md := range mds.Items {
		names, err := selectedProviderNames(md.Spec.Providers, provs.Items)
		if err != nil {
			continue
		}
		for _, n := range names {
			if providers[n] {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: md.Namespace, Name: md.Name}})
				break
			}
		}
	}
	return out
}

// deploymentsInNamespace returns every ModelDeployment of a namespace.
func deploymentsInNamespace(ctx context.Context, c client.Client, ns string) []reconcile.Request {
	var mds v1alpha1.ModelDeploymentList
	if err := c.List(ctx, &mds, client.InNamespace(ns)); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(mds.Items))
	for _, md := range mds.Items {
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: md.Namespace, Name: md.Name}})
	}
	return out
}

// deploymentsOnProfile returns every ModelDeployment that allows a Provider of
// the profile.
func deploymentsOnProfile(ctx context.Context, c client.Client, profile string) []reconcile.Request {
	var provs v1alpha1.ProviderList
	if err := c.List(ctx, &provs); err != nil {
		return nil
	}
	set := map[string]bool{}
	for _, p := range provs.Items {
		if p.Spec.PlatformProfileRef.Name == profile {
			set[p.Name] = true
		}
	}
	if len(set) == 0 {
		return nil
	}
	return deploymentsAllowing(ctx, c, set)
}

// readiness tracks whether the controller serves agents: it leads, and its
// caches are synced. It runs only on the leader, so /readyz fails until the
// caches have synced and the Lease is held (`@hpc-burst-science-agent`, 31 Oct
// 2026, for #111: leading counts). Alpha runs one controller; a standby
// replica after alpha would need a readiness state of its own, since this one
// would call it not ready.
type readiness struct {
	mgr manager.Manager
	ok  atomic.Bool
}

func (r *readiness) Start(ctx context.Context) error {
	if !r.mgr.GetCache().WaitForCacheSync(ctx) {
		return errors.New("lifecycle: caches did not sync")
	}
	r.ok.Store(true)
	<-ctx.Done()
	r.ok.Store(false)
	return nil
}

func (r *readiness) NeedLeaderElection() bool { return true }

func (r *readiness) serving() bool { return r.ok.Load() }

func (r *readiness) check() error {
	if !r.ok.Load() {
		return errors.New("not leading, or caches not synced")
	}
	return nil
}

// agentBuilds hands out the SHA-256 of each agent build, read once per file.
type agentBuilds struct {
	dir string
	mu  sync.Mutex
	sum map[string]buildSum
}

// buildSum is a build's digest with the size and modification time it was
// read at, so a replaced build is read again without a restart.
type buildSum struct {
	size    int64
	modTime time.Time
	digest  string
}

func (b *agentBuilds) digest(cpuArch string) (string, error) {
	if b.dir == "" {
		return "", errors.New("no agent builds directory is configured")
	}
	path := filepath.Join(b.dir, agentapi.AgentBuildName(cpuArch))
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := b.sum[cpuArch]; ok && c.size == fi.Size() && c.modTime.Equal(fi.ModTime()) {
		return c.digest, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	if b.sum == nil {
		b.sum = map[string]buildSum{}
	}
	d := hex.EncodeToString(h.Sum(nil))
	b.sum[cpuArch] = buildSum{size: fi.Size(), modTime: fi.ModTime(), digest: d}
	return d, nil
}

// collectorRegistration registers the lifecycle controller's collectors, the
// engine gauges and the accounting gauges, while the manager runs, on every
// replica (the engine gauges' inventory is empty but on the leader), and
// unregisters them at the stop, so a manager started again in the same process
// registers afresh.
type collectorRegistration struct {
	reg prometheus.Registerer
	cs  []prometheus.Collector
}

func (m *collectorRegistration) Start(ctx context.Context) error {
	for i, c := range m.cs {
		if err := m.reg.Register(c); err != nil {
			for _, done := range m.cs[:i] {
				m.reg.Unregister(done)
			}
			return fmt.Errorf("lifecycle: register the metrics collectors: %w", err)
		}
	}
	<-ctx.Done()
	for _, c := range m.cs {
		m.reg.Unregister(c)
	}
	return nil
}

func (m *collectorRegistration) NeedLeaderElection() bool { return false }
