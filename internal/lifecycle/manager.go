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
	toolscache "k8s.io/client-go/tools/cache"
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

// ShutdownGrace is the lifecycle manager's graceful shutdown timeout: how long
// its stop waits for its runnables, the Provider supervisors and their
// managers among them, before it returns. It is controller-runtime's default
// (v0.23.1, pkg/manager/internal.go:57), set here so that the controller's
// stop budget (launcher.ControllerStopBudget) can name it. The lifecycle
// Lease's release comes after it.
const ShutdownGrace = 30 * time.Second

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
	grace := ShutdownGrace
	mopts := manager.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        health,
		LeaderElection:                o.LeaderElection,
		LeaderElectionID:              v1alpha1.LeaseLifecycle,
		LeaderElectionNamespace:       v1alpha1.ControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		GracefulShutdownTimeout:       &grace,
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
	c := mgr.GetClient()
	watches := lifecycleWatches(c)
	kinds := []client.Object{&v1alpha1.ModelDeployment{}}
	for _, w := range watches {
		kinds = append(kinds, w.obj)
	}
	ready := &readiness{cache: mgr.GetCache(), elected: mgr.Elected(), leaderElection: o.LeaderElection, kinds: kinds}
	if err := mgr.Add(ready); err != nil {
		return nil, nil, err
	}
	if err := mgr.AddReadyzCheck(ReadyzCaches, func(*http.Request) error { return ready.checkCaches() }); err != nil {
		return nil, nil, err
	}
	if err := mgr.AddReadyzCheck(ReadyzLeading, func(*http.Request) error { return ready.checkLeading() }); err != nil {
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
	b := ctrl.NewControllerManagedBy(mgr).
		Named("lifecycle").
		For(&v1alpha1.ModelDeployment{})
	for _, w := range watches {
		b = b.Watches(w.obj, w.handler)
	}
	err = b.
		WatchesRawSource(source.Channel(events, &handler.TypedEnqueueRequestForObject[*v1alpha1.ModelDeployment]{})).
		WithOptions(controllerOptions(workers)).
		Complete(r)
	if err != nil {
		return nil, nil, fmt.Errorf("lifecycle: controller: %w", err)
	}
	return mgr, backend, nil
}

// lifecycleWatch is a kind the lifecycle controller watches besides the
// ModelDeployments it is For, and the handler that maps it to deployments.
type lifecycleWatch struct {
	obj     client.Object
	handler handler.EventHandler
}

// lifecycleWatches are the controller's Watches. One list serves both the
// builder in NewManager and the readiness check "caches", which starts the
// informers of these kinds, and of ModelDeployment, before the controller is
// elected (readiness.Start); a kind added to the controller is therefore added
// to the check at the same line.
func lifecycleWatches(c client.Client) []lifecycleWatch {
	return []lifecycleWatch{
		{&v1alpha1.Allocation{}, handler.EnqueueRequestsFromMapFunc(allocationToDeployment)},
		{&v1alpha1.Instance{}, handler.EnqueueRequestsFromMapFunc(instanceToDeployment)},
		{&v1alpha1.Provider{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return deploymentsAllowing(ctx, c, map[string]bool{o.GetName(): true})
		})},
		{&v1alpha1.PlatformProfile{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return deploymentsOnProfile(ctx, c, o.GetName())
		})},
		// An identity record that changes re-evaluates a collision with it
		// (HBEP-3): every deployment of its namespace is enqueued.
		{&appsv1.ControllerRevision{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if o.GetLabels()[v1alpha1.LabelRecord] != v1alpha1.RecordIdentity {
				return nil
			}
			return deploymentsInNamespace(ctx, c, o.GetNamespace())
		})},
	}
}

// controllerOptions are the lifecycle controller's work-queue options.
//
// A failing key retries with backoff capped at the requeue bound, not
// controller-runtime's 1000 s, so one Instance's failing write does not hold
// the deployment back for minutes.
//
// The queue is client-go's, not controller-runtime's priority queue (its
// default). The priority queue of controller-runtime v0.23.1 can deadlock on
// shutdown: after the cancel, handleReadyItems still counts a worker that has
// left GetWithPriority, sends it an item on an unbuffered channel and blocks
// there holding the queue's locks; a worker that then finishes its reconcile
// waits in Done forever, and the manager gives up after its 30 s grace with
// "failed waiting for all runnables to end" (issue #136). Upstream fixed it
// in https://github.com/kubernetes-sigs/controller-runtime/pull/3540, first
// released in v0.25.0; no v0.23 release has the fix.
func controllerOptions(workers int) controller.Options {
	priorityQueue := false
	return controller.Options{
		MaxConcurrentReconciles: workers,
		RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, maxRequeue),
		UsePriorityQueue:        &priorityQueue,
		SkipNameValidation:      new(true), // a second manager in one process, as in a test's -count=N
	}
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

// The names of the lifecycle controller's two readiness checks, each served on
// its own at /readyz/<name> besides the aggregate /readyz. They are an
// interface: `hpcburst status` reads them by status code to say "starting" or
// "not leading" (internal/launcher, readyz.go), and HBEP-11 names them. A
// controller older than the split registered the one check "serving", which
// the launcher does not know to tell the two apart.
const (
	// ReadyzCaches passes once the informers of every kind the controller
	// watches have synced. It does not depend on the Lease.
	ReadyzCaches = "caches"
	// ReadyzLeading passes while the controller leads its Lease, and always
	// under --leader-elect=false (`@hpc-burst-science-agent`, 1 Nov 2026, #147).
	ReadyzLeading = "leading"
)

// cacheRetry is the wait between two tries to start an informer that the API
// server cannot serve yet (a CRD not installed): controller-runtime's own
// Kind source retries in the same way. The check stays failing meanwhile.
const cacheRetry = 2 * time.Second

// readiness tracks whether the controller serves agents, as two facts that
// /readyz reports on their own so that a failing one can be told from the
// other: its caches have synced, and it leads (`@hpc-burst-science-agent`,
// 31 Oct 2026, for #111: leading counts; 1 Nov 2026, #147: told apart).
//
// It is not a leader-election runnable. The manager starts such a runnable
// only once the Lease is held, and then "caches" could not be told from
// "leading". It starts with the other runnables after the manager's caches
// have started (controller-runtime v0.23.1, pkg/manager/internal.go:
// Caches.Start, then Others.Start, then the leader election). The caches hold
// no informer yet by then: the controller's sources start them only on being
// elected, and WaitForCacheSync with none is vacuously true. So it starts the
// informer of each kind the controller watches itself (the shared cache hands
// the controller the same ones later) and waits for each of those informers:
// "caches" is true only when the controller could read what it watches
// (`@hpc-burst-science-agent`, 1 Nov 2026, #147: eager start accepted).
//
// Alpha runs one controller; a standby replica after alpha would need a
// readiness state of its own, since this one would call it not ready.
type readiness struct {
	cache cache.Cache
	// elected is the manager's Elected channel: closed once the Lease is held,
	// or at once without leader election (pkg/manager/internal.go:476-480
	// without, :620-625 with).
	elected        <-chan struct{}
	leaderElection bool
	kinds          []client.Object
	// retry is the wait between two tries to start an informer; zero means
	// cacheRetry.
	retry  time.Duration
	synced atomic.Bool
}

func (r *readiness) Start(ctx context.Context) error {
	defer r.synced.Store(false)
	// Start every kind's informer without waiting, then wait for each of these informers itself:
	// controller-runtime v0.23.1's cache.WaitForCacheSync waits only for the informers that exist,
	// so on its own it passes with none of ours started or synced, and it would also wait for
	// informers the controller does not watch. Starting them all first lets the six sync together.
	var synced []toolscache.InformerSynced
	for _, k := range r.kinds {
		inf, ok := r.startInformer(ctx, k)
		if !ok {
			return nil // a stop, not a failure of the controller
		}
		synced = append(synced, inf.HasSynced)
	}
	if !toolscache.WaitForCacheSync(ctx.Done(), synced...) {
		return nil // the manager is stopping
	}
	r.synced.Store(true)
	<-ctx.Done()
	return nil
}

// startInformer returns the cache's informer for k without waiting for it to sync, trying again
// while the API server cannot serve the kind. It reports false when ctx ends first.
func (r *readiness) startInformer(ctx context.Context, k client.Object) (cache.Informer, bool) {
	retry := r.retry
	if retry == 0 {
		retry = cacheRetry
	}
	for {
		inf, err := r.cache.GetInformer(ctx, k, cache.BlockUntilSynced(false))
		if err == nil {
			return inf, true
		}
		select {
		case <-time.After(retry):
		case <-ctx.Done():
			return nil, false
		}
	}
}

// NeedLeaderElection is false: see readiness.
func (r *readiness) NeedLeaderElection() bool { return false }

func (r *readiness) isElected() bool {
	select {
	case <-r.elected:
		return true
	default:
		return false
	}
}

// serving is what the Backend answers agents by: the caches have synced and
// the controller is elected, with or without leader election.
func (r *readiness) serving() bool { return r.synced.Load() && r.isElected() }

// checkCaches is the readiness check ReadyzCaches.
func (r *readiness) checkCaches() error {
	if !r.synced.Load() {
		return errors.New("caches not synced")
	}
	return nil
}

// checkLeading is the readiness check ReadyzLeading. Without leader election
// there is no Lease to lead, so it passes, even before Elected closes
// (`@hpc-burst-science-agent`, 1 Nov 2026, #147).
func (r *readiness) checkLeading() error {
	if !r.leaderElection || r.isElected() {
		return nil
	}
	return errors.New("not leading its Lease")
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
