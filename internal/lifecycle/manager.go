package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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
	// LeaderElection takes the Lease v1alpha1.LeaseLifecycle in
	// v1alpha1.ControllerNamespace, a guard against a stale second copy.
	LeaderElection bool
	// Workers is the work queue's worker count; 0 means DefaultWorkers.
	Workers int
	// Checkpoints reads checkpoints; nil means the Hub, cached.
	Checkpoints CheckpointSource
	// Clock is the time source; nil means SystemClock.
	Clock Clock
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

// NewManager builds the lifecycle controller: one controller-runtime manager
// whose work queue is keyed per ModelDeployment, and the agent Backend that the
// agent API server calls (HBEP-7, Controllers). Metrics are not served: the
// default listener binds every interface.
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
	mgr, err := ctrl.NewManager(o.Config, manager.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:        health,
		LeaderElection:                o.LeaderElection,
		LeaderElectionID:              v1alpha1.LeaseLifecycle,
		LeaderElectionNamespace:       v1alpha1.ControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
	})
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
		WatchesRawSource(source.Channel(events, &handler.TypedEnqueueRequestForObject[*v1alpha1.ModelDeployment]{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers}).
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
// caches are synced. It runs only on the leader.
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
	sum map[string]string
}

func (b *agentBuilds) digest(cpuArch string) (string, error) {
	if b.dir == "" {
		return "", errors.New("no agent builds directory is configured (--agent-builds-dir)")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if d, ok := b.sum[cpuArch]; ok {
		return d, nil
	}
	path := filepath.Join(b.dir, agentapi.AgentBuildName(cpuArch))
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
		b.sum = map[string]string{}
	}
	d := hex.EncodeToString(h.Sum(nil))
	b.sum[cpuArch] = d
	return d, nil
}
