package providerctl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// ManagerOptions are a Provider manager's leader-election timings and its
// shutdown grace; zero means controller-runtime's defaults (15 s, 10 s, 2 s;
// a 30 s grace).
type ManagerOptions struct {
	LeaseDuration, RenewDeadline, RetryPeriod time.Duration
	// GracefulShutdownTimeout bounds how long the manager waits for its
	// controllers when it stops. A manager that runs inside another, as in the
	// controller binary, needs one shorter than its parent's (review of PR
	// 132: N3).
	GracefulShutdownTimeout time.Duration
}

// NewProviderManager returns a controller-runtime manager for one Provider
// (HBEP-11, Controllers; docs/development.md, Process layout): its own leader
// Lease, v1alpha1.ProviderLeaseName(provider), in v1alpha1.ControllerNamespace,
// released when the manager stops; no metrics, health or pprof listener; and
// controller names that may repeat when the manager is started again.
func NewProviderManager(cfg *rest.Config, scheme *runtime.Scheme, provider string, o ManagerOptions) (manager.Manager, error) {
	skip := true
	mo := manager.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:        "0",
		PprofBindAddress:              "0",
		LeaderElection:                true,
		LeaderElectionID:              v1alpha1.ProviderLeaseName(provider),
		LeaderElectionNamespace:       v1alpha1.ControllerNamespace,
		LeaderElectionResourceLock:    resourcelock.LeasesResourceLock,
		LeaderElectionReleaseOnCancel: true,
		Controller:                    config.Controller{SkipNameValidation: &skip},
	}
	if o.LeaseDuration > 0 {
		mo.LeaseDuration = &o.LeaseDuration
	}
	if o.RenewDeadline > 0 {
		mo.RenewDeadline = &o.RenewDeadline
	}
	if o.RetryPeriod > 0 {
		mo.RetryPeriod = &o.RetryPeriod
	}
	if o.GracefulShutdownTimeout > 0 {
		mo.GracefulShutdownTimeout = &o.GracefulShutdownTimeout
	}
	return ctrl.NewManager(cfg, mo)
}

// Supervisor runs one manager per Provider of one kind, batch by default
// (docs/development.md: "A supervisor starts a manager when a Provider appears
// and stops it when the Provider goes, so one Provider's failure does not stop
// the others"). The controller binary's main (issue #61) calls Start, once per
// kind: a second Supervisor with Kind managed and #69's managed.Setup runs the
// managed Providers.
//
// Each manager runs in its own goroutine with its own context. A manager that
// cannot be built or set up, or that stops on its own, is started again with
// exponential backoff while its Provider exists; the other Providers' managers
// are untouched. A panic in the manager's Start is recovered as a failure;
// panics in goroutines a manager starts itself are outside its reach.
type Supervisor struct {
	// Kind is the providerKind of the PlatformProfiles whose Providers this
	// supervisor runs; "" means v1alpha1.PlatformBatch.
	Kind v1alpha1.PlatformKind
	// Client lists Providers and reads PlatformProfiles.
	Client client.Reader
	// NewManager builds the manager of one Provider, for example with
	// NewProviderManager.
	NewManager func(provider string) (manager.Manager, error)
	// Setup registers the Provider's controller with its manager, for
	// example with Setup.
	Setup func(mgr manager.Manager, provider string) error
	// Interval is how often the Providers are listed; zero means 5 s.
	Interval time.Duration
	// MinBackoff and MaxBackoff bound the restart backoff; zero means 1 s and
	// 1 min.
	MinBackoff, MaxBackoff time.Duration
	// Log; nil discards.
	Log logging.Logger

	mu   sync.Mutex
	runs map[string]*run
}

type run struct {
	cancel    context.CancelFunc
	done      chan struct{}
	err       error
	failures  int
	nextStart time.Time
	running   bool
	starts    int
}

// Status is one supervised Provider's state, for tests and diagnostics.
type Status struct {
	Provider string
	Running  bool
	Starts   int
	LastErr  error
}

// Statuses returns the state of every Provider the supervisor has seen.
func (s *Supervisor) Statuses() []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Status, 0, len(s.runs))
	for name, r := range s.runs {
		out = append(out, Status{Provider: name, Running: r.running, Starts: r.starts, LastErr: r.err})
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Provider < out[k].Provider })
	return out
}

// Start supervises until ctx is done, then stops every manager and waits for
// them. It returns nil after a clean stop.
func (s *Supervisor) Start(ctx context.Context) error {
	if s.Client == nil || s.NewManager == nil || s.Setup == nil {
		return errors.New("supervisor: Client, NewManager and Setup are required")
	}
	if s.Interval <= 0 {
		s.Interval = 5 * time.Second
	}
	if s.MinBackoff <= 0 {
		s.MinBackoff = time.Second
	}
	if s.MaxBackoff <= 0 {
		s.MaxBackoff = time.Minute
	}
	if s.Log == nil {
		s.Log = logging.NewNopLogger()
	}
	s.mu.Lock()
	s.runs = map[string]*run{}
	s.mu.Unlock()
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		if err := s.pass(ctx); err != nil && ctx.Err() == nil {
			s.Log.Info("cannot list the Providers; the running managers are kept", "error", err)
		}
		select {
		case <-ctx.Done():
			s.stopAll()
			return nil
		case <-t.C:
		}
	}
}

// desired returns the Providers of the supervisor's kind: those whose
// PlatformProfile exists and has that providerKind.
func (s *Supervisor) desired(ctx context.Context) (map[string]bool, error) {
	var list v1alpha1.ProviderList
	if err := s.Client.List(ctx, &list); err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, p := range list.Items {
		if !p.DeletionTimestamp.IsZero() {
			continue
		}
		var pp v1alpha1.PlatformProfile
		err := s.Client.Get(ctx, client.ObjectKey{Name: p.Spec.PlatformProfileRef.Name}, &pp)
		switch {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return nil, fmt.Errorf("get PlatformProfile %s: %w", p.Spec.PlatformProfileRef.Name, err)
		}
		if pp.Spec.ProviderKind == s.kind() {
			want[p.Name] = true
		}
	}
	return want, nil
}

func (s *Supervisor) kind() v1alpha1.PlatformKind {
	if s.Kind == "" {
		return v1alpha1.PlatformBatch
	}
	return s.Kind
}

// pass starts and stops managers to match the Providers of the kind.
func (s *Supervisor) pass(ctx context.Context) error {
	want, err := s.desired(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	var stop []*run
	for name, r := range s.runs {
		if !want[name] {
			stop = append(stop, r)
			delete(s.runs, name)
		}
	}
	s.mu.Unlock()
	for _, r := range stop {
		r.stop()
	}
	now := time.Now()
	for name := range want {
		s.mu.Lock()
		r := s.runs[name]
		if r == nil {
			r = &run{}
			s.runs[name] = r
		}
		start := !r.running && !now.Before(r.nextStart)
		s.mu.Unlock()
		if start {
			s.start(ctx, name, r)
		}
	}
	return nil
}

func (s *Supervisor) backoff(failures int) time.Duration {
	d := s.MinBackoff
	for i := 1; i < failures && d < s.MaxBackoff; i++ {
		d *= 2
	}
	if d > s.MaxBackoff {
		d = s.MaxBackoff
	}
	return d
}

// fail records a failure of the Provider's manager and schedules a restart.
func (s *Supervisor) fail(name string, r *run, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.running = false
	r.err = err
	r.failures++
	r.nextStart = time.Now().Add(s.backoff(r.failures))
	s.Log.Info("the Provider's manager stopped; it is started again after a backoff", "provider", name, "error", err, "in", s.backoff(r.failures))
}

func (s *Supervisor) start(ctx context.Context, name string, r *run) {
	mgr, err := s.NewManager(name)
	if err == nil {
		err = s.Setup(mgr, name)
	}
	s.mu.Lock()
	r.starts++
	s.mu.Unlock()
	if err != nil {
		s.fail(name, r, fmt.Errorf("build the manager: %w", err))
		return
	}
	mctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.mu.Lock()
	r.cancel, r.done, r.running = cancel, done, true
	s.mu.Unlock()
	go func() {
		defer close(done)
		var err error
		func() {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("the manager panicked: %v", p)
				}
			}()
			err = mgr.Start(mctx)
		}()
		if mctx.Err() != nil {
			s.mu.Lock()
			r.running = false
			s.mu.Unlock()
			return // stopped on purpose
		}
		if err == nil {
			err = errors.New("the manager stopped on its own")
		}
		s.fail(name, r, err)
	}()
}

// stopAll stops every manager and waits for them. They stop at once, not one
// after another: each manager's context derives from Start's (start), so all
// of them see it end at the same moment, each starting its own grace
// (ManagerOptions.GracefulShutdownTimeout) then, and the supervisor's stop
// lasts as long as the slowest manager's, at most one grace, not one per
// Provider (review of PR 132: N3; TestSupervisorStopsEveryManagerAtOnce).
func (s *Supervisor) stopAll() {
	s.mu.Lock()
	runs := s.runs
	s.runs = map[string]*run{}
	s.mu.Unlock()
	for _, r := range runs {
		r.stop()
	}
}

// stop cancels a run's manager and waits for it. A run whose manager was never
// built or set up (NewManager or Setup failed) has no cancel and no done, so
// there is nothing to stop; one Provider's failed start never panics the
// supervisor the others share.
func (r *run) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
}
