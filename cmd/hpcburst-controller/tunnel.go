package main

// The control plane's half of HBEP-6's alpha tunnel (#90), in the lifecycle
// controller's manager. addTunnel, listed in extraRunnables, adds two
// components (the `component` type of components.go, which logs their start
// and stop):
//
//   - frps-plugin, in every copy of the binary like the agent API: frps's HTTP
//     server plugin (internal/frpsplugin.Handler) on agentapi.Host.FrpsPluginAddr
//     (base+8), answering from a MemIndex that a feeder keeps current from the
//     manager's cache of Allocations and Instances and the credential files
//     (HBEP-6, Admission);
//   - tunnel-state (tunnel_state.go), a leader component: it reads frps's proxy
//     list and writes each Instance's TunnelConnected condition (HBEP-6, Path
//     state and the inventory), so only the copy that leads the lifecycle Lease
//     writes.
//
// frpsplugin.Trigger joins them: the plugin tells it about every accepted
// NewProxy and every CloseProxy, and tunnel-state reads the list about a second
// after a signal. Both components take the manager's cache, clients and the
// Reader as narrow interfaces, so their loops run in tests with no manager.
//
// No component here is a controller (ctrl.NewControllerManagedBy): none has a
// work queue. The informer events only wake the feeder.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The feeder's timings: a credential file can appear with no object change, so
// the feeder refreshes every feedPeriod besides on each event, and never more
// often than feedMinGap under a burst of events. They are variables so that a
// test can shorten them; no test in this package runs in parallel, and each
// restores what it set once the run it started has returned.
var (
	feedPeriod = 2 * time.Second
	feedMinGap = 100 * time.Millisecond
)

// The plugin's server. The handler's own deadline is under a second and starts
// once the headers are read (frpsplugin.Handler); frps's client has no timeout
// of its own, so the server bounds the header read and the write.
const (
	pluginReadHeaderTimeout = 5 * time.Second
	pluginWriteTimeout      = 5 * time.Second
	pluginIdleTimeout       = 2 * time.Minute
	pluginShutdownGrace     = 2 * time.Second
)

// tunnelCache is what the components use of the manager's cache: the wait for
// its first sync, and the informers whose events wake the feeder. cache.Cache
// has both.
type tunnelCache interface {
	WaitForCacheSync(ctx context.Context) bool
	GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error)
}

var _ tunnelCache = cache.Cache(nil)

// addTunnel builds the tunnel's parts, adds the leader component tunnel-state
// to the manager itself, and returns the component frps-plugin for run to add:
// a leader component (`component.leader`) starts only in the copy that leads
// the lifecycle Lease, a plain one in every copy. frps need not run when this
// starts; the reads then say Unknown until it does.
func addTunnel(mgr manager.Manager, host agentapi.Host, _ agentapi.Backend) (manager.Runnable, error) {
	reader, err := frpsplugin.NewHostReader(host)
	if err != nil {
		return nil, err
	}
	trigger := frpsplugin.NewTrigger()
	index := &frpsplugin.MemIndex{}
	handler, err := frpsplugin.NewHandler(index, frpsplugin.Options{Notifier: trigger})
	if err != nil {
		return nil, err
	}
	base := ctrl.Log.WithName("hpcburst-controller").WithValues("portBase", host.PortBase)
	named := func(name string, kv ...any) logr.Logger {
		return base.WithValues(append([]any{"component", name}, kv...)...)
	}
	state := &pathState{
		cache: mgr.GetCache(), cached: mgr.GetClient(), fresh: mgr.GetAPIReader(), writer: mgr.GetClient(),
		reader: reader, trigger: trigger.C(), log: named("tunnel-state"),
	}
	if err := mgr.Add(component{name: "tunnel-state", log: state.log, leader: true, start: state.run}); err != nil {
		return nil, err
	}
	feedLog := named("frps-plugin", "addr", host.FrpsPluginAddr())
	server := &pluginServer{
		addr: host.FrpsPluginAddr(), handler: handler, log: feedLog,
		feed: &pluginFeed{cache: mgr.GetCache(), reader: mgr.GetClient(), index: index, credDir: host.CredentialsDir(), log: feedLog},
	}
	return component{name: "frps-plugin", log: feedLog, start: server.start}, nil
}

// pluginServer serves the plugin's handler and runs the feeder beside it.
type pluginServer struct {
	addr    string
	handler http.Handler
	feed    *pluginFeed
	log     logr.Logger
	// listen binds the endpoint; nil means addr on TCP. A test replaces it.
	listen func(ctx context.Context) (net.Listener, error)
}

// start binds the endpoint (a failure to bind is an error: frps would refuse
// every login with no plugin to ask), serves it, and runs the feeder, until ctx
// ends. A stop returns nil; a feeder or server that fails first is the error.
func (p *pluginServer) start(ctx context.Context) error {
	listen := p.listen
	if listen == nil {
		listen = func(ctx context.Context) (net.Listener, error) {
			var lc net.ListenConfig
			return lc.Listen(ctx, "tcp", p.addr)
		}
	}
	ln, err := listen(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped before it could bind
		}
		return fmt.Errorf("bind the plugin endpoint %s: %w", p.addr, err)
	}
	srv := &http.Server{
		Handler:           p.handler,
		ReadHeaderTimeout: pluginReadHeaderTimeout,
		WriteTimeout:      pluginWriteTimeout,
		IdleTimeout:       pluginIdleTimeout,
		ErrorLog:          slog.NewLogLogger(logr.ToSlogHandler(p.log), slog.LevelWarn),
	}
	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	served, fed := make(chan error, 1), make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	go func() { fed <- p.feed.run(feedCtx) }()

	var first error
	var served1, fed1 bool
	select {
	case <-ctx.Done():
	case err := <-served:
		served1, first = true, fmt.Errorf("serve the plugin endpoint: %w", err)
	case err := <-fed:
		fed1, first = true, feederReturned(ctx, err)
	}
	stopFeed()
	grace, cancel := context.WithTimeout(context.Background(), pluginShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(grace); err != nil {
		_ = srv.Close()
	}
	// Join both goroutines, so nothing outlives the component.
	if !served1 {
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) && first == nil {
			first = fmt.Errorf("serve the plugin endpoint: %w", err)
		}
	}
	if !fed1 {
		if err := <-fed; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// feederReturned says what the feeder's return means for the component. The feeder returns nil
// once its context ends, which is also what a stop does to the component's, so when both are
// ready select may pick either: nil with ctx done is the stop, and reading it as a failure
// would make a clean SIGTERM end the manager's run with an error. A return with ctx alive is the
// feeder's own failure, nil included.
func feederReturned(ctx context.Context, err error) error {
	if err == nil && ctx.Err() == nil {
		return errors.New("the plugin's feeder returned before the component was stopped")
	}
	return err
}

// pluginFeed keeps a MemIndex current (HBEP-6, Admission: the plugin answers
// from the controller's caches, and the credential files are read here, never
// inside a plugin call).
type pluginFeed struct {
	cache   tunnelCache
	reader  client.Reader
	index   frpsplugin.Feeder
	credDir string
	log     logr.Logger

	skipped map[frpsplugin.Skipped]struct{} // what the last refresh skipped, to log changes only
}

// run waits for the cache, then refreshes the index when an Allocation or an
// Instance changes (coalesced), and every feedPeriod: a credential file can
// appear with no object change. Only after the first refresh that the index
// accepted does it call MarkSynced, so the plugin refuses every Login until
// then (HBEP-6). It returns nil once ctx ends.
func (f *pluginFeed) run(ctx context.Context) error {
	if !f.cache.WaitForCacheSync(ctx) {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("frps plugin feed: the cache did not sync")
	}
	wake := make(chan struct{}, 1)
	handler := toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { kick(wake) },
		UpdateFunc: func(_, _ any) { kick(wake) },
		DeleteFunc: func(any) { kick(wake) },
	}
	// Registered before the first refresh, so that no change falls between the
	// list and the handler.
	for _, obj := range []client.Object{&v1alpha1.Allocation{}, &v1alpha1.Instance{}} {
		inf, err := f.cache.GetInformer(ctx, obj)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("frps plugin feed: the %T informer: %w", obj, err)
		}
		reg, err := inf.AddEventHandler(handler)
		if err != nil {
			return fmt.Errorf("frps plugin feed: watch %T: %w", obj, err)
		}
		defer func() { _ = inf.RemoveEventHandler(reg) }()
	}
	tick := time.NewTicker(feedPeriod)
	defer tick.Stop()
	synced := false
	for {
		switch err := f.refresh(ctx); {
		case err == nil && !synced:
			f.index.MarkSynced()
			synced = true
			f.log.Info("the plugin's index is loaded; logins are admitted")
		case err != nil && ctx.Err() == nil:
			f.log.Error(err, "the plugin's index was not refreshed; the last state stays")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(feedMinGap):
		}
	}
}

func kick(c chan<- struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// refresh lists the Allocations and Instances from the cache, builds the
// snapshot (frpsplugin.BuildSnapshot: ended Allocations absent, unreadable or
// shared credentials skipped) and swaps it in.
func (f *pluginFeed) refresh(ctx context.Context) error {
	var allocs v1alpha1.AllocationList
	if err := f.reader.List(ctx, &allocs); err != nil {
		return fmt.Errorf("list Allocations: %w", err)
	}
	var insts v1alpha1.InstanceList
	if err := f.reader.List(ctx, &insts); err != nil {
		return fmt.Errorf("list Instances: %w", err)
	}
	snap, skipped := frpsplugin.BuildSnapshot(allocs.Items, insts.Items, func(uid string) (string, error) {
		return agentapi.ReadKeyFile(agentapi.AllocationCredentialPath(f.credDir, uid))
	})
	if err := f.index.Replace(snap); err != nil {
		// BuildSnapshot never builds a snapshot Replace refuses; if this fires, the
		// two disagree, and the old state (which may admit an ended Allocation)
		// stays until the next refresh.
		return fmt.Errorf("the index refused the snapshot: %w", err)
	}
	f.logSkipped(skipped)
	return nil
}

// logSkipped names, by UID and reason (never a credential), what the last
// refresh skipped that the one before did not. An Allocation still Pending has
// no credential file yet, which is ordinary and goes to the debug level.
func (f *pluginFeed) logSkipped(skipped []frpsplugin.Skipped) {
	now := make(map[frpsplugin.Skipped]struct{}, len(skipped))
	for _, s := range skipped {
		now[s] = struct{}{}
		if _, seen := f.skipped[s]; seen {
			continue
		}
		log := f.log.WithValues("kind", s.Kind, "uid", s.UID, "reason", s.Reason)
		if s.Reason == frpsplugin.SkipNoCredentialFile {
			log.V(1).Info("an Allocation is not admitted yet")
		} else {
			log.Info("the plugin cannot admit this object")
		}
	}
	f.skipped = now
}
