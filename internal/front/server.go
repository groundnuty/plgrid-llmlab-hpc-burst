package front

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Options are the inputs of Run.
type Options struct {
	Config  Config
	Host    agentapi.Host
	Mode    string // ModeDirect, ModeSSH, ModeHostDialed, or "" when the launcher recorded no front
	Clock   clock.Clock
	Dynamic dynamic.Interface
	// Kube, when set, is the client for the front's heartbeat Lease. Nil: the
	// front keeps no Lease (a test, or a run without a control plane), and a
	// usage failure raises no event, as it has no Lease to regard.
	Kube       kubernetes.Interface
	Recorder   Recorder
	Transports Transports // nil: HBEP-6's Tunnel through frps, registered by the front's probe
	// PathEvents receives the probe's results when the front builds its own
	// Tunnel; nil raises HBEP-6's degraded event, reason PathProbeFailing,
	// through Recorder (recorderPathEvents), as the command does.
	PathEvents PathEvents
	Log        *slog.Logger
	// The listeners. Nil ones are opened on the configured addresses; the
	// health listener is opened only when HealthAddr is set.
	ClientListener, DialHomeListener, HealthListener net.Listener
	HealthAddr                                       string
	// Ready, when set, is called once the front serves.
	Ready func(*Front)
	// ShutdownGrace is how long a stop waits for requests in flight before
	// it cuts them; 0 means DefaultShutdownGrace.
	ShutdownGrace time.Duration
	// FreeBytes reads the free space behind hpcburst_usage_dir_free_bytes;
	// nil is statfs. A test moves it.
	FreeBytes FreeBytesFunc
	// stop, when set, replaces stopServing: a test reads the two bounds Run
	// passes it (#432). Nil in production.
	stop stopFunc
	// BulkPath turns the bulk path on (beta; HBEP-6, The bulk path): the dial-home listener
	// serves the launcher's second certificate to the bulk server name and passes that name's
	// /~!frp to the second frps (dialhome_bulk.go). Off, the front is as before.
	BulkPath bool
}

// stopFunc is stopServing's signature, for Options.stop.
type stopFunc func(servers []*http.Server, cancelBase context.CancelCauseFunc, p *Proxy, shutdownGrace, handlerWait time.Duration, log *slog.Logger)

// DefaultShutdownGrace is how long a stop lets requests in flight finish.
const DefaultShutdownGrace = 10 * time.Second

// Front is a running front, for its health endpoint and for tests.
type Front struct {
	Router  *Router
	Watch   *inventory.Watch
	Proxy   *Proxy
	Sink    *usage.Sink
	Metrics *Metrics
	// Tunnel is HBEP-6's transport, when the front built its own, and Prober
	// its end-to-end probe.
	Tunnel *Tunnel
	Prober *Prober
	Client net.Addr
	Dial   net.Addr
	Health net.Addr
	// bulkFrp is the bulk path's /~!frp passthrough to the second frps; nil without the bulk path.
	bulkFrp *frpPassthrough
	// bulk is the bulk path's transport and prober; nil without the bulk path.
	bulk *bulkPath
}

// Run starts the front and serves until ctx ends: it reads its keys, opens
// the usage sink, waits for the control plane's first list, then opens its
// listeners and, when Options.Kube is set, starts renewing the front's
// heartbeat Lease (HBEP-8). On return the listeners are closed, the Lease
// keeper is stopped and the usage sink flushed. The health listener serves
// /healthz and /metrics.
// The client listener's server sets no read or write deadline: the front's
// own maxRequestDuration and idleTimeout bound each request, and its idle
// client connections are kept clientKeepAlive (HBEP-5, Time-outs and
// keep-alive).
func Run(ctx context.Context, o Options) error {
	cfg := o.Config
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	// With no dial-home mode recorded no job dials home, so frps's addresses
	// may be unknown; then no Instance can be reached and nothing is passed
	// to frps, as dialHomeTLS allows a plain listener then.
	frpsBind, frpsMux, ferr := cfg.FrpsAddrs(o.Host)
	if ferr != nil && ValidMode(o.Mode) {
		return ferr
	}
	var bulkBind, bulkMux string
	if o.BulkPath {
		if err := checkBulkMode(o.Mode); err != nil {
			return err
		}
		var err error
		if bulkBind, bulkMux, err = cfg.FrpsBulkAddrs(o.Host); err != nil {
			return err
		}
	}
	var tunnel *Tunnel
	switch {
	case o.Transports != nil:
	case ferr != nil:
		o.Transports = NewStaticTransports()
	default:
		tunnel = NewTunnel(frpsMux, cfg.TunnelSetupTimeout.D())
		o.Transports = tunnel
	}
	keys, err := ReadClientKeys(cfg.ClientKeysFile)
	if err != nil {
		return err
	}
	// Each failed batch is logged at Error; a record goes to the file of the
	// UTC day of the front's clock.
	sink, err := usage.OpenOptions(cfg.UsageDir, usage.Options{Logger: o.Log, Now: o.Clock.Now})
	if err != nil {
		return fmt.Errorf("usage records: %w", err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			o.Log.Error("usage sink", "error", err.Error())
		}
	}()
	// The front's /metrics exports the raised-events counter its recorder
	// counts into, with the front's series created at 0 (HBEP-8, Raised events).
	metrics := NewMetrics(sink, cfg.UsageDir, o.FreeBytes, raisedEvents(o.Recorder))
	router := NewRouter(cfg, o.Clock, o.Transports, o.Recorder, o.Log, metrics)
	watch, err := startWatch(ctx, o, router)
	if err != nil {
		return err
	}
	proxy := NewProxy(ProxyOptions{Config: cfg, Host: o.Host, Clock: o.Clock, Router: router, Transports: o.Transports, Keys: keys, Sink: sink, Metrics: metrics, Log: o.Log})
	f := &Front{Router: router, Watch: watch, Proxy: proxy, Sink: sink, Metrics: metrics, Tunnel: tunnel}
	if tunnel != nil {
		var stop func()
		f.Prober, stop = startProber(ctx, cfg, tunnel, o, watch)
		defer stop()
	}
	if o.BulkPath {
		// The bulk path's transport, pool and probe (bulk.go; HBEP-6, The bulk path).
		f.bulk = newBulkPath(cfg, bulkMux, o.Clock, func() []v1alpha1.Instance {
			snap, _ := watch.Snapshot()
			return snap.Instances
		}, o.Log)
		proxy.bulk = f.bulk
		metrics.enableBulk(f.bulk.tun.registeredCount)
		defer f.bulk.run(ctx)()
	}
	var frp *frpPassthrough
	if ferr == nil {
		frp = newFrpPassthrough(frpsBind, cfg.TunnelSetupTimeout.D(), o.Log)
	}
	if o.BulkPath {
		f.bulkFrp = newFrpPassthrough(bulkBind, cfg.TunnelSetupTimeout.D(), o.Log)
	}
	// The client server's base context: cancelled at a stop, after the
	// grace, so the requests still in flight are cut with front_shutdown.
	base, cancelBase := context.WithCancelCause(context.Background())
	defer cancelBase(nil)
	servers, err := listen(o, f, frp, base)
	if err != nil {
		return err
	}
	o.Log.Info("front serving", "client", f.Client.String(), "dialHome", f.Dial.String(), "mode", o.Mode)
	if o.Kube != nil {
		// The heartbeat Lease, and with it the ObservationGap of a restart.
		// It stops with ctx, and Run waits for it so nothing outlives Run.
		var keeper sync.WaitGroup
		keeper.Add(1)
		go func() {
			defer keeper.Done()
			k := newLeaseKeeper(o.Kube, o.Clock, o.Recorder, router.lease, o.Log)
			k.onHeld = func() {
				router.flushUsageEvent()
				// The probe's heartbeat is judged here, apart from its loop.
				if f.Prober != nil {
					f.Prober.raiseHeartbeatMissed(o.Recorder, router.lease)
				}
			}
			k.run(ctx)
		}()
		defer keeper.Wait()
	}
	if o.Ready != nil {
		o.Ready(f)
	}
	<-ctx.Done()
	grace := o.ShutdownGrace
	if grace <= 0 {
		grace = DefaultShutdownGrace
	}
	// One configured grace bounds both waits of a stop; #432 split them in
	// stopServing only so that a test can wait on state.
	stop := o.stop
	if stop == nil {
		stop = stopServing
	}
	stop(servers, cancelBase, proxy, grace, grace, o.Log)
	// The tunnels carried the requests that just ended.
	if frp != nil {
		frp.Close()
	}
	if f.bulkFrp != nil {
		f.bulkFrp.Close()
	}
	return nil // the deferred Close then flushes the usage sink
}

// stopServing stops the listeners and lets the requests in flight finish for
// shutdownGrace. Then it cuts the rest, cutBy front_shutdown: each stream gets
// one error event and each request answers, and it waits for their handlers
// for at most handlerWait, so every record is written before the usage sink
// closes and none is lost without a gap (@hpc-burst-science-agent, 31 Oct
// 2026; PR #95 review, Opus B5).
//
// The two bounds are separate arguments so that a test can keep a short
// shutdownGrace, which only makes it fast, and give handlerWait a generous
// bound: the wait ends as soon as the handlers have returned, so a slow
// runner cannot lose a record to a fixed window (#432). Run passes its one
// configured grace for both.
func stopServing(servers []*http.Server, cancelBase context.CancelCauseFunc, p *Proxy, shutdownGrace, handlerWait time.Duration, log *slog.Logger) {
	shut, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Shutdown(shut)
		}()
	}
	wg.Wait()
	cancelBase(errCutShutdown)
	if !p.waitHandlers(handlerWait) {
		log.Error("requests still run after the shutdown cut; their usage records may be lost", "waited", handlerWait.String())
	}
}

// startWatch starts the watch and the loop that feeds the router: after a
// burst of changes the router is rebuilt once from a fresh snapshot.
func startWatch(ctx context.Context, o Options, router *Router) (*inventory.Watch, error) {
	changed := make(chan struct{}, 1)
	watch := inventory.NewWatch(o.Dynamic, o.Config.Namespaces, func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	})
	if err := watch.Start(ctx); err != nil {
		return nil, err
	}
	update := func() {
		snap, problems := watch.Snapshot()
		for _, p := range problems {
			o.Log.Warn("object skipped", "problem", p)
		}
		router.Update(snap)
	}
	update()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-changed:
				update()
			}
		}
	}()
	return watch, nil
}

// startProber runs the probe of HBEP-6 over the watched Instances until ctx
// ends or the returned stop is called; stop returns once it has stopped. With
// no Options.PathEvents its PathProbeFailing goes to the front's Recorder.
func startProber(ctx context.Context, cfg Config, tunnel *Tunnel, o Options, watch *inventory.Watch) (*Prober, func()) {
	source := func() []v1alpha1.Instance {
		snap, _ := watch.Snapshot()
		return snap.Instances
	}
	events := o.PathEvents
	if events == nil {
		events = recorderPathEvents{rec: o.Recorder}
	}
	p := NewProber(cfg, tunnel, o.Clock, source, events, o.Log)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	return p, func() { cancel(); <-done }
}

// listen opens the three listeners and starts their servers. The dial-home
// listener passes /agent/v1/ to the agent endpoint in direct mode and frpc's
// websocket to frps in both modes (HBEP-6, Dial-home modes).
func listen(o Options, f *Front, frp *frpPassthrough, base context.Context) ([]*http.Server, error) {
	cfg := o.Config
	var servers []*http.Server
	var opened []net.Listener
	fail := func(err error) ([]*http.Server, error) {
		for _, l := range opened {
			l.Close()
		}
		return nil, err
	}
	// The health listener comes first: it serves the front's frps plugin, and
	// frps refuses every new proxy while that does not answer, so it must
	// answer before the dial-home listener lets any frpc reach frps (#371).
	if o.HealthListener != nil || o.HealthAddr != "" {
		hl, err := listenerOr(o.HealthListener, o.HealthAddr)
		if err != nil {
			return fail(fmt.Errorf("health: %w", err))
		}
		opened = append(opened, hl)
		f.Health = hl.Addr()
		servers = append(servers, serve(&http.Server{Handler: healthHandler(f, o.Log), ReadHeaderTimeout: 10 * time.Second}, hl, o.Log))
	} else if f.Tunnel != nil {
		o.Log.Warn("the front serves no health address, so not its frps plugin either: an frps that posts its NewProxy calls to the front refuses every new proxy (HBEP-6, Admission)")
	}
	cl, err := listenerOr(o.ClientListener, cfg.ClientListen)
	if err != nil {
		return fail(fmt.Errorf("clientListen: %w", err))
	}
	opened = append(opened, cl)
	var tunnel http.Handler // nil, not a nil *frpPassthrough, when there is none
	if frp != nil {
		tunnel = frp
	}
	dh, err := NewDialHome(o.Mode, o.Host, tunnel)
	if err != nil {
		return fail(err)
	}
	if f.bulkFrp != nil {
		dh.withBulk(f.bulkFrp)
	}
	tlsCfg, err := dialHomeTLS(o.Mode, o.Host)
	if err != nil {
		return fail(err)
	}
	if f.bulkFrp != nil {
		// One gate for both halves of the split, the handler above and the certificate here (the
		// review of #459): a bulk handler is never served without the second certificate.
		if tlsCfg, err = withBulkCertificate(tlsCfg, o.Host); err != nil {
			return fail(err)
		}
	}
	dialAddr, err := cfg.DialHomeAddr(o.Mode, o.Host)
	if err != nil && o.DialHomeListener == nil {
		return fail(err)
	}
	dl, err := listenerOr(o.DialHomeListener, dialAddr)
	if err != nil {
		return fail(fmt.Errorf("dialHomeListen: %w", err))
	}
	opened = append(opened, dl)
	if tlsCfg != nil {
		dl = tls.NewListener(dl, tlsCfg)
	}
	f.Client, f.Dial = cl.Addr(), dl.Addr()
	servers = append(servers, serve(clientServer(cfg, f.Proxy, base), cl, o.Log), serve(&http.Server{Handler: dh, ReadHeaderTimeout: time.Minute}, dl, o.Log))
	return servers, nil
}

// clientServer is the client listener's server: no read, write or total
// deadline of its own (each request's cuts bound its I/O, clientIO), idle
// connections kept clientKeepAlive. Its requests' contexts derive from base.
func clientServer(cfg Config, h http.Handler, base context.Context) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: time.Minute,
		IdleTimeout:       cfg.ClientKeepAlive.D(),
		BaseContext:       func(net.Listener) context.Context { return base },
	}
}

func listenerOr(l net.Listener, addr string) (net.Listener, error) {
	if l != nil {
		return l, nil
	}
	return net.Listen("tcp", addr)
}

func serve(s *http.Server, l net.Listener, log *slog.Logger) *http.Server {
	var once sync.Once
	go func() {
		if err := s.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			once.Do(func() { log.Error("listener stopped", "addr", l.Addr().String(), "error", err.Error()) })
		}
	}()
	return s
}

// usageHealth is the /healthz body's usage member (HBEP-8, Usage records,
// Failures are never silent).
type usageHealth struct {
	// State is "ok" or "failing": failing from a batch or counter block that
	// failed until a later batch is written and synced.
	State string `json:"state"`
	// LostRecords is the records not written since the front started: the
	// same count as hpcburst_usage_write_failures_total
	// (@hpc-burst-science-agent, 31 Oct 2026: named lostRecords).
	LostRecords uint64 `json:"lostRecords"`
	// Since is when the sink entered its state, in UTC.
	Since string `json:"since"`
	// Error is the latest failure; absent while the state is ok.
	Error string `json:"error,omitempty"`
}

func usageHealthOf(st usage.Stats) usageHealth {
	h := usageHealth{State: "ok", LostRecords: st.Lost, Since: usage.Time(st.Since)}
	if st.Err != nil {
		h.State, h.Error = "failing", st.Err.Error()
	}
	return h
}

// healthHandler serves GET /healthz: 200 once the watches have synced, with
// the age of the membership view and of each watch (HBEP-5, Operational
// readiness 1), and GET /metrics (HBEP-8, Metrics). A usage failure is stated
// in the body and never changes the status: the front keeps serving, and a
// readiness failure would restart it (HBEP-8, Usage records).
func healthHandler(f *Front, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", f.Metrics.Handler())
	if f.Tunnel != nil {
		// frps's second server plugin (frpsPlugin; HBEP-6, Admission).
		mux.Handle(agentapi.FrontFrpsPluginPath, &frpsPlugin{tun: f.Tunnel, prober: f.Prober, log: log})
	}
	if f.bulk != nil {
		// The second frps's front plugin, at its own path (HBEP-6, The bulk path).
		mux.Handle(agentapi.FrontFrpsBulkPluginPath, f.bulk.plugin(log))
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		st := f.Router.Status()
		ages := map[string]string{}
		for k, t := range f.Watch.LastEvents() {
			ages[k] = now.Sub(t).Round(time.Second).String()
		}
		body := map[string]any{
			"synced": f.Watch.Synced(), "viewAge": now.Sub(st.BuiltAt).Round(time.Second).String(),
			"watchAges": ages, "deployments": st.Deployments, "managedTargets": st.Managed, "waiting": st.Waiting,
			"instances": st.Instances, "leaseEnds": st.LeaseEnds,
		}
		// The probe's state per Instance, unknown while the probe is stale
		// (HBEP-8, Observers). It never changes the status.
		if f.Prober != nil {
			body["probe"] = f.Prober.Health()
		}
		// A sink that fails loses records: the body shows the state, the count
		// and the latest failure, the status stays 200, and each failure is
		// logged and raises a UsageWriteFailed event (@hpc-burst-science-agent,
		// 31 Oct 2026). usageError repeats the error at the top level.
		if f.Sink != nil {
			u := usageHealthOf(f.Sink.Stats())
			body["usage"] = u
			if u.Error != "" {
				body["usageError"] = u.Error
			}
		}
		code := http.StatusOK
		if !f.Watch.Synced() {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	})
	return mux
}
