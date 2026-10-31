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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Options are the inputs of Run.
type Options struct {
	Config  Config
	Host    agentapi.Host
	Mode    string // ModeDirect, ModeSSH, or "" when the launcher recorded no front
	Clock   clock.Clock
	Dynamic dynamic.Interface
	// Kube, when set, is the client for the front's heartbeat Lease. Nil: the
	// front keeps no Lease (a test, or a run without a control plane).
	Kube       kubernetes.Interface
	Recorder   Recorder
	Transports Transports // nil: a StaticTransports with no entry
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
}

// DefaultShutdownGrace is how long a stop lets requests in flight finish.
const DefaultShutdownGrace = 10 * time.Second

// Front is a running front, for its health endpoint and for tests.
type Front struct {
	Router  *Router
	Watch   *inventory.Watch
	Proxy   *Proxy
	Sink    *usage.Sink
	Metrics *Metrics
	Client  net.Addr
	Dial    net.Addr
	Health  net.Addr
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
	if o.Transports == nil {
		o.Transports = NewStaticTransports()
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
	metrics := NewMetrics(sink, cfg.UsageDir, o.FreeBytes)
	router := NewRouter(cfg, o.Clock, o.Transports, o.Recorder, o.Log, metrics)
	watch, err := startWatch(ctx, o, router)
	if err != nil {
		return err
	}
	proxy := NewProxy(ProxyOptions{Config: cfg, Host: o.Host, Clock: o.Clock, Router: router, Transports: o.Transports, Keys: keys, Sink: sink, Metrics: metrics, Log: o.Log})
	f := &Front{Router: router, Watch: watch, Proxy: proxy, Sink: sink, Metrics: metrics}
	// The client server's base context: cancelled at a stop, after the
	// grace, so the requests still in flight are cut with front_shutdown.
	base, cancelBase := context.WithCancelCause(context.Background())
	defer cancelBase(nil)
	servers, err := listen(o, f, base)
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
			newLeaseKeeper(o.Kube, o.Clock, o.Recorder, router.lease, o.Log).run(ctx)
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
	stopServing(servers, cancelBase, proxy, grace, o.Log)
	return nil // the deferred Close then flushes the usage sink
}

// stopServing stops the listeners and lets the requests in flight finish for
// grace. Then it cuts the rest, cutBy front_shutdown: each stream gets one
// error event and each request answers, and it waits for their handlers,
// so every record is written before the usage sink closes and none is lost
// without a gap (@hpc-burst-science-agent, 31 Oct 2026; PR #95 review, Opus
// B5).
func stopServing(servers []*http.Server, cancelBase context.CancelCauseFunc, p *Proxy, grace time.Duration, log *slog.Logger) {
	shut, cancel := context.WithTimeout(context.Background(), grace)
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
	if !p.waitHandlers(grace) {
		log.Error("requests still run after the shutdown cut; their usage records may be lost", "waited", grace.String())
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

// listen opens the three listeners and starts their servers.
func listen(o Options, f *Front, base context.Context) ([]*http.Server, error) {
	cfg := o.Config
	var servers []*http.Server
	var opened []net.Listener
	fail := func(err error) ([]*http.Server, error) {
		for _, l := range opened {
			l.Close()
		}
		return nil, err
	}
	cl, err := listenerOr(o.ClientListener, cfg.ClientListen)
	if err != nil {
		return fail(fmt.Errorf("clientListen: %w", err))
	}
	opened = append(opened, cl)
	dh, err := NewDialHome(o.Mode, o.Host, nil)
	if err != nil {
		return fail(err)
	}
	tlsCfg, err := dialHomeTLS(o.Mode, o.Host)
	if err != nil {
		return fail(err)
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
	if o.HealthListener != nil || o.HealthAddr != "" {
		hl, err := listenerOr(o.HealthListener, o.HealthAddr)
		if err != nil {
			return fail(fmt.Errorf("health: %w", err))
		}
		f.Health = hl.Addr()
		servers = append(servers, serve(&http.Server{Handler: healthHandler(f), ReadHeaderTimeout: 10 * time.Second}, hl, o.Log))
	}
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
	// Failures is the records not written since the front started: the same
	// count as hpcburst_usage_write_failures_total.
	Failures uint64 `json:"failures"`
	// Since is when the sink entered its state, in UTC.
	Since string `json:"since"`
	// Error is the latest failure; absent while the state is ok.
	Error string `json:"error,omitempty"`
}

func usageHealthOf(st usage.Stats) usageHealth {
	h := usageHealth{State: "ok", Failures: st.Lost, Since: usage.Time(st.Since)}
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
func healthHandler(f *Front) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", f.Metrics.Handler())
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
