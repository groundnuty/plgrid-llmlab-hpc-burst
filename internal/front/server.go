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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Options are the inputs of Run.
type Options struct {
	Config     Config
	Host       agentapi.Host
	Mode       string // ModeDirect, ModeSSH, or "" when the launcher recorded no front
	Clock      clock.Clock
	Dynamic    dynamic.Interface
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
}

// DefaultShutdownGrace is how long a stop lets requests in flight finish.
const DefaultShutdownGrace = 10 * time.Second

// Front is a running front, for its health endpoint and for tests.
type Front struct {
	Router *Router
	Watch  *inventory.Watch
	Proxy  *Proxy
	Sink   *usage.Sink
	Client net.Addr
	Dial   net.Addr
	Health net.Addr
}

// Run starts the front and serves until ctx ends: it reads its keys, opens
// the usage sink, waits for the control plane's first list, then opens its
// listeners. On return the listeners are closed and the usage sink flushed.
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
	sink, err := usage.Open(cfg.UsageDir)
	if err != nil {
		return fmt.Errorf("usage records: %w", err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			o.Log.Error("usage sink", "error", err.Error())
		}
	}()
	router := NewRouter(cfg, o.Clock, o.Transports, o.Recorder, o.Log)
	watch, err := startWatch(ctx, o, router)
	if err != nil {
		return err
	}
	proxy := NewProxy(ProxyOptions{Config: cfg, Host: o.Host, Clock: o.Clock, Router: router, Transports: o.Transports, Keys: keys, Sink: sink, Log: o.Log})
	f := &Front{Router: router, Watch: watch, Proxy: proxy, Sink: sink}
	// The client server's base context: cancelled at a stop, after the
	// grace, so the requests still in flight are cut with front_shutdown.
	base, cancelBase := context.WithCancelCause(context.Background())
	defer cancelBase(nil)
	servers, err := listen(o, f, base)
	if err != nil {
		return err
	}
	o.Log.Info("front serving", "client", f.Client.String(), "dialHome", f.Dial.String(), "mode", o.Mode)
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

// healthHandler serves GET /healthz: 200 once the watches have synced, with
// the age of the membership view and of each watch (HBEP-5, Operational
// readiness 1).
func healthHandler(f *Front) http.Handler {
	mux := http.NewServeMux()
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
		// A sink that failed loses records: shown here and logged with each
		// record it refuses. HBEP-5 does not say to refuse traffic for it,
		// so the front keeps serving.
		if f.Sink != nil {
			if err := f.Sink.Err(); err != nil {
				body["usageError"] = err.Error()
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
