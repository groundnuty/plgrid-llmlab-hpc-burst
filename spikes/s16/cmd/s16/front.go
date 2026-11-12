package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// runFront runs the front, unchanged (front.Run), in direct or host-dialed
// mode, over the configuration's fixture served by a fake dynamic client: no
// control plane, no Lease (Kube nil). It builds its own Tunnel through frps,
// its Prober, its frps plugin on the health address, its proxy on the client
// listener and its dial-home listener with the /~!frp passthrough. What this
// command adds is only observation:
//   - -probes: every probe's result, with the UID's registration epoch after it;
//   - -reg: every change of a UID's registration (registered or not) and its epoch,
//     read when the Tunnel tells its subscribers;
//   - -calls: each call to the front's frps plugin, timed at its listener as the
//     admission plugin's are;
//   - -life: when it served, when it was told to stop, and when it returned.
func runFront(args []string) error {
	fs := flag.NewFlagSet("front", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the spec")
	probesPath := fs.String("probes", "", "every probe, one JSON line each")
	regPath := fs.String("reg", "", "every registration change, one JSON line each")
	callsPath := fs.String("calls", "", "every call to the front's frps plugin")
	lifePath := fs.String("life", "", "the front's life events")
	logPath := fs.String("log", "", "the front's own log")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *probesPath == "" || *regPath == "" || *callsPath == "" || *lifePath == "" || *logPath == "" {
		return errors.New("-probes, -reg, -calls, -life and -log are required")
	}
	cfg, err := front.LoadConfig(sp.FrontConfig)
	if err != nil {
		return err
	}
	objs, err := fixtureObjects(sp, time.Now())
	if err != nil {
		return err
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log := slog.New(slog.NewTextHandler(lf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	probes, err := openLines(*probesPath)
	if err != nil {
		return err
	}
	defer probes.Close()
	reg, err := openLines(*regPath)
	if err != nil {
		return err
	}
	defer reg.Close()
	life, err := openLines(*lifePath)
	if err != nil {
		return err
	}
	defer life.Close()
	calls, err := openCallLog(*callsPath)
	if err != nil {
		return err
	}
	defer calls.Close()
	stopFlush := make(chan struct{})
	defer close(stopFlush)
	go calls.flushEvery(time.Second, stopFlush)
	go probes.flushEvery(time.Second, stopFlush)
	go reg.flushEvery(time.Second, stopFlush)

	index := map[types.UID]int{}
	for _, t := range sp.Tunnels {
		index[types.UID(t.UID)] = t.Index
	}
	hl, err := net.Listen("tcp4", sp.FrontHealth)
	if err != nil {
		return err
	}
	rec := &probeRecorder{w: probes, index: index}
	ready := make(chan *front.Front, 1)
	o := front.Options{Config: cfg, Host: agentapi.Host{StateDir: sp.StateDir, PortBase: sp.PortBase}, Mode: sp.Mode,
		Dynamic: fixtureClient(objs), Recorder: &countRecorder{log: log}, PathEvents: rec, Log: log,
		HealthListener: newTimedListener(hl, "front", calls), Ready: func(f *front.Front) { ready <- f }}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- front.Run(ctx, o) }()
	select {
	case f := <-ready:
		rec.setTunnel(f)
		watchRegistrations(f, index, reg)
		life.add(map[string]any{"event": "ready", "t_ns": time.Now().UnixNano(), "pid": os.Getpid()})
		life.Flush()
	case err := <-ran:
		return err
	}
	<-ctx.Done()
	life.add(map[string]any{"event": "term", "t_ns": time.Now().UnixNano()})
	life.Flush()
	err = <-ran
	life.add(map[string]any{"event": "exit", "t_ns": time.Now().UnixNano()})
	return err
}

// watchRegistrations writes each UID's registration change, read whenever the
// Tunnel calls its subscribers (a probe's verdict that changes it, or a
// plugin's Bump of a registered UID); a Bump of an unregistered UID moves only
// its epoch, which the probe log carries.
func watchRegistrations(f *front.Front, index map[types.UID]int, w *lines) {
	if f.Tunnel == nil {
		return
	}
	var mu sync.Mutex
	last := map[types.UID]bool{}
	scan := func() {
		mu.Lock()
		defer mu.Unlock()
		now := time.Now().UnixNano()
		for uid, i := range index {
			r := f.Tunnel.Registered(uid)
			if r != last[uid] {
				last[uid] = r
				w.add(map[string]any{"t_ns": now, "i": i, "reg": r, "ep": f.Tunnel.Epoch(uid)})
			}
		}
	}
	f.Tunnel.Subscribe(scan)
	scan()
}

// probeRecorder is the front's PathEvents: every probe, as one line.
type probeRecorder struct {
	w     *lines
	index map[types.UID]int
	mu    sync.Mutex
	f     *front.Front
}

func (p *probeRecorder) setTunnel(f *front.Front) {
	p.mu.Lock()
	p.f = f
	p.mu.Unlock()
}

func (p *probeRecorder) Probed(r front.ProbeResult) {
	p.mu.Lock()
	f := p.f
	p.mu.Unlock()
	var ep uint64
	if f != nil && f.Tunnel != nil {
		ep = f.Tunnel.Epoch(r.UID)
	}
	rec := map[string]any{"t_ns": time.Now().UnixNano(), "i": p.index[r.UID], "pass": r.Passed, "reg": r.Registered,
		"ep": ep, "ms": float64(r.Duration.Microseconds()) / 1000, "status": r.Status}
	if r.Error != "" {
		e := r.Error
		if len(e) > 160 {
			e = e[:160]
		}
		rec["err"] = e
	}
	p.w.add(rec)
}

func (p *probeRecorder) PathProbeFailing(_ *v1alpha1.Instance, r front.ProbeResult) {
	p.w.add(map[string]any{"t_ns": time.Now().UnixNano(), "i": p.index[r.UID], "path_probe_failing": true})
}

// countRecorder logs the front's events instead of sending them to an API
// server, which this configuration does not have.
type countRecorder struct{ log *slog.Logger }

func (c *countRecorder) Event(_ runtime.Object, warning bool, typ, code, message string) {
	c.log.Info("event", "warning", warning, "type", typ, "code", code, "message", message)
}

// lines is a buffered file of JSON lines.
type lines struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func openLines(path string) (*lines, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &lines{f: f, w: bufio.NewWriterSize(f, 64<<10)}, nil
}

func (l *lines) add(v any) {
	b, _ := json.Marshal(v)
	l.mu.Lock()
	_, _ = l.w.Write(append(b, '\n'))
	l.mu.Unlock()
}

func (l *lines) Flush() {
	l.mu.Lock()
	_ = l.w.Flush()
	l.mu.Unlock()
}

func (l *lines) Close() error {
	l.Flush()
	return l.f.Close()
}

func (l *lines) flushEvery(d time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.Flush()
		case <-stop:
			return
		}
	}
}

// jsonLine writes v as one JSON line, unbuffered.
func jsonLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func newSlog(w io.Writer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }
