package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frpsStopGrace is how long a stop waits after TERM before KILL.
const frpsStopGrace = 5 * time.Second

// frpsEvent is one step of frps's life under the supervisor.
type frpsEvent struct {
	Event  string `json:"event"` // started, ready, stop, stopped
	T      int64  `json:"t_ns"`
	TookMs int64  `json:"took_ms,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// runFrps runs frps as the launcher does (launcher.ResolveFrpsFile, which
// checks the binary against frp-pins.txt, StartFrps and FrpsProc.Stop), and
// restarts it on SIGUSR1: a stop the launcher's way (TERM, KILL after the
// grace), then a start on the same configuration and ports. SIGTERM stops it
// and ends. Each step goes to -events with its time.
func runFrps(args []string) error {
	fs := flag.NewFlagSet("frps", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the spec")
	bin := fs.String("bin", "", "the frps binary")
	eventsPath := fs.String("events", "", "frps's events, one JSON line each")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *bin == "" || *eventsPath == "" {
		return errors.New("-bin and -events are required")
	}
	ev, err := os.OpenFile(*eventsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer ev.Close()
	emit := func(e frpsEvent) {
		if e.T == 0 {
			e.T = time.Now().UnixNano()
		}
		_ = jsonLine(ev, e)
	}
	host := agentapi.Host{StateDir: sp.StateDir, PortBase: sp.PortBase}
	start := func() (*launcher.FrpsProc, error) {
		b, err := launcher.ResolveFrpsFile(*bin, launcher.DefaultFrpVersion)
		if err != nil {
			return nil, err
		}
		defer b.Close()
		t0 := time.Now()
		p, err := launcher.StartFrps(launcher.FrpsSpec{Bin: b, Config: sp.FrpsConfig, Log: launcher.FrpsLogFile(host), Dir: sp.Dir})
		if err != nil {
			return nil, err
		}
		emit(frpsEvent{Event: "started", T: t0.UnixNano()})
		if err := waitHealthz("http://"+host.FrpsWebAddr()+"/healthz", 30*time.Second); err != nil {
			return p, err
		}
		emit(frpsEvent{Event: "ready", TookMs: time.Since(t0).Milliseconds()})
		return p, nil
	}
	stop := func(p *launcher.FrpsProc) error {
		emit(frpsEvent{Event: "stop"})
		rep, err := p.Stop(context.Background(), frpsStopGrace)
		emit(frpsEvent{Event: "stopped", TookMs: rep.TookMS, Detail: rep.Signals})
		return err
	}
	p, err := start()
	if err != nil {
		if p != nil {
			_ = stop(p)
		}
		return err
	}
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGUSR1, syscall.SIGTERM, syscall.SIGINT)
	for s := range sig {
		if err := stop(p); err != nil {
			return fmt.Errorf("stop frps: %w", err)
		}
		if s != syscall.SIGUSR1 {
			return nil
		}
		if p, err = start(); err != nil {
			if p != nil {
				_ = stop(p)
			}
			return fmt.Errorf("restart frps: %w", err)
		}
	}
	return nil
}

// waitHealthz waits until url answers 200, polling every 20 ms.
func waitHealthz(url string, within time.Duration) error {
	c := &http.Client{Timeout: time.Second}
	for end := time.Now().Add(within); ; time.Sleep(20 * time.Millisecond) {
		if resp, err := c.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(end) {
			return fmt.Errorf("%s did not answer 200 within %s", url, within)
		}
	}
}
