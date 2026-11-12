package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The server settings of the lifecycle controller's plugin endpoint
// (cmd/hpcburst-controller/tunnel.go: pluginReadHeaderTimeout,
// pluginWriteTimeout, pluginIdleTimeout).
const (
	admitReadHeaderTimeout = 5 * time.Second
	admitWriteTimeout      = 5 * time.Second
	admitIdleTimeout       = 2 * time.Minute
)

// runAdmit serves the lifecycle controller's frps plugin, unchanged
// (frpsplugin.NewHandler with its default deadline and in-flight cap), on
// base+8, from a MemIndex fed one snapshot of the configuration's fixture:
// one live Allocation per tunnel, with the credential that tunnel's frpc logs
// in with, and that tunnel's Instance placed on it. The controller's feeder
// (pluginFeed over its caches) is what stands in; the Index and the handler
// are the product's. Each call's latency and verdict go to -calls.
func runAdmit(args []string) error {
	fs := flag.NewFlagSet("admit", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the spec")
	callsPath := fs.String("calls", "", "where each call goes, one JSON line")
	readyPath := fs.String("ready", "", "written once the plugin answers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *callsPath == "" || *readyPath == "" {
		return errors.New("-calls and -ready are required")
	}
	idx := &frpsplugin.MemIndex{}
	if err := idx.Replace(fixtureSnapshot(sp)); err != nil {
		return err
	}
	idx.MarkSynced()
	trig := frpsplugin.NewTrigger()
	var triggers atomic.Int64
	h, err := frpsplugin.NewHandler(idx, frpsplugin.Options{Notifier: trig})
	if err != nil {
		return err
	}
	calls, err := openCallLog(*callsPath)
	if err != nil {
		return err
	}
	defer calls.Close()
	host := agentapi.Host{StateDir: sp.StateDir, PortBase: sp.PortBase}
	ln, err := net.Listen("tcp4", host.FrpsPluginAddr())
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: admitReadHeaderTimeout, WriteTimeout: admitWriteTimeout, IdleTimeout: admitIdleTimeout}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(newTimedListener(ln, "admit", calls)) }()
	stop := make(chan struct{})
	defer close(stop)
	go calls.flushEvery(time.Second, stop)
	// The controller drains the trigger to re-read frps's list; here the
	// drain only counts, so the Notifier never blocks.
	go func() {
		for {
			select {
			case <-trig.C():
				triggers.Add(1)
			case <-stop:
				return
			}
		}
	}()
	if err := writeJSONFile(*readyPath, map[string]any{"pid": os.Getpid(), "addr": host.FrpsPluginAddr()}); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	select {
	case <-ctx.Done():
	case err := <-served:
		return err
	}
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)
	return nil
}

// fixtureSnapshot is the Index's state: every tunnel's Allocation, live, with
// its credential and its one Instance.
func fixtureSnapshot(sp Spec) frpsplugin.Snapshot {
	var s frpsplugin.Snapshot
	for _, t := range sp.Tunnels {
		s.Allocations = append(s.Allocations, frpsplugin.AllocationEntry{UID: t.Allocation, Credential: t.Token, Instances: []string{t.UID}})
	}
	return s
}
