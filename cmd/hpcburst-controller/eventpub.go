package main

// The controller's publisher to the durable log (beta; HBEP-8, The durable log, Who writes; issue #625,
// PR E2). With --event-log-token-file the lifecycle controller, its tunnel-state reader and every
// Provider manager (batch or managed) publish their typed events to HB_EVENTS through one publisher
// per process, whose address is the launcher's base + 16. Without the flag there is none, and every
// recorder is built exactly as before.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// eventPublisherGrace is how long an orderly stop gives the publisher to publish what is queued. The
// controller's stop has a budget of its own (launcher.ControllerTermGrace), so this is short: what is
// not published by then is counted as not logged, as a crash loses it.
const eventPublisherGrace = 2 * time.Second

// newEventPublisher starts the process's publisher, nil when the command line has no token file. The
// token file is read at the start, so a token that is not there, or not a token, fails the command
// before anything runs, not at the first event.
func newEventPublisher(f flags) (*eventcount.Publisher, error) {
	if f.eventLogTokenFile == "" {
		return nil, nil
	}
	token, err := eventlog.ReadToken(f.eventLogTokenFile)
	if err != nil {
		return nil, fmt.Errorf("--event-log-token-file: %w", err)
	}
	return eventcount.NewPublisher(eventcount.PublisherOptions{
		Addr: f.host.EventLogAddr(), Token: token,
		Logger: slog.New(logr.ToSlogHandler(ctrl.Log.WithName("hpcburst-controller").WithName("event-publisher").WithValues("portBase", f.host.PortBase))),
	}), nil
}

// stopEventPublisher stops p, if there is one, after giving it eventPublisherGrace.
func stopEventPublisher(p *eventcount.Publisher) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventPublisherGrace)
	defer cancel()
	p.Shutdown(ctx)
}
