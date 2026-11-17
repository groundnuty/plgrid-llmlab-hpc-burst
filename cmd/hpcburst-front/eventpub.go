package main

// The front's publisher to the durable log's HB_EVENTS stream (beta; HBEP-8, The durable log, Who
// writes; issue #625, PR E2). With --event-log-token-file the front's recorder publishes its typed
// events through one publisher whose address is the launcher's base + 16, a second client beside the
// usage forwarder's. Without the flag there is none.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// eventPublisherGrace is how long an orderly stop gives the publisher to publish what is queued; what
// is not published by then is counted as not logged, as a crash loses it.
const eventPublisherGrace = 2 * time.Second

// newEventPublisher starts the front's publisher, nil when there is no token file. The token file is
// read at the start, so a token that is not there, or not a token, fails the command before anything
// runs, not at the first event.
func newEventPublisher(tokenFile string, host agentapi.Host) (*eventcount.Publisher, error) {
	if tokenFile == "" {
		return nil, nil
	}
	token, err := eventlog.ReadToken(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("--event-log-token-file: %w", err)
	}
	return eventcount.NewPublisher(eventcount.PublisherOptions{
		Addr: host.EventLogAddr(), Token: token,
		Logger: slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", "event-publisher"),
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
