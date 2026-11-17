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

	"k8s.io/client-go/kubernetes"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
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

// newRecorder builds the front's event recorder (beta; HBEP-8, The durable log, Who writes). With a token file
// its typed events also go to the log's HB_EVENTS stream through a publisher of its own, a second client beside
// the usage forwarder's, and the front raises EventsNotLogged regarding its own Lease when the log cannot take
// one; without one it is the recorder it was. The returned stop flushes the recorder, then gives the publisher
// its grace.
func newRecorder(cs kubernetes.Interface, tokenFile string, host agentapi.Host) (front.KubeRecorder, func(), error) {
	pub, err := newEventPublisher(tokenFile, host)
	if err != nil {
		return front.KubeRecorder{}, nil, err
	}
	rec, stop, err := front.NewKubeRecorderWithLog(cs, pub)
	if err != nil {
		stopEventPublisher(pub)
		return front.KubeRecorder{}, nil, err
	}
	return rec, func() { stop(); stopEventPublisher(pub) }, nil
}
