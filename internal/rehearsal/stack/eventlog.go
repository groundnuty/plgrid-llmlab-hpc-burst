package stack

// The durable log's part of the host (HBEP-8, The durable log; issue #625, PR E2; issue #710): up
// --event-log with the sizes a hosted runner's disk takes, and the read of HB_EVENTS that a scenario
// makes with a client of its own, from the first sequence, as a person reading the log would.
//
// Origin of the read: nodetest.Dir.ReadAll (internal/eventlog/node/nodetest, this repository, PR #639
// and #697): a fresh client with the token, an ordered consumer from the first sequence. This copy
// returns an error instead of failing the test, so that a scenario can poll it, and finds the token
// and the address from the host as a client does (agentapi.Host).

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// The sizes EventLog gives up: the free space the log must leave on its file system, and each stream's cap.
// They are the values of #639's own k3s rows (cmd/hpcburst/eventlog_k3s_test.go): the floor is low enough
// for a hosted runner, and the caps small enough that the log's store, and the upgrade's copy of it, fit
// under it.
const (
	eventLogFloor = "1GB"
	eventLogCap   = "64MiB"
)

// EventLog makes StartHost's up run the durable log: hpcburst up --event-log, with the test floor and caps.
// The log takes base+16 and base+17 of the port block, and each controller and the front publish their typed
// events to it through the token in <state>/eventlog/token. If the free space is under the floor, up still
// succeeds and starts the controllers and the front without the log (hpcburst status says "degraded: event
// log not started"); ReadEvents then fails, which a scenario reports as that.
func EventLog(h *Host) {
	h.upExtra = append(h.upExtra, "--event-log", "--event-log-floor", eventLogFloor,
		"--event-log-events-cap", eventLogCap, "--event-log-usage-cap", eventLogCap)
	h.Ports.EventLog = true
}

// LoggedEvent is one message of HB_EVENTS as a fresh client reads it: the stream's sequence, the NATS subject
// it was published on (hb.ev.<kind>.<UID of the regarding object>), the Nats-Msg-Id, the envelope's time and
// the Kubernetes Event's fields, decoded.
type LoggedEvent struct {
	Seq     uint64
	Subject string
	ID      string
	Time    time.Time
	Data    eventcount.Data
}

// ReadEvents reads every message of the host's HB_EVENTS, from sequence 1, with a client of its own: it reads
// the token from <state>/eventlog/token and connects to base+16, as the controllers and the front do, and
// closes the connection before it returns. It never reads through a publisher, whose acknowledgements prove
// only what the publisher saw. A log that is not up, a token it refuses or a message that is not an envelope
// is an error; an empty stream is no events and no error.
func (h *Host) ReadEvents(ctx context.Context) ([]LoggedEvent, error) {
	ah := h.AgentHost()
	return readEvents(ctx, ah.EventLogAddr(), ah.EventLogTokenFile())
}

// readEvents is ReadEvents for the log at addr with the token in tokenFile.
func readEvents(ctx context.Context, addr, tokenFile string) ([]LoggedEvent, error) {
	token, err := eventlog.ReadToken(tokenFile)
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect("nats://"+addr, nats.Token(token), nats.NoReconnect(), nats.Timeout(3*time.Second), nats.Name("hpcburst-rehearsal-reader"))
	if err != nil {
		return nil, fmt.Errorf("connect to the durable log at %s with the token: %w", addr, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	s, err := js.Stream(ctx, eventlog.StreamEvents)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", eventlog.StreamEvents, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream %s info: %w", eventlog.StreamEvents, err)
	}
	if info.State.Msgs == 0 {
		return nil, nil
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		return nil, fmt.Errorf("ordered consumer on %s: %w", eventlog.StreamEvents, err)
	}
	var out []LoggedEvent
	for uint64(len(out)) < info.State.Msgs {
		msg, err := cons.Next(jetstream.FetchMaxWait(20 * time.Second))
		if err != nil {
			return nil, fmt.Errorf("read %s: message %d of %d: %w", eventlog.StreamEvents, len(out)+1, info.State.Msgs, err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return nil, err
		}
		ev, data, err := eventcount.Decode(msg.Data())
		if err != nil {
			return nil, fmt.Errorf("message %d of %s is not an event envelope: %w", md.Sequence.Stream, eventlog.StreamEvents, err)
		}
		out = append(out, LoggedEvent{Seq: md.Sequence.Stream, Subject: msg.Subject(), ID: msg.Headers().Get(nats.MsgIdHdr), Time: ev.Time(), Data: data})
	}
	return out, nil
}

// WantLogged is one event the log must hold: on this NATS subject, with this reason and action, from this
// reporting controller (the Event's field manager). A scenario writes it from HBEP-8's tables and the objects
// it made, never from what the log returned.
type WantLogged struct {
	Controller string // data.reportingController
	Subject    string // hb.ev.<kind>.<UID>
	Reason     string // data.reason
	Action     string // data.action
}

// Phrase is the event in a failure's words: action/reason on the subject, from the controller.
func (w WantLogged) Phrase() string {
	return fmt.Sprintf("%s/%s on %s from %s", w.Action, w.Reason, w.Subject, w.Controller)
}

// Matches reports whether e is the event w names: all four fields agree.
func (w WantLogged) Matches(e LoggedEvent) bool {
	return e.Subject == w.Subject && e.Data.Reason == w.Reason && e.Data.Action == w.Action && e.Data.ReportingController == w.Controller
}

// MissingLogged returns the wanted events that none of got is, in the order of want.
func MissingLogged(got []LoggedEvent, want []WantLogged) []WantLogged {
	var missing []WantLogged
	for _, w := range want {
		found := false
		for _, e := range got {
			if w.Matches(e) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, w)
		}
	}
	return missing
}

// DescribeMissing is a failure's account of missing, for a wait's "last state": the reporting controllers whose
// events are absent, each with the event it was expected to raise, and what the log held (at most 30 events, by
// sequence, subject, reason, action and reporting controller), so that a controller whose events the log lacks
// is named, and an event on the wrong subject or from another controller can be seen.
func DescribeMissing(missing []WantLogged, got []LoggedEvent) string {
	if len(missing) == 0 {
		return "no event is missing"
	}
	var b strings.Builder
	b.WriteString("the log holds no event from")
	seen := map[string]bool{}
	for _, w := range missing {
		if !seen[w.Controller] {
			seen[w.Controller] = true
			fmt.Fprintf(&b, " %s", w.Controller)
		}
	}
	b.WriteString(", missing:")
	for _, w := range missing {
		fmt.Fprintf(&b, " [%s]", w.Phrase())
	}
	fmt.Fprintf(&b, "; the log holds %d events", len(got))
	const shown = 30
	for i, e := range got {
		if i == shown {
			fmt.Fprintf(&b, " ... (%d more)", len(got)-shown)
			break
		}
		fmt.Fprintf(&b, " [#%d %s %s/%s from %s]", e.Seq, e.Subject, e.Data.Action, e.Data.Reason, e.Data.ReportingController)
	}
	return b.String()
}
