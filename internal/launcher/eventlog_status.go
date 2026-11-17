package launcher

// What status says about the durable log (HBEP-8, The durable log, and Retention, In the durable log;
// HBEP-11, Single-user host; issue #625, PR E1). The log is one supervised entry, judged like the
// others (identity through its pidfd, its two loopback ports, GET /healthz on base+17), and then:
//
//   - "degraded: event log not started: ..." while its start gate is closed (the caps do not fit the
//     disk): the exit code stays 0, and the entry's record names why;
//   - "unhealthy" when it exits refusing its settings, naming them (the refusal it wrote before it
//     exited), in its backoff and through each restart attempt, until it is healthy (#680);
//   - "degraded: event log full" when HB_EVENTS has no room left, with the remedy: the events stream
//     then refuses every new event and keeps every old one;
//   - otherwise healthy, with each stream's bytes and messages.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// StreamHealth is one stream's size as nats-server's monitoring reports it: the bytes are logical
// bytes, which is what its cap counts.
type StreamHealth struct {
	Name     string `json:"name"`
	Messages uint64 `json:"messages"`
	Bytes    uint64 `json:"bytes"`
	MaxBytes int64  `json:"max_bytes"`
}

// DegradedEventLogNotStarted is EntryHealth.Degraded's prefix for a log held back by its start gate:
// status prints "degraded: event log not started: <free> free on the store's file system, caps ...".
const DegradedEventLogNotStarted = "event log not started: "

// DegradedEventLogFull is EntryHealth.Degraded's prefix for a log whose events stream is full:
// status prints "degraded: event log full", then the remedy.
const DegradedEventLogFull = "event log full"

// eventLogFullRemedy is what a person does about a full events stream (HBEP-8, Retention, In the
// durable log): raise the cap, a launcher setting applied at the log's next start, or, at stable,
// prune after a person confirms.
const eventLogFullRemedy = "raise --event-log-events-cap (hpcburst down, then hpcburst up --event-log with the new value), or, once hpcburst eventlog prune exists (stable), prune the events older than the grant's end after a person confirms"

// readEventLogStreams asks the log's monitoring port for its streams' sizes: GET /jsz with the streams
// and their configurations (the cap is in the configuration).
func readEventLogStreams(addr string) ([]StreamHealth, error) {
	code, body, err := probeEndpoint(addr, "/jsz?streams=true&config=true", healthzTimeout)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("answered %d", code)
	}
	var j struct {
		Accounts []struct {
			Streams []struct {
				Name   string `json:"name"`
				Config *struct {
					MaxBytes int64 `json:"max_bytes"`
				} `json:"config"`
				State struct {
					Messages uint64 `json:"messages"`
					Bytes    uint64 `json:"bytes"`
				} `json:"state"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if err := json.Unmarshal(body, &j); err != nil {
		return nil, fmt.Errorf("the answer is not JSON: %w", err)
	}
	var out []StreamHealth
	for _, a := range j.Accounts {
		for _, s := range a.Streams {
			h := StreamHealth{Name: s.Name, Messages: s.State.Messages, Bytes: s.State.Bytes}
			if s.Config != nil {
				h.MaxBytes = s.Config.MaxBytes
			}
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// eventsFull reports whether the events stream has no room for another event of the size it holds
// on average: its bytes and one more average message would pass its cap. A stream refuses an event
// when its bytes and that event's size would pass the cap, so it is full before its bytes equal the
// cap; the average message size stands for the next event's, which is not known. A stream with no
// cap, or no message, is not full.
func eventsFull(s StreamHealth) bool {
	if s.MaxBytes <= 0 || s.Messages == 0 {
		return false
	}
	return s.Bytes+s.Bytes/s.Messages > uint64(s.MaxBytes)
}

// eventLogStreamsExist reports whether the log's monitoring port lists both streams. /healthz alone
// answers once JetStream is up, a moment before the streams are made. It does not say that a client
// can connect: nats-server lists the streams it restored before it opens the client port, so the start
// barrier asks eventLogAcceptsClient (eventlog_ready.go), which adds a token connection.
func eventLogStreamsExist(addr string) bool {
	streams, err := readEventLogStreams(addr)
	if err != nil {
		return false
	}
	have := map[string]bool{}
	for _, s := range streams {
		have[s.Name] = true
	}
	return have[eventlog.StreamEvents] && have[eventlog.StreamUsage]
}

// checkEventLogStreams is checkEntry's part for the log once its /healthz answered: it reads the
// streams, which must both be there, and states a full events stream as degraded, never as a fault:
// the log keeps serving, refusing new events and keeping the old ones.
func checkEventLogStreams(addr string, eh *EntryHealth, add func(string, ...any)) {
	streams, err := readEventLogStreams(addr)
	if err != nil {
		add("GET /jsz on %s: %v", addr, err)
		return
	}
	eh.Streams = streams
	have := map[string]bool{}
	for _, s := range streams {
		have[s.Name] = true
		if s.Name == eventlog.StreamEvents && eventsFull(s) {
			eh.addDegraded(fmt.Sprintf("%s: %s holds %s of its %s cap and refuses new events, keeping every old one; %s",
				DegradedEventLogFull, s.Name, FormatByteSize(int64(s.Bytes)), FormatByteSize(s.MaxBytes), eventLogFullRemedy))
		}
	}
	for _, want := range []string{eventlog.StreamEvents, eventlog.StreamUsage} {
		if !have[want] {
			add("stream %s is missing from %s", want, addr)
		}
	}
}

// gatedEventLogHealth is the log's EntryHealth while its start gate is closed: not running, on
// purpose, and degraded, with the reason the supervisor wrote. It is no problem, so the host stays
// healthy and status exits 0.
func gatedEventLogHealth(e EntryRecord) EntryHealth {
	eh := EntryHealth{Name: e.Name, State: e.State, Restarts: e.Restarts, LastExit: e.LastExit, Health: e.Health, Gate: e.Gate}
	eh.addDegraded(DegradedEventLogNotStarted + e.Gate)
	return eh
}

// eventLogRefusalShown reports whether status adds the refusal's reason to the log's entry: while
// the log is not running, and while its last exit was a refusal (eventlog.ExitRefused) and the
// current process has not answered /healthz, which covers each restart attempt of a log in a crash
// loop (#680): a fresh process that does not hold its health address yet would otherwise leave
// only that generic line, and a person would not see the setting to fix. The exit stays on the
// entry's record after the log recovers, so "healthy" is what ends it, read from this look's own
// /healthz answer (eh.HealthzOK), not from the refusal file, which the log removes only a moment
// after its /healthz answers (node.Run).
func eventLogRefusalShown(e EntryRecord, eh EntryHealth) bool {
	if !eh.Running {
		return true
	}
	refused := e.LastExit != nil && e.LastExit.Code == eventlog.ExitRefused
	healthy := eh.HealthzOK != nil && *eh.HealthzOK
	return refused && !healthy
}

// eventLogRefusalProblem is the problem status reports for a log that left a refusal and is not
// healthy (eventLogRefusalShown): it names the settings, one line each, joined. "" when there is
// none.
func eventLogRefusalProblem(l layout) string {
	text := readEventLogRefusal(l)
	if text == "" {
		return ""
	}
	return "unhealthy: the event log refuses to serve: " + strings.Join(strings.Split(text, "\n"), "; ")
}
