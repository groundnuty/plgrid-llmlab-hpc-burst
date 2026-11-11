package scenario_test

// The keeper rehearsal's streams, status and metrics (issue #343, PR 5).
//
// Every stream is open-ended: it asks for far more tokens than any event
// takes, and the client ends it once what the test waits for has been
// observed and a margin of tokens has arrived after it, so no stream of a
// fixed length spans an event of unbounded length. What the client received
// is held to the same request's quiet reply, which the engine double makes the
// same every time (internal/agent/enginedouble, doc.go): every event
// well-formed, and the content a prefix of the reply. The reply is taken
// after the event, read to at least as many tokens, then ended by its client.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// openTokens is every keeper stream's max_tokens and min_tokens: 50,000
// tokens at tokenDelay, about 17 minutes, so a stream never ends before its
// client ends it. It fits the keeper deployment's max-model-len and
// maxRequestDuration (keeperTiming).
const openTokens = 50000

// marginTokens is how many tokens a stream must receive after the event's
// outcome is observed before its client ends it: about 5 s at tokenDelay.
const marginTokens = 250

// openStreamBound bounds an open stream: the keeper deployment's
// maxRequestDuration (keeperTiming), past which the front would cut it.
const openStreamBound = 20 * time.Minute

// openStream is a streamed request whose client ends it.
type openStream struct {
	what   string
	cancel context.CancelFunc
	first  chan struct{}
	done   chan struct{}

	mu     sync.Mutex
	id     string
	status int
	// events are the data payloads received, each from a complete line, in
	// order; ticks when each token-bearing one arrived; content their text.
	events   []string
	ticks    []time.Time
	content  strings.Builder
	endErr   error // how the body ended: the client's cancel, EOF, a cut
	endAt    time.Time
	byClient bool // the client ended it
}

// startOpenStream sends the open stream's request with the conversation key.
func (r *rig) startOpenStream(what, key string) *openStream {
	r.t.Helper()
	deadline, _ := stack.BoundedDeadline(r.t, openStreamBound)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	r.t.Cleanup(cancel)
	s := &openStream{what: what, cancel: cancel, first: make(chan struct{}), done: make(chan struct{})}
	body := stack.ChatBody(stack.Model, openTokens, openTokens, true, "")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.ports.ClientAddr+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		r.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+r.h.ClientKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Session-Id", key)
	go s.read(req)
	return s
}

// read reads the reply line by line until it ends: a line counts only once
// it is complete, so a stream its client ends holds whole events only.
func (s *openStream) read(req *http.Request) {
	var once sync.Once
	signal := func() { once.Do(func() { close(s.first) }) }
	defer close(s.done)
	defer signal()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.ended(err)
		return
	}
	defer resp.Body.Close()
	s.mu.Lock()
	s.id, s.status = resp.Header.Get("Hpcburst-Request-Id"), resp.StatusCode
	s.mu.Unlock()
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			s.ended(err)
			return
		}
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "data:")
		if !ok {
			continue
		}
		payload := strings.TrimSpace(v)
		text := deltaContent(payload)
		s.mu.Lock()
		s.events = append(s.events, payload)
		if text != "" {
			s.ticks = append(s.ticks, time.Now())
			s.content.WriteString(text)
		}
		s.mu.Unlock()
		if text != "" {
			signal()
		}
	}
}

func (s *openStream) ended(err error) {
	s.mu.Lock()
	s.endErr, s.endAt = err, time.Now()
	s.mu.Unlock()
}

// deltaContent is a chat chunk's delta content, "" for any other payload.
func deltaContent(payload string) string {
	var ev struct {
		Choices []struct {
			Delta struct {
				Content *string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(payload), &ev) != nil || len(ev.Choices) != 1 || ev.Choices[0].Delta.Content == nil {
		return ""
	}
	return *ev.Choices[0].Delta.Content
}

// state is the reply's status and how its body ended, and whether its client
// ended it.
func (s *openStream) state() (status int, endErr error, byClient bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, s.endErr, s.byClient
}

// tokens is how many token-bearing events have arrived, and since how many
// of them arrived after t.
func (s *openStream) tokens(t time.Time) (all, since int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, at := range s.ticks {
		if at.After(t) {
			since++
		}
	}
	return len(s.ticks), since
}

// text is the content received so far.
func (s *openStream) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.content.String()
}

// waitFirst waits for the stream's first token.
func (r *rig) waitFirst(s *openStream) {
	r.t.Helper()
	select {
	case <-s.first:
	case <-time.After(stack.WaitDeadline):
		r.t.Fatalf("%s: the stream sent no first token", s.what)
	}
	if n, _ := s.tokens(time.Time{}); n == 0 {
		status, endErr, _ := s.state()
		r.t.Fatalf("%s: the stream ended before its first token: %d %v", s.what, status, endErr)
	}
}

// waitTokens waits until the stream has received n tokens after t, or fails
// naming how many it has.
func (r *rig) waitTokens(s *openStream, n int, t time.Time, why string) {
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline, fmt.Sprintf("%s: %d tokens of the stream %s", s.what, n, why), func() (bool, string) {
		all, since := s.tokens(t)
		select {
		case <-s.done:
			_, endErr, _ := s.state()
			return false, fmt.Sprintf("the stream ended after %d tokens (%v)", all, endErr)
		default:
		}
		return since >= n, fmt.Sprintf("%d tokens, %d of them after %s", all, since, t.Format(time.RFC3339Nano))
	})
}

// closeNow ends the stream from its client and waits for its reader.
func (r *rig) closeNow(s *openStream) {
	r.t.Helper()
	select {
	case <-s.done:
		_, endErr, _ := s.state()
		r.t.Fatalf("%s: the stream ended before its client ended it (%v)", s.what, endErr)
	default:
	}
	s.mu.Lock()
	s.byClient = true
	s.mu.Unlock()
	s.cancel()
	r.waitEnd(s, stack.WaitDeadline)
}

// waitEnd waits for the stream's reader to end, within d.
func (r *rig) waitEnd(s *openStream, d time.Duration) {
	r.t.Helper()
	select {
	case <-s.done:
	case <-time.After(d):
		r.t.Fatalf("%s: the stream did not end within %s", s.what, d)
	}
}

// openEventProblems holds every event the client received to a stream that has
// not finished: each one chunk of one choice with no finish reason. With cut
// set, the last may be the front's stream_cut error, which ends a cut stream;
// no other error and no [DONE] may come.
func openEventProblems(events []string, cut bool) []string {
	var out []string
	for i, p := range events {
		if p == "[DONE]" {
			out = append(out, fmt.Sprintf("event %d of %d is [DONE]: the stream finished", i, len(events)))
			continue
		}
		var ev struct {
			Choices []struct {
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Error *stack.FrontError `json:"error"`
		}
		switch err := json.Unmarshal([]byte(p), &ev); {
		case err != nil:
			out = append(out, fmt.Sprintf("event %d does not parse: %v: %.120s", i, err, p))
		case ev.Error != nil && cut && i == len(events)-1 && ev.Error.Code == "stream_cut":
		case ev.Error != nil:
			out = append(out, fmt.Sprintf("event %d is an error: %.200s", i, p))
		case len(ev.Choices) != 1 || ev.Choices[0].FinishReason != nil:
			out = append(out, fmt.Sprintf("event %d is not one unfinished choice: %.200s", i, p))
		}
	}
	return out
}

// clientEnded checks a stream its client ended: every event well-formed,
// and the front's record of it cut by its client, on the Instance. It returns
// the record.
func (r *rig) clientEnded(s *openStream, in *v1alpha1.Instance) stack.UsageRow {
	r.t.Helper()
	r.waitEnd(s, stack.WaitDeadline)
	if status, endErr, byClient := s.state(); !byClient || status != http.StatusOK {
		r.t.Fatalf("%s: the stream: status %d, ended %v, by its client %v", s.what, status, endErr, byClient)
	}
	if p := openEventProblems(s.events, false); len(p) > 0 {
		r.t.Errorf("%s: the stream's events: %s", s.what, strings.Join(p, "; "))
	}
	row := r.fc.RowOf(s.id)
	switch {
	case row.InstanceUID != string(in.UID):
		r.t.Errorf("%s: the stream went to %s (%s), not %s", s.what, row.Instance, row.InstanceUID, in.Name)
	case !row.Stream || row.CutBy == nil || *row.CutBy != "client" || row.Finish == nil || *row.Finish != "cut":
		r.t.Errorf("%s: the stream's record: stream %v, finish %s, cut by %s; want cut by its client, exactly", s.what, row.Stream, strOrNil(row.Finish), strOrNil(row.CutBy))
	}
	return row
}

// openReference is the open stream's request sent with nothing else
// happening, read until it has n tokens, then ended by its client: what every
// stream of the request must begin with.
func (r *rig) openReference(what, key string, in *v1alpha1.Instance, n int) string {
	r.t.Helper()
	defer r.stepTimer(what)()
	s := r.startOpenStream(what, key)
	r.waitFirst(s)
	r.waitTokens(s, n, time.Time{}, "in all")
	r.closeNow(s)
	r.clientEnded(s, in)
	return s.text()
}

// inOrder checks that what a stream received is the start of the request's
// reply ref, which is at least as long: every token, in order.
func (r *rig) inOrder(s *openStream, ref string) {
	r.t.Helper()
	got := s.text()
	if len(got) > len(ref) {
		r.t.Fatalf("%s: the stream received %d bytes, more than its reference's %d: the reference shows nothing of its end", s.what, len(got), len(ref))
	}
	if got == "" || !strings.HasPrefix(ref, got) {
		i := 0
		for i < len(got) && got[i] == ref[i] {
			i++
		}
		r.t.Errorf("%s: what the stream received differs from the same request's quiet reply at byte %d of %d: %q vs %q",
			s.what, i, len(got), got[i:min(len(got), i+60)], ref[i:min(len(ref), i+60)])
	}
}

// frontCutsByHop is hpcburst_front_cuts_total on the front's /metrics, summed
// by cut_by.
func (r *rig) frontCutsByHop() map[string]float64 {
	r.t.Helper()
	ss, err := obscheck.Scrape(r.ports.URL(4, "/metrics"))
	if err != nil {
		r.t.Fatalf("the front's /metrics: %v", err)
	}
	out := map[string]float64{}
	for _, s := range obscheck.Find(ss, "hpcburst_front_cuts_total", nil) {
		out[s.Labels["cut_by"]] += s.Value
	}
	return out
}

// oneClientCut checks that the front counted exactly one cut between two
// reads, its client's, and none by any other hop.
func (r *rig) oneClientCut(what string, before, after map[string]float64) {
	r.t.Helper()
	for _, hop := range frontCuts {
		want := 0.0
		if hop == "client" {
			want = 1
		}
		if d := after[hop] - before[hop]; d != want {
			r.t.Errorf("%s: the front counted %g cuts by %s, want %g", what, d, hop, want)
		}
	}
}

// launcherStatus is hpcburst status --json, in the parts read here.
type launcherStatus struct {
	Health      string   `json:"health"`
	Reasons     []string `json:"reasons"`
	Controllers []struct {
		Name         string `json:"name"`
		Running      bool   `json:"running"`
		PID          int    `json:"pid"`
		HealthSocket string `json:"health_socket"`
		PingOK       *bool  `json:"ping_ok"`
		KeeperEpoch  int64  `json:"keeper_epoch"`
		Detail       string `json:"detail"`
	} `json:"controllers"`
}

// statusJSON runs hpcburst status --json, with the API's checks but its
// garbage-collector probe, which waits up to a minute, and returns its
// report, its output and its error.
func (r *rig) statusJSON() (launcherStatus, string, error) {
	r.t.Helper()
	out, err := stack.Run(r.t, stack.Within(r.t, 2*stack.StatusTimeout), r.h.Env, r.h.B.HPCBurst, "status", "--state-dir", r.h.State, "--json", "--no-gc-probe")
	var st launcherStatus
	if jerr := json.NewDecoder(strings.NewReader(out)).Decode(&st); jerr != nil {
		return st, out, fmt.Errorf("status --json (%v): %w", err, jerr)
	}
	return st, out, err
}

// agentReported waits until the serving controller, a process started after
// since, exports the Instance's engine-metrics read time: it holds a report of
// the Instance's agent, which its Backend keeps only from an agent registered
// with it (internal/lifecycle, Backend.Report), so the agent registered again
// with this process. It returns when that was first seen.
func (r *rig) agentReported(what string, in *v1alpha1.Instance, since time.Time) time.Time {
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline, what+": the new controller holding a report of the agent", func() (bool, string) {
		ss, err := obscheck.Scrape(r.ports.URL(12, "/metrics"))
		if err != nil {
			return false, err.Error()
		}
		start := obscheck.Find(ss, "process_start_time_seconds", nil)
		if len(start) != 1 || start[0].Value < float64(since.Unix()-1) {
			return false, fmt.Sprintf("process_start_time_seconds %+v, the event at %d", start, since.Unix())
		}
		read := obscheck.Find(ss, "hpcburst_instance_metrics_read_timestamp_seconds", map[string]string{"instance_uid": string(in.UID)})
		return len(read) > 0, fmt.Sprintf("%d read-time series for the Instance", len(read))
	})
	return time.Now()
}

// tokensLine is a log line's account of a stream.
func tokensLine(s *openStream, t time.Time) string {
	all, since := s.tokens(t)
	return strconv.Itoa(all) + " tokens, " + strconv.Itoa(since) + " after the outcome"
}
