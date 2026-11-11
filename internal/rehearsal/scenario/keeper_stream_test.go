package scenario_test

// The keeper rehearsal's streams, status and metrics (issue #343, PR 5): what
// a client of the front received, compared with the same request's quiet
// reply; hpcburst status --json; and the serving controller's /metrics.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// contentOf is a streamed reply's content as the client received it: every
// event's delta content, in order.
func contentOf(body []byte) string {
	var b strings.Builder
	for _, l := range strings.Split(string(body), "\n") {
		v, ok := strings.CutPrefix(strings.TrimSpace(l), "data:")
		if !ok {
			continue
		}
		var ev struct {
			Choices []struct {
				Delta struct {
					Content *string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(v)), &ev) != nil {
			continue
		}
		for _, c := range ev.Choices {
			if c.Delta.Content != nil {
				b.WriteString(*c.Delta.Content)
			}
		}
	}
	return b.String()
}

// keeperStream waits for a keeper stream's end.
func (r *rig) keeperStream(what string, done <-chan stack.Streamed) stack.Streamed {
	r.t.Helper()
	select {
	case s := <-done:
		return s
	case <-time.After(r.fc.RequestTimeout()):
		r.t.Fatalf("%s: the stream did not end within %s", what, r.fc.RequestTimeout())
	}
	return stack.Streamed{}
}

// firstToken waits for a stream's first token.
func (r *rig) firstToken(what string, first <-chan struct{}) {
	r.t.Helper()
	select {
	case <-first:
	case <-time.After(stack.WaitDeadline):
		r.t.Fatalf("%s: the stream sent no first token", what)
	}
}

// wholeStream checks that a keeper stream arrived whole, as the client and
// the front's record see it, on the Instance, and, with ref set, that its
// content is ref's exactly: every token, in order. It returns its content and
// record.
func (r *rig) wholeStream(what string, s stack.Streamed, in *v1alpha1.Instance, ref string) (string, stack.UsageRow) {
	r.t.Helper()
	if s.Err != nil || s.Status != http.StatusOK {
		r.t.Fatalf("%s: the stream: %d %v: %.300s", what, s.Status, s.Err, s.Body)
	}
	got := contentOf(s.Body)
	if p := streamProblems(sseParse(s.Body), keeperStreamTokens); len(p) > 0 || strings.Contains(string(s.Body), "stream_cut") {
		r.t.Errorf("%s: the stream as the client received it: %s; its end: %.300s", what, strings.Join(p, "; "), s.Body[max(0, len(s.Body)-300):])
	}
	if ref != "" && got != ref {
		i := 0
		for i < len(got) && i < len(ref) && got[i] == ref[i] {
			i++
		}
		r.t.Errorf("%s: the stream's content differs from the same request's quiet reply at byte %d of %d (reply %d bytes): %q vs %q",
			what, i, len(got), len(ref), got[i:min(len(got), i+60)], ref[i:min(len(ref), i+60)])
	}
	row := r.fc.RowOf(s.ID)
	switch {
	case row.InstanceUID != string(in.UID):
		r.t.Errorf("%s: the stream went to %s (%s), not %s", what, row.Instance, row.InstanceUID, in.Name)
	case !row.Stream || row.Finish == nil || *row.Finish != "length" || row.CutBy != nil:
		r.t.Errorf("%s: the stream's record: stream %v, finish %s, cut by %s; want it finished by length, uncut", what, row.Stream, strOrNil(row.Finish), strOrNil(row.CutBy))
	case row.CompletionTokens == nil || *row.CompletionTokens != keeperStreamTokens:
		r.t.Errorf("%s: the stream's record has %v completion tokens, want %d", what, row.CompletionTokens, keeperStreamTokens)
	}
	return got, row
}

// quietReference sends the keeper stream's request with nothing else
// happening, while during, when set, runs once its first token has arrived,
// and returns its content once it has arrived whole: what every later stream
// of the request must be.
func (r *rig) quietReference(what string, in *v1alpha1.Instance, key string, during func()) string {
	r.t.Helper()
	defer r.stepTimer(what)()
	first, done := r.fc.StartStream(keeperStreamBody(), key)
	r.firstToken(what, first)
	if during != nil {
		during()
	}
	ref, _ := r.wholeStream(what, r.keeperStream(what, done), in, "")
	if n := len(strings.Fields(ref)); n != keeperStreamTokens {
		r.t.Fatalf("%s: the reference has %d tokens, want %d", what, n, keeperStreamTokens)
	}
	return ref
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
		ss, err := scrape(r.ports.URL(12, "/metrics"))
		if err != nil {
			return false, err.Error()
		}
		start := find(ss, "process_start_time_seconds", nil)
		if len(start) != 1 || start[0].value < float64(since.Unix()-1) {
			return false, fmt.Sprintf("process_start_time_seconds %+v, the event at %d", start, since.Unix())
		}
		read := find(ss, "hpcburst_instance_metrics_read_timestamp_seconds", map[string]string{"instance_uid": string(in.UID)})
		return len(read) > 0, fmt.Sprintf("%d read-time series for the Instance", len(read))
	})
	return time.Now()
}
