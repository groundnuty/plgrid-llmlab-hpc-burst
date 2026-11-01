package front

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-5, Test plan, "Stream completion per endpoint", and Contract,
// Endpoints:
//   - /v1/chat/completions and /v1/completions: complete when the stream
//     carried a finish reason and then [DONE]. One that ends without a
//     finish reason is recorded "truncated" and gets one error event, never
//     [DONE].
//   - /v1/responses: complete when it carried response.completed; one that
//     ends before it is truncated.
//   - a path outside the list is relayed as it came, with a null finish.
//   - a Responses follow-up that names previous_response_id and sends the
//     session header reaches the Instance that stored the response.

// stPost sends body with the client key on a connection of its own; extra
// headers are given as name, value pairs.
func stPost(t *testing.T, h *harness, path, body string, hdr ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := ctHTTP().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// stEvents reads a whole SSE body and returns its raw text and its events.
func stEvents(t *testing.T, resp *http.Response) (string, []string) {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the stream: %v", err)
	}
	var out []string
	for _, e := range strings.Split(string(b), "\n\n") {
		if strings.TrimSpace(e) != "" {
			out = append(out, e)
		}
	}
	return string(b), out
}

// stData is an event's data payload.
func stData(event string) string {
	var data []string
	for _, line := range strings.Split(event, "\n") {
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimPrefix(v, " "))
		}
	}
	return strings.Join(data, "\n")
}

// stErrorEvents returns the events whose data is an {"error": ...} object.
func stErrorEvents(events []string) []map[string]any {
	var out []map[string]any
	for _, e := range events {
		var v struct {
			Error map[string]any `json:"error"`
		}
		if json.Unmarshal([]byte(stData(e)), &v) == nil && v.Error != nil {
			out = append(out, v.Error)
		}
	}
	return out
}

// stFix is a front with one ready Instance of "example-27b" on an engine
// double.
type stFix struct {
	*harness
	e *enginetest.Engine
}

func newStFix(t *testing.T, b enginetest.Behaviour) *stFix {
	t.Helper()
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	e.Set(b)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.apply()
	return &stFix{harness: h, e: e}
}

// stRecord is the usage record of a response.
func stRecord(h *harness, resp *http.Response) usage.Record {
	h.t.Helper()
	return h.record(resp.Header.Get("hpcburst-request-id"))
}

func stFinish(r usage.Record) string {
	if r.Finish == nil {
		return "<null>"
	}
	return *r.Finish
}

// stCheckStreamCut asserts the shape HBEP-5 gives a stream that ended without
// its end: the content events as they came, then one error event with code
// stream_cut, and no [DONE] anywhere.
func stCheckStreamCut(t *testing.T, raw string, events []string, wantContent int) {
	t.Helper()
	if strings.Contains(raw, "[DONE]") {
		t.Errorf("the stream carries [DONE]; the front must never send it for a stream that did not complete:\n%s", raw)
	}
	errs := stErrorEvents(events)
	if len(errs) != 1 {
		t.Fatalf("the stream carries %d error events, want exactly 1:\n%s", len(errs), raw)
	}
	if errs[0]["code"] != "stream_cut" {
		t.Errorf("the error event has code %v, want stream_cut", errs[0]["code"])
	}
	if last := events[len(events)-1]; len(stErrorEvents([]string{last})) != 1 {
		t.Errorf("the error event is not the stream's last: %q", last)
	}
	if got := len(events) - 1; got != wantContent {
		t.Errorf("the client got %d events before the error event, want the engine's %d", got, wantContent)
	}
}

// TestStreamEndingWithoutFinishIsTruncated: the engine double sends tokens,
// no finish reason, and then [DONE] (NoFinish alone: the front must withhold
// that [DONE]), on both chat endpoints; and the control, a stream that
// finishes, shows the same body would pass when it should.
func TestStreamEndingWithoutFinishIsTruncated(t *testing.T) {
	const content = 3 // Behaviour.Tokens: three content events
	cases := []struct {
		name, path, body string
		b                enginetest.Behaviour
	}{
		{"chat, no finish reason", "/v1/chat/completions",
			`{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`,
			enginetest.Behaviour{Tokens: content, NoFinish: true}},
		{"completions, no finish reason", "/v1/completions",
			`{"model":"example-27b","stream":true,"prompt":"hello there"}`,
			enginetest.Behaviour{Tokens: content, NoFinish: true}},
		{"chat, no finish reason and no [DONE]", "/v1/chat/completions",
			`{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`,
			enginetest.Behaviour{Tokens: content, NoFinish: true, NoDone: true}},
		{"completions, a finish reason but no [DONE]", "/v1/completions",
			`{"model":"example-27b","stream":true,"prompt":"hello there"}`,
			// Complete is "a finish reason and then [DONE]" (Contract). The
			// finish chunk is a fourth event the client gets before the error.
			enginetest.Behaviour{Tokens: content, NoDone: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStFix(t, tc.b)
			resp := stPost(t, f.harness, tc.path, tc.body)
			raw, events := stEvents(t, resp)
			want := content
			if !tc.b.NoFinish {
				want++ // the finish chunk
			}
			stCheckStreamCut(t, raw, events, want)
			rec := stRecord(f.harness, resp)
			if stFinish(rec) != "truncated" {
				t.Errorf("finish %s, want truncated", stFinish(rec))
			}
			if rec.Status != http.StatusOK {
				t.Errorf("status %d, want the 200 the headers carried", rec.Status)
			}
		})
	}

	t.Run("control: a stream that finishes is complete", func(t *testing.T) {
		f := newStFix(t, enginetest.Behaviour{Tokens: content})
		resp := stPost(t, f.harness, "/v1/chat/completions", `{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`)
		raw, events := stEvents(t, resp)
		if n := len(stErrorEvents(events)); n != 0 {
			t.Errorf("a complete stream carries %d error events", n)
		}
		if last := stData(events[len(events)-1]); last != "[DONE]" {
			t.Errorf("a complete stream ends with %q, want [DONE]:\n%s", last, raw)
		}
		if fin := stFinish(stRecord(f.harness, resp)); fin != "stop" {
			t.Errorf("finish %s, want stop", fin)
		}
	})
}

// TestResponsesStreamIsCompleteAtResponseCompleted: a /v1/responses stream that
// ends with response.completed is complete and gets no error event; one that
// ends before it is truncated, with one error event.
func TestResponsesStreamIsCompleteAtResponseCompleted(t *testing.T) {
	const body = `{"model":"example-27b","stream":true,"input":"hello there"}`
	t.Run("ends with response.completed", func(t *testing.T) {
		f := newStFix(t, enginetest.Behaviour{Tokens: 3})
		resp := stPost(t, f.harness, "/v1/responses", body)
		raw, events := stEvents(t, resp)
		if n := len(stErrorEvents(events)); n != 0 {
			t.Errorf("a complete Responses stream carries %d error events:\n%s", n, raw)
		}
		if strings.Contains(raw, "stream_cut") {
			t.Errorf("a complete Responses stream mentions stream_cut:\n%s", raw)
		}
		if !strings.Contains(events[len(events)-1], "response.completed") {
			t.Errorf("the stream's last event is %q, want response.completed", events[len(events)-1])
		}
		if fin := stFinish(stRecord(f.harness, resp)); fin != "completed" {
			t.Errorf("finish %s, want the final response's status, completed", fin)
		}
	})
	t.Run("ends before response.completed", func(t *testing.T) {
		f := newStFix(t, enginetest.Behaviour{Tokens: 3, NoFinish: true})
		resp := stPost(t, f.harness, "/v1/responses", body)
		raw, events := stEvents(t, resp)
		if strings.Contains(raw, "response.completed") {
			t.Errorf("the engine sent no response.completed, but the client got one:\n%s", raw)
		}
		// response.created, three deltas, then the error event.
		stCheckStreamCut(t, raw, events, 4)
		if fin := stFinish(stRecord(f.harness, resp)); fin != "truncated" {
			t.Errorf("finish %s, want truncated", fin)
		}
	})
}

// TestStreamOnAPathOutsideTheListIsRelayedAsItCame: /v1/other is passed
// through with no truncation judgment: the bytes the engine wrote, no error
// event, and a null finish in the usage record.
func TestStreamOnAPathOutsideTheListIsRelayedAsItCame(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 3})
	resp := stPost(t, f.harness, "/v1/other", `{"model":"example-27b","stream":true}`)
	raw, _ := stEvents(t, resp)
	// The engine double answers this path with "data: chunk <i>" events.
	want := "data: chunk 0\n\ndata: chunk 1\n\ndata: chunk 2\n\n"
	if raw != want {
		t.Errorf("relayed %q, want exactly what the engine wrote, %q", raw, want)
	}
	rec := stRecord(f.harness, resp)
	if rec.Finish != nil {
		t.Errorf("finish %q, want null on a path the front does not judge", *rec.Finish)
	}
	if rec.Status != http.StatusOK || !rec.Stream || rec.Path != "/v1/other" {
		t.Errorf("record: status %d stream %v path %q", rec.Status, rec.Stream, rec.Path)
	}
}

// TestResponsesFollowUpReachesTheInstanceThatStoredTheResponse: with two
// Instances up, a request that names previous_response_id and sends the
// session header reaches the Instance the first request of that session
// reached (vLLM keeps stored responses in memory per engine; HBEP-5,
// Contract). Twelve sessions are spread over both Instances, so a front that
// ignored the header would fail.
func TestResponsesFollowUpReachesTheInstanceThatStoredTheResponse(t *testing.T) {
	h := newHarness(t, nil)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 2, "helios-a")
	var engines []*enginetest.Engine
	for i := 0; i < 2; i++ {
		key := newKey(t)
		e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
		engines = append(engines, e)
		h.instance("md", fmt.Sprintf("md-%d", i), "helios-a", e, key, instanceOpts{})
	}
	h.apply()

	// which engine got the requests of one session, by its header value.
	seen := func(session string) (idx []int, count int) {
		for i, e := range engines {
			n := 0
			for _, r := range e.Requests() {
				if r.Header.Get("X-Session-Id") == session {
					n++
				}
			}
			if n > 0 {
				idx = append(idx, i)
			}
			count += n
		}
		return idx, count
	}
	homes := map[int]int{}
	for s := 0; s < 12; s++ {
		session := fmt.Sprintf("session-%d", s)
		first := stPost(t, h, "/v1/responses", `{"model":"example-27b","input":"hello there"}`, "x-session-id", session)
		_ = readAll(t, first)
		followUp := stPost(t, h, "/v1/responses", `{"model":"example-27b","previous_response_id":"resp-1","input":"and then"}`, "x-session-id", session)
		_ = readAll(t, followUp)
		idx, count := seen(session)
		if count != 2 || len(idx) != 1 {
			t.Fatalf("%s: its two requests reached engines %v (%d requests); want both on one", session, idx, count)
		}
		homes[idx[0]]++
	}
	if len(homes) != 2 {
		t.Errorf("the sessions all went to one Instance (%v): the test cannot tell affinity from a single target", homes)
	}
}
