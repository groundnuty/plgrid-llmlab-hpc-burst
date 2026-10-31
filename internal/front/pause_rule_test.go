package front

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The pause rule as #96 states it (@hpc-burst-science-agent, 31 Oct 2026):
// connection failures, a 5xx and every upstream-side cut after the headers
// count, in a row; a completed request resets the run, a capped reply
// included; a 4xx neither counts nor resets; a client disconnect does not
// count. PR #95 review (Astra B5, Opus B3): the success reported on a 200's
// headers reset the run, so streams that failed after their headers never
// reached the threshold.

const prInst = "ns/md-0"

// The engine's chat error event as vLLM sends it mid-stream, then its [DONE].
const (
	vllmChatError    = `data: {"error":{"message":"the engine failed","type":"InternalServerError","param":null,"code":500}}` + "\n\n"
	vllmChatBadError = `data: {"error": the engine failed, not JSON` + "\n\n"
	vllmDone         = "data: [DONE]\n\n"
	chatToken        = `data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"t0 "},"finish_reason":null}]}` + "\n\n"
	respCreated      = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\"}}\n\n"
	respDelta        = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"t0 \"}\n\n"
	respError        = "event: error\ndata: {\"type\":\"error\",\"code\":\"server_error\",\"message\":\"the engine failed\",\"param\":null,\"sequence_number\":3}\n\n"
	// vLLM v0.32.0 ends a capped reply with response.completed, status
	// incomplete: a clean end.
	respCappedEnd = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"incomplete\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
)

// sseEngine answers every request with a 200 event stream of these events.
func sseEngine(t *testing.T, events ...string) string {
	return rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for _, ev := range events {
			_, _ = io.WriteString(w, ev)
			_ = http.NewResponseController(w).Flush()
		}
	})
}

// errorEvents counts the events of an SSE body that are error events: named
// error, or with data that starts as an error object.
func errorEvents(body string) int {
	n := 0
	for _, ev := range strings.Split(body, "\n\n") {
		if strings.Contains(ev, "event: error") || strings.Contains(ev, `data: {"error"`) {
			n++
		}
	}
	return n
}

// prSend sends one request and reads it whole; it returns the response, its
// body and its usage record.
func (f *scFix) prSend(t *testing.T, path, body string) (*http.Response, string) {
	t.Helper()
	resp := f.errPost(path, body)
	b := readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	return resp, b
}

func (f *scFix) wantFailures(t *testing.T, what string, n int) {
	t.Helper()
	if got := f.router.failures(prInst); got != n {
		t.Fatalf("after %s: %d failures in a row, want %d", what, got, n)
	}
}

const (
	prChatStream = `{"model":"` + scModel + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	prRespStream = `{"model":"` + scModel + `","stream":true,"input":"hi"}`
)

// TestPauseRunOfMixedFailures: a 500, a 404, a client disconnect, a
// Responses stream that just stops, and a chat stream with the engine's
// error event. The 404 neither resets nor counts, the disconnect does not
// count, so the third failure (the error event) pauses the Instance. After
// pauseDuration it is eligible again.
func TestPauseRunOfMixedFailures(t *testing.T) {
	f := newScFix(t, scOpts{})
	e := f.engines[0]

	e.Set(enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"boom","type":"server_error","code":500}}`})
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	f.wantFailures(t, "a 500", 1)

	e.Set(enginetest.Behaviour{Status: 404, ErrorBody: `{"error":{"message":"no such thing","type":"not_found","code":404}}`})
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	f.wantFailures(t, "a 404 (neither counts nor resets)", 1)

	// The client reads the first event and goes away.
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, chatToken)
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	resp := f.errPost(scChat, prChatStream)
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	id := resp.Header.Get("hpcburst-request-id")
	f.waitRecord(id)
	if rec := f.record(id); rec.CutBy == nil || *rec.CutBy != cutClient {
		t.Fatalf("the disconnect's cutBy is %v, want client", rec.CutBy)
	}
	f.wantFailures(t, "a client disconnect (does not count)", 1)

	// A Responses stream that just stops: the front sends its one stream_cut.
	f.retarget(f.uids[0], e.Addr())
	e.Set(enginetest.Behaviour{NoFinish: true})
	_, body := f.prSend(t, "/v1/responses", prRespStream)
	if n := errorEvents(body); n != 1 || !strings.Contains(body, `"code":"stream_cut"`) {
		t.Errorf("a stopped Responses stream gave %d error events, want the front's one stream_cut: %q", n, body)
	}
	f.wantFailures(t, "a Responses stream that stopped", 2)

	f.retarget(f.uids[0], sseEngine(t, chatToken, vllmChatError, vllmDone))
	f.prSend(t, scChat, prChatStream)
	if !f.router.Paused(prInst) {
		t.Fatalf("after three failures of mixed kinds (and a 404 and a disconnect between), the Instance is not paused; failures %d", f.router.failures(prInst))
	}
	f.clk.Advance(f.cfg.PauseDuration.D())
	if f.router.Paused(prInst) {
		t.Error("the Instance is still paused after pauseDuration")
	}
}

// TestPauseThreeResponsesFailed (Opus B3's reproduction): three
// response.failed streams in a row pause the Instance; a 500 run is the
// control that pauses today.
func TestPauseThreeResponsesFailed(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{ResponsesEnd: "failed"})
	for i := 1; i <= 3; i++ {
		f.prSend(t, "/v1/responses", prRespStream)
	}
	if !f.router.Paused(prInst) {
		t.Fatalf("three response.failed streams left the Instance unpaused, failures %d", f.router.failures(prInst))
	}
}

// TestPauseCompletedRequestResetsTheRun: two 500s, then a completed request:
// the run starts over. A capped Responses reply (response.completed with
// status incomplete) resets it too.
func TestPauseCompletedRequestResetsTheRun(t *testing.T) {
	f := newScFix(t, scOpts{})
	e := f.engines[0]
	fail := enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"boom"}}`}
	e.Set(fail)
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	f.wantFailures(t, "two 500s", 2)
	e.Set(enginetest.Behaviour{})
	f.prSend(t, scChat, prChatStream)
	f.wantFailures(t, "a completed stream", 0)

	e.Set(fail)
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	f.wantFailures(t, "a 500", 1)
	f.retarget(f.uids[0], sseEngine(t, respCreated, respDelta, respCappedEnd))
	resp, body := f.prSend(t, "/v1/responses", prRespStream)
	if n := errorEvents(body); n != 0 {
		t.Errorf("a capped reply gave %d error events, want none: %q", n, body)
	}
	if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.Finish == nil || *rec.Finish != "incomplete" || rec.CutBy != nil {
		t.Errorf("capped reply: finish %v cutBy %v, want incomplete and none", rec.Finish, rec.CutBy)
	}
	f.wantFailures(t, "a capped reply", 0)
}

// TestEngineErrorEventPassesThrough (@hpc-burst-science-agent, 31 Oct 2026):
// a vLLM stream that fails mid-way sends its error event. It passes through
// as the client's one error event, the engine's [DONE] is dropped, the
// record says truncated with the event's code, and it counts. An error
// event that is not JSON is withheld and the front's stream_cut replaces it.
func TestEngineErrorEventPassesThrough(t *testing.T) {
	cases := []struct {
		name, path, req string
		events          []string
		wantEngineEvent string // the engine's event the client must get, or "" for the front's stream_cut
		wantCode        string
	}{
		{"chat error event", scChat, prChatStream, []string{chatToken, vllmChatError, vllmDone}, vllmChatError, "500"},
		{"chat error event not JSON", scChat, prChatStream, []string{chatToken, vllmChatBadError, vllmDone}, "", ""},
		{"responses error event", "/v1/responses", prRespStream, []string{respCreated, respDelta, respError}, respError, "server_error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			f.retarget(f.uids[0], sseEngine(t, c.events...))
			resp, body := f.prSend(t, c.path, c.req)
			if n := errorEvents(body); n != 1 {
				t.Errorf("the client got %d error events, want exactly 1: %q", n, body)
			}
			if strings.Contains(body, "[DONE]") {
				t.Errorf("the engine's [DONE] reached the client after its error: %q", body)
			}
			switch {
			case c.wantEngineEvent != "" && !strings.Contains(body, c.wantEngineEvent):
				t.Errorf("the engine's error event did not pass through unchanged: %q", body)
			case c.wantEngineEvent == "" && (!strings.Contains(body, `"code":"stream_cut"`) || strings.Contains(body, "not JSON\n")):
				t.Errorf("want the front's stream_cut in place of the engine's unreadable event: %q", body)
			}
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			if rec.Finish == nil || *rec.Finish != "truncated" || rec.CutBy == nil || *rec.CutBy != cutUpstream {
				t.Errorf("finish %v cutBy %v, want truncated and upstream", rec.Finish, rec.CutBy)
			}
			if got := ""; rec.ErrorCode != nil {
				got = *rec.ErrorCode
				if got != c.wantCode {
					t.Errorf("errorCode %q, want %q", got, c.wantCode)
				}
			} else if c.wantCode != "" {
				t.Errorf("errorCode missing, want %q", c.wantCode)
			}
			f.wantFailures(t, c.name, 1)
		})
	}
}
