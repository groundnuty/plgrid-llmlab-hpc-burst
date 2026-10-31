package front

import (
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The science agent's ruling on how a Responses stream ends (31 Oct 2026):
// response.incomplete is a clean end with no pause count; response.failed is
// an engine error after the stream started, passed through unchanged, not
// resent, with no cutBy, recorded failed with its error.code, and counted
// toward the Instance's pause like a 5xx; a 4xx before the stream starts
// counts nothing.

func oneInstance(t *testing.T) (*harness, *enginetest.Engine) {
	t.Helper()
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "m", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.apply()
	return h, e
}

const responsesStream = `{"model":"m","stream":true,"input":"hello"}`

func TestResponsesIncompleteIsACleanEndWithNoPauseCount(t *testing.T) {
	h, e := oneInstance(t)
	e.Set(enginetest.Behaviour{ResponsesEnd: "incomplete"})
	resp := h.post("/v1/responses", responsesStream)
	body := readAll(t, resp)
	if !strings.Contains(body, "event: response.incomplete") || strings.Contains(body, "stream_cut") {
		t.Errorf("want response.incomplete passed through and no stream_cut: %s", body)
	}
	rec := h.record(resp.Header.Get(HeaderRequestID))
	if rec.Finish == nil || *rec.Finish != "incomplete" || rec.CutBy != nil || rec.ErrorCode != nil {
		t.Errorf("finish %v cutBy %v errorCode %v; want incomplete, no cut, no error", rec.Finish, rec.CutBy, rec.ErrorCode)
	}
	if n := h.router.failures("ns/md-0"); n != 0 {
		t.Errorf("pause count %d after response.incomplete, want 0", n)
	}
}

func TestResponsesFailedCountsTowardThePause(t *testing.T) {
	h, e := oneInstance(t)
	e.Set(enginetest.Behaviour{ResponsesEnd: "failed"})
	resp := h.post("/v1/responses", responsesStream)
	body := readAll(t, resp)
	// The terminal event exactly as the engine double writes it.
	want := `event: response.failed` + "\n" + `data: {"response":{"error":{"code":"server_error","message":"the engine failed"},"id":"resp-1","status":"failed"},"type":"response.failed"}`
	if !strings.Contains(body, want) {
		t.Errorf("the terminal event was not passed through unchanged:\n%s", body)
	}
	if strings.Contains(body, "stream_cut") {
		t.Errorf("response.failed is not a cut: %s", body)
	}
	rec := h.record(resp.Header.Get(HeaderRequestID))
	if rec.Finish == nil || *rec.Finish != "failed" || rec.CutBy != nil || rec.ErrorCode == nil || *rec.ErrorCode != "server_error" {
		t.Errorf("finish %v cutBy %v errorCode %v; want failed, no cut, server_error", rec.Finish, rec.CutBy, rec.ErrorCode)
	}
	if n := h.router.failures("ns/md-0"); n != 1 {
		t.Errorf("pause count %d after response.failed, want 1", n)
	}
	if n := len(e.Requests()); n != 1 {
		t.Errorf("the engine saw %d requests: a started stream is never resent", n)
	}
}

func TestA5xxCountsTowardThePauseAndA4xxDoesNot(t *testing.T) {
	h, e := oneInstance(t)
	e.Set(enginetest.Behaviour{Status: http.StatusBadRequest, ErrorBody: `{"error":{"message":"bad"}}`})
	readAll(t, h.post("/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"x"}]}`))
	if n := h.router.failures("ns/md-0"); n != 0 {
		t.Errorf("pause count %d after a 400, want 0", n)
	}
	e.Set(enginetest.Behaviour{Status: http.StatusInternalServerError, ErrorBody: `{"error":{"message":"boom"}}`})
	readAll(t, h.post("/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"x"}]}`))
	if n := h.router.failures("ns/md-0"); n != 1 {
		t.Errorf("pause count %d after a 500, want 1", n)
	}
}
