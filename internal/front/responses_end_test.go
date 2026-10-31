package front

import (
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// TestCappedResponsesStreamThroughLLMLabEndsCleanly: LLMLab, as OpenAI's API
// does, ends a capped Responses stream with response.incomplete and status
// "incomplete" (the double's Model.MaxTokensCap). That is an explicit end, so
// the front adds no stream_cut event and records the final response's status
// as the finish (HBEP-5, Usage records), not "truncated".
func TestCappedResponsesStreamThroughLLMLabEndsCleanly(t *testing.T) {
	h := newHarness(t, nil)
	const upstreamKey = "llmlab-upstream-key-for-the-cap-test"
	ll := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{upstreamKey},
		Models: []llmlabtest.Model{{ID: "m-cat", MaxTokensCap: 2}}})
	ll.SetReply("one two three four five")
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab", "llmlab", "llmlab", ll.BaseURL(), upstreamKey, "m-cat")
	h.managedMD("mm", "m", "llmlab", "m-cat", "llmlab")
	h.apply()
	resp := h.post("/v1/responses", `{"model":"m","stream":true,"input":"hi"}`)
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "response.incomplete") {
		t.Fatalf("the double did not cap the stream, so the case is not tested: %s", body)
	}
	if strings.Contains(body, "stream_cut") {
		t.Errorf("the front cut an explicitly ended stream: %s", body)
	}
	rec := h.record(resp.Header.Get(HeaderRequestID))
	if rec.Finish == nil || *rec.Finish != "incomplete" || rec.CutBy != nil {
		t.Errorf("finish %v, cutBy %v; want the final response's status, incomplete, and no cut", rec.Finish, rec.CutBy)
	}
}
