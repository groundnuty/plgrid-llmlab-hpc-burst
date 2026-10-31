package front

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestSmokeChatThroughOneInstance is the path every scenario builds on: a
// client request reaches the engine with only model's value changed, the
// Instance key and x-session-id set, and comes back with the attribution
// headers and a usage record.
func TestSmokeChatThroughOneInstance(t *testing.T) {
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	uid := h.instance("md", "md-0", "helios-a", e, key, instanceOpts{deployID: strings.Repeat("ab", 32)})
	h.apply()

	body := `{"model": "example-27b", "messages": [{"role": "user", "content": "hello there"}], "unknown_field": {"x": 1}}`
	resp := h.post("/v1/chat/completions", body, "x-session-id", "conv-1")
	got := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, got)
	}
	for k, want := range map[string]string{
		HeaderEmitter: "instance", HeaderProvider: "helios-a", HeaderDeploymentID: "abababababab",
		HeaderInstance: "ns/md-0", HeaderInstanceUID: string(uid),
	} {
		if v := resp.Header.Get(k); v != want {
			t.Errorf("header %s = %q, want %q", k, v, want)
		}
	}
	reqs := e.Requests()
	if len(reqs) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	// The served name is spec.model here, so the body must arrive unchanged.
	if !bytes.Equal(r.Body, []byte(body)) {
		t.Errorf("the engine's body differs:\n got %s\nwant %s", r.Body, body)
	}
	if r.Header.Get("Authorization") != "Bearer "+key {
		t.Errorf("the engine did not get the Instance key")
	}
	if r.Header.Get("X-Session-Id") != "conv-1" {
		t.Errorf("x-session-id = %q, want conv-1", r.Header.Get("X-Session-Id"))
	}
	rec := h.record(resp.Header.Get(HeaderRequestID))
	if rec.Instance != "ns/md-0" || rec.Status != 200 || rec.ClientKeyID != "tester" || rec.Level != "model" {
		t.Errorf("usage record %+v", rec)
	}
	if rec.Finish == nil || *rec.Finish != "stop" {
		t.Errorf("finish = %v, want stop", rec.Finish)
	}
}
