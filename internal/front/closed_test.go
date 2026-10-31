package front

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestClosedBeforeHeadersIsUpstreamClosedAndNotResent: the connection to an
// Instance closes after the request was written and before any response
// header, as frps closes a request it accepted for a dead tunnel. HBEP-5's
// Errors table, as HBEP-6 amends it: 502 upstream_closed with detail tunnel,
// cutBy tunnel, not resent, and it counts toward the pause: after
// pauseAfterFailures (3) of them the Instance is paused.
func TestClosedBeforeHeadersIsUpstreamClosedAndNotResent(t *testing.T) {
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	e.Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	other := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "m", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.apply()
	for i := 1; i <= 3; i++ {
		resp := h.post("/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"x"}]}`)
		body := readAll(t, resp)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("request %d: status %d, want 502: %s", i, resp.StatusCode, body)
		}
		eb := decodeErrorBody(t, body)
		if eb.Code != "upstream_closed" || eb.Detail == nil || *eb.Detail != "tunnel" {
			t.Errorf("request %d: code %q detail %v, want upstream_closed and tunnel", i, eb.Code, eb.Detail)
		}
		if resp.Header.Get(HeaderEmitter) != "front" || resp.Header.Get(HeaderInstance) != "ns/md-0" {
			t.Errorf("request %d: emitter %q instance %q, want the front and the target", i, resp.Header.Get(HeaderEmitter), resp.Header.Get(HeaderInstance))
		}
		rec := h.record(resp.Header.Get(HeaderRequestID))
		if rec.CutBy == nil || *rec.CutBy != "tunnel" {
			t.Errorf("request %d: cutBy %v, want tunnel", i, rec.CutBy)
		}
		if n := len(e.Requests()); n != i {
			t.Errorf("after request %d the engine saw %d requests: a written request must not be resent", i, n)
		}
	}
	if len(other.Requests()) != 0 {
		t.Errorf("a written request went to another engine")
	}
	if !h.router.Paused("ns/md-0") {
		t.Error("three closes before the headers must pause the Instance (they count as failed connections)")
	}
}

// decodeErrorBody reads the front's error body.
func decodeErrorBody(t *testing.T, body string) errorFields {
	t.Helper()
	var b errorBody
	if err := json.Unmarshal([]byte(body), &b); err != nil {
		t.Fatalf("not an error body: %s", body)
	}
	return b.Error
}
