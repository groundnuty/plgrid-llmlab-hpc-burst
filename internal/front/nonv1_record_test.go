package front

import (
	"net/http"
	"testing"
)

// TestPathsOutsideV1GetARecord (PR #95 review, Opus nit; HBEP-5 keeps one
// usage record per request): a request off /v1/ answers 404, attributed, and
// is recorded.
func TestPathsOutsideV1GetARecord(t *testing.T) {
	h := newHarness(t, nil)
	resp := h.errGet("/agent/v1/register")
	readAll(t, resp)
	id := resp.Header.Get("hpcburst-request-id")
	if resp.StatusCode != http.StatusNotFound || id == "" {
		t.Fatalf("status %d request id %q, want an attributed 404", resp.StatusCode, id)
	}
	h.waitRecord(id)
	if rec := h.record(id); rec.Status != http.StatusNotFound || rec.Path != "/agent/v1/register" {
		t.Errorf("record status %d path %q, want 404 and the path", rec.Status, rec.Path)
	}
}
