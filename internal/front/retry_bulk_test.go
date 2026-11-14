package front

// The retry on the bulk path (#539, Q4): a bulk request written and then closed before its headers, or
// answered 5xx before any body byte, is retried to another Instance like a token request. A close on the
// bulk transport still counts nothing toward the pause: it takes the bulk registration away and
// re-probes it (HBEP-6, The bulk path). The fixture's Instances share one bulk engine and one token
// engine; the second Instance's bulk transport is unregistered, so its retry reaches the token engine and
// the two attempts are told apart by the engine that received each.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

func bulkRetryFix(t *testing.T) (*bulkFront, string) {
	t.Helper()
	f := newBulkFront(t, 2, false)
	key := scKeys(t, f.keys, f.keys[0], 1)[0]
	f.bulk.unregister(f.uids[1])
	return f, key
}

func TestABulkRequestClosedBeforeItsHeadersIsRetriedAndRaisesNoPause(t *testing.T) {
	f, key := bulkRetryFix(t)
	f.engBulk.Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	resp := f.post("/v1/chat/completions", chatPlain, "x-session-id", key)
	body := readAll(t, resp)
	if resp.StatusCode != 200 || scInstanceOf(resp) != f.keys[1] || resp.Header.Get(headerShouldRetry) != "" {
		t.Fatalf("status %d from %q, x-should-retry %q, want 200 from %s and none: %s", resp.StatusCode, scInstanceOf(resp), resp.Header.Get(headerShouldRetry), f.keys[1], body)
	}
	if nb, nt := len(f.engBulk.Requests()), len(f.engToken.Requests()); nb != 1 || nt != 1 {
		t.Errorf("the bulk engine saw %d requests and the token engine %d, want one each", nb, nt)
	}
	if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
		t.Errorf("the bulk close counted toward the pause: %d failures", n)
	}
	if f.bulk.tun.Registered(f.uids[0]) {
		t.Error("the bulk close left the first Instance's bulk transport registered")
	}
	if _, ok := f.bulk.prober.reprobeAt(); !ok {
		t.Error("the bulk close did not put the bulk transport on the re-probe schedule")
	}
}

func TestABulk5xxIsRetriedAndCountsTowardThePause(t *testing.T) {
	f, key := bulkRetryFix(t)
	f.engBulk.Set(enginetest.Behaviour{Status: http.StatusInternalServerError, ErrorBody: `{"error":{"message":"boom"}}`})
	resp := f.post("/v1/chat/completions", chatPlain, "x-session-id", key)
	body := readAll(t, resp)
	if resp.StatusCode != 200 || scInstanceOf(resp) != f.keys[1] || strings.Contains(body, "boom") {
		t.Fatalf("status %d from %q: %s; want 200 from %s, never the 500", resp.StatusCode, scInstanceOf(resp), body, f.keys[1])
	}
	if nb, nt := len(f.engBulk.Requests()), len(f.engToken.Requests()); nb != 1 || nt != 1 {
		t.Errorf("the bulk engine saw %d requests and the token engine %d, want one each", nb, nt)
	}
	if n := f.router.failures(f.keys[0]); n != 1 {
		t.Errorf("%d failures on %s after its 5xx, want 1, as on the token path", n, f.keys[0])
	}
}
