//go:build realtime

package front

// The Test plan's 7,000-token stream in wall-clock time (HBEP-5, Test plan, Deadlines): one token
// per second, about 117 minutes, through the front at its default 2 h maxRequestDuration and
// 30 min idleTimeout, with every timer on the real clock. The fake-clock version runs in every
// test run; this one is built only with the realtime tag, so it neither slows nor skips in CI:
//
//	go test -tags realtime -run TestRealTime7000TokenStream -timeout 3h ./internal/front/

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

func TestRealTime7000TokenStream(t *testing.T) {
	h := newHarnessReal(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key})
	e.Set(enginetest.Behaviour{Tokens: 7000, PerToken: time.Second})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	h.apply()
	start := time.Now()
	resp := h.post("/v1/chat/completions", `{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"count"}]}`)
	body := readAll(t, resp)
	took := time.Since(start)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) || !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") || strings.Contains(body, "stream_cut") {
		t.Fatalf("the stream did not complete: it ends %q", body[max(0, len(body)-300):])
	}
	if n := strings.Count(body, `"content":"t`); n != 7000 {
		t.Errorf("%d content chunks, want 7000", n)
	}
	rec := h.record(resp.Header.Get(HeaderRequestID))
	if rec.CutBy != nil || rec.Finish == nil || *rec.Finish != "stop" {
		t.Errorf("usage record: finish %v, cutBy %v", rec.Finish, rec.CutBy)
	}
	t.Logf("7,000 tokens in %s of wall time (the default maxRequestDuration is 2h)", took.Round(time.Second))
	if took < 6990*time.Second {
		t.Errorf("the stream took %s: the pace was not one token per second, so it proves nothing about the 2 h deadline", took)
	}
}
