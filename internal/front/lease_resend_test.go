package front

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestManagedResendDropsTheInstanceLease (PR #95 review: Astra B11, Opus
// B4): the Instance's connection is refused, the one resend goes to the
// attached LLMLab target, and the Instance's lease end passes while LLMLab
// answers. The lease timer belonged to the Instance, so it must not cut the
// LLMLab request. Window 15 min and lease end t0 + 20 min: the Instance is
// routable at t0 (drain start t0 + 5 min); the clock then moves 21 min.
func TestManagedResendDropsTheInstanceLease(t *testing.T) {
	f := newScFix(t, scOpts{profileWindow: "15m", inst: func(_ int, o *instanceOpts) { o.leaseEnd = t0.Add(20 * time.Minute) }})
	var arrived atomic.Int32
	hold := make(chan struct{})
	llm := rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		arrived.Add(1)
		select {
		case <-hold:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	})
	f.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	f.managedProvider("llmlab-a", "llmlab", "llmlab", "http://"+llm+"/v1", "llmlab-upstream-key-for-lease-test-01", chCatalog)
	f.managedMD("md-llm", scModel, "llmlab", chCatalog, "llmlab-a")
	f.trans.Set(f.uids[0], refusedAddr)
	f.apply()

	call := scGo(f.harness, errChatBody(scModel, false, "hello"))
	errWaitFor(t, "the resend to reach LLMLab", func() bool { return arrived.Load() == 1 })
	f.clk.Advance(21 * time.Minute)
	close(hold)
	resp, body := call.wait(t)
	if !strings.Contains(f.logs.String(), "no connection to the target") {
		t.Fatalf("the Instance was not tried first, so its lease timer was never armed:\n%s", f.logs.String())
	}
	if resp.StatusCode != 200 || scEmitterOf(resp) != emitterProvider {
		t.Fatalf("status %d emitter %q, want 200 from LLMLab: %s", resp.StatusCode, scEmitterOf(resp), body)
	}
	if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.CutBy != nil {
		t.Errorf("cutBy %q, want none", *rec.CutBy)
	}
}
