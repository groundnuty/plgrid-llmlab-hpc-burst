package front

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestShutdownCutsStreamsAndKeepsTheirRecords (@hpc-burst-science-agent, 31
// Oct 2026; PR #95 review, Opus B5): the front stops with three streams
// running. After the grace each stream gets exactly one error event, detail
// shutdown, and no [DONE]; every usage record is written, cutBy
// front_shutdown, before the sink closes, with no gap in the counters.
func TestShutdownCutsStreamsAndKeepsTheirRecords(t *testing.T) {
	f := newScFix(t, scOpts{instances: 3, engineReal: true})
	f.setAll(enginetest.Behaviour{Tokens: 100000, PerToken: 20 * time.Millisecond})
	reqs := []struct{ path, body string }{{scChat, prChatStream}, {scChat, prChatStream}, {"/v1/responses", prRespStream}}
	bodies := make([]string, len(reqs))
	var wg sync.WaitGroup
	for i, rq := range reqs {
		resp := f.errPost(rq.path, rq.body)
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status %d", i, resp.StatusCode)
		}
		br := bufio.NewReader(resp.Body)
		first, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("request %d: first event: %v", i, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rest, _ := io.ReadAll(br)
			resp.Body.Close()
			bodies[i] = first + string(rest)
		}()
	}

	stopServing([]*http.Server{f.srv}, f.cancelBase, f.proxy, 300*time.Millisecond, discardLog())
	if err := f.sink.Close(); err != nil {
		t.Fatalf("sink: %v", err)
	}
	wg.Wait()

	for i, b := range bodies {
		if n := errorEvents(b); n != 1 || !strings.Contains(b, `"detail":"shutdown"`) || strings.Contains(b, "[DONE]") {
			t.Errorf("stream %d: %d error events (want exactly 1, detail shutdown, no [DONE]); its end: %q", i, n, b[max(0, len(b)-300):])
		}
	}
	recs := f.records()
	if len(recs) != len(reqs) {
		t.Fatalf("%d usage records after the stop, want %d", len(recs), len(reqs))
	}
	for i, r := range recs {
		if r.CutBy == nil || *r.CutBy != cutFrontShutdown {
			t.Errorf("record %d: cutBy %v, want front_shutdown", i, r.CutBy)
		}
		if i > 0 && r.Counter != recs[i-1].Counter+1 {
			t.Errorf("counters %d then %d: a gap", recs[i-1].Counter, r.Counter)
		}
	}
}

// TestShutdownBeforeFirstByte: requests the stop meets before their first
// byte, one waiting for the engine's headers and one queued for a full
// Instance, get 503 shutting_down with Retry-After 5, and records with
// cutBy front_shutdown.
func TestShutdownBeforeFirstByte(t *testing.T) {
	one := int32(1)
	f := newScFix(t, scOpts{md: func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &one} }})
	hold := make(chan struct{})
	defer close(hold)
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel, false, "held"))
	errWaitFor(t, "the first request to reach the engine", func() bool { return f.engines[0].Open() == 1 })
	queued := scGo(f.harness, errChatBody(scModel, false, "queued"))
	errWaitFor(t, "the second request to queue", func() bool { return f.router.Status().Waiting == 1 })

	stopServing([]*http.Server{f.srv}, f.cancelBase, f.proxy, 200*time.Millisecond, discardLog())
	for _, c := range []*scCall{held, queued} {
		resp, body := c.wait(t)
		checkErrorBody(t, body, codeShuttingDown, "")
		if resp.StatusCode != 503 || resp.Header.Get("Retry-After") != "5" {
			t.Errorf("status %d Retry-After %q, want 503 and 5: %s", resp.StatusCode, resp.Header.Get("Retry-After"), body)
		}
		if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.CutBy == nil || *rec.CutBy != cutFrontShutdown {
			t.Errorf("cutBy %v, want front_shutdown", rec.CutBy)
		}
	}
}
