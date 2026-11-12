package front

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// The two waits of a stop in these tests (#432). The streams never finish on
// their own, so http.Server.Shutdown always runs out its grace: it is kept
// short, which only makes the tests fast. The wait for the cut handlers ends
// the moment they have returned, so its bound is generous: a test waits on that
// state, and a slow runner cannot lose a record to a window of a few hundred
// milliseconds.
const (
	testShutdownGrace = 300 * time.Millisecond
	testHandlerWait   = 30 * time.Second
)

// TestShutdownCutsStreamsAndKeepsTheirRecords (@hpc-burst-science-agent, 31
// Oct 2026; PR #95 review, Opus B5): the front stops with three streams
// running. After the grace each stream gets exactly one error event, detail
// shutdown, and no [DONE]; every usage record is written, cutBy
// front_shutdown, before the sink closes, with no gap in the counters.
func TestShutdownCutsStreamsAndKeepsTheirRecords(t *testing.T) {
	f := newScFix(t, scOpts{instances: 3, engineReal: true})
	stopThreeStreams(t, f, nil)
}

// TestShutdownWaitsForAHandlerSlowerThanTheShutdownGrace (#432): one handler is
// held, after the cut and before its usage record's write, for a time well past
// the Shutdown grace. The stop still waits for it, by state and not by a
// window: its record is there, and the handler had returned when stopServing
// did. With the handler wait bound back at the Shutdown grace, stopServing
// gives up at 300 ms, the sink closes first and the record is lost: "2 usage
// records after the stop, want 3", the line CI showed.
func TestShutdownWaitsForAHandlerSlowerThanTheShutdownGrace(t *testing.T) {
	f := newScFix(t, scOpts{instances: 3, engineReal: true})
	held := holdFirstRecord(f.proxy, func() bool { return context.Cause(f.base) == errCutShutdown }, time.Second)
	stopThreeStreams(t, f, func() {
		if !held.released.Load() {
			t.Errorf("stopServing returned while a handler was still held before its record's write: it waited for a window, not for the handlers")
		}
	})
	if !held.entered.Load() {
		t.Fatal("no handler reached its record's write, so the test held nothing")
	}
	if !held.afterCut.Load() {
		t.Fatal("the handler was held before the shutdown cut, so the stop's wait for handlers was never at stake")
	}
}

// heldRecord is a handler held in finish before its record's write, and what
// the test saw of it.
type heldRecord struct {
	entered  atomic.Bool // a handler reached the hook
	afterCut atomic.Bool // the cut had happened (cause: the shutdown) when it did
	released atomic.Bool // its hold is over
}

// holdFirstRecord holds the first handler of p to reach its record's write for
// hold, and notes whether cut said the shutdown cut had happened by then. The
// hold is a delay the code under test has to outlast, not a window the test
// asserts within: a stop waits for the handlers' return, however long that
// takes, up to its bound. Call it before any request, so that no handler reads
// the hook while it is written.
func holdFirstRecord(p *Proxy, cut func() bool, hold time.Duration) *heldRecord {
	h := &heldRecord{}
	p.beforeWrite = func(string) {
		if !h.entered.CompareAndSwap(false, true) {
			return
		}
		h.afterCut.Store(cut())
		time.Sleep(hold)
		h.released.Store(true)
	}
	return h
}

// stopThreeStreams opens three streams on f, stops the front with
// testShutdownGrace and testHandlerWait, and checks the streams' ends and the
// usage records. afterStop, when set, runs as soon as stopServing has returned.
func stopThreeStreams(t *testing.T, f *scFix, afterStop func()) {
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

	stopServing([]*http.Server{f.srv}, f.cancelBase, f.proxy, testShutdownGrace, testHandlerWait, discardLog())
	if afterStop != nil {
		afterStop()
	}
	if err := f.sink.Close(); err != nil {
		t.Fatalf("sink: %v", err)
	}
	wg.Wait()
	// The next start continues the counters with no gap.
	next, err := usage.Open(f.cfg.UsageDir)
	if err != nil {
		t.Fatal(err)
	}
	after, err := next.Write(usage.Record{RequestID: "after-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}

	for i, b := range bodies {
		if n := errorEvents(b); n != 1 || !strings.Contains(b, `"detail":"shutdown"`) || strings.Contains(b, "[DONE]") {
			t.Errorf("stream %d: %d error events (want exactly 1, detail shutdown, no [DONE]); its end: %q", i, n, b[max(0, len(b)-300):])
		}
	}
	recs := f.records()
	// The restart's own record is told by its ID, not by the count: with a
	// stream's record missing, a count would take it for that stream's.
	if n := len(recs); n > 0 && recs[n-1].RequestID == "after-restart" {
		if n > 1 && after != recs[n-2].Counter+1 {
			t.Errorf("the record after a restart has counter %d, want %d: a clean stop left a gap", after, recs[n-2].Counter+1)
		}
		recs = recs[:n-1]
	}
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
// byte, a non-streamed one and a streamed one waiting for their headers on
// the two Instances (cap 1 each) and a third queued behind them, get 503
// shutting_down, detail shutdown, with Retry-After 5, and records with cutBy
// front_shutdown.
func TestShutdownBeforeFirstByte(t *testing.T) {
	one := int32(1)
	f := newScFix(t, scOpts{instances: 2, md: func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &one} }})
	var arrived atomic.Int32
	silent := rawEngine(t, func(w http.ResponseWriter, r *http.Request) { arrived.Add(1); <-r.Context().Done() })
	f.retarget(f.uids[0], silent)
	f.retarget(f.uids[1], silent)
	whole := scGo(f.harness, errChatBody(scModel, false, "whole"))
	errWaitFor(t, "the non-streamed request to reach an engine", func() bool { return arrived.Load() == 1 })
	streamed := scGo(f.harness, errChatBody(scModel, true, "streamed"))
	errWaitFor(t, "the streamed request to reach the other engine", func() bool { return arrived.Load() == 2 })
	queued := scGo(f.harness, errChatBody(scModel, false, "queued"))
	errWaitFor(t, "the third request to queue", func() bool { return f.router.Status().Waiting == 1 })

	stopServing([]*http.Server{f.srv}, f.cancelBase, f.proxy, 200*time.Millisecond, testHandlerWait, discardLog())
	for i, c := range []*scCall{whole, streamed, queued} {
		resp, body := c.wait(t)
		checkErrorBody(t, body, codeShuttingDown, "shutdown")
		if resp.StatusCode != 503 || resp.Header.Get("Retry-After") != "5" {
			t.Errorf("request %d: status %d Retry-After %q, want 503 and 5: %s", i, resp.StatusCode, resp.Header.Get("Retry-After"), body)
		}
		if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.CutBy == nil || *rec.CutBy != cutFrontShutdown {
			t.Errorf("request %d: cutBy %v, want front_shutdown", i, rec.CutBy)
		}
	}
}
