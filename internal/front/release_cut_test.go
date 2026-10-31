package front

import (
	"bufio"
	"io"
	"net/http"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// PR #95 review (Astra B2, Opus B1): every cut of a body the front copies
// raw ends in http.ErrAbortHandler, and the reservation must still be
// released (HBEP-5, Request path step 9: release exactly once). Each case
// ends the request's only reservation, so the Instance must count 0 after.

// TestReleaseOnRawStreamCut: a stream of a path the front does not judge,
// killed by the engine after 2 chunks.
func TestReleaseOnRawStreamCut(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})
	resp := f.errPost("/v1/audio/speech", `{"model":"`+scModel+`","stream":true,"input":"hi"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	id := resp.Header.Get("hpcburst-request-id")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	f.waitRecord(id)
	if rec := f.record(id); rec.CutBy == nil || *rec.CutBy != cutUpstream {
		t.Errorf("cutBy %v, want upstream", rec.CutBy)
	}
	f.zeroCounts(scDep, f.names[0])
}

// TestReleaseOnWholeBodyCut: a non-streamed reply whose body is cut after
// its headers (Content-Length 100, 10 bytes sent).
func TestReleaseOnWholeBodyCut(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.trans.Set(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		abortAfter(w, 200, map[string]string{"Content-Type": "application/json", "Content-Length": "100"}, `{"id":"x",`)
	}))
	// The front aborts the connection, so the client may see no headers.
	if resp, err := f.tryPost(scChat, errChatBody(scModel, false, "hello")); err == nil {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	errWaitFor(t, "the usage record", func() bool { return len(f.records()) == 1 })
	if rec := f.records()[0]; rec.CutBy == nil || *rec.CutBy != cutUpstream {
		t.Errorf("cutBy %v, want upstream", rec.CutBy)
	}
	f.zeroCounts(scDep, f.names[0])
}

// TestReleaseOnClientGoneRawStream: the client reads one chunk of an unjudged
// stream and goes away; the front's next write fails.
func TestReleaseOnClientGoneRawStream(t *testing.T) {
	f := newScFix(t, scOpts{engineReal: true})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 100000, PerToken: 2_000_000}) // 2 ms per chunk, on the wall clock
	resp := f.errPost("/v1/audio/speech", `{"model":"`+scModel+`","stream":true,"input":"hi"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	id := resp.Header.Get("hpcburst-request-id")
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	resp.Body.Close()
	f.waitRecord(id)
	f.zeroCounts(scDep, f.names[0])
}
