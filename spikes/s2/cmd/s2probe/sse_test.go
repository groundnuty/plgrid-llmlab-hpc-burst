package main

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Fixtures for the stream verdict at the byte level. Each one serves exact bytes through doStream,
// so they exercise the whole client, not a helper, and a changed verdict shows up as a failing test.

const (
	evRole   = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n"
	evTok    = "data: {\"choices\":[{\"delta\":{\"content\":\"tok\"},\"finish_reason\":null}]}\n\n"
	evFinish = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n"
	evDone   = "data: [DONE]\n\n"
)

// body assembles a stream of n content events between the role chunk and the finish chunk.
func body(n int, extra ...string) string {
	return evRole + strings.Repeat(evTok, n) + strings.Join(extra, "") + evFinish + evDone
}

// rawStream writes each chunk as its own flushed write, then ends the response cleanly.
func rawStream(chunks ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, c := range chunks {
			fmt.Fprint(w, c)
			fl.Flush()
			time.Sleep(time.Millisecond)
		}
	}
}

func wantComplete(t *testing.T, res streamResult, tokens int) {
	t.Helper()
	if !res.Complete || res.Tokens != tokens || res.Error != "" {
		t.Fatalf("want a complete %d-token stream, got %+v", tokens, res)
	}
}

func wantRejected(t *testing.T, res streamResult, errPart string) {
	t.Helper()
	if res.Complete {
		t.Fatalf("must not count as complete: %+v", res)
	}
	if res.CutAfterSec <= 0 || !strings.Contains(res.Error, errPart) {
		t.Fatalf("a rejected stream must record when and why (error containing %q): %+v", errPart, res)
	}
}

// --- valid streams the assembler must accept however the transport slices them ---

func TestSSESplitIntoSingleBytes(t *testing.T) {
	all := body(5)
	chunks := make([]string, 0, len(all))
	for i := range all {
		chunks = append(chunks, all[i:i+1])
	}
	wantComplete(t, run(t, rawStream(chunks...), 5, 5), 5)
}

func TestSSECoalescedIntoOneRead(t *testing.T) {
	wantComplete(t, run(t, rawStream(body(5)), 5, 5), 5)
}

func TestSSETerminatorSplitAcrossReadsStillCompletes(t *testing.T) {
	all := body(5)
	cut := len(all) - 1 // the blank line of [DONE] arrives in a second read
	wantComplete(t, run(t, rawStream(all[:cut], all[cut:]), 5, 5), 5)
}

func TestSSECRLFLineEndings(t *testing.T) {
	wantComplete(t, run(t, rawStream(strings.ReplaceAll(body(5), "\n", "\r\n")), 5, 5), 5)
}

func TestSSECommentsAreIgnoredAndCounted(t *testing.T) {
	res := run(t, rawStream(": keep-alive\n\n"+evRole+evTok+": another\n\n"+strings.Repeat(evTok, 4)+evFinish+evDone), 5, 5)
	wantComplete(t, res, 5)
	if res.Comments != 2 {
		t.Fatalf("want 2 comment lines counted, got %d", res.Comments)
	}
}

// --- streams an SSE consumer must not accept ---

// The last event is not complete until its blank line arrives: [DONE] with zero or one trailing newline is unfinished.
func TestSSETruncatedTerminatorZeroTrailingNewlines(t *testing.T) {
	all := strings.TrimSuffix(body(5), "\n\n")
	wantRejected(t, run(t, rawStream(all), 5, 5), "unfinished")
}

func TestSSETruncatedTerminatorOneTrailingNewline(t *testing.T) {
	all := strings.TrimSuffix(body(5), "\n")
	wantRejected(t, run(t, rawStream(all), 5, 5), "unfinished")
}

// The same bytes as the previous case, but the connection ends while the second half is still in flight.
func TestSSETerminatorSplitAcrossReadsThenCut(t *testing.T) {
	all := body(5)
	wantRejected(t, run(t, rawStream(all[:len(all)-1]), 5, 5), "unfinished")
}

// A finish chunk with no blank line before [DONE] merges both into one event whose payload is not JSON.
func TestSSEMissingSeparatorBeforeDone(t *testing.T) {
	bad := evRole + strings.Repeat(evTok, 5) + strings.TrimSuffix(evFinish, "\n") + evDone
	wantRejected(t, run(t, rawStream(bad), 5, 5), "malformed")
}

// Two content chunks glued into one event must not count as two tokens.
func TestSSEMissingSeparatorBetweenTokens(t *testing.T) {
	bad := evRole + strings.Repeat(evTok, 3) + strings.TrimSuffix(evTok, "\n") + evTok + evFinish + evDone
	wantRejected(t, run(t, rawStream(bad), 5, 5), "malformed")
}

func TestSSEErrorPayloadMidStream(t *testing.T) {
	e := "data: {\"error\":{\"message\":\"engine died\",\"type\":\"server_error\"}}\n\n"
	wantRejected(t, run(t, rawStream(body(5, e)), 5, 5), "error payload")
}

func TestSSEErrorEventType(t *testing.T) {
	e := "event: error\ndata: {\"message\":\"upstream reset\"}\n\n"
	wantRejected(t, run(t, rawStream(body(5, e)), 5, 5), "error event")
}

func TestSSEMalformedPayload(t *testing.T) {
	wantRejected(t, run(t, rawStream(body(5, "data: {not json\n\n")), 5, 5), "malformed")
}

func TestSSEEventAfterDone(t *testing.T) {
	e := "data: {\"error\":{\"message\":\"late\"}}\n\n"
	wantRejected(t, run(t, rawStream(body(5)+e), 5, 5), "after [DONE]")
}
