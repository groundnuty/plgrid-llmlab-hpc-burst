package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// fakeClock returns start+10ms, start+20ms, ... on successive calls.
func fakeClock(start time.Time) func() time.Time {
	n := 0
	return func() time.Time { n++; return start.Add(time.Duration(n) * 10 * time.Millisecond) }
}

// The events of a complete stream. Each ends with the blank line that completes an SSE event.
const (
	evRole    = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n"
	evHello   = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n"
	evWorld   = "data: {\"choices\":[{\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n"
	evFinish  = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n"
	evUsage   = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":768}}}\n\n"
	evUsageNC = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n"
	evDone    = "data: [DONE]\n\n"
)

const okStream = evRole + evHello + evWorld + evFinish + evUsage + evDone

func read(t *testing.T, s string, want int) (Result, error) {
	t.Helper()
	start := time.Unix(1000, 0)
	return readStream(strings.NewReader(s), start, fakeClock(start), want)
}

func wantRejected(t *testing.T, s string, want int, errPart string) {
	t.Helper()
	_, err := read(t, s, want)
	if err == nil {
		t.Fatalf("stream accepted, want an error containing %q", errPart)
	}
	if !strings.Contains(err.Error(), errPart) {
		t.Fatalf("error %q does not contain %q", err, errPart)
	}
}

func TestReadStreamComplete(t *testing.T) {
	r, err := read(t, okStream, 2)
	if err != nil {
		t.Fatal(err)
	}
	// The role-only first chunk carries no content: TTFT is the first non-empty delta (first clock call).
	if r.TTFT != 10*time.Millisecond || r.Latency != 20*time.Millisecond {
		t.Fatalf("ttft %v latency %v, want 10ms and 20ms", r.TTFT, r.Latency)
	}
	if r.Content != "Hello world" || r.PromptTokens != 1000 || r.CachedTokens != 768 || r.CompletionTokens != 2 {
		t.Fatalf("got %+v", r)
	}
	if _, err := read(t, okStream, 0); err != nil {
		t.Fatalf("with no exact count required: %v", err)
	}
}

func TestReadStreamNoCacheDetailsIsStillASuccess(t *testing.T) {
	r, err := read(t, evHello+evWorld+evFinish+evUsageNC+evDone, 2)
	if err != nil {
		t.Fatal(err)
	}
	if r.CachedTokens != -1 {
		t.Fatalf("cached tokens %d, want -1 when the server sends no details", r.CachedTokens)
	}
}

// Valid streams, however the transport slices them or ends its lines.
func TestReadStreamSplitIntoSingleBytes(t *testing.T) {
	start := time.Unix(1000, 0)
	r, err := readStream(iotest.OneByteReader(strings.NewReader(okStream)), start, fakeClock(start), 2)
	if err != nil || r.Content != "Hello world" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestReadStreamFinalBlankLineInASecondRead(t *testing.T) {
	cut := len(okStream) - 1
	start := time.Unix(1000, 0)
	body := io.MultiReader(strings.NewReader(okStream[:cut]), strings.NewReader(okStream[cut:]))
	if _, err := readStream(body, start, fakeClock(start), 2); err != nil {
		t.Fatal(err)
	}
}

func TestReadStreamCRLF(t *testing.T) {
	if _, err := read(t, strings.ReplaceAll(okStream, "\n", "\r\n"), 2); err != nil {
		t.Fatal(err)
	}
}

func TestReadStreamIgnoresComments(t *testing.T) {
	if _, err := read(t, ": keep-alive\n\n"+evHello+": another\n\n"+evWorld+evFinish+evUsage+evDone, 2); err != nil {
		t.Fatal(err)
	}
}

// Streams that must not count as a measured request.
func TestReadStreamWithoutDoneIsAnError(t *testing.T) {
	wantRejected(t, strings.TrimSuffix(okStream, evDone), 2, "without [DONE]")
}

func TestReadStreamDoneWithoutItsBlankLine(t *testing.T) {
	// Phase B's first client accepted these two: the last event is not complete until its blank line arrives.
	wantRejected(t, strings.TrimSuffix(okStream, "\n\n"), 2, "unfinished")
	wantRejected(t, strings.TrimSuffix(okStream, "\n"), 2, "unfinished")
}

func TestReadStreamMissingSeparatorGluesEvents(t *testing.T) {
	// Without the blank line between them, the two events merge into one payload that is not JSON.
	wantRejected(t, evRole+strings.TrimSuffix(evHello, "\n")+evWorld+evFinish+evUsage+evDone, 2, "malformed")
	wantRejected(t, evRole+evHello+evWorld+strings.TrimSuffix(evFinish, "\n")+evUsage+evDone, 2, "malformed")
}

func TestReadStreamErrorEvent(t *testing.T) {
	wantRejected(t, "data: {\"error\":{\"message\":\"boom\"}}\n\n"+evDone, 2, "error payload")
	wantRejected(t, evHello+"event: error\ndata: boom\n\n"+evDone, 2, "error event")
}

func TestReadStreamWithoutUsage(t *testing.T) {
	wantRejected(t, evHello+evWorld+evFinish+evDone, 2, "no usage")
}

func TestReadStreamWithoutFinishReason(t *testing.T) {
	wantRejected(t, evHello+evWorld+evUsage+evDone, 2, "no finish_reason")
}

func TestReadStreamWithoutContent(t *testing.T) {
	wantRejected(t, evRole+evFinish+evUsage+evDone, 2, "no content")
}

func TestReadStreamUsageCounts(t *testing.T) {
	zeroCompletion := strings.Replace(evUsage, "\"completion_tokens\":2", "\"completion_tokens\":0", 1)
	wantRejected(t, evHello+evWorld+evFinish+zeroCompletion+evDone, 0, "invalid usage counts")
	zeroPrompt := strings.Replace(evUsage, "\"prompt_tokens\":1000", "\"prompt_tokens\":0", 1)
	wantRejected(t, evHello+evWorld+evFinish+zeroPrompt+evDone, 0, "invalid usage counts")
	emptyUsage := "data: {\"choices\":[],\"usage\":{}}\n\n"
	wantRejected(t, evHello+evWorld+evFinish+emptyUsage+evDone, 0, "invalid usage counts")
	// The request asked for 48 tokens; a server that reports another count gave a short or overlong reply.
	wantRejected(t, okStream, 48, "completion_tokens 2, want 48")
}

// chat() over a real HTTP server: the whole client path, including the header and the status check.
func TestChatEndToEnd(t *testing.T) {
	serve := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("x-session-id") != "run-s000" {
				http.Error(w, fmt.Sprintf("session header %q", r.Header.Get("x-session-id")), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, body)
		}))
	}
	req := chatRequest{Model: "m", Messages: []message{{Role: "user", Content: "hi"}}, MaxTokens: 2, MinTokens: 2, Stream: true}
	for _, tc := range []struct{ name, body, errPart string }{
		{"complete", okStream, ""},
		{"no usage", evHello + evWorld + evFinish + evDone, "no usage"},
		{"cut before the final blank line", strings.TrimSuffix(okStream, "\n"), "unfinished"},
		{"short reply", strings.Replace(okStream, "\"completion_tokens\":2", "\"completion_tokens\":1", 1), "want 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(tc.body)
			defer srv.Close()
			res, err := chat(context.Background(), srv.Client(), srv.URL, "x-session-id", "run-s000", req)
			switch {
			case tc.errPart == "" && err != nil:
				t.Fatalf("complete stream failed: %v", err)
			case tc.errPart != "" && (err == nil || !strings.Contains(err.Error(), tc.errPart)):
				t.Fatalf("error %v, want one containing %q", err, tc.errPart)
			}
			if res.Status != http.StatusOK {
				t.Fatalf("status %d", res.Status)
			}
		})
	}
	srv := serve(okStream)
	defer srv.Close()
	if _, err := chat(context.Background(), srv.Client(), srv.URL, "x-session-id", "other", req); err == nil || !strings.Contains(err.Error(), "http 400") {
		t.Fatalf("a non-200 status must be an error, got %v", err)
	}
}
