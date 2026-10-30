package main

import (
	"strings"
	"testing"
	"time"
)

// fakeClock returns start+10ms, start+20ms, ... on successive calls.
func fakeClock(start time.Time) func() time.Time {
	n := 0
	return func() time.Time { n++; return start.Add(time.Duration(n) * 10 * time.Millisecond) }
}

const okStream = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
	"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":768}}}\n\n" +
	"data: [DONE]\n\n"

func TestReadStreamComplete(t *testing.T) {
	start := time.Unix(1000, 0)
	r, err := readStream(strings.NewReader(okStream), start, fakeClock(start))
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
}

func TestReadStreamWithoutDoneIsAnError(t *testing.T) {
	cut := strings.Replace(okStream, "data: [DONE]\n\n", "", 1)
	if _, err := readStream(strings.NewReader(cut), time.Now(), time.Now); err == nil {
		t.Fatal("a stream without [DONE] was accepted")
	}
}

func TestReadStreamErrorEvent(t *testing.T) {
	s := "data: {\"error\":{\"message\":\"boom\"}}\n\ndata: [DONE]\n\n"
	if _, err := readStream(strings.NewReader(s), time.Now(), time.Now); err == nil {
		t.Fatal("an error event was accepted")
	}
}

func TestReadStreamNoCacheDetails(t *testing.T) {
	s := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"
	r, err := readStream(strings.NewReader(s), time.Now(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if r.CachedTokens != -1 {
		t.Fatalf("cached tokens %d, want -1 when the server sends no details", r.CachedTokens)
	}
}
