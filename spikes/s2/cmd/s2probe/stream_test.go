package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeStream serves n content chunks, then (optionally) a finish chunk and [DONE].
// With cut set, it ends the response after the content chunks, as a router deadline does.
func fakeStream(n int, cut bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		for i := 0; i < n; i++ {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"tok\"},\"finish_reason\":null}]}\n\n")
			fl.Flush()
			time.Sleep(5 * time.Millisecond)
		}
		if cut {
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n")
	}
}

func run(t *testing.T, h http.HandlerFunc, tokens, expect int) streamResult {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	dir := t.TempDir()
	res := doStream(srv.URL, "m", tokens, expect, "s", "t", dir, time.Hour, nil)
	if _, err := os.Stat(filepath.Join(dir, "t.gaps.csv")); err != nil {
		t.Fatalf("gaps file not written: %v", err)
	}
	return res
}

func TestCompleteStream(t *testing.T) {
	res := run(t, fakeStream(20, false), 20, 20)
	if !res.Complete || res.Tokens != 20 || res.FinishReason != "length" || !res.DoneSeen {
		t.Fatalf("want a complete 20-token stream, got %+v", res)
	}
}

// A reply that ends without finish_reason and [DONE] is a cut even though the HTTP exchange ended cleanly.
func TestStreamWithoutDoneIsACut(t *testing.T) {
	res := run(t, fakeStream(20, true), 20, 20)
	if res.Complete {
		t.Fatalf("a stream with no finish reason and no [DONE] must not count as complete: %+v", res)
	}
	if res.CutAfterSec <= 0 || res.Error == "" {
		t.Fatalf("a cut must record when and why: %+v", res)
	}
}

// All the chunks arrive with a finish reason, but fewer than requested: the context window case
// that made a short stream look complete.
func TestShortCountIsNotComplete(t *testing.T) {
	res := run(t, fakeStream(10, false), 20, 20)
	if res.Complete {
		t.Fatalf("10 of 20 tokens with a finish reason must not count as complete: %+v", res)
	}
}

func TestHTTPErrorIsNotComplete(t *testing.T) {
	res := run(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "", 500) }, 5, 5)
	if res.Complete || res.HTTPStatus != 500 {
		t.Fatalf("an HTTP 500 must not count as complete: %+v", res)
	}
}

func TestScrapeSumsLabelSetsAndAcceptsAbsentCounter(t *testing.T) {
	page := "vllm:num_requests_running{model_name=\"m\"} 1\n" +
		"vllm:request_success_total{finish_reason=\"length\",model_name=\"m\"} 3\n" +
		"vllm:request_success_total{finish_reason=\"stop\",model_name=\"m\"} 2\n" +
		"vllm:request_success_total_created{model_name=\"m\"} 9\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page) }))
	defer srv.Close()
	if v, err := scrape(srv.URL, metricSuccess); err != nil || v != 5 {
		t.Fatalf("want 5 (labels summed, _created ignored), got %v, %v", v, err)
	}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "vllm:num_requests_running{model_name=\"m\"} 0\n")
	}))
	defer srv2.Close()
	if v, err := scrape(srv2.URL, metricSuccess); err != nil || v != 0 {
		t.Fatalf("an absent counter on a simulator page is zero, got %v, %v", v, err)
	}
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "hello\n") }))
	defer srv3.Close()
	if _, err := scrape(srv3.URL, metricSuccess); err == nil {
		t.Fatal("a page that is not a simulator's metrics must be an error, not zero")
	}
}

func TestSummariseGapsSkipsTimeToFirstToken(t *testing.T) {
	r := streamResult{}
	summariseGaps(&r, []float64{5000, 1000, 1000, 3000})
	if r.GapMaxMs != 3000 || r.GapMinMs != 1000 || r.GapsOver2s != 1 || r.GapMaxAtToken != 4 {
		t.Fatalf("the first gap is time to first token and must be excluded: %+v", r)
	}
}
