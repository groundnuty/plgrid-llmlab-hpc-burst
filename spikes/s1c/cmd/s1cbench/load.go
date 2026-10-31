package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// loadSummary is one cell of the capacity matrix. Times are in milliseconds unless named _s.
type loadSummary struct {
	Label       string         `json:"label"`
	Concurrency int            `json:"concurrency"`
	MaxTokens   int            `json:"max_tokens"`
	RampS       float64        `json:"ramp_s"`
	WindowS     float64        `json:"window_s"`
	Requests    int            `json:"requests"` // started inside the window
	Completed   int            `json:"completed"`
	Failed      int            `json:"failed"`
	Errors      map[string]int `json:"errors"`
	TTFTms      dist           `json:"ttft_ms"`
	GapMs       dist           `json:"gap_ms"`
	RateTokS    dist           `json:"rate_tok_s_per_stream"`
	TokensPerS  float64        `json:"tokens_per_s"` // all token arrivals inside the window
	BytesPerTok float64        `json:"body_bytes_per_token"`
	BodyBytesPS float64        `json:"body_bytes_per_s"`
	AllRequests int            `json:"all_requests"`
	AllFailed   int            `json:"all_failed"`
	AllTokens   int            `json:"all_tokens"`
	WallS       float64        `json:"wall_s"`
}

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	target := fs.String("target", "", "engine address, or frps's tcpmux CONNECT port when -mux is set (127.0.0.1:port)")
	mux := fs.String("mux", "", "tcpmux host to CONNECT to (empty = direct)")
	n := fs.Int("n", 1, "concurrent streams (closed loop: each worker starts a new request when one ends)")
	window := fs.Duration("window", 24*time.Second, "measurement window, after the ramp")
	ramp := fs.Duration("ramp", 3*time.Second, "worker i starts at ramp*i/n")
	maxTokens := fs.Int("max-tokens", 60, "tokens per reply (min_tokens = max_tokens, ignore_eos)")
	model := fs.String("model", "sim", "model name")
	reqTimeout := fs.Duration("req-timeout", 180*time.Second, "per-request deadline (counted as a failure)")
	label := fs.String("label", "", "label written into the summary")
	out := fs.String("out", "-", "summary JSON path")
	raw := fs.String("raw", "", "optional per-request CSV path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireLoopback(*target); err != nil {
		return err
	}
	if *n < 1 || *maxTokens < 2 {
		return fmt.Errorf("-n must be >= 1 and -max-tokens >= 2")
	}
	client := newClient(*target, *mux)
	t0 := time.Now()
	stopAt := t0.Add(*ramp + *window)
	var mu sync.Mutex
	var results []*streamResult
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(int64(*ramp) * int64(i) / int64(*n)))
			for time.Now().Before(stopAt) {
				ctx, cancel := context.WithTimeout(context.Background(), *reqTimeout)
				r := streamOnce(ctx, client, *model, *maxTokens)
				cancel()
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
				if r.Err != nil && time.Since(r.Start) < 50*time.Millisecond {
					time.Sleep(50 * time.Millisecond) // a path that refuses at once must not spin
				}
			}
		}(i)
	}
	wg.Wait()
	s := summarizeLoad(results, t0, *ramp, *window)
	s.Label, s.Concurrency, s.MaxTokens = *label, *n, *maxTokens
	s.WallS = time.Since(t0).Seconds()
	if *raw != "" {
		if err := writeRaw(*raw, results, t0); err != nil {
			return err
		}
	}
	return writeJSON(*out, s)
}

func summarizeLoad(results []*streamResult, t0 time.Time, ramp, window time.Duration) loadSummary {
	s := loadSummary{RampS: ramp.Seconds(), WindowS: window.Seconds(), Errors: map[string]int{}}
	wStart, wEnd := t0.Add(ramp), t0.Add(ramp+window)
	var ttft, gaps, rates []float64
	var tokensInWindow int
	var bytes, toks int64
	for _, r := range results {
		s.AllRequests++
		s.AllTokens += len(r.Tokens)
		if !r.Complete() {
			s.AllFailed++
		}
		for _, tk := range r.Tokens {
			at := r.Start.Add(tk)
			if !at.Before(wStart) && at.Before(wEnd) {
				tokensInWindow++
			}
		}
		if r.Start.Before(wStart) || !r.Start.Before(wEnd) {
			continue
		}
		s.Requests++
		if !r.Complete() {
			s.Failed++
			key := "incomplete"
			if r.Err != nil {
				key = r.Err.Error()
				if len(key) > 120 {
					key = key[:120]
				}
			}
			s.Errors[key]++
			continue
		}
		s.Completed++
		ttft = append(ttft, float64(r.TTFT)/1e6)
		for i := 1; i < len(r.Tokens); i++ {
			gaps = append(gaps, float64(r.Tokens[i]-r.Tokens[i-1])/1e6)
		}
		span := (r.Tokens[len(r.Tokens)-1] - r.Tokens[0]).Seconds()
		if span > 0 {
			rates = append(rates, float64(len(r.Tokens)-1)/span)
		}
		bytes += r.Bytes
		toks += int64(len(r.Tokens))
	}
	s.TTFTms, s.GapMs, s.RateTokS = summarize(ttft), summarize(gaps), summarize(rates)
	s.TokensPerS = float64(tokensInWindow) / window.Seconds()
	if toks > 0 {
		s.BytesPerTok = float64(bytes) / float64(toks)
		s.BodyBytesPS = s.TokensPerS * s.BytesPerTok
	}
	return s
}

func writeRaw(path string, results []*streamResult, t0 time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"start_s", "ttft_ms", "tokens", "complete", "bytes", "last_token_ms", "err"})
	sorted := append([]*streamResult(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start.Before(sorted[j].Start) })
	for _, r := range sorted {
		last := 0.0
		if len(r.Tokens) > 0 {
			last = float64(r.Tokens[len(r.Tokens)-1]) / 1e6
		}
		e := ""
		if r.Err != nil {
			e = r.Err.Error()
		}
		_ = w.Write([]string{
			strconv.FormatFloat(r.Start.Sub(t0).Seconds(), 'f', 3, 64),
			strconv.FormatFloat(float64(r.TTFT)/1e6, 'f', 3, 64),
			strconv.Itoa(len(r.Tokens)), strconv.FormatBool(r.Complete()),
			strconv.FormatInt(r.Bytes, 10), strconv.FormatFloat(last, 'f', 3, 64), e,
		})
	}
	w.Flush()
	return w.Error()
}
