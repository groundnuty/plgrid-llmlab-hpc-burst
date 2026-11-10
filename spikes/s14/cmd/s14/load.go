package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// loadOut is one configuration's load, as the client saw it. Times are in
// milliseconds unless named _s or _ns.
type loadOut struct {
	Mode      string  `json:"mode"`
	Tunnels   int     `json:"tunnels"`
	Streams   int     `json:"streams_per_tunnel"`
	MaxTokens int     `json:"max_tokens"`
	RampS     float64 `json:"ramp_s"`
	WindowS   float64 `json:"window_s"`
	BucketS   float64 `json:"bucket_s"`
	// The window in wall-clock time, for the driver's CPU samples.
	T0Ns          int64 `json:"t0_unix_ns"`
	WindowStartNs int64 `json:"window_start_unix_ns"`
	WindowEndNs   int64 `json:"window_end_unix_ns"`
	EndNs         int64 `json:"end_unix_ns"`

	// Requests started inside the window.
	Requests  int            `json:"requests"`
	Completed int            `json:"completed"`
	Failed    int            `json:"failed"`
	Errors    map[string]int `json:"errors"`
	// Over the completed requests started inside the window.
	TTFTms   dist `json:"ttft_ms"`
	GapMs    dist `json:"gap_ms"`
	TTFTHist hist `json:"ttft_hist_ms"`
	GapHist  hist `json:"gap_hist_ms"`
	// Every token that arrived inside the window, per second.
	TokensPerS float64 `json:"tokens_per_s"`
	// Every request of the run, whenever it started.
	AllRequests int `json:"all_requests"`
	AllFailed   int `json:"all_failed"`

	ProbesOK     int  `json:"probes_ok"`
	ProbesFailed int  `json:"probes_failed"`
	ProbeMs      dist `json:"probe_ms"`

	PerTunnel []tunnelOut `json:"per_tunnel"`
}

// tunnelOut is one tunnel's load inside the window: the evidence that it
// streamed throughout (Buckets, LongestSilenceMs) and how much.
type tunnelOut struct {
	Index            int     `json:"index"`
	Requests         int     `json:"requests"`  // started inside the window
	Completed        int     `json:"completed"` // of those
	Failed           int     `json:"failed"`
	Tokens           int     `json:"tokens"`  // arrived inside the window
	Buckets          []int   `json:"buckets"` // tokens per BucketS of the window
	LongestSilenceMs float64 `json:"longest_silence_ms"`
	GapP99Ms         float64 `json:"gap_p99_ms"`
	ProbesOK         int     `json:"probes_ok"`
	ProbesFailed     int     `json:"probes_failed"`
}

// runLoad drives every tunnel at once: streams-per-tunnel closed-loop workers
// per tunnel, each starting a new streamed request (60 tokens by default) on a
// new connection as soon as its last one ends, worker j of all starting at
// ramp*j/(tunnels*streams); and the front's probe on each tunnel every
// probe-interval. Statistics cover the requests started, and the tokens that
// arrived, in the window after the ramp.
func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	mux := fs.String("mux", "127.0.0.1:29921", "frps's tcpmux CONNECT port")
	streams := fs.Int("streams", 1, "concurrent streams per tunnel (0: the probes alone, for the idle cost)")
	ramp := fs.Duration("ramp", 5*time.Second, "worker j starts at ramp*j/(tunnels*streams)")
	window := fs.Duration("window", 30*time.Second, "measurement window, after the ramp")
	bucket := fs.Duration("bucket", time.Second, "the per-tunnel token buckets' width")
	maxTokens := fs.Int("max-tokens", 60, "tokens per reply (min_tokens = max_tokens)")
	model := fs.String("model", "model", "the engine double's served model name")
	probeEvery := fs.Duration("probe-interval", 10*time.Second, "the front's probe interval per tunnel (0: none)")
	probeTimeout := fs.Duration("probe-timeout", 10*time.Second, "the probe's timeout")
	reqTimeout := fs.Duration("req-timeout", 180*time.Second, "per-request deadline (counted as a failure)")
	out := fs.String("out", "", "summary JSON path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if err := requireLoopback(*mux); err != nil {
		return err
	}
	if *streams < 0 || *maxTokens < 2 || *window < *bucket || *bucket <= 0 {
		return fmt.Errorf("-streams must be >= 0, -max-tokens >= 2, and -window at least one -bucket")
	}
	sockets := map[string]string{}
	for _, t := range spec.Tunnels {
		sockets[t.UID] = t.Socket
	}
	client := newClient(spec.Mode, *mux, sockets)
	n := len(spec.Tunnels)
	workers := n * *streams
	t0 := time.Now()
	wStart, wEnd := t0.Add(*ramp), t0.Add(*ramp+*window)

	var mu sync.Mutex
	var results []*streamResult
	probes := make([][]probeResult, n)
	var wg sync.WaitGroup
	for j := 0; j < workers; j++ {
		ti := j / *streams
		uid := spec.Tunnels[ti].UID
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Until(t0.Add(time.Duration(int64(*ramp) * int64(j) / int64(workers)))))
			for time.Now().Before(wEnd) {
				ctx, cancel := context.WithTimeout(context.Background(), *reqTimeout)
				r := streamOnce(ctx, client, uid, *model, *maxTokens)
				cancel()
				r.Tunnel = ti
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
				if r.Err != nil && time.Since(r.Start) < 50*time.Millisecond {
					time.Sleep(50 * time.Millisecond) // a path that refuses at once must not spin
				}
			}
		}()
	}
	if *probeEvery > 0 {
		for ti, t := range spec.Tunnels {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// A random phase, as the front's probes of many Instances do
				// not fire together.
				next := t0.Add(time.Duration(rand.Int64N(int64(*probeEvery))))
				for next.Before(wEnd) {
					time.Sleep(time.Until(next))
					d, err := probeOnce(client, t.UID, *probeTimeout)
					probes[ti] = append(probes[ti], probeResult{At: time.Now(), Took: d, Err: err})
					next = next.Add(*probeEvery)
				}
			}()
		}
	}
	wg.Wait()
	// The driver reads the CPU samples over [window start, window end]: the
	// client ends no earlier, even when its last request or probe finished
	// first (a probes-only idle phase otherwise ended up to a probe interval
	// early, and its CPU was taken over a shorter window).
	time.Sleep(time.Until(wEnd))
	s := summarizeLoad(results, probes, n, wStart, wEnd, *bucket)
	s.Mode, s.Tunnels, s.Streams, s.MaxTokens = spec.Mode, n, *streams, *maxTokens
	s.RampS, s.WindowS, s.BucketS = ramp.Seconds(), window.Seconds(), bucket.Seconds()
	s.T0Ns, s.WindowStartNs, s.WindowEndNs, s.EndNs = t0.UnixNano(), wStart.UnixNano(), wEnd.UnixNano(), time.Now().UnixNano()
	if *out == "" {
		return fmt.Errorf("-out is required")
	}
	return writeJSONFile(*out, s)
}

type probeResult struct {
	At   time.Time
	Took time.Duration
	Err  error
}

// summarizeLoad reduces the run's requests and probes to the configuration's
// load and each tunnel's. It decides nothing about validity: that is the
// analysis's rule (analysis.py), from Buckets.
func summarizeLoad(results []*streamResult, probes [][]probeResult, tunnels int, wStart, wEnd time.Time, bucket time.Duration) loadOut {
	s := loadOut{Errors: map[string]int{}}
	nb := int((wEnd.Sub(wStart) + bucket - 1) / bucket)
	per := make([]tunnelOut, tunnels)
	arrivals := make([][]time.Duration, tunnels) // inside the window, from wStart
	tunnelGaps := make([][]float64, tunnels)
	for i := range per {
		per[i] = tunnelOut{Index: i, Buckets: make([]int, nb)}
	}
	var ttft, gaps []float64
	tokensInWindow := 0
	for _, r := range results {
		s.AllRequests++
		if !r.Complete() {
			s.AllFailed++
		}
		t := &per[r.Tunnel]
		for _, tk := range r.Tokens {
			at := r.Start.Add(tk)
			if at.Before(wStart) || !at.Before(wEnd) {
				continue
			}
			tokensInWindow++
			t.Tokens++
			t.Buckets[int(at.Sub(wStart)/bucket)]++
			arrivals[r.Tunnel] = append(arrivals[r.Tunnel], at.Sub(wStart))
		}
		if r.Start.Before(wStart) || !r.Start.Before(wEnd) {
			continue
		}
		s.Requests++
		t.Requests++
		if !r.Complete() {
			s.Failed++
			t.Failed++
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
		t.Completed++
		ttft = append(ttft, float64(r.TTFT)/1e6)
		for i := 1; i < len(r.Tokens); i++ {
			g := float64(r.Tokens[i]-r.Tokens[i-1]) / 1e6
			gaps = append(gaps, g)
			tunnelGaps[r.Tunnel] = append(tunnelGaps[r.Tunnel], g)
		}
	}
	span := wEnd.Sub(wStart)
	for i := range per {
		per[i].LongestSilenceMs = round3(float64(longestSilence(arrivals[i], span)) / 1e6)
		per[i].GapP99Ms = summarize(tunnelGaps[i]).P99
	}
	var probeMs []float64
	for i, ps := range probes {
		for _, p := range ps {
			if p.Err != nil {
				s.ProbesFailed++
				per[i].ProbesFailed++
				continue
			}
			s.ProbesOK++
			per[i].ProbesOK++
			probeMs = append(probeMs, float64(p.Took)/1e6)
		}
	}
	s.TTFTms, s.GapMs, s.ProbeMs = summarize(ttft), summarize(gaps), summarize(probeMs)
	s.TTFTHist, s.GapHist = histOf(ttft), histOf(gaps)
	s.TokensPerS = round3(float64(tokensInWindow) / span.Seconds())
	s.PerTunnel = per
	return s
}

// longestSilence is the longest stretch of the window [0, span) with no token
// arriving: before the first, between two, or after the last. No token at all
// is the whole window.
func longestSilence(at []time.Duration, span time.Duration) time.Duration {
	if len(at) == 0 {
		return span
	}
	s := append([]time.Duration(nil), at...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	longest := s[0]
	for i := 1; i < len(s); i++ {
		longest = max(longest, s[i]-s[i-1])
	}
	return max(longest, span-s[len(s)-1])
}
