package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// holWindow summarises the fast streams' tokens that arrived in one phase of the run.
type holWindow struct {
	FromS      float64 `json:"from_s"`
	ToS        float64 `json:"to_s"`
	GapMs      dist    `json:"gap_ms"`
	LatenessMs dist    `json:"lateness_ms"`
}

type holSummary struct {
	Label        string    `json:"label"`
	FastStreams  int       `json:"fast_streams"`
	FastComplete int       `json:"fast_complete"`
	FastErrors   []string  `json:"fast_errors"`
	BulkBytes    int64     `json:"bulk_bytes_read"`
	BulkWant     int64     `json:"bulk_bytes_wanted"`
	BulkReadRate int64     `json:"bulk_read_rate_Bps"`
	BulkStartS   float64   `json:"bulk_start_s"`
	BulkFirstS   float64   `json:"bulk_first_byte_s"`
	BulkEndS     float64   `json:"bulk_end_s"`
	BulkBps      float64   `json:"bulk_Bps"`
	BulkErr      string    `json:"bulk_err"`
	Before       holWindow `json:"before"`
	During       holWindow `json:"during"`
	After        holWindow `json:"after"`
	Series       []float64 `json:"max_lateness_ms_per_250ms"`
}

func runHOL(args []string) error {
	fs := flag.NewFlagSet("hol", flag.ContinueOnError)
	fastTarget := fs.String("fast-target", "", "path to the token engine (frps tcpmux port, or the engine/shaper for direct)")
	fastMux := fs.String("fast-mux", "", "tcpmux host of the token engine (empty = direct)")
	bulkTarget := fs.String("bulk-target", "", "path to the bulk engine")
	bulkMux := fs.String("bulk-mux", "", "tcpmux host of the bulk engine (empty = direct)")
	n := fs.Int("n", 16, "fast token streams")
	total := fs.Duration("total", 30*time.Second, "length of each fast stream")
	itl := fs.Duration("itl", 50*time.Millisecond, "engine inter-token latency (sets the token count)")
	bulkAt := fs.Duration("bulk-at", 8*time.Second, "when the bulk request starts")
	bulkBytes := fs.Int64("bulk-bytes", 4<<20, "bytes the bulk reply carries")
	readRate := fs.Int64("bulk-read-rate", 0, "the bulk client's read rate in bytes/s (0 = as fast as it arrives: a large reply; >0 = a slow reader)")
	bulkMax := fs.Duration("bulk-max", 15*time.Second, "the bulk client gives up (closes) after this long")
	model := fs.String("model", "sim", "model name")
	label := fs.String("label", "", "label")
	out := fs.String("out", "-", "summary JSON path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, a := range []string{*fastTarget, *bulkTarget} {
		if err := requireLoopback(a); err != nil {
			return err
		}
	}
	tokens := int(*total / *itl)
	fast := newClient(*fastTarget, *fastMux)
	t0 := time.Now()
	results := make([]*streamResult, *n)
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(int64(time.Second) * int64(i) / int64(*n)))
			ctx, cancel := context.WithTimeout(context.Background(), *total+5*time.Minute)
			defer cancel()
			results[i] = streamOnce(ctx, fast, *model, tokens)
		}(i)
	}
	s := holSummary{Label: *label, FastStreams: *n, BulkWant: *bulkBytes, BulkReadRate: *readRate}
	time.Sleep(time.Until(t0.Add(*bulkAt)))
	bulkStart := time.Now()
	read, first, end, berr := bulkFetch(*bulkTarget, *bulkMux, *bulkBytes, *readRate, *bulkMax)
	s.BulkBytes = read
	s.BulkStartS = bulkStart.Sub(t0).Seconds()
	s.BulkFirstS = first.Sub(t0).Seconds()
	s.BulkEndS = end.Sub(t0).Seconds()
	if d := end.Sub(first).Seconds(); d > 0 {
		s.BulkBps = float64(read) / d
	}
	if berr != nil {
		s.BulkErr = berr.Error()
	}
	wg.Wait()
	phases := [3][2]float64{
		{s.BulkStartS - 5, s.BulkStartS},
		{s.BulkStartS, s.BulkEndS},
		{s.BulkEndS, s.BulkEndS + 5},
	}
	var gaps, late [3][]float64
	bins := make([]float64, int(time.Since(t0)/(250*time.Millisecond))+1)
	for _, r := range results {
		if r.Complete() {
			s.FastComplete++
		} else if r.Err != nil {
			s.FastErrors = append(s.FastErrors, fmt.Sprintf("started %.1f s, %d tokens: %v",
				r.Start.Sub(t0).Seconds(), len(r.Tokens), r.Err))
		}
		// Lateness of token k: its arrival minus the stream's own schedule, first token plus k times
		// the stream's median gap (the engine's real pace, slightly above the nominal 50 ms), shifted
		// so the earliest token of the stream has lateness 0. It is the queueing delay of that token.
		pace := streamPace(r.Tokens, itl.Seconds())
		base := 1e18
		for k, tk := range r.Tokens {
			if v := tk.Seconds() - float64(k)*pace; v < base {
				base = v
			}
		}
		for k, tk := range r.Tokens {
			at := r.Start.Add(tk).Sub(t0).Seconds()
			l := (tk.Seconds() - float64(k)*pace - base) * 1000
			if b := int(at / 0.25); b >= 0 && b < len(bins) && l > bins[b] {
				bins[b] = l
			}
			if k == 0 {
				continue
			}
			g := float64(tk-r.Tokens[k-1]) / 1e6
			for p, ph := range phases {
				if at >= ph[0] && at < ph[1] {
					gaps[p] = append(gaps[p], g)
					late[p] = append(late[p], l)
				}
			}
		}
	}
	ws := []*holWindow{&s.Before, &s.During, &s.After}
	for p := range ws {
		*ws[p] = holWindow{FromS: phases[p][0], ToS: phases[p][1], GapMs: summarize(gaps[p]), LatenessMs: summarize(late[p])}
	}
	for i := range bins {
		bins[i] = ms(bins[i])
	}
	s.Series = bins
	return writeJSON(*out, s)
}

// bulkFetch reads GET /bulk?bytes=N through the path, at most readRate bytes/s when readRate > 0,
// and gives up after max. It returns the bytes read and the first-byte and end times.
func bulkFetch(target, host string, want, readRate int64, max time.Duration) (int64, time.Time, time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), max)
	defer cancel()
	client := newClient(target, host)
	start := time.Now()
	first := start
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://bulk/bulk?bytes=%d", want), nil)
	if err != nil {
		return 0, start, time.Now(), err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, start, time.Now(), err
	}
	defer resp.Body.Close()
	buf := make([]byte, 32*1024)
	if readRate > 0 && readRate < int64(len(buf)) {
		buf = buf[:readRate/4+1] // small reads, so a slow reader really reads slowly
	}
	var got int64
	for {
		k, rerr := resp.Body.Read(buf)
		if got == 0 && k > 0 {
			first = time.Now()
		}
		got += int64(k)
		if readRate > 0 {
			due := first.Add(time.Duration(float64(got) / float64(readRate) * float64(time.Second)))
			if d := time.Until(due); d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
				}
			}
		}
		if rerr != nil {
			end := time.Now()
			if rerr == io.EOF && got == want {
				return got, first, end, nil
			}
			if ctx.Err() != nil {
				return got, first, end, fmt.Errorf("gave up after %s (by design for a slow reader)", max)
			}
			return got, first, end, rerr
		}
		if ctx.Err() != nil {
			return got, first, time.Now(), fmt.Errorf("gave up after %s (by design for a slow reader)", max)
		}
	}
}

// streamPace is the median gap between a stream's tokens in seconds, or nominal for a stream with
// fewer than two tokens.
func streamPace(tokens []time.Duration, nominal float64) float64 {
	if len(tokens) < 2 {
		return nominal
	}
	gaps := make([]float64, 0, len(tokens)-1)
	for k := 1; k < len(tokens); k++ {
		gaps = append(gaps, (tokens[k] - tokens[k-1]).Seconds())
	}
	return summarize(gaps).P50
}
