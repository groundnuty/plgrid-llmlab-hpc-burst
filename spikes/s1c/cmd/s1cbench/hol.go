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
	var times [][]float64
	for _, r := range results {
		if r.Complete() {
			s.FastComplete++
		} else if r.Err != nil {
			s.FastErrors = append(s.FastErrors, fmt.Sprintf("started %.1f s, %d tokens: %v",
				r.Start.Sub(t0).Seconds(), len(r.Tokens), r.Err))
		}
		at := make([]float64, len(r.Tokens))
		for k, tk := range r.Tokens {
			at[k] = r.Start.Add(tk).Sub(t0).Seconds()
		}
		times = append(times, at)
	}
	s.Before, s.During, s.After, s.Series = holPhases(times, itl.Seconds(), s.BulkStartS, s.BulkEndS, time.Since(t0).Seconds())
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
	if resp.StatusCode != http.StatusOK {
		return 0, start, time.Now(), fmt.Errorf("bulk status %d", resp.StatusCode)
	}
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

// holPhases splits the fast streams' gaps and lateness into the 5 s before the bulk transfer, the
// transfer itself and the 5 s after it. times holds each stream's token arrival times in seconds
// since the run started.
//
//   - A gap belongs to the transfer when it overlaps it: it ends after the transfer starts and
//     begins before the transfer ends. A stall that ends only after the transfer completed is
//     therefore still counted as the transfer's.
//   - A token's lateness is its arrival minus its stream's own schedule (first token plus k times
//     the stream's median gap, shifted so the stream's earliest token has lateness 0), the queueing
//     delay of that token. It is attributed by the token's SCHEDULED time, so a token due during
//     the transfer but delivered after it counts as the transfer's.
//
// series is the largest lateness among the tokens arriving in each 250 ms of the run.
func holPhases(times [][]float64, nominal, bulkStart, bulkEnd, runS float64) (before, during, after holWindow, series []float64) {
	type span struct{ from, to float64 }
	ph := [3]span{{bulkStart - 5, bulkStart}, {bulkStart, bulkEnd}, {bulkEnd, bulkEnd + 5}}
	var gaps, late [3][]float64
	series = make([]float64, int(runS/0.25)+1)
	for _, at := range times {
		pace := streamPace(at, nominal)
		base := 1e18
		for k, t := range at {
			if v := t - float64(k)*pace; v < base {
				base = v
			}
		}
		for k, t := range at {
			sched := base + float64(k)*pace
			l := (t - sched) * 1000
			if b := int(t / 0.25); b >= 0 && b < len(series) && l > series[b] {
				series[b] = l
			}
			for p := range ph {
				if sched >= ph[p].from && sched < ph[p].to {
					late[p] = append(late[p], l)
				}
			}
			if k == 0 {
				continue
			}
			g, from := (t-at[k-1])*1000, at[k-1]
			switch {
			case t > bulkStart && from < bulkEnd:
				gaps[1] = append(gaps[1], g)
			case t <= bulkStart && from >= ph[0].from:
				gaps[0] = append(gaps[0], g)
			case from >= bulkEnd && t < ph[2].to:
				gaps[2] = append(gaps[2], g)
			}
		}
	}
	w := func(p int) holWindow {
		return holWindow{FromS: ph[p].from, ToS: ph[p].to, GapMs: summarize(gaps[p]), LatenessMs: summarize(late[p])}
	}
	for i := range series {
		series[i] = ms(series[i])
	}
	return w(0), w(1), w(2), series
}

// streamPace is the median gap between a stream's token arrival times (seconds), or nominal for a
// stream with fewer than two tokens.
func streamPace(at []float64, nominal float64) float64 {
	if len(at) < 2 {
		return nominal
	}
	gaps := make([]float64, 0, len(at)-1)
	for k := 1; k < len(at); k++ {
		gaps = append(gaps, at[k]-at[k-1])
	}
	return summarize(gaps).P50
}
