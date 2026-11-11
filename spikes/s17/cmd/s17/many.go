package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// bulkReq is one bulk request of one Instance in the many-Instances run (issue #413): a download of
// -bytes through the Instance's bulk proxy. Times are nanoseconds since T0Ns.
type bulkReq struct {
	Instance int    `json:"instance"`
	StartNs  int64  `json:"start_ns"` // before the dial
	RespNs   int64  `json:"resp_ns"`  // the response's headers arrived
	FirstNs  int64  `json:"first_ns"` // the first body byte arrived: the time to first byte is FirstNs - StartNs
	EndNs    int64  `json:"end_ns"`
	Bytes    int64  `json:"bytes"`
	Status   int    `json:"status"`
	Err      string `json:"err,omitempty"`
}

// manyOut is the many-Instances run as the client saw it.
type manyOut struct {
	T0Ns       int64         `json:"t0_unix_ns"`
	Instances  int           `json:"instances"`
	StreamsPer int           `json:"streams_per_instance"`
	WindowFrom int64         `json:"window_start_ns"`
	WindowTo   int64         `json:"window_end_ns"`
	EndNs      int64         `json:"end_ns"`
	IntervalNs int64         `json:"interval_ns"`
	Bytes      int64         `json:"bytes"`
	BulkHosts  string        `json:"bulk_suffix"`
	StreamRuns [][]streamRun `json:"streams_runs"` // instance-major: instance i's stream j is i*streams+j
	Bulk       []bulkReq     `json:"bulk"`
}

// runMany is issue #413's many-Instances load: every Instance of the spec has -streams token streams (each
// one long request, as in load) and makes one bulk download of -bytes every -interval, the Instances'
// requests evenly staggered over the interval, for -duration after a -warmup that follows the streams'
// ramp. A request that is still running when its Instance's next is due is not skipped: the next starts
// (a request every interval, whatever the last took), and the validity rule refuses a run in which that
// happened (analysis.validity_many).
func runMany(args []string) error {
	fs := flag.NewFlagSet("many", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	mux := fs.String("mux", "", "frps's tcpmux CONNECT port, for the streams")
	bulkMux := fs.String("bulk-mux", "", "the CONNECT port for the bulk requests (default -mux)")
	suffix := fs.String("bulk-suffix", "", "appended to an Instance's UID for its bulk proxy's host (layout c: -bulk)")
	streams := fs.Int("streams", 1, "token streams per Instance")
	streamTokens := fs.Int("stream-tokens", 2000, "tokens per stream request")
	size := fs.Int64("bytes", 1<<20, "each bulk download's size")
	interval := fs.Duration("interval", 10*time.Second, "one bulk request per Instance each interval")
	duration := fs.Duration("duration", 60*time.Second, "the measured window")
	warmup := fs.Duration("warmup", 4*time.Second, "after the streams' ramp, before the window")
	ramp := fs.Duration("ramp", 2*time.Second, "the streams start over this long")
	model := fs.String("model", "model", "the engine double's served model name")
	out := fs.String("out", "", "output JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *bulkMux == "" {
		*bulkMux = *mux
	}
	for _, a := range []string{*mux, *bulkMux} {
		if err := requireLoopback(a); err != nil {
			return err
		}
	}
	if *out == "" || *streams < 0 || *size < 1 || *interval <= 0 || *duration <= 0 {
		return errors.New("-out is required; -streams >= 0; -bytes >= 1; -interval and -duration > 0")
	}
	n := len(spec.Tunnels)
	streamClient := newClient(ModeDirect, *mux, nil)
	bulkClient := newClient(ModeDirect, *bulkMux, nil)

	t0 := time.Now()
	rel := func(t time.Time) int64 { return t.Sub(t0).Nanoseconds() }
	o := manyOut{T0Ns: t0.UnixNano(), Instances: n, StreamsPer: *streams, IntervalNs: interval.Nanoseconds(), Bytes: *size,
		BulkHosts: *suffix, StreamRuns: make([][]streamRun, n**streams)}
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	total := n * *streams
	for i := 0; i < n; i++ {
		for j := 0; j < *streams; j++ {
			idx, uid := i**streams+j, spec.Tunnels[i].UID
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Until(t0.Add(time.Duration(int64(*ramp) * int64(idx) / int64(max(total, 1))))))
				for ctx.Err() == nil {
					r := streamOnce(ctx, streamClient, uid, *model, *streamTokens)
					run := streamRun{StartNs: rel(r.Start), EndNs: rel(time.Now()), Status: r.Status}
					for _, tk := range r.Tokens {
						run.TokensNs = append(run.TokensNs, rel(r.Start.Add(tk)))
					}
					if r.Err != nil && ctx.Err() == nil {
						run.Err = r.Err.Error()
					}
					o.StreamRuns[idx] = append(o.StreamRuns[idx], run)
					if r.Err != nil && ctx.Err() == nil && time.Since(r.Start) < 50*time.Millisecond {
						time.Sleep(50 * time.Millisecond)
					}
				}
			}()
		}
	}
	windowStart := t0.Add(*ramp + time.Second + *warmup)
	windowEnd := windowStart.Add(*duration)
	o.WindowFrom, o.WindowTo = rel(windowStart), rel(windowEnd)
	var mu sync.Mutex
	var bw sync.WaitGroup
	for i := 0; i < n; i++ {
		offset := time.Duration(int64(*interval) * int64(i) / int64(n))
		host := spec.Tunnels[i].UID + *suffix
		bw.Add(1)
		go func() {
			defer bw.Done()
			// A request every interval from the window's start, this Instance's offset after it, whatever the
			// last took: each in a goroutine of its own, so a slow one does not delay the next.
			var rw sync.WaitGroup
			for at := windowStart.Add(offset); at.Before(windowEnd); at = at.Add(*interval) {
				time.Sleep(time.Until(at))
				rw.Add(1)
				go func() {
					defer rw.Done()
					r := downloadOnce(bulkClient, "http://"+host+":80", *size, rel)
					r.Instance = i
					mu.Lock()
					o.Bulk = append(o.Bulk, r)
					mu.Unlock()
				}()
			}
			rw.Wait()
		}()
	}
	bw.Wait()
	time.Sleep(2 * time.Second) // the streams run on past the last request
	o.EndNs = rel(time.Now())
	stop()
	wg.Wait()
	return writeJSONFile(*out, o)
}

// downloadOnce is one bulk download as the many-Instances run records it: its headers, its first body
// byte and its end.
func downloadOnce(c *http.Client, base string, size int64, rel func(time.Time) int64) bulkReq {
	r := bulkReq{StartNs: rel(time.Now())}
	resp, err := c.Get(base + "/bulk/download?bytes=" + strconv.FormatInt(size, 10))
	if err != nil {
		r.Err = err.Error()
		r.EndNs = rel(time.Now())
		return r
	}
	defer resp.Body.Close()
	r.RespNs, r.Status = rel(time.Now()), resp.StatusCode
	buf := make([]byte, 64*1024)
	for {
		k, rerr := resp.Body.Read(buf)
		if k > 0 {
			if r.Bytes == 0 {
				r.FirstNs = rel(time.Now())
			}
			r.Bytes += int64(k)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			r.Err = rerr.Error()
			break
		}
	}
	r.EndNs = rel(time.Now())
	switch {
	case r.Err != "":
	case r.Status != http.StatusOK:
		r.Err = fmt.Sprintf("status %d", r.Status)
	case r.Bytes != size:
		r.Err = fmt.Sprintf("received %d of %d bytes", r.Bytes, size)
	}
	return r
}
