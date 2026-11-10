package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Transfer kinds (issue #336).
const (
	KindNone       = "none"
	KindUpload     = "upload"
	KindDownload   = "download"
	KindEmbeddings = "embeddings"
)

// loadOut is one configuration's run as the client saw it. Times are
// nanoseconds since T0Ns unless named otherwise.
type loadOut struct {
	Kind        string        `json:"kind"`
	Streams     int           `json:"streams"`
	T0Ns        int64         `json:"t0_unix_ns"`
	RefStartNs  int64         `json:"reference_start_ns"`
	RefEndNs    int64         `json:"reference_end_ns"`
	EndNs       int64         `json:"end_ns"` // the streams were stopped
	StreamRuns  [][]streamRun `json:"streams_runs"`
	Transfers   []transfer    `json:"transfers"`
	BulkHost    string        `json:"bulk_host"`
	BulkMux     string        `json:"bulk_mux"`
	UploadBytes int64         `json:"upload_bytes"`
	Inputs      int           `json:"inputs"`
}

// streamRun is one request of one stream: when it started and ended, and
// every content token's arrival.
type streamRun struct {
	StartNs  int64   `json:"start_ns"`
	EndNs    int64   `json:"end_ns"`
	TokensNs []int64 `json:"tokens_ns"`
	Status   int     `json:"status"`
	Err      string  `json:"err,omitempty"`
}

// transfer is one bulk transfer. RecvFirstNs and RecvLastNs are at the
// receiver: the node for an upload (its receipt), the client otherwise.
type transfer struct {
	Kind        string `json:"kind"`
	StartNs     int64  `json:"start_ns"` // before the dial
	SentNs      int64  `json:"sent_ns"`  // the client wrote the last body byte (upload)
	RespNs      int64  `json:"resp_ns"`  // the response's headers arrived
	RecvFirstNs int64  `json:"recv_first_ns"`
	RecvLastNs  int64  `json:"recv_last_ns"`
	EndNs       int64  `json:"end_ns"` // the client had the whole reply
	Bytes       int64  `json:"bytes"`  // payload at the receiver
	Status      int    `json:"status"`
	Err         string `json:"err,omitempty"`
}

// runLoad runs the configuration's schedule: the streams start over -ramp,
// each one long request (-stream-tokens tokens at the engine's 20 tokens/s,
// longer than the run, so no stream is between requests while a transfer
// runs); after a -reference window with no transfer, -count transfers of
// -kind, one at a time, each after -settle; then -tail, and the streams are
// stopped. A stream whose request ends early starts another, recorded.
func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	mux := fs.String("mux", "127.0.0.1:29981", "frps's tcpmux CONNECT port, for the streams")
	bulkMux := fs.String("bulk-mux", "", "the CONNECT port for the bulk transfers (default -mux)")
	bulkHost := fs.String("bulk-host", "", "the bulk transfers' CONNECT host (default the Instance's UID)")
	streams := fs.Int("streams", 8, "token streams")
	streamTokens := fs.Int("stream-tokens", 2000, "tokens per stream request")
	ramp := fs.Duration("ramp", 2*time.Second, "stream j starts at ramp*j/streams")
	reference := fs.Duration("reference", 10*time.Second, "the window with no transfer, after the ramp and a second")
	kind := fs.String("kind", KindNone, "upload, download, embeddings or none")
	count := fs.Int("count", 5, "transfers")
	settle := fs.Duration("settle", 4*time.Second, "the pause before each transfer")
	tail := fs.Duration("tail", 3*time.Second, "after the last transfer, before the streams stop")
	size := fs.Int64("bytes", 64<<20, "the upload's and the download's size")
	inputs := fs.Int("inputs", 2000, "the embeddings batch's inputs")
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
	if *bulkHost == "" {
		*bulkHost = spec.Tunnels[0].UID
	}
	for _, a := range []string{*mux, *bulkMux} {
		if err := requireLoopback(a); err != nil {
			return err
		}
	}
	switch *kind {
	case KindNone, KindUpload, KindDownload, KindEmbeddings:
	default:
		return fmt.Errorf("-kind %q", *kind)
	}
	if *out == "" || *streams < 0 || *count < 0 || *size < 1 || *inputs < 1 {
		return errors.New("-out is required; -streams, -count must be >= 0; -bytes, -inputs >= 1")
	}
	uid := spec.Tunnels[0].UID
	streamClient := newClient(ModeDirect, *mux, nil)
	bulkClient := newClient(ModeDirect, *bulkMux, nil)
	var embBody []byte
	if *kind == KindEmbeddings {
		embBody = embeddingRequest(*model, *inputs)
	}

	t0 := time.Now()
	rel := func(t time.Time) int64 { return t.Sub(t0).Nanoseconds() }
	o := loadOut{Kind: *kind, Streams: *streams, T0Ns: t0.UnixNano(), BulkHost: *bulkHost, BulkMux: *bulkMux,
		UploadBytes: *size, Inputs: *inputs, StreamRuns: make([][]streamRun, *streams)}
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for j := 0; j < *streams; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Until(t0.Add(time.Duration(int64(*ramp) * int64(j) / int64(*streams)))))
			for ctx.Err() == nil {
				r := streamOnce(ctx, streamClient, uid, *model, *streamTokens)
				run := streamRun{StartNs: rel(r.Start), EndNs: rel(time.Now()), Status: r.Status}
				for _, tk := range r.Tokens {
					run.TokensNs = append(run.TokensNs, rel(r.Start.Add(tk)))
				}
				if r.Err != nil && ctx.Err() == nil {
					run.Err = r.Err.Error()
				}
				o.StreamRuns[j] = append(o.StreamRuns[j], run)
				if r.Err != nil && ctx.Err() == nil && time.Since(r.Start) < 50*time.Millisecond {
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()
	}
	base := "http://" + *bulkHost + ":80"
	refStart := t0.Add(*ramp + time.Second)
	refEnd := refStart.Add(*reference)
	o.RefStartNs, o.RefEndNs = rel(refStart), rel(refEnd)
	time.Sleep(time.Until(refEnd))
	for i := 0; i < *count && *kind != KindNone; i++ {
		time.Sleep(*settle)
		var tr transfer
		switch *kind {
		case KindUpload:
			tr = doUpload(bulkClient, base, *size, rel)
		case KindDownload:
			tr = doDownload(bulkClient, base, *size, rel)
		case KindEmbeddings:
			tr = doEmbeddings(bulkClient, base, embBody, rel)
		}
		o.Transfers = append(o.Transfers, tr)
	}
	time.Sleep(*tail)
	o.EndNs = rel(time.Now())
	stop()
	wg.Wait()
	return writeJSONFile(*out, o)
}

// countingBody is an upload's body, generated as it is read, never held: it
// records when its last byte was handed to the transport.
type countingBody struct {
	left int64
	done func()
}

func (b *countingBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		if b.done != nil {
			b.done()
			b.done = nil
		}
		return 0, io.EOF
	}
	n := min(int64(len(p)), b.left)
	for i := range p[:n] {
		p[i] = byte(i)
	}
	b.left -= n
	return int(n), nil
}

func doUpload(c *http.Client, base string, size int64, rel func(time.Time) int64) transfer {
	tr := transfer{Kind: KindUpload, StartNs: rel(time.Now())}
	body := &countingBody{left: size, done: func() { tr.SentNs = rel(time.Now()) }}
	req, err := http.NewRequest(http.MethodPost, base+"/bulk/upload", body)
	if err != nil {
		tr.Err = err.Error()
		return tr
	}
	req.ContentLength = size
	resp, err := c.Do(req)
	if err != nil {
		tr.Err = err.Error()
		return tr
	}
	defer resp.Body.Close()
	tr.RespNs, tr.Status = rel(time.Now()), resp.StatusCode
	var rc struct {
		Bytes   int64 `json:"bytes"`
		FirstNs int64 `json:"first_byte_unix_ns"`
		LastNs  int64 `json:"last_byte_unix_ns"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&rc); err != nil {
		tr.Err = "the receipt: " + err.Error()
		return tr
	}
	tr.EndNs = rel(time.Now())
	tr.Bytes = rc.Bytes
	// The node's clock is this host's: the receipt's wall-clock times are on
	// the same timeline as the client's.
	tr.RecvFirstNs = rel(time.Unix(0, rc.FirstNs))
	tr.RecvLastNs = rel(time.Unix(0, rc.LastNs))
	if rc.Bytes != size {
		tr.Err = fmt.Sprintf("the node received %d of %d bytes", rc.Bytes, size)
	}
	return tr
}

// readBody reads a reply's body to its end, counting, and returns when its
// first and last bytes arrived.
func readBody(r io.Reader, rel func(time.Time) int64) (n, first, last int64, err error) {
	buf := make([]byte, 64*1024)
	for {
		k, rerr := r.Read(buf)
		if k > 0 {
			now := rel(time.Now())
			if n == 0 {
				first = now
			}
			n += int64(k)
			last = now
		}
		if rerr == io.EOF {
			return n, first, last, nil
		}
		if rerr != nil {
			return n, first, last, rerr
		}
	}
}

func doDownload(c *http.Client, base string, size int64, rel func(time.Time) int64) transfer {
	tr := transfer{Kind: KindDownload, StartNs: rel(time.Now())}
	resp, err := c.Get(base + "/bulk/download?bytes=" + strconv.FormatInt(size, 10))
	if err != nil {
		tr.Err = err.Error()
		return tr
	}
	defer resp.Body.Close()
	tr.RespNs, tr.Status = rel(time.Now()), resp.StatusCode
	n, first, last, err := readBody(resp.Body, rel)
	tr.Bytes, tr.RecvFirstNs, tr.RecvLastNs, tr.EndNs = n, first, last, rel(time.Now())
	switch {
	case err != nil:
		tr.Err = err.Error()
	case n != size:
		tr.Err = fmt.Sprintf("received %d of %d bytes", n, size)
	}
	return tr
}

// embeddingRequest is an embeddings batch of inputs short sentences.
func embeddingRequest(model string, inputs int) []byte {
	in := make([]string, inputs)
	for i := range in {
		in[i] = "passage " + strconv.Itoa(i) + ": " + strings.Repeat("a few words of a document to embed, ", 6)
	}
	b, _ := json.Marshal(map[string]any{"model": model, "input": in})
	return b
}

func doEmbeddings(c *http.Client, base string, body []byte, rel func(time.Time) int64) transfer {
	tr := transfer{Kind: KindEmbeddings, StartNs: rel(time.Now())}
	resp, err := c.Post(base+"/v1/embeddings", "application/json", bytes.NewReader(body))
	if err != nil {
		tr.Err = err.Error()
		return tr
	}
	defer resp.Body.Close()
	tr.RespNs, tr.Status = rel(time.Now()), resp.StatusCode
	n, first, last, err := readBody(resp.Body, rel)
	tr.Bytes, tr.RecvFirstNs, tr.RecvLastNs, tr.EndNs = n, first, last, rel(time.Now())
	switch {
	case err != nil:
		tr.Err = err.Error()
	case resp.StatusCode != 200:
		tr.Err = fmt.Sprintf("status %d", resp.StatusCode)
	case resp.ContentLength > 0 && n != resp.ContentLength:
		tr.Err = fmt.Sprintf("received %d of %d bytes", n, resp.ContentLength)
	}
	return tr
}
