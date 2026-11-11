package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The load client. Closed loop, as in S1c and S14: each worker starts a new
// streamed chat completion of exactly max-tokens tokens on a new connection as
// soon as its last one ends, worker j of all starting at ramp*j/workers. It
// runs until -duration has passed or it is told to stop, and writes one JSON
// line per request when the request ends: when it started, every content
// token's arrival, how it ended, and, for a cut, why. It decides nothing: the
// windows, the validity rule and the cuts at a restart are the analysis's.
//
// -via front sends each request to the front's client listener with the
// client's key, naming the tunnel's model (the front's proxy routes it);
// -via tcpmux sends it through frps's tcpmux port to the tunnel's UID with
// the Instance's key, as S14's client did (the front not in the path).

// reqRec is one request as the client saw it. Token arrivals are tenths of a
// millisecond from Start.
type reqRec struct {
	Tunnel int     `json:"i"`
	Worker int     `json:"w"`
	Start  int64   `json:"s"` // Unix ns, before the dial
	Tokens []int64 `json:"k"`
	Status int     `json:"st"`
	Err    string  `json:"e,omitempty"`
	Fin    bool    `json:"fin"`  // a chunk with finish_reason arrived
	Done   bool    `json:"done"` // [DONE] arrived
	Bytes  int64   `json:"b"`
	End    int64   `json:"end"` // Unix ns
}

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the spec")
	via := fs.String("via", "front", "front or tcpmux")
	mux := fs.String("mux", "", "frps's tcpmux port (default: the spec's base+10)")
	streams := fs.Int("streams", 1, "concurrent streams per tunnel")
	ramp := fs.Duration("ramp", 5*time.Second, "worker j starts at ramp*j/workers")
	duration := fs.Duration("duration", 60*time.Second, "how long the client runs")
	maxTokens := fs.Int("max-tokens", 60, "tokens per reply (min_tokens = max_tokens)")
	reqTimeout := fs.Duration("req-timeout", 300*time.Second, "per-request deadline")
	out := fs.String("out", "", "the requests, one JSON line each")
	readyPath := fs.String("ready", "", "written once every worker has started")
	holdoff := fs.Duration("holdoff", 0, "after a request that failed or was cut, wait this long before the next (0: retry at once, then 50 ms doubling to 500 ms while requests fail without a token)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *via != "front" && *via != "tcpmux" {
		return fmt.Errorf("-via %q", *via)
	}
	if *mux == "" {
		*mux = fmt.Sprintf("127.0.0.1:%d", sp.PortBase+10)
	}
	if err := requireLoopback(*mux); err != nil {
		return err
	}
	if *streams < 1 || *maxTokens < 2 || *out == "" {
		return errors.New("-streams >= 1, -max-tokens >= 2 and -out are required")
	}
	w, err := openLines(*out)
	if err != nil {
		return err
	}
	defer w.Close()
	stopFlush := make(chan struct{})
	defer close(stopFlush)
	go w.flushEvery(time.Second, stopFlush)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	ctx, cancelRun := context.WithTimeout(ctx, *duration)
	defer cancelRun()
	client := loadClient(*via, *mux)
	n := len(sp.Tunnels)
	workers := n * *streams
	t0 := time.Now()
	var started sync.WaitGroup
	started.Add(workers)
	var wg sync.WaitGroup
	for j := 0; j < workers; j++ {
		t := sp.Tunnels[j / *streams]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sleepCtx(ctx, time.Until(t0.Add(time.Duration(int64(*ramp)*int64(j)/int64(workers)))))
			started.Done()
			backoff := 50 * time.Millisecond
			for ctx.Err() == nil {
				rctx, rcancel := context.WithTimeout(ctx, *reqTimeout)
				r := streamOnce(rctx, client, sp, t, *via, *maxTokens)
				rcancel()
				r.Worker = j
				if ctx.Err() != nil && r.Err != "" {
					r.Err = "end: " + r.Err
				}
				w.add(r)
				if *holdoff > 0 && r.Err != "" {
					// The control client: it sends nothing for the hold-off after a failure or a cut.
					sleepCtx(ctx, *holdoff)
					continue
				}
				if len(r.Tokens) > 0 {
					backoff = 50 * time.Millisecond
				} else if r.Err != "" {
					// A path that refuses at once must not spin: 50 ms, doubling to 500 ms.
					sleepCtx(ctx, backoff)
					backoff = min(2*backoff, 500*time.Millisecond)
				}
			}
		}()
	}
	if *readyPath != "" {
		go func() {
			started.Wait()
			_ = writeJSONFile(*readyPath, map[string]any{"t0_ns": t0.UnixNano(), "workers": workers})
		}()
	}
	wg.Wait()
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// loadClient opens a new connection for every request, as S1c and S14 did for
// each stream: to the front's client listener, or through frps's tcpmux port
// to the request host's tunnel.
func loadClient(via, mux string) *http.Client {
	tr := &http.Transport{DisableKeepAlives: true, DisableCompression: true, MaxIdleConnsPerHost: -1}
	if via == "tcpmux" {
		tr.DialContext = func(ctx context.Context, _, addr string) (net.Conn, error) {
			uid, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return dialTunnel(ctx, mux, uid)
		}
	} else {
		d := &net.Dialer{}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := d.DialContext(ctx, "tcp4", addr)
			if err == nil {
				_ = c.(*net.TCPConn).SetNoDelay(true)
			}
			return c, err
		}
	}
	return &http.Client{Transport: tr}
}

// streamOnce sends one streamed chat completion of exactly maxTokens tokens and
// reads it to its end as Server-Sent Events.
func streamOnce(ctx context.Context, client *http.Client, sp Spec, t Tunnel, via string, maxTokens int) (r reqRec) {
	start := time.Now()
	r = reqRec{Tunnel: t.Index, Start: start.UnixNano()}
	defer func() { r.End = time.Now().UnixNano() }()
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":true,`+
		`"max_tokens":%d,"min_tokens":%d,"ignore_eos":true}`, t.Model, maxTokens, maxTokens)
	url, key := "http://"+sp.ClientListen+"/v1/chat/completions", sp.ClientKey
	if via == "tcpmux" {
		url, key = "http://"+t.UID+":80/v1/chat/completions", t.InstanceKey
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		r.Err = err.Error()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		r.Err = shortErr(err)
		return r
	}
	defer resp.Body.Close()
	r.Status = resp.StatusCode
	cr := &countingReader{r: resp.Body}
	defer func() { r.Bytes = cr.n }()
	if resp.StatusCode != http.StatusOK {
		head, _ := io.ReadAll(io.LimitReader(cr, 200))
		_, _ = io.Copy(io.Discard, io.LimitReader(cr, 1<<20))
		r.Err = fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(head)))
		return r
	}
	if err := readEvents(bufio.NewReaderSize(cr, 16<<10), start, &r); err != nil {
		r.Err = shortErr(err)
	}
	return r
}

type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Error json.RawMessage `json:"error"`
}

// readEvents reads the stream's events until the body ends. An error event
// (the front's cut) is the request's error.
func readEvents(br *bufio.Reader, start time.Time, r *reqRec) error {
	var cut string
	for {
		line, err := br.ReadString('\n')
		if strings.HasPrefix(line, "data: ") && strings.HasSuffix(line, "\n") {
			at := time.Since(start)
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if payload == "[DONE]" {
				r.Done = true
			} else {
				var ch sseChunk
				if jerr := json.Unmarshal([]byte(payload), &ch); jerr != nil {
					return fmt.Errorf("bad event: %v", jerr)
				}
				if len(ch.Error) > 0 && string(ch.Error) != "null" {
					cut = "error event: " + string(ch.Error)
				}
				for _, c := range ch.Choices {
					if c.Delta.Content != "" {
						r.Tokens = append(r.Tokens, int64(at/(100*time.Microsecond)))
					}
					if c.FinishReason != nil {
						r.Fin = true
					}
				}
			}
		}
		if err != nil {
			switch {
			case cut != "":
				return errors.New(cut)
			case err != io.EOF:
				return err
			case !r.Done:
				return errors.New("cut: EOF before [DONE]")
			}
			return nil
		}
	}
}

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
