package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// The dial and the stream reader follow spike S1c's s1cbench
// (spikes/s1c/cmd/s1cbench/dial.go), with the target chosen per request: the
// request's host is the Instance's UID, which the client sends frps in its
// CONNECT, as the front's transport does (HBEP-6, The front's half).

// errRefused is a definite "no" from the path: frps answered the CONNECT with
// a non-200 status (no route to that Instance).
type errRefused struct{ status string }

func (e *errRefused) Error() string { return "refused: " + e.status }

// dialTunnel opens a TCP connection to frps's tcpmux port and returns once
// frps answered 200 to "CONNECT <uid>:80".
func dialTunnel(ctx context.Context, mux, uid string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp4", mux)
	if err != nil {
		return nil, err
	}
	_ = c.(*net.TCPConn).SetNoDelay(true)
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if _, err := fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid); err != nil {
		c.Close()
		return nil, err
	}
	status, err := readHeaderBlock(c)
	_ = c.SetDeadline(time.Time{})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("CONNECT: %w", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") && !strings.HasPrefix(status, "HTTP/1.0 200") {
		c.Close()
		return nil, &errRefused{status: status}
	}
	return c, nil
}

// readHeaderBlock reads one byte at a time up to the blank line that ends an
// HTTP header block, so nothing after it is consumed, and returns the status
// line.
func readHeaderBlock(r io.Reader) (string, error) {
	var buf []byte
	one := make([]byte, 1)
	for len(buf) < 8192 {
		if _, err := io.ReadFull(r, one); err != nil {
			return "", err
		}
		buf = append(buf, one[0])
		if len(buf) >= 4 && string(buf[len(buf)-4:]) == "\r\n\r\n" {
			line, _, _ := strings.Cut(string(buf), "\r\n")
			return line, nil
		}
	}
	return "", errors.New("header block over 8 KiB")
}

// newClient returns an HTTP client whose every request opens a fresh
// connection (no keep-alive), as S1c's did for each new stream. In tunnel
// mode the connection goes through frps's tcpmux port to the request host's
// tunnel; in baseline mode straight to that Instance's engine socket.
func newClient(mode, mux string, sockets map[string]string) *http.Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			uid, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			if mode == ModeBaseline {
				sock, ok := sockets[uid]
				if !ok {
					return nil, fmt.Errorf("no engine socket for %s", uid)
				}
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			}
			return dialTunnel(ctx, mux, uid)
		},
		DisableKeepAlives:  true,
		DisableCompression: true,
	}
	return &http.Client{Transport: tr}
}

// streamResult is one streamed completion as the client saw it.
type streamResult struct {
	Tunnel    int
	Start     time.Time
	TTFT      time.Duration   // first content token, from before the dial
	Tokens    []time.Duration // arrival of every content token, from Start
	Bytes     int64
	Status    int
	Finished  bool // a chunk with finish_reason arrived
	Done      bool // the [DONE] event arrived
	Err       error
	WantToken int
}

// Complete is true only for a 200 reply with a finish_reason chunk, [DONE] and
// exactly the requested number of content tokens.
func (r *streamResult) Complete() bool {
	return r.Err == nil && r.Status == 200 && r.Finished && r.Done && len(r.Tokens) == r.WantToken
}

type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
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

// streamOnce sends one streamed chat completion of exactly maxTokens tokens to
// the Instance uid and reads it to the end as Server-Sent Events.
func streamOnce(ctx context.Context, client *http.Client, uid, model string, maxTokens int) *streamResult {
	res := &streamResult{Start: time.Now(), WantToken: maxTokens}
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":true,`+
		`"max_tokens":%d,"min_tokens":%d,"ignore_eos":true}`, model, maxTokens, maxTokens)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+uid+":80/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		res.Err = err
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		res.Err = err
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	cr := &countingReader{r: resp.Body}
	defer func() { res.Bytes = cr.n }()
	if resp.StatusCode != 200 {
		head, _ := io.ReadAll(io.LimitReader(cr, 120))
		_, _ = io.Copy(io.Discard, cr)
		res.Err = fmt.Errorf("status %d: %q", resp.StatusCode, head)
		return res
	}
	res.Err = readEvents(bufio.NewReaderSize(cr, 16*1024), res)
	return res
}

// readEvents reads the stream's events into res until the body ends.
func readEvents(br *bufio.Reader, res *streamResult) error {
	for {
		line, err := br.ReadString('\n')
		if strings.HasPrefix(line, "data: ") && strings.HasSuffix(line, "\n") {
			now := time.Since(res.Start)
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if payload == "[DONE]" {
				res.Done = true
			} else {
				var ch sseChunk
				if jerr := json.Unmarshal([]byte(payload), &ch); jerr != nil {
					return fmt.Errorf("bad event: %v", jerr)
				}
				for _, c := range ch.Choices {
					if c.Delta.Content != "" {
						if len(res.Tokens) == 0 {
							res.TTFT = now
						}
						res.Tokens = append(res.Tokens, now)
					}
					if c.FinishReason != nil {
						res.Finished = true
					}
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			if !res.Done {
				return errors.New("cut: EOF before [DONE]")
			}
			return nil
		}
	}
}

// probeOnce is the front's probe (HBEP-6, The front's half): a new connection
// through the transport, then GET /health with no key; it passes on a 200
// within timeout.
func probeOnce(client *http.Client, uid string, timeout time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+uid+":80/health", nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return time.Since(start), err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Since(start), fmt.Errorf("status %d", resp.StatusCode)
	}
	return time.Since(start), nil
}
