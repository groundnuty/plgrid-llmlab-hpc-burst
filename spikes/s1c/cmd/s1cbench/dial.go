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

// errRefused is a definite "no" from the path: frps answered the CONNECT with a non-200 status
// (no route to that instance), as opposed to a connection that hangs or breaks.
type errRefused struct{ status string }

func (e *errRefused) Error() string { return "refused: " + e.status }

// requireLoopback refuses any listen or target address that is not on 127.0.0.1.
func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q: %w", addr, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("address %q is not on 127.0.0.1 (loopback only)", addr)
	}
	return nil
}

// dialPath opens a TCP connection to target. When host is non-empty, target is frps's tcpmux
// HTTP CONNECT port: the function sends "CONNECT host:80" and returns once the 200 reply is read.
func dialPath(ctx context.Context, target, host string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if host == "" {
		return c, nil
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if _, err := fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", host, host); err != nil {
		c.Close()
		return nil, err
	}
	status, err := readHeaderBlock(c)
	_ = c.SetDeadline(time.Time{})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("CONNECT %s: %w", host, err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") && !strings.HasPrefix(status, "HTTP/1.0 200") {
		c.Close()
		return nil, &errRefused{status: status}
	}
	return c, nil
}

// readHeaderBlock reads one byte at a time up to the blank line that ends an HTTP header block,
// so that nothing after it is consumed, and returns the status line.
func readHeaderBlock(r io.Reader) (string, error) {
	var buf []byte
	one := make([]byte, 1)
	for len(buf) < 8192 {
		if _, err := io.ReadFull(r, one); err != nil {
			return "", err
		}
		buf = append(buf, one[0])
		if strings.HasSuffix(string(buf), "\r\n\r\n") {
			line, _, _ := strings.Cut(string(buf), "\r\n")
			return line, nil
		}
	}
	return "", errors.New("header block over 8 KiB")
}

// newClient returns an HTTP client whose every request opens a fresh connection through the path
// (no keep-alive), as a front would for a new stream through tcpmux.
func newClient(target, host string) *http.Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialPath(ctx, target, host)
		},
		DisableKeepAlives:  true,
		DisableCompression: true,
		MaxConnsPerHost:    0,
	}
	return &http.Client{Transport: tr}
}

// streamResult is one streamed completion as the client saw it.
type streamResult struct {
	Start     time.Time
	TTFT      time.Duration   // first content token, from before the dial
	Tokens    []time.Duration // arrival of every content token, from Start
	Bytes     int64           // body bytes read
	Status    int
	Finished  bool // a chunk with finish_reason arrived
	Done      bool // the [DONE] event arrived
	Err       error
	WantToken int
}

// Complete is true only for a 200 reply with a finish_reason chunk, [DONE] and exactly the
// requested number of content tokens (the simulator sends one token per chunk).
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

// streamOnce sends one streaming chat completion of exactly maxTokens tokens and reads it to the
// end as Server-Sent Events.
func streamOnce(ctx context.Context, client *http.Client, model string, maxTokens int) *streamResult {
	res := &streamResult{Start: time.Now(), WantToken: maxTokens}
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":true,`+
		`"max_tokens":%d,"min_tokens":%d,"ignore_eos":true}`, model, maxTokens, maxTokens)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://engine/v1/chat/completions", strings.NewReader(body))
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
	br := bufio.NewReaderSize(cr, 64*1024)
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
					res.Err = fmt.Errorf("bad event: %v", jerr)
					return res
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
				res.Err = err
			} else if !res.Done {
				res.Err = errors.New("cut: EOF before [DONE]")
			}
			return res
		}
	}
}
