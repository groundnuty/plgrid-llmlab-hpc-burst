package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// headerList is a repeatable -header "Name: value" flag.
type headerList []string

func (h *headerList) String() string     { return strings.Join(*h, ", ") }
func (h *headerList) Set(v string) error { *h = append(*h, v); return nil }

// streamResult is the verdict of one streamed completion. Complete is true only when a
// finish_reason chunk and the [DONE] sentinel both arrived and the token count equals the
// expected count; a clean EOF without them is a cut, whatever the HTTP layer says.
type streamResult struct {
	Label         string            `json:"label"`
	URL           string            `json:"url"`
	Session       string            `json:"session,omitempty"`
	StartedUTC    string            `json:"started_utc"`
	ExpectTokens  int               `json:"expect_tokens"`
	Tokens        int               `json:"tokens"`
	FinishReason  string            `json:"finish_reason"`
	DoneSeen      bool              `json:"done_seen"`
	HTTPStatus    int               `json:"http_status"`
	RespHeaders   map[string]string `json:"resp_headers,omitempty"`
	Error         string            `json:"error,omitempty"`
	ElapsedSec    float64           `json:"elapsed_sec"`
	FirstTokenSec float64           `json:"first_token_sec"`
	LastTokenSec  float64           `json:"last_token_sec"`
	GapMinMs      float64           `json:"gap_min_ms"`
	GapMeanMs     float64           `json:"gap_mean_ms"`
	GapP50Ms      float64           `json:"gap_p50_ms"`
	GapP99Ms      float64           `json:"gap_p99_ms"`
	GapMaxMs      float64           `json:"gap_max_ms"`
	GapMaxAtToken int               `json:"gap_max_at_token"`
	GapsOver2s    int               `json:"gaps_over_2s"`
	Comments      int               `json:"sse_comment_lines"`
	Complete      bool              `json:"complete"`
	CutAfterSec   float64           `json:"cut_after_sec,omitempty"`
}

type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// chatBody builds a streamed chat request whose output length is exactly n tokens:
// min_tokens == max_tokens pins the simulator's random response length.
func chatBody(model string, tokens int, stream bool, messages []map[string]string) []byte {
	b, _ := json.Marshal(map[string]any{
		"model": model, "messages": messages, "max_tokens": tokens, "min_tokens": tokens,
		"ignore_eos": true, "stream": stream,
	})
	return b
}

func newClient() *http.Client {
	// No client-side deadline of any kind: every cut seen must come from a hop under test.
	return &http.Client{Transport: &http.Transport{DisableCompression: true, DisableKeepAlives: true}}
}

func runStream(args []string) int {
	fs := flag.NewFlagSet("stream", flag.ExitOnError)
	url := fs.String("url", "", "chat completions URL")
	model := fs.String("model", "sim-model", "model name")
	tokens := fs.Int("tokens", 100, "tokens to request (min_tokens == max_tokens)")
	expect := fs.Int("expect", 0, "expected token count (default: -tokens)")
	session := fs.String("session", "", "x-session-id header value (optional)")
	label := fs.String("label", "stream", "label for output files")
	outdir := fs.String("outdir", ".", "directory for <label>.json and <label>.gaps.csv")
	progress := fs.Duration("progress", 5*time.Minute, "progress line interval on stderr")
	var hdrs headerList
	fs.Var(&hdrs, "header", "extra request header 'Name: value' (repeatable)")
	fs.Parse(args)
	if *url == "" {
		fmt.Fprintln(os.Stderr, "stream: -url is required")
		return 64
	}
	if *expect == 0 {
		*expect = *tokens
	}
	res := doStream(*url, *model, *tokens, *expect, *session, *label, *outdir, *progress, hdrs)
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if err := os.WriteFile(filepath.Join(*outdir, *label+".json"), b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write summary:", err)
	}
	if res.Complete {
		return 0
	}
	return 2
}

func doStream(url, model string, tokens, expect int, session, label, outdir string, progress time.Duration, hdrs []string) streamResult {
	res := streamResult{Label: label, URL: url, Session: session, ExpectTokens: expect,
		StartedUTC: time.Now().UTC().Format(time.RFC3339), RespHeaders: map[string]string{}}
	gapsFile, err := os.Create(filepath.Join(outdir, label+".gaps.csv"))
	if err != nil {
		res.Error = "create gaps file: " + err.Error()
		return res
	}
	defer gapsFile.Close()
	gw := bufio.NewWriter(gapsFile)
	defer gw.Flush()
	fmt.Fprintln(gw, "token,elapsed_ms,gap_ms")

	body := chatBody(model, tokens, true, []map[string]string{{"role": "user", "content": "Write a very long story. " + label}})
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if session != "" {
		req.Header.Set("x-session-id", session)
	}
	for _, h := range hdrs {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	start := time.Now()
	resp, err := newClient().Do(req)
	if err != nil {
		res.Error = "request: " + err.Error()
		res.ElapsedSec = time.Since(start).Seconds()
		res.CutAfterSec = res.ElapsedSec
		return res
	}
	defer resp.Body.Close()
	res.HTTPStatus = resp.StatusCode
	for k, v := range resp.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-") || lk == "server" || lk == "via" {
			res.RespHeaders[lk] = strings.Join(v, ",")
		}
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		res.Error = fmt.Sprintf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		res.ElapsedSec = time.Since(start).Seconds()
		res.CutAfterSec = res.ElapsedSec
		return res
	}

	var gaps []float64
	last := start
	nextProgress := start.Add(progress)
	rd := bufio.NewReaderSize(resp.Body, 64<<10)
	for {
		line, rerr := rd.ReadString('\n')
		now := time.Now()
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, ":"):
			res.Comments++
		case strings.HasPrefix(line, "data:"):
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				res.DoneSeen = true
				break
			}
			var ch chatChunk
			if json.Unmarshal([]byte(data), &ch) != nil || len(ch.Choices) == 0 {
				break
			}
			c := ch.Choices[0]
			if c.FinishReason != nil && *c.FinishReason != "" {
				res.FinishReason = *c.FinishReason
			}
			if c.Delta.Content != "" {
				res.Tokens++
				gap := float64(now.Sub(last)) / 1e6
				last = now
				gaps = append(gaps, gap)
				if res.Tokens == 1 {
					res.FirstTokenSec = now.Sub(start).Seconds()
				}
				fmt.Fprintf(gw, "%d,%d,%.1f\n", res.Tokens, now.Sub(start).Milliseconds(), gap)
				res.LastTokenSec = now.Sub(start).Seconds()
			}
		}
		if now.After(nextProgress) {
			fmt.Fprintf(os.Stderr, "%s %s elapsed=%.0fs tokens=%d\n", now.UTC().Format("15:04:05"), label, now.Sub(start).Seconds(), res.Tokens)
			gw.Flush()
			nextProgress = now.Add(progress)
		}
		if rerr != nil {
			if rerr != io.EOF {
				res.Error = "read: " + rerr.Error()
			} else if !res.DoneSeen {
				res.Error = "clean EOF before [DONE]"
			}
			break
		}
	}
	res.ElapsedSec = time.Since(start).Seconds()
	summariseGaps(&res, gaps)
	res.Complete = res.FinishReason != "" && res.DoneSeen && res.Tokens == res.ExpectTokens && res.Error == ""
	if !res.Complete {
		res.CutAfterSec = res.ElapsedSec
		if res.Error == "" {
			res.Error = fmt.Sprintf("incomplete: finish_reason=%q done=%v tokens=%d/%d", res.FinishReason, res.DoneSeen, res.Tokens, res.ExpectTokens)
		}
	}
	return res
}

func summariseGaps(r *streamResult, gaps []float64) {
	if len(gaps) == 0 {
		return
	}
	// The first "gap" is time to first token, not an inter-token gap.
	inter := gaps[1:]
	if len(inter) == 0 {
		return
	}
	sorted := append([]float64(nil), inter...)
	sort.Float64s(sorted)
	sum := 0.0
	for i, g := range inter {
		sum += g
		if g > r.GapMaxMs {
			r.GapMaxMs, r.GapMaxAtToken = g, i+2
		}
		if g > 2000 {
			r.GapsOver2s++
		}
	}
	r.GapMinMs = sorted[0]
	r.GapMeanMs = sum / float64(len(inter))
	r.GapP50Ms = sorted[len(sorted)/2]
	r.GapP99Ms = sorted[int(float64(len(sorted)-1)*0.99)]
}
