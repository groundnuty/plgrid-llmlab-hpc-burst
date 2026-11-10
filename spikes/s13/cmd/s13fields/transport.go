package main

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/recorder"
)

// TransportCheck is one check of how a router carries a request, beyond the bodies of the cases.
type TransportCheck struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Status   int    `json:"status"`
	Arrivals int    `json:"arrivals"`
	// Chunked checks: whether the worker got the client's bytes, and the length it was told
	// (-1: chunked, as the client sent it).
	Identical           bool   `json:"identical_bytes,omitempty"`
	WorkerContentLength int64  `json:"worker_content_length,omitempty"`
	WorkerContentType   string `json:"worker_content_type,omitempty"`
	Sent                string `json:"sent,omitempty"`
	Received            string `json:"received,omitempty"`
	// Stream checks: whether the client read the first text chunk while the worker still held the
	// rest of the stream (a pass-through), or only after the hold ran out (a buffering hop).
	StreamedThrough bool   `json:"streamed_through,omitempty"`
	Held            bool   `json:"held,omitempty"`
	TimedOut        bool   `json:"timed_out,omitempty"`
	Error           string `json:"error,omitempty"`
}

// chunkedVariants are sent again with no Content-Length, as Transfer-Encoding: chunked.
var chunkedVariants = []string{"chat/minimal#plain", "completions/minimal#plain", "chat/tools-function-strict#stream", "chat/real/agent-turn#stream"}

// unsized hides a body's length from net/http, which then sends it chunked.
type unsized struct{ r io.Reader }

func (u unsized) Read(p []byte) (int, error) { return u.r.Read(p) }

func transportChecks(client *http.Client, target string, rec *recorder.Recorder) []TransportCheck {
	byKey := map[string]cases.Variant{}
	for _, c := range cases.All() {
		for _, v := range c.Variants() {
			byKey[v.Key()] = v
		}
	}
	var out []TransportCheck
	for _, k := range chunkedVariants {
		out = append(out, chunked(client, target, rec, byKey[k]))
	}
	out = append(out,
		held(client, target, rec, "chat", cases.Chat, `{"model":"`+recorder.Model+`","messages":[{"role":"user","content":"hello"}],"stream":true}`, `"content":"ok"`),
		held(client, target, rec, "completions", cases.Completions, `{"model":"`+recorder.Model+`","prompt":"hello","stream":true}`, `"text":"ok"`))
	return out
}

func chunked(client *http.Client, target string, rec *recorder.Recorder, v cases.Variant) TransportCheck {
	key := "transport/chunked/" + v.Key()
	tc := TransportCheck{Name: "chunked request body", Key: key, Sent: string(v.Body)}
	req, err := http.NewRequest(http.MethodPost, target+string(v.Case.Endpoint), unsized{bytes.NewReader(v.Body)})
	if err != nil {
		tc.Error = err.Error()
		return tc
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(recorder.CaseHeader, key)
	req.Header.Set("x-session-id", "s13-"+key)
	resp, err := client.Do(req)
	if err != nil {
		tc.Error = err.Error()
	} else {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		tc.Status = resp.StatusCode
	}
	arr := rec.ByCase(key)
	tc.Arrivals = len(arr)
	if len(arr) > 0 {
		tc.Received = string(arr[0].Body)
		tc.Identical = tc.Received == tc.Sent
		tc.WorkerContentLength = arr[0].ContentLength
		tc.WorkerContentType = arr[0].Header.Get("Content-Type")
	}
	return tc
}

// held streams body with the worker holding the stream after its first text chunk; the client
// releases it on reading marker. The hold lasts at most 10 s.
func held(client *http.Client, target string, rec *recorder.Recorder, name string, ep cases.Endpoint, body, marker string) TransportCheck {
	key := "transport/held-stream/" + name
	tc := TransportCheck{Name: "stream held after its first chunk", Key: key}
	g := rec.Hold(key, 10*time.Second)
	defer g.Release()
	req, err := http.NewRequest(http.MethodPost, target+string(ep), strings.NewReader(body))
	if err != nil {
		tc.Error = err.Error()
		return tc
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(recorder.CaseHeader, key)
	req.Header.Set("x-session-id", "s13-"+key)
	resp, err := client.Do(req)
	if err != nil {
		tc.Error = err.Error()
		return tc
	}
	defer resp.Body.Close()
	tc.Status = resp.StatusCode
	sc := bufio.NewScanner(resp.Body)
	seen := false
	for sc.Scan() {
		if !seen && strings.Contains(sc.Text(), marker) {
			seen = true
			_, timedOut := g.Outcome()
			tc.StreamedThrough = !timedOut
			g.Release()
		}
	}
	tc.Held, tc.TimedOut = g.Outcome()
	tc.Arrivals = len(rec.ByCase(key))
	if !seen {
		tc.Error = "the stream had no text chunk"
	}
	return tc
}
