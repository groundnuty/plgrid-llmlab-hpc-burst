package stack

// The front's client side: requests keyed by a conversation key, the usage
// record of each request by its request ID, a stream read as it arrives, and
// the front's health and metrics (HBEP-5, HBEP-8). Copied from
// internal/rehearsal/scenario's client_test.go and front_test.go (issue #251
// moves the scenario onto it); the model and the deadline are the Client's.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// Client sends requests to one stack's front.
type Client struct {
	T T
	H *Host
	// Model is the plain name the requests ask for.
	Model string
	// MaxRequest is the deployment's routing.maxRequestDuration: the front cuts
	// every request at it, so one with no answer by then and a margin hangs.
	MaxRequest time.Duration
}

// RequestTimeout bounds a request: the deployment's deadline and 30 s.
func (c *Client) RequestTimeout() time.Duration { return c.MaxRequest + 30*time.Second }

// Reply is one answer of the front, as the client got it.
type Reply struct {
	Status int
	Header http.Header
	Body   []byte
	Err    error  // the client's error: no answer, or a body cut
	ID     string // the front's request ID (Hpcburst-Request-Id)
}

// RequestCtx bounds one request by RequestTimeout and WaitCap.
func (c *Client) RequestCtx() (context.Context, context.CancelFunc) {
	d, _ := BoundedDeadline(c.T, c.RequestTimeout())
	return context.WithDeadline(context.Background(), d)
}

// Send posts body to the front's client listener with the client key and,
// when key is set, the x-session-id conversation key.
func (c *Client) Send(path, body, key string) Reply {
	rp, _ := c.SendTimed(path, body, key)
	return rp
}

// SendTimed is Send, and also returns when the client wrote the request's
// last byte to its connection (net/http/httptrace's WroteRequest): the end of
// the body's send as the client sees it, the bytes then in the connection's
// buffers, not yet at the far end. Zero when the request was not written.
//
// The transport may hand back the response before its writer has called the
// hook, so once a response has come, SendTimed waits for the hook's call (a
// channel, at most wroteWait): it comes at once for a request that was written.
func (c *Client) SendTimed(path, body, key string) (Reply, time.Time) {
	var (
		once  sync.Once
		sent  time.Time
		wrote = make(chan struct{})
	)
	ctx, cancel := c.RequestCtx()
	defer cancel()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			once.Do(func() { sent = time.Now(); close(wrote) })
		}
	}})
	rp := c.send(ctx, path, body, key)
	wait := time.NewTimer(wroteWait)
	defer wait.Stop()
	if rp.Status != 0 {
		select {
		case <-wrote:
			return rp, sent
		case <-wait.C:
		}
	}
	select {
	case <-wrote:
		return rp, sent
	default:
		return rp, time.Time{}
	}
}

// wroteWait bounds SendTimed's wait for the transport's write hook after a
// response came: the hook runs as the writer returns, within microseconds.
const wroteWait = 5 * time.Second

// send is one request of Send and SendTimed under ctx.
func (c *Client) send(ctx context.Context, path, body, key string) Reply {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.H.Ports.ClientAddr+path, strings.NewReader(body))
	if err != nil {
		return Reply{Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+c.H.ClientKey)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-Session-Id", key)
	}
	resp, err := (&http.Client{Timeout: c.RequestTimeout()}).Do(req)
	if err != nil {
		return Reply{Err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return Reply{Status: resp.StatusCode, Header: resp.Header, Body: b, Err: err, ID: resp.Header.Get("Hpcburst-Request-Id")}
}

// ChatBody is a chat request for the plain model name. minTokens equal to
// maxTokens fixes the output length (the engine double's doc). pad, when not
// empty, is a further user message, so that a request carries a body of the
// size the run needs.
func ChatBody(model string, maxTokens, minTokens int, stream bool, pad string) string {
	msgs := `[{"role":"user","content":"hello"}]`
	if pad != "" {
		p, _ := json.Marshal(pad)
		msgs = `[{"role":"user","content":"hello"},{"role":"user","content":` + string(p) + `}]`
	}
	b := `{"model":"` + model + `","messages":` + msgs + `,"max_tokens":` + strconv.Itoa(maxTokens)
	if minTokens > 0 {
		b += `,"min_tokens":` + strconv.Itoa(minTokens)
	}
	if stream {
		b += `,"stream":true,"stream_options":{"include_usage":true}`
	}
	return b + "}"
}

// UsageRow is a usage record (internal/front/usage, Record).
type UsageRow = usage.Record

// UsageRows reads every usage record so far, by counter.
func (c *Client) UsageRows() []UsageRow {
	c.T.Helper()
	files, _ := filepath.Glob(filepath.Join(c.H.UsageDir, "usage*.jsonl"))
	var rows []UsageRow
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			c.T.Fatalf("usage file %s: %v", f, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var row UsageRow
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
				c.T.Fatalf("usage record in %s: %v: %s", f, err, sc.Bytes())
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Counter < rows[j].Counter })
	return rows
}

// RowOf waits for the usage record of a request, by its request ID: the front
// writes it when the request ends, which may be after the client read the
// last byte.
func (c *Client) RowOf(id string) UsageRow {
	c.T.Helper()
	if id == "" {
		c.T.Fatal("a reply without the front's request ID (Hpcburst-Request-Id)")
	}
	var row UsageRow
	WaitFor(c.T, WaitDeadline, "the usage record of request "+id, func() bool {
		for _, x := range c.UsageRows() {
			if x.RequestID == id {
				row = x
				return true
			}
		}
		return false
	})
	return row
}

// KeyFor finds a conversation key that the front's ring maps to the Instance
// now: it sends short requests with candidate keys and reads which Instance
// each usage record names. Never a guess (HBEP-5, Conversation key).
func (c *Client) KeyFor(in *v1alpha1.Instance) string {
	c.T.Helper()
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("rehearsal-conversation-%d", i)
		rp := c.Send("/v1/chat/completions", ChatBody(c.Model, 1, 0, false, ""), key)
		if rp.Err != nil || rp.Status != http.StatusOK {
			c.T.Fatalf("a key search request: %d %v: %.300s", rp.Status, rp.Err, rp.Body)
		}
		if c.RowOf(rp.ID).InstanceUID == string(in.UID) {
			return key
		}
	}
	c.T.Fatalf("no key of 64 maps to Instance %s on the front's ring", in.Name)
	return ""
}

// Streamed is a streamed request's outcome.
type Streamed struct {
	Reply
	FirstAt, EndAt time.Time // the first data line and the end of the body
	// Ticks are the arrival times of the body's token-bearing data lines
	// (TokenEvent), in order: a stream's tokens as the client read them.
	Ticks []time.Time
}

// StartStream sends a streamed request and reads it as it arrives. first is
// closed at the first data line (or at the end, if none comes); done delivers
// the outcome once the body ends.
func (c *Client) StartStream(body, key string) (first <-chan struct{}, done <-chan Streamed) {
	f, d := make(chan struct{}), make(chan Streamed, 1)
	go func() {
		var out Streamed
		defer func() { d <- out }()
		once := func() {
			select {
			case <-f:
			default:
				close(f)
			}
		}
		defer once()
		ctx, cancel := c.RequestCtx()
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.H.Ports.ClientAddr+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			out.Err = err
			return
		}
		req.Header.Set("Authorization", "Bearer "+c.H.ClientKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Session-Id", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out.Err = err
			return
		}
		defer resp.Body.Close()
		out.Status, out.Header, out.ID = resp.StatusCode, resp.Header, resp.Header.Get("Hpcburst-Request-Id")
		var buf bytes.Buffer
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data:") {
				now := time.Now()
				if TokenEvent(sc.Text()) {
					out.Ticks = append(out.Ticks, now)
				}
				if out.FirstAt.IsZero() {
					out.FirstAt = now
					once()
				}
			}
			buf.Write(sc.Bytes())
			buf.WriteByte('\n')
			if buf.Len() > 8<<20 {
				out.Err = fmt.Errorf("the stream passed 8 MiB")
				break
			}
		}
		if out.Err == nil {
			out.Err = sc.Err()
		}
		out.Body, out.EndAt = buf.Bytes(), time.Now()
	}()
	return f, d
}

// FrontHealth reads the front's /healthz (base+4).
func FrontHealth(p Ports) (map[string]any, bool) {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", p.Base+4))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var h map[string]any
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&h) != nil {
		return nil, false
	}
	return h, true
}

// Registered reports whether the front's probe holds the Instance's tunnel
// transport registered, which an Instance needs to be eligible (HBEP-6, The
// front's half), from /healthz's probe.instances; known is false when the
// front does not report the Instance.
func Registered(p Ports, in *v1alpha1.Instance) (reg, known bool) {
	h, ok := FrontHealth(p)
	if !ok {
		return false, false
	}
	probe, _ := h["probe"].(map[string]any)
	insts, _ := probe["instances"].(map[string]any)
	st, ok := insts[in.Namespace+"/"+in.Name].(map[string]any)
	if !ok {
		return false, false
	}
	reg, known = st["registered"].(bool)
	return reg, known
}

// FrontError is the error object of a reply the front raised itself.
type FrontError struct {
	Code   string  `json:"code"`
	Detail *string `json:"detail"`
}

// ErrorOf is the error object of a reply's body; zero when it has none.
func ErrorOf(body []byte) FrontError {
	var e struct {
		Error FrontError `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return e.Error
}

// TokenEvent reports whether an SSE line is a data line that carries tokens: a
// chunk with a choice whose text (a completions stream's) or delta (a chat
// stream's content, reasoning or tool calls) is not empty. The role-only first
// chunk, a finish-only or usage chunk, an error event and [DONE] carry none,
// so a late one is no gap between two tokens (the head-of-line measure, issue
// #224; Astra's nit on PR 255's delta round).
func TokenEvent(line string) bool {
	payload, ok := strings.CutPrefix(line, "data:")
	if !ok {
		return false
	}
	var c struct {
		Choices []struct {
			Text  string `json:"text"`
			Delta struct {
				Content          string            `json:"content"`
				ReasoningContent string            `json:"reasoning_content"`
				Reasoning        string            `json:"reasoning"`
				ToolCalls        []json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(payload)), &c) != nil {
		return false
	}
	for _, ch := range c.Choices {
		d := ch.Delta
		if ch.Text != "" || d.Content != "" || d.ReasoningContent != "" || d.Reasoning != "" || len(d.ToolCalls) > 0 {
			return true
		}
	}
	return false
}

// SSEDone reports whether a streamed body ends with "data: [DONE]" and has a
// chunk with a finish reason.
func SSEDone(body []byte) (done, finished bool) {
	var last string
	for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "data:") {
			continue
		}
		last = strings.TrimSpace(strings.TrimPrefix(l, "data:"))
		if strings.Contains(last, `"finish_reason":"`) {
			finished = true
		}
	}
	return last == "[DONE]", finished
}

// StreamErrorCode is the code of an error event the front put in a stream
// (HBEP-5, Errors: for example stream_cut); "" when there is none.
func StreamErrorCode(body []byte) string {
	for _, l := range strings.Split(string(body), "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "data:") {
			continue
		}
		if e := ErrorOf([]byte(strings.TrimSpace(strings.TrimPrefix(l, "data:")))); e.Code != "" {
			return e.Code
		}
	}
	return ""
}
