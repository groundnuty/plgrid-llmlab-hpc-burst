package scenario_test

// The front's client side as steps (b) to (f) use it: requests keyed by a
// conversation key, the usage record of each request by its request ID, a
// stream read as it arrives, and the front's health and metrics (HBEP-5,
// HBEP-8).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// requestTimeout bounds a request the rehearsal sends: the front cuts every
// request at maxRequestDuration, so one with no answer by then and a margin
// hangs.
const requestTimeout = maxRequestDuration + 30*time.Second

// reply is one answer of the front, as the client got it.
type reply struct {
	status int
	header http.Header
	body   []byte
	err    error  // the client's error: no answer, or a body cut
	id     string // the front's request ID (Hpcburst-Request-Id)
}

// send posts body to the front's client listener with the client key and,
// when key is set, the x-session-id conversation key.
func (r *rig) send(path, body, key string) reply {
	req, err := http.NewRequest(http.MethodPost, "http://"+clientAddr+path, strings.NewReader(body))
	if err != nil {
		return reply{err: err}
	}
	req.Header.Set("Authorization", "Bearer "+r.h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-Session-Id", key)
	}
	resp, err := (&http.Client{Timeout: requestTimeout}).Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return reply{status: resp.StatusCode, header: resp.Header, body: b, err: err, id: resp.Header.Get("Hpcburst-Request-Id")}
}

// chatBody is a chat request for the plain model name. minTokens equal to
// maxTokens fixes the output length (the engine double's doc).
func chatBody(maxTokens, minTokens int, stream bool) string {
	b := `{"model":"` + model + `","messages":[{"role":"user","content":"hello"}],"max_tokens":` + strconv.Itoa(maxTokens)
	if minTokens > 0 {
		b += `,"min_tokens":` + strconv.Itoa(minTokens)
	}
	if stream {
		b += `,"stream":true,"stream_options":{"include_usage":true}`
	}
	return b + "}"
}

// rowOf waits for the usage record of a request, by its request ID: the front
// writes it when the request ends, which may be after the client read the
// last byte.
func (r *rig) rowOf(id string) usageRow {
	r.t.Helper()
	if id == "" {
		r.t.Fatal("a reply without the front's request ID (Hpcburst-Request-Id)")
	}
	var row usageRow
	waitFor(r.t, waitDeadline, "the usage record of request "+id, func() bool {
		for _, x := range r.usageRows() {
			if x.RequestID == id {
				row = x
				return true
			}
		}
		return false
	})
	return row
}

// keyFor finds a conversation key that the front's ring maps to the Instance
// now: it sends short requests with candidate keys and reads which Instance
// each usage record names. Never a guess (HBEP-5, Conversation key).
func (r *rig) keyFor(in *v1alpha1.Instance) string {
	r.t.Helper()
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("rehearsal-conversation-%d", i)
		rp := r.send("/v1/chat/completions", chatBody(1, 0, false), key)
		if rp.err != nil || rp.status != http.StatusOK {
			r.t.Fatalf("a key search request: %d %v: %.300s", rp.status, rp.err, rp.body)
		}
		if r.rowOf(rp.id).InstanceUID == string(in.UID) {
			return key
		}
	}
	r.t.Fatalf("no key of 64 maps to Instance %s on the front's ring", in.Name)
	return ""
}

// streamed is a streamed request's outcome.
type streamed struct {
	reply
	firstAt, endAt time.Time // the first data line and the end of the body
}

// startStream sends a streamed request and reads it as it arrives. first is
// closed at the first data line (or at the end, if none comes); done delivers
// the outcome once the body ends.
func (r *rig) startStream(body, key string) (first <-chan struct{}, done <-chan streamed) {
	f, d := make(chan struct{}), make(chan streamed, 1)
	go func() {
		var out streamed
		defer func() { d <- out }()
		once := func() {
			select {
			case <-f:
			default:
				close(f)
			}
		}
		defer once()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+clientAddr+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			out.err = err
			return
		}
		req.Header.Set("Authorization", "Bearer "+r.h.clientKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Session-Id", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out.err = err
			return
		}
		defer resp.Body.Close()
		out.status, out.header, out.id = resp.StatusCode, resp.Header, resp.Header.Get("Hpcburst-Request-Id")
		var buf bytes.Buffer
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data:") && out.firstAt.IsZero() {
				out.firstAt = time.Now()
				once()
			}
			buf.Write(sc.Bytes())
			buf.WriteByte('\n')
			if buf.Len() > 8<<20 {
				out.err = fmt.Errorf("the stream passed 8 MiB")
				break
			}
		}
		if out.err == nil {
			out.err = sc.Err()
		}
		out.body, out.endAt = buf.Bytes(), time.Now()
	}()
	return f, d
}

// frontHealth reads the front's /healthz (base+4).
func frontHealth() (map[string]any, bool) {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", portBase+4))
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

// registered reports whether the front's probe holds the Instance's tunnel
// transport registered, which an Instance needs to be eligible (HBEP-6, The
// front's half), from /healthz's probe.instances; known is false when the
// front does not report the Instance.
func registered(in *v1alpha1.Instance) (reg, known bool) {
	h, ok := frontHealth()
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

// frontCounter sums a counter's samples on the front's /metrics (base+4).
func frontCounter(name string) (float64, bool) {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", portBase+4))
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var sum float64
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 8<<20))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := sc.Text()
		if !strings.HasPrefix(l, name+"{") && !strings.HasPrefix(l, name+" ") {
			continue
		}
		f := strings.Fields(l)
		v, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err == nil {
			sum += v
		}
	}
	return sum, resp.StatusCode == http.StatusOK
}

// frontError is the error object of a reply the front raised itself.
type frontError struct {
	Code   string  `json:"code"`
	Detail *string `json:"detail"`
}

func errorOf(body []byte) frontError {
	var e struct {
		Error frontError `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return e.Error
}
