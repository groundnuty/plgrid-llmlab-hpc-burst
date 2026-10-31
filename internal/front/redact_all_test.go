package front

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rule as #102 states it (@hpc-burst-science-agent, 31 Oct 2026): every
// header and body byte the front forwards from a target, on every path and
// status, raw streams included, has every target credential replaced with
// [redacted], byte by byte with state carried across reads, with nothing
// else changed. Toward a target the front asks for identity; gzip and
// deflate are decoded before redacting, and any other encoding is 502
// upstream_unreachable, detail encoding.

// TestRedactReaderEverySplit: the secret split at every byte, read one byte
// at a time, and twice in a row: no copy survives and nothing else changes.
func TestRedactReaderEverySplit(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef"
	in := "head " + key + " middle " + key + key + " tail"
	want := "head [redacted] middle [redacted][redacted] tail"
	got, err := io.ReadAll(newRedactReader(iotest.OneByteReader(strings.NewReader(in)), []string{key, "another-secret-key"}))
	if err != nil || string(got) != want {
		t.Fatalf("got %q (%v), want %q", got, err, want)
	}
}

// TestRedactReaderDoesNotHoldAnEventEnd: a read whose tail cannot begin a
// secret passes whole at once, so a stream's events are not delayed.
func TestRedactReaderDoesNotHoldAnEventEnd(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	z := newRedactReader(pr, []string{"0123456789abcdef"})
	ev := "data: {\"x\":1}\n\n"
	go func() { _, _ = pw.Write([]byte(ev)) }()
	buf := make([]byte, 64)
	done := make(chan string, 1)
	go func() {
		n, _ := z.Read(buf)
		done <- string(buf[:n])
	}()
	select {
	case got := <-done:
		if got != ev {
			t.Errorf("first read %q, want the whole event %q", got, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the event's end was held waiting for more bytes")
	}
}

// TestRedactRawStreamAndHeaderOn200: a 200 stream of a path the front does
// not judge holds the key twice, once split across two writes, and a header
// holds it too.
func TestRedactRawStreamAndHeaderOn200(t *testing.T) {
	f := newScFix(t, scOpts{})
	key, _ := rdKeysOne(t, f)
	half := len(key) / 2
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Echo", "key="+key)
		w.WriteHeader(200)
		rc := http.NewResponseController(w)
		_, _ = io.WriteString(w, "data: chunk 0 "+key[:half])
		_ = rc.Flush()
		_, _ = io.WriteString(w, key[half:]+" and "+key+"\n\n")
		_ = rc.Flush()
	}))
	resp := f.errPost("/v1/audio/speech", `{"model":"`+scModel+`","stream":true,"input":"hi"}`)
	got := readAll(t, resp)
	if want := "data: chunk 0 [redacted] and [redacted]\n\n"; got != want {
		t.Errorf("body %q, want %q", got, want)
	}
	if v := resp.Header.Get("X-Echo"); v != "key=[redacted]" {
		t.Errorf("header X-Echo %q, want key=[redacted]", v)
	}
}

// TestGzipFromATargetIsDecodedAndRedacted: the front sends Accept-Encoding
// identity; a target that gzips anyway has its body decoded, redacted and
// passed uncompressed.
func TestGzipFromATargetIsDecodedAndRedacted(t *testing.T) {
	f := newScFix(t, scOpts{})
	key, _ := rdKeysOne(t, f)
	plain := `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"echo ` + key + `"},"finish_reason":"stop"}]}`
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(plain))
	_ = zw.Close()
	var accept string
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		accept = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gz.Bytes())
	}))
	req, _ := http.NewRequest(http.MethodPost, f.url+scChat, strings.NewReader(errChatBody(scModel, false, "hi")))
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	req.Header.Set("Accept-Encoding", "gzip, br") // the client's own wish is not passed on
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	got := readAll(t, resp)
	if accept != "identity" {
		t.Errorf("the target got Accept-Encoding %q, want identity", accept)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("Content-Encoding %q reached the client, want none: the body is decoded", ce)
	}
	if want := strings.ReplaceAll(plain, key, "[redacted]"); got != want {
		t.Errorf("body %q, want %q", got, want)
	}
}

// TestUnknownEncodingFromATargetIs502: a br body cannot be redacted.
func TestUnknownEncodingFromATargetIs502(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "br")
		_, _ = w.Write([]byte{0x1b, 0x02, 0x00})
	}))
	resp := f.errPost(scChat, errChatBody(scModel, false, "hi"))
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502: %s", resp.StatusCode, body)
	}
	checkErrorBody(t, body, codeUpstreamUnreachable, "encoding")
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("Content-Encoding %q on the front's own error", ce)
	}
}

// rdKeysOne is the one Instance's key of an scFix.
func rdKeysOne(t *testing.T, f *scFix) (string, string) {
	t.Helper()
	b, err := os.ReadFile(agentapi.InstanceKeyPath(f.host.CredentialsDir(), string(f.uids[0])))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b)), ""
}

// TestRedactResponsesFailedAnd2xxAndGzip401 (HBEP-5, Test plan,
// Redaction): the key echoed in a response.failed event, in a plain 2xx body
// and in a gzipped 401 leaves no copy in the client's bytes, and the rest is
// unchanged.
func TestRedactResponsesFailedAnd2xxAndGzip401(t *testing.T) {
	gzipped := func(s string) []byte {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = zw.Write([]byte(s))
		_ = zw.Close()
		return b.Bytes()
	}
	cases := []struct {
		name, path, req string
		status          int
		body            func(key string) string
		gzip            bool
	}{
		{"response.failed", "/v1/responses", prRespStream, 200, func(k string) string {
			return respCreated + "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"r\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"bad key " + k + "\"}}}\n\n"
		}, false},
		{"2xx body", scChat, errChatBody(scModel, false, "hi"), 200, func(k string) string {
			return `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"` + k + `"},"finish_reason":"stop"}]}`
		}, false},
		{"gzipped 401", scChat, errChatBody(scModel, false, "hi"), 401, func(k string) string {
			return `{"error":{"message":"refused Bearer ` + k + `","type":"auth","code":401}}`
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			key, _ := rdKeysOne(t, f)
			plain := c.body(key)
			f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
				ct := "application/json"
				if strings.HasPrefix(plain, "event:") {
					ct = "text/event-stream"
				}
				w.Header().Set("Content-Type", ct)
				if c.gzip {
					w.Header().Set("Content-Encoding", "gzip")
					w.WriteHeader(c.status)
					_, _ = w.Write(gzipped(plain))
					return
				}
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, plain)
			}))
			resp := f.errPost(c.path, c.req)
			got := readAll(t, resp)
			if resp.StatusCode != c.status {
				t.Fatalf("status %d, want %d: %s", resp.StatusCode, c.status, got)
			}
			if want := strings.ReplaceAll(plain, key, "[redacted]"); got != want {
				t.Errorf("body\n got %q\nwant %q", got, want)
			}
		})
	}
}
