// Copied from spikes/s13/recorder/recorder_test.go (spike S13, PR #324), byte for byte below the next line, with
// only S13's import paths mapped to this directory: #429's Bytes test needs S13's cases, its raw JSON
// comparison and its recorder in the root module (team-lead's answer T4). drift_test.go checks the copy.
// End of provenance.

package recorder

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var bs = string(rune(92)) // one backslash; see rawjson's tests

func post(t *testing.T, url, key string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(CaseHeader, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The recorder must store the connection's bytes, not a re-encoding: whitespace, member order,
// escapes and number spellings that any decode-and-encode would change all survive.
func TestStoresTheExactBytes(t *testing.T) {
	r := &Recorder{}
	srv := httptest.NewServer(r.Handler("a"))
	defer srv.Close()
	body := []byte("{ \"stream\" :false,\n\t\"model\":\"" + Model + "\", \"messages\":[{\"role\":\"user\",\"content\":\"caf" + bs + "u00e9 <&>\"}]," +
		"\"temperature\":1.0, \"top_p\":1e-3, \"seed\":9223372036854775807, \"x\":{\"b\":1,\"a\":2} }  ")
	if code, _ := post(t, srv.URL+"/v1/chat/completions", "k1", body); code != 200 {
		t.Fatalf("status %d", code)
	}
	got := r.ByCase("k1")
	if len(got) != 1 || !bytes.Equal(got[0].Body, body) {
		t.Fatalf("stored %d arrivals (%+v), sent %q", len(got), got, body)
	}
	if got[0].ContentLength != int64(len(body)) {
		t.Fatalf("content length %d, body %d", got[0].ContentLength, len(body))
	}
}

func TestStreamsWhenAsked(t *testing.T) {
	r := &Recorder{}
	srv := httptest.NewServer(r.Handler("a"))
	defer srv.Close()
	for _, c := range []struct{ path, body, want string }{
		{"/v1/chat/completions", `{"model":"m","messages":[],"stream":true}`, `"delta":{"content":"ok"}`},
		{"/v1/completions", `{"model":"m","prompt":"p","stream":true,"stream_options":{"include_usage":true}}`, `"usage":`},
	} {
		code, out := post(t, srv.URL+c.path, "s", []byte(c.body))
		if code != 200 || !strings.Contains(out, c.want) || !strings.HasSuffix(out, "data: [DONE]\n\n") {
			t.Errorf("%s: status %d, body %q", c.path, code, out)
		}
	}
	code, out := post(t, srv.URL+"/v1/completions", "n", []byte(`{"model":"m","prompt":"p"}`))
	if code != 200 || !strings.Contains(out, `"object":"text_completion"`) || strings.Contains(out, "data:") {
		t.Errorf("non-stream: status %d, body %q", code, out)
	}
}

func TestRefusesWhatItCannotDecodeButRecordsIt(t *testing.T) {
	r := &Recorder{}
	srv := httptest.NewServer(r.Handler("a"))
	defer srv.Close()
	if code, _ := post(t, srv.URL+"/v1/completions", "bad", []byte(`{"prompt":`)); code != 400 {
		t.Fatalf("status %d", code)
	}
	if got := r.ByCase("bad"); len(got) != 1 || string(got[0].Body) != `{"prompt":` {
		t.Fatalf("record %+v", got)
	}
	if code, _ := post(t, srv.URL+"/v1/completions", "big", bytes.Repeat([]byte(" "), MaxBody+1)); code != 413 {
		t.Fatalf("oversized body: status %d", code)
	}
}

func TestServesTheGaugesARouterScrapes(t *testing.T) {
	r := &Recorder{}
	srv := httptest.NewServer(r.Handler("a"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, name := range []string{"vllm:num_requests_running{", "vllm:num_requests_waiting{", "vllm:kv_cache_usage_perc{", "vllm:cache_config_info{"} {
		if !strings.Contains(string(b), name) {
			t.Errorf("/metrics lacks %s", name)
		}
	}
}

// readGated streams a chat request through url, releasing the gate when the first text chunk arrives.
func readGated(t *testing.T, url string, g *Gate) (sawBeforeRelease bool, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[],"stream":true}`))
	req.Header.Set(CaseHeader, "gated")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		b.WriteString(line + "\n")
		if strings.Contains(line, `"content":"ok"`) && !sawBeforeRelease {
			// Through a pass-through hop the chunk arrives while the hold is still on (or before it
			// starts); through a buffering one only after the hold has run out.
			_, timedOut := g.Outcome()
			sawBeforeRelease = !timedOut
			g.Release()
		}
	}
	return sawBeforeRelease, b.String()
}

// The gate tells a pass-through from a buffering hop by state: whether the client read the first text
// chunk while the recorder still held the rest.
func TestGateTellsStreamingFromBuffering(t *testing.T) {
	r := &Recorder{}
	srv := httptest.NewServer(r.Handler("a"))
	defer srv.Close()

	g := r.Hold("gated", 10*time.Second)
	saw, body := readGated(t, srv.URL, g)
	if held, timedOut := g.Outcome(); !saw || !held || timedOut || !strings.Contains(body, "[DONE]") {
		t.Fatalf("direct: saw %v, held %v, timed out %v, body %q", saw, held, timedOut, body)
	}

	// A hop that reads the whole upstream response before it answers.
	buffering := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		up, err := http.NewRequest(req.Method, srv.URL+req.URL.Path, req.Body)
		if err != nil {
			t.Error(err)
			return
		}
		up.Header = req.Header.Clone()
		resp, err := http.DefaultClient.Do(up)
		if err != nil {
			t.Error(err)
			return
		}
		all, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		_, _ = w.Write(all)
	}))
	defer buffering.Close()
	g = r.Hold("gated", 200*time.Millisecond)
	saw, body = readGated(t, buffering.URL, g)
	if held, timedOut := g.Outcome(); saw || !held || !timedOut || !strings.Contains(body, "[DONE]") {
		t.Fatalf("buffering: saw %v, held %v, timed out %v, body %q", saw, held, timedOut, body)
	}
}
