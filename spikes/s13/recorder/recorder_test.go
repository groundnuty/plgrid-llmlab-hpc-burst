package recorder

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
