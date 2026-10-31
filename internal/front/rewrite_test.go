package front

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Test plan, "No change but model": the front's outgoing body differs
// from the client's only in model's value, byte for byte; its Content-Length
// equals the rewritten body's length, for a rewrite that shortens the name and
// one that lengthens it; the engine answers without waiting for bytes;
// unknown request fields reach the engine with their values; prompt token
// counts match a direct call to the same engine, so no text was injected.
//
// The expected body is built from the same pieces as the client's, around the
// model literal, never by replacing text in the client's body (a replace would
// also change the nested member below) and never by parsing and writing JSON
// again (that would change the bytes the test is about).

// rwBS is a backslash, so that this file holds no JSON escape the editor
// could decode.
const rwBS = "\\"

// rwPieces returns the client body's bytes before and after the top-level
// model value. It holds what a parser that writes JSON again would change:
// spaces around the colons, keys out of order, numbers as written (1.50, 1e3),
// characters an encoder escapes (<, >, &), a JSON escape (the e-acute below),
// an unknown member with nested values, and a nested "model" member with the
// client's name, which only the top-level one may change.
func rwPieces(clientModel string) (pre, post string) {
	pre = "{ \"messages\" : [ {\"role\":\"user\",\"content\":\"caf" + rwBS + "u00e9 <b>&amp; hello there\"} ],\n" +
		" \"temperature\":1.50, \"top_p\":1e3,\n" +
		" \"metadata\":{\"model\":\"" + clientModel + "\",\"note\":\"x\"},\n" +
		" \"x_unknown\":{\"b\":2,\"a\":[1.0,null,true,\"s\"]},\n" +
		" \"model\" : "
	post = " , \"stream\":false,\n \"max_tokens\" : 7 }"
	return pre, post
}

// rwBody returns the client's body and the body expected at the target.
func rwBody(t *testing.T, clientModel, served string) (client, want string) {
	t.Helper()
	pre, post := rwPieces(clientModel)
	client = pre + `"` + clientModel + `"` + post
	want = pre + `"` + served + `"` + post
	if !strings.Contains(client, rwBS+"u00e9") {
		t.Fatal("the body lost its JSON escape; the test would prove nothing")
	}
	if client == want {
		t.Fatal("the rewrite changes nothing; the test would prove nothing")
	}
	return client, want
}

// rwSend posts body with the given bearer key, within five seconds, which is
// how a request is seen to wait for bytes that do not come. It returns the
// status, the body and the time taken.
func rwSend(t *testing.T, url, key string, body io.Reader) (int, string, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := ctHTTP().Do(req)
	if err != nil {
		t.Fatalf("the request failed after %v: %v", time.Since(start), err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the answer after %v: %v", time.Since(start), err)
	}
	return resp.StatusCode, string(b), time.Since(start)
}

// rwUsage reads usage.prompt_tokens of a chat answer.
func rwUsage(t *testing.T, answer string) int64 {
	t.Helper()
	var v struct {
		Usage struct {
			Prompt int64 `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(answer), &v); err != nil {
		t.Fatalf("the answer %q is not JSON: %v", answer, err)
	}
	return v.Usage.Prompt
}

// rwCheckUnknownFields decodes a body only to compare values with the literals
// the test wrote: the unknown member and the numbers.
func rwCheckUnknownFields(t *testing.T, where string, body []byte) {
	t.Helper()
	var v struct {
		Unknown     map[string]any `json:"x_unknown"`
		Temperature json.Number    `json:"temperature"`
		TopP        json.Number    `json:"top_p"`
		Metadata    struct {
			Model string `json:"model"`
		} `json:"metadata"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("%s: the body is not JSON: %v", where, err)
	}
	want := map[string]any{"b": json.Number("2"), "a": []any{json.Number("1.0"), nil, true, "s"}}
	if !reflect.DeepEqual(v.Unknown, want) {
		t.Errorf("%s: x_unknown = %#v, want %#v", where, v.Unknown, want)
	}
	if v.Temperature.String() != "1.50" || v.TopP.String() != "1e3" {
		t.Errorf("%s: temperature %s, top_p %s; want 1.50 and 1e3 as written", where, v.Temperature, v.TopP)
	}
}

type rwEngine struct {
	*harness
	e *enginetest.Engine
}

// newRwEngine builds a front with one Instance of "example-27b" whose deploy
// ID is 64 times "ab", so the pinned name's display form is "abababababab".
func newRwEngine(t *testing.T) *rwEngine {
	t.Helper()
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{deployID: strings.Repeat("ab", 32)})
	h.apply()
	return &rwEngine{harness: h, e: e}
}

func (r *rwEngine) only(t *testing.T) enginetest.Request {
	t.Helper()
	reqs := r.e.Requests()
	if len(reqs) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(reqs))
	}
	return reqs[0]
}

// rwCheckFraming checks the rewritten body's bytes and its framing at the target:
// a Content-Length header of exactly the new body's length, no chunking.
func rwCheckFraming(t *testing.T, where string, gotBody []byte, header http.Header, contentLength int64, want string) {
	t.Helper()
	if !bytes.Equal(gotBody, []byte(want)) {
		t.Errorf("%s: the body differs from the client's in more than model's value:\n got %q\nwant %q", where, gotBody, want)
	}
	if got := header.Get("Content-Length"); got != strconv.Itoa(len(want)) {
		t.Errorf("%s: Content-Length %q, want %d", where, got, len(want))
	}
	if contentLength >= 0 && contentLength != int64(len(want)) {
		t.Errorf("%s: the server read a length of %d, want %d", where, contentLength, len(want))
	}
	if te := header.Get("Transfer-Encoding"); te != "" {
		t.Errorf("%s: Transfer-Encoding %q on a body the front frames itself", where, te)
	}
}

// TestRewriteShortensTheNameAndChangesNothingElse: a pinned name
// m@provider/<12 hex> becomes the served name m, which is 24 bytes shorter.
func TestRewriteShortensTheNameAndChangesNothingElse(t *testing.T) {
	r := newRwEngine(t)
	client, want := rwBody(t, "example-27b@helios-a/abababababab", "example-27b")
	if len(want) >= len(client) {
		t.Fatal("this case must shorten the body")
	}
	status, answer, took := rwSend(t, r.url+"/v1/chat/completions", r.clientKey, strings.NewReader(client))
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, answer)
	}
	got := r.only(t)
	rwCheckFraming(t, "engine", got.Body, got.Header, got.ContentLength, want)
	rwCheckUnknownFields(t, "engine", got.Body)
	if took > 3*time.Second {
		t.Errorf("the engine took %v to answer: it waited for bytes", took)
	}
}

// TestRewriteWithAChunkedClientBody: a client that sends its body chunked
// (length unknown) gets it framed by the front with a Content-Length.
func TestRewriteWithAChunkedClientBody(t *testing.T) {
	r := newRwEngine(t)
	client, want := rwBody(t, "example-27b@helios-a/abababababab", "example-27b")
	status, answer, _ := rwSend(t, r.url+"/v1/chat/completions", r.clientKey, io.NopCloser(strings.NewReader(client)))
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, answer)
	}
	got := r.only(t)
	rwCheckFraming(t, "engine", got.Body, got.Header, got.ContentLength, want)
}

// TestRewritePromptCountsMatchADirectCall: the same request through the front
// and straight to the same engine is counted alike, so the front added no
// text. The expected count is the words of the content literal.
func TestRewritePromptCountsMatchADirectCall(t *testing.T) {
	r := newRwEngine(t)
	client, direct := rwBody(t, "example-27b@helios-a/abababababab", "example-27b")
	// The content literal: "caf<e-acute> <b>&amp; hello there", four words.
	const words = 4
	_, viaFront, _ := rwSend(t, r.url+"/v1/chat/completions", r.clientKey, strings.NewReader(client))
	var engineKey string
	if reqs := r.e.Requests(); len(reqs) == 1 {
		engineKey = strings.TrimPrefix(reqs[0].Header.Get("Authorization"), "Bearer ")
	}
	if engineKey == "" {
		t.Fatal("the engine got no Instance key")
	}
	status, viaEngine, _ := rwSend(t, "http://"+r.e.Addr()+"/v1/chat/completions", engineKey, strings.NewReader(direct))
	if status != http.StatusOK {
		t.Fatalf("the direct call: status %d: %s", status, viaEngine)
	}
	if f, d := rwUsage(t, viaFront), rwUsage(t, viaEngine); f != d || f != words {
		t.Errorf("prompt tokens through the front %d, direct %d, want both %d", f, d, words)
	}
}

// rwManaged is a front with a managed Provider on an LLMLab double, and a
// managed-source ModelDeployment of "short" for a catalog entry whose name is
// longer than the model name.
type rwManaged struct {
	*harness
	llm *llmlabtest.Server
}

const (
	rwUpstreamKey = "llmlab-upstream-key-for-rewrite-tests-91c2"
	rwCatalog     = "llmlab-catalog-name-that-is-much-longer-than-short"
)

func newRwManaged(t *testing.T) *rwManaged {
	t.Helper()
	h := newHarness(t, nil)
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{rwUpstreamKey},
		Models: []llmlabtest.Model{{ID: rwCatalog}}})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), rwUpstreamKey, rwCatalog)
	h.managedMD("md-managed", "short", "llmlab", rwCatalog, "llmlab-a")
	h.apply()
	return &rwManaged{harness: h, llm: llm}
}

// only waits for the double's one logged request: it logs after the answer
// ends, so the client can be ahead of it.
func (m *rwManaged) only(t *testing.T) llmlabtest.Request {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reqs := m.llm.Requests(); len(reqs) == 1 {
			return reqs[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the LLMLab double logged %d requests, want 1", len(m.llm.Requests()))
	return llmlabtest.Request{}
}

// TestRewriteLengthensTheNameToTheCatalogNameAndChangesNothingElse: the
// catalog name is 46 bytes longer than the model name, so the body grows; the
// Content-Length must follow, and the double (which reads exactly that many
// bytes) must still answer at once.
func TestRewriteLengthensTheNameToTheCatalogNameAndChangesNothingElse(t *testing.T) {
	m := newRwManaged(t)
	client, want := rwBody(t, "short", rwCatalog)
	if len(want) <= len(client) {
		t.Fatal("this case must lengthen the body")
	}
	status, answer, took := rwSend(t, m.url+"/v1/chat/completions", m.clientKey, strings.NewReader(client))
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, answer)
	}
	got := m.only(t)
	rwCheckFraming(t, "LLMLab", got.Body, got.Header, -1, want) // the double records no parsed length
	rwCheckUnknownFields(t, "LLMLab", got.Body)
	if took > 3*time.Second {
		t.Errorf("LLMLab took %v to answer: it waited for bytes", took)
	}
	if got.Model != rwCatalog {
		t.Errorf("LLMLab's request named model %q, want %q", got.Model, rwCatalog)
	}
	if got.Header.Get("X-Session-Id") != "" {
		t.Errorf("the front added x-session-id %q toward a managed provider (HBEP-5, The LLMLab route)", got.Header.Get("X-Session-Id"))
	}
}

// TestRewriteLLMLabPromptCountsMatchADirectCall: the same request through the
// front and straight to LLMLab's double is counted alike.
func TestRewriteLLMLabPromptCountsMatchADirectCall(t *testing.T) {
	m := newRwManaged(t)
	client, direct := rwBody(t, "short", rwCatalog)
	const words = 4
	_, viaFront, _ := rwSend(t, m.url+"/v1/chat/completions", m.clientKey, strings.NewReader(client))
	status, viaDouble, _ := rwSend(t, m.llm.BaseURL()+"/chat/completions", rwUpstreamKey, strings.NewReader(direct))
	if status != http.StatusOK {
		t.Fatalf("the direct call: status %d: %s", status, viaDouble)
	}
	if f, d := rwUsage(t, viaFront), rwUsage(t, viaDouble); f != d || f != words {
		t.Errorf("prompt tokens through the front %d, direct %d, want both %d", f, d, words)
	}
}
