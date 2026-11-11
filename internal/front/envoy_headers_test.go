package front

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Request headers: toward an Instance the front drops the client's
// x-envoy-* headers and X-Forwarded-For, because Envoy obeys x-envoy-* control
// headers on a request it deems internal (retries, an upstream time-out), and
// deems one internal when it comes from loopback, as frpc's do, or carries one
// private X-Forwarded-For address. Other headers pass unchanged, and what goes
// toward LLMLab is unchanged. Test plan, Beta, the router, "Headers": a
// client's x-envoy-* and X-Forwarded-For headers don't reach the router, and a
// worker refuses a request without the Instance key (the second half is
// TestKeysEngineRefusesARequestWithoutItsInstanceKey, keys_test.go).
//
// Every expected header below is a literal. None is built by the helper the
// front uses, and the engine double records what it was sent: a positive
// control posts the same headers to it directly, so a double that dropped them
// itself would show.

// ehStripped returns the client's headers the front removes toward an Instance,
// written as they go on the wire: the Go client sends map keys as written.
// (The front's own server reads them canonicalized, so a case-sensitive match
// against the canonical prefix cannot be told from the right one end to end;
// TestEnvoyHeadersStripMatchesNamesInAnyCase covers it.)
func ehStripped() http.Header {
	return http.Header{
		"X-Envoy-Upstream-Rq-Timeout-Ms": {"1"},
		"x-envoy-retry-on":               {"5xx,reset"},
		"X-ENVOY-ORIGINAL-PATH":          {"/admin"},
		// The values are private (RFC 1918) addresses on purpose: Envoy deems
		// a request internal when X-Forwarded-For holds a private address,
		// so a private value is what makes this request model the attack.
		"X-Forwarded-For": {"10.1.2.3", "192.168.7.7"},
	}
}

// ehKept returns the client's other headers, which HBEP-5 passes unchanged. Two of
// them sit next to the removed names: X-Forwarded-Proto is not X-Forwarded-For,
// and X-Envoyish does not start with x-envoy-.
func ehKept() http.Header {
	return http.Header{
		"Content-Type":      {"application/json"},
		"User-Agent":        {"eh-test/1"},
		"X-Client-Trace":    {"t-1"},
		"X-Forwarded-Proto": {"https"},
		"X-Envoyish":        {"kept"},
	}
}

// ehPost posts body to the front's chat completions with the client key and
// the given headers, set as written in the map.
func ehPost(t *testing.T, h *harness, body string, hdr ...http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	for _, set := range hdr {
		for k, vs := range set {
			req.Header[k] = vs
		}
	}
	resp, err := ctHTTP().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// ehWant fails the test unless got has the header with exactly these values.
func ehWant(t *testing.T, where string, got http.Header, name string, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got.Values(name), want) {
		t.Errorf("%s: %s = %q, want %q", where, name, got.Values(name), want)
	}
}

// TestEnvoyHeadersFrontRemovesClientEnvoyAndForwardedForTowardAnInstance: a
// request carrying X-Envoy-Upstream-Rq-Timeout-Ms, x-envoy-retry-on, an
// upper-case X-ENVOY-ORIGINAL-PATH and two X-Forwarded-For values reaches the
// engine with none of them, and with every other header the client sent.
func TestEnvoyHeadersFrontRemovesClientEnvoyAndForwardedForTowardAnInstance(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	const body = `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`

	resp := ehPost(t, f.harness, body, ehStripped(), ehKept())
	if got := readAll(t, resp); resp.StatusCode != http.StatusOK {
		t.Fatalf("through the front: status %d: %s", resp.StatusCode, got)
	}
	via := f.e.Requests()
	if len(via) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(via))
	}
	got := via[0].Header

	for _, name := range []string{"X-Envoy-Upstream-Rq-Timeout-Ms", "X-Envoy-Retry-On", "X-Envoy-Original-Path", "X-Forwarded-For"} {
		ehWant(t, "through the front", got, name, nil)
	}
	for k := range got { // and no other x-envoy-* name, whatever its case
		if l := strings.ToLower(k); strings.HasPrefix(l, "x-envoy-") || l == "x-forwarded-for" {
			t.Errorf("through the front: the engine received %s: %q", k, got[k])
		}
	}
	ehWant(t, "through the front", got, "Content-Type", []string{"application/json"})
	ehWant(t, "through the front", got, "User-Agent", []string{"eh-test/1"})
	ehWant(t, "through the front", got, "X-Client-Trace", []string{"t-1"})
	ehWant(t, "through the front", got, "X-Forwarded-Proto", []string{"https"})
	ehWant(t, "through the front", got, "X-Envoyish", []string{"kept"})
	// What the front sets toward an Instance is still there: its key, not
	// the client's, and the session key.
	auth := got.Get("Authorization")
	instanceKey := strings.TrimPrefix(auth, "Bearer ")
	if instanceKey == "" || instanceKey == auth || strings.Contains(auth, f.clientKey) {
		t.Fatalf("the front sent Authorization %q to the engine, want Bearer <Instance key>", auth)
	}
	if got.Get("X-Session-Id") == "" {
		t.Errorf("through the front: the engine received no X-Session-Id")
	}

	// The positive control: the same headers sent to the engine directly,
	// with its key, are all recorded, so the engine double does not drop them.
	req, err := http.NewRequest(http.MethodPost, "http://"+f.e.Addr()+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+instanceKey)
	for _, set := range []http.Header{ehStripped(), ehKept()} {
		for k, vs := range set {
			req.Header[k] = vs
		}
	}
	dresp, err := ctHTTP().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if b := readAll(t, dresp); dresp.StatusCode != http.StatusOK {
		t.Fatalf("the engine, called directly: status %d: %s", dresp.StatusCode, b)
	}
	direct := f.e.Requests()
	if len(direct) != 2 {
		t.Fatalf("the engine saw %d requests, want 2", len(direct))
	}
	d := direct[1].Header
	ehWant(t, "directly", d, "X-Envoy-Upstream-Rq-Timeout-Ms", []string{"1"})
	ehWant(t, "directly", d, "X-Envoy-Retry-On", []string{"5xx,reset"})
	ehWant(t, "directly", d, "X-Envoy-Original-Path", []string{"/admin"})
	ehWant(t, "directly", d, "X-Forwarded-For", []string{"10.1.2.3", "192.168.7.7"})
}

// TestEnvoyHeadersStripMatchesNamesInAnyCase: the helper removes a name in any
// case and keeps the others, on a map whose keys are not canonical, as a key
// set directly (not read by net/http) can be. The expected map is a literal.
func TestEnvoyHeadersStripMatchesNamesInAnyCase(t *testing.T) {
	h := http.Header{
		"x-envoy-retry-on":               {"5xx"},
		"X-ENVOY-ORIGINAL-PATH":          {"/admin"},
		"X-Envoy-Upstream-Rq-Timeout-Ms": {"1", "2"},
		"x-forwarded-for":                {"10.0.0.1"},
		"X-FORWARDED-FOR":                {"10.0.0.2"},
		"X-Forwarded-For":                {"10.0.0.3"},
		"x-envoy-":                       {"nothing after the dash"},
		"x-envoy":                        {"no dash: kept"},
		"X-Envoyish":                     {"kept"},
		"X-Forwarded-Proto":              {"https"},
		"x-client-trace":                 {"t-1"},
		"Content-Type":                   {"application/json"},
	}
	stripEnvoyControl(h)
	want := http.Header{
		"x-envoy":           {"no dash: kept"},
		"X-Envoyish":        {"kept"},
		"X-Forwarded-Proto": {"https"},
		"x-client-trace":    {"t-1"},
		"Content-Type":      {"application/json"},
	}
	if !reflect.DeepEqual(h, want) {
		t.Errorf("after the removal the headers are %v, want %v", h, want)
	}
}

// TestEnvoyHeadersLLMLabPathStillForwardsThem: toward LLMLab nothing changes,
// so the client's x-envoy-* and X-Forwarded-For headers reach it as before
// (HBEP-5, Request headers: the removal is "toward an Instance").
func TestEnvoyHeadersLLMLabPathStillForwardsThem(t *testing.T) {
	m := newErrManagedFix(t, nil)
	resp := ehPost(t, m.harness, errChatBody(efModel, false, "hello there"), ehStripped(), ehKept())
	if got := readAll(t, resp); resp.StatusCode != http.StatusOK {
		t.Fatalf("through the front to LLMLab: status %d: %s", resp.StatusCode, got)
	}
	reqs := kyLogged(t, m.llm, 1)
	if len(reqs) != 1 {
		t.Fatalf("LLMLab saw %d requests, want 1", len(reqs))
	}
	got := reqs[0].Header
	ehWant(t, "toward LLMLab", got, "X-Envoy-Upstream-Rq-Timeout-Ms", []string{"1"})
	ehWant(t, "toward LLMLab", got, "X-Envoy-Retry-On", []string{"5xx,reset"})
	ehWant(t, "toward LLMLab", got, "X-Envoy-Original-Path", []string{"/admin"})
	ehWant(t, "toward LLMLab", got, "X-Forwarded-For", []string{"10.1.2.3", "192.168.7.7"})
	ehWant(t, "toward LLMLab", got, "X-Client-Trace", []string{"t-1"})
	ehWant(t, "toward LLMLab", got, "X-Forwarded-Proto", []string{"https"})
	ehWant(t, "toward LLMLab", got, "X-Envoyish", []string{"kept"})
	if a := got.Get("Authorization"); a != "Bearer "+efUpstreamKey {
		t.Errorf("toward LLMLab: Authorization %q, want the Provider's key", a)
	}
}
