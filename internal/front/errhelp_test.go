package front

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The helpers of the Errors and Attribution tests. Header names, codes and
// details are written as literals from HBEP-5 (Errors, Attribution headers),
// not taken from the front's constants, so a misspelt constant cannot pass its
// own test.

const (
	// efModel is the name clients send; the ModelDeployment's spec.model.
	efModel = "example-27b"
	// efDeployID is an Instance's full deployment ID; its display form is the
	// first 12 hex characters (HBEP-3, Deployment ID).
	efDeployID      = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	efDeployDisplay = "0123456789ab"
	// efManagedID is a catalog entry's managed ID, likewise.
	efManagedID      = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	efManagedDisplay = "fedcba987654"
	// efCatalog is the name LLMLab lists for the managed entry.
	efCatalog = "llmlab-catalog-27b"
)

// errFix is a front with one batch Provider, one ModelDeployment of
// efModel and one ready Instance on an engine double.
type errFix struct {
	*harness
	e   *enginetest.Engine
	uid types.UID
	key string // the Instance key
}

// newErrFix builds the standard fixture. edit changes the configuration; o
// the Instance's settings (its deploy ID is fixed). profileWindow, when set, is
// the profile's drainWindow.
func newErrFix(t *testing.T, edit func(*Config), o instanceOpts, profileWindow string) *errFix {
	t.Helper()
	return newErrFixClock(t, edit, o, profileWindow, false)
}

// newErrFixClock is newErrFix; realEngineClock gives the engine double the
// real clock, so its pace (PerToken) is real time and does not move the
// front's fake clock: the test then moves the fake clock itself, at a moment
// it chose.
func newErrFixClock(t *testing.T, edit func(*Config), o instanceOpts, profileWindow string, realEngineClock bool) *errFix {
	t.Helper()
	h := newHarness(t, edit)
	key := newKey(t)
	eo := enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk}
	if realEngineClock {
		eo.Clock = nil
	}
	e := enginetest.Start(t, eo)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, profileWindow, "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", efModel, 1, "helios-a")
	o.deployID = efDeployID
	uid := h.instance("md", "md-0", "helios-a", e, key, o)
	h.apply()
	return &errFix{harness: h, e: e, uid: uid, key: key}
}

// errChatBody is a chat request for the model with n words of content; stream
// adds "stream": true.
func errChatBody(model string, stream bool, content string) string {
	b, _ := json.Marshal(map[string]any{"model": model, "stream": stream,
		"messages": []map[string]string{{"role": "user", "content": content}}})
	return string(b)
}

// errClient never reuses a connection. The tests' servers come and go on the
// same few ports, and a pooled connection to a server that has just been
// closed can fail a POST on its first use, which Go does not send again
// because a POST is not replayable. The harness's own post uses
// http.DefaultClient, which pools; the fixtures below shadow it with errPost.
var errClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

// errPost is the harness's post on errClient.
func (h *harness) errPost(path, body string, hdr ...string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := errClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// post shadows the harness's post for the fixtures of these tests.
func (s *errFix) post(path, body string, hdr ...string) *http.Response {
	s.t.Helper()
	return s.harness.errPost(path, body, hdr...)
}

// post shadows the harness's post for the managed fixture.
func (m *errManagedFix) post(path, body string, hdr ...string) *http.Response {
	m.t.Helper()
	return m.harness.errPost(path, body, hdr...)
}

// errPostRaw sends a POST with only the given headers (no client key unless the
// caller adds one).
func (h *harness) errPostRaw(path, body string, hdr map[string]string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := errClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// errGet sends a GET with the client key.
func (h *harness) errGet(path string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, h.url+path, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	resp, err := errClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// wantError is a row of HBEP-5's Errors table.
type wantError struct {
	status     int
	code       string
	detail     string // "" means the body's detail is null
	retryAfter string // "" means no Retry-After header
}

// checkError asserts the status, the Retry-After header and the body of an
// error the front raised: exactly the five members, type hpcburst_error, the
// row's code and detail.
func checkError(t *testing.T, resp *http.Response, body string, want wantError) {
	t.Helper()
	if resp.StatusCode != want.status {
		t.Errorf("status = %d, want %d (body %s)", resp.StatusCode, want.status, body)
	}
	if got := resp.Header.Values("Retry-After"); want.retryAfter == "" && len(got) != 0 {
		t.Errorf("Retry-After = %q, want none", got)
	} else if want.retryAfter != "" && (len(got) != 1 || got[0] != want.retryAfter) {
		t.Errorf("Retry-After = %q, want %q", got, want.retryAfter)
	}
	checkErrorBody(t, body, want.code, want.detail)
}

// checkErrorBody asserts the body of an error the front raised: exactly the
// five members, type hpcburst_error, the code and the detail (null when "").
func checkErrorBody(t *testing.T, body, code, detail string) {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &top); err != nil || len(top) != 1 || top["error"] == nil {
		t.Fatalf("body %q is not {\"error\": {...}}: %v", body, err)
	}
	var obj map[string]any
	if err := json.Unmarshal(top["error"], &obj); err != nil {
		t.Fatalf("error member of %q: %v", body, err)
	}
	for _, k := range []string{"message", "type", "code", "param", "detail"} {
		if _, ok := obj[k]; !ok {
			t.Errorf("error body has no %q member: %s", k, body)
		}
	}
	if len(obj) != 5 {
		t.Errorf("error body has %d members, want 5: %s", len(obj), body)
	}
	if msg, _ := obj["message"].(string); msg == "" {
		t.Errorf("message is empty: %s", body)
	}
	if obj["type"] != "hpcburst_error" {
		t.Errorf("type = %v, want hpcburst_error", obj["type"])
	}
	if obj["code"] != code {
		t.Errorf("code = %v, want %q", obj["code"], code)
	}
	if detail == "" {
		if obj["detail"] != nil {
			t.Errorf("detail = %v, want null", obj["detail"])
		}
	} else if obj["detail"] != detail {
		t.Errorf("detail = %v, want %q", obj["detail"], detail)
	}
}

// wantAttr is the attribution a response should carry.
type wantAttr struct {
	emitter  string
	provider string // "" means the header is absent
	deployID string // display form
	instance string // namespace/name
	uid      string
}

// checkAttribution asserts the five headers other than the request ID: each
// is present exactly once with the value, or absent when want has none. It
// returns the request ID, which is checked against the usage record by the
// caller (it has no value to compare to until then).
func checkAttribution(t *testing.T, resp *http.Response, want wantAttr) string {
	t.Helper()
	for header, v := range map[string]string{
		"hpcburst-emitter": want.emitter, "hpcburst-provider": want.provider, "hpcburst-deployment-id": want.deployID,
		"hpcburst-instance": want.instance, "hpcburst-instance-uid": want.uid,
	} {
		got := resp.Header.Values(header)
		switch {
		case v == "" && len(got) != 0:
			t.Errorf("%s = %q, want the header absent", header, got)
		case v != "" && (len(got) != 1 || got[0] != v):
			t.Errorf("%s = %q, want %q", header, got, v)
		}
	}
	ids := resp.Header.Values("hpcburst-request-id")
	if len(ids) != 1 || ids[0] == "" {
		t.Fatalf("hpcburst-request-id = %q, want exactly one value", ids)
	}
	return ids[0]
}

// efFrontOnly is the attribution of an error raised before a target is chosen:
// the request ID and emitter front, and no target headers.
var efFrontOnly = wantAttr{emitter: "front"}

// instanceAttr is the attribution of a response from the standard fixture's
// Instance, with the given emitter.
func (s *errFix) instanceAttr(emitter string) wantAttr {
	return wantAttr{emitter: emitter, provider: "helios-a", deployID: efDeployDisplay, instance: "ns/md-0", uid: string(s.uid)}
}

// readEvent reads one SSE event (lines up to a blank line) from br.
func readEvent(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	var ev strings.Builder
	for {
		line, err := br.ReadString('\n')
		ev.WriteString(line)
		if err != nil {
			t.Fatalf("reading an event: %v (got %q)", err, ev.String())
		}
		if strings.TrimRight(line, "\r\n") == "" {
			return ev.String()
		}
	}
}

// checkFrontError asserts an error the front raised before it chose a target:
// the row's status, code, detail and Retry-After, only the request ID and
// emitter front in the headers, and a usage record under that request ID with
// the same status and emitter front. The record is what gives the request ID
// a value to check: the ID is the front's own, so it cannot be known before.
func (h *harness) checkFrontError(t *testing.T, resp *http.Response, body string, want wantError) {
	t.Helper()
	checkError(t, resp, body, want)
	id := checkAttribution(t, resp, efFrontOnly)
	rec := h.record(id)
	if rec.Status != want.status || rec.Emitter != "front" {
		t.Errorf("usage record under %s: status %d emitter %q, want %d front", id, rec.Status, rec.Emitter, want.status)
	}
}

// errWaitFor polls cond until it holds or five seconds pass.
func errWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// errManagedFix is a front with a managed Provider backed by an LLMLab
// double, and a managed-source ModelDeployment of efModel for its entry.
type errManagedFix struct {
	*harness
	llm *llmlabtest.Server
	key string // the upstream key
}

const efUpstreamKey = "llmlab-upstream-key-for-tests-7d41"

func newErrManagedFix(t *testing.T, edit func(*Config)) *errManagedFix {
	t.Helper()
	h := newHarness(t, edit)
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{efUpstreamKey},
		Models: []llmlabtest.Model{{ID: efCatalog}}})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), efUpstreamKey, efCatalog)
	// A distinctive managed ID: the harness's default has display form 000000000000.
	h.snap.Providers[len(h.snap.Providers)-1].Status.Catalog[0].ManagedID = v1alpha1.FullDeploymentID(efManagedID)
	h.managedMD("md-managed", efModel, "llmlab", efCatalog, "llmlab-a")
	h.apply()
	return &errManagedFix{harness: h, llm: llm, key: efUpstreamKey}
}

// efManagedAttr is the attribution of a response from the managed entry.
var efManagedAttr = wantAttr{emitter: "provider", provider: "llmlab-a", deployID: efManagedDisplay}

// direct posts body to url with the given bearer key, as a client of the
// target would, and returns the status and body.
func errDirect(t *testing.T, url, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}
