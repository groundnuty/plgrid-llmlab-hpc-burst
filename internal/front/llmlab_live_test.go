package front

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// A real LLMLab behind the front (issue #64). Every other test of the front
// answers a managed target from an LLMLab double (internal/managed/llmlabtest),
// which was built from LLMLab's documentation. This one sends a handful of
// requests through the front to the real gateway, with a managed
// ModelDeployment, to see that the double and the gateway agree on what the
// front depends on: the status, the usage member that fills the usage record,
// the finish reason and [DONE] of a stream.
//
// It spends grant credits, so it is a smoke test and nothing more: two chat
// requests of at most llMaxTokens tokens reach the gateway, plus one read of
// its model list (the read the Provider's controller makes every minute). The
// other three requests are answered by the front itself. It runs only when
// HPCBURST_LLMLAB_CREDENTIALS names an absolute path to a managed credentials
// file (pkg/provider.ManagedCredentials: key and baseURL, mode 600), and skips
// with a message otherwise. HPCBURST_LLMLAB_MODEL, when set, replaces the
// default model.
//
// Reading "the usage record of each request": only the two chat requests
// reach LLMLab, so only their records carry the managed Provider, the managed
// deployment ID and the engine's counts. The other three records are checked
// for the status and for emitter front, with no Provider.
//
// The unattached name (HBEP-5, Test plan, Keys: "plain model returns 503
// model_inactive, although LLMLab serves that model") is taken from the
// catalog read: a model LLMLab lists, other than the chat model, that no
// ModelDeployment of this test carries. The test fails when there is none, so
// the case cannot lapse unseen, and a model override cannot collide with it
// and send a third chat.
//
// What the redaction check is worth. The test searches every response header
// and body for the upstream key, and stops at once, with a fixed message, when
// it finds it. The real gateway does not echo the key, so this search cannot
// fire here: it is an absence check, and it would stay green with the front's
// redaction removed. The evidence that the front redacts is the
// positive-control tests, which make an upstream echo the key and read the
// client's bytes: TestRedactErrorBodyAndHeaders, TestRedactSSEErrorEvent and
// redact_all_test.go (PR #95). Here the check guards against the front
// forwarding a credential of its own making.
//
// What the output may hold. The output must stay safe if the front's
// redaction fails, so no diagnostic prints a string that came from a response
// or from the credentials file: only fixed texts, numbers, booleans, header
// names from a fixed list, and values checked against a closed set (llEnum).
const (
	llCredsEnv = "HPCBURST_LLMLAB_CREDENTIALS"
	llModelEnv = "HPCBURST_LLMLAB_MODEL"

	// llDefaultModel is a small instruct model that LLMLab lists.
	llDefaultModel = "Qwen/Qwen3-VL-8B-Instruct"
	// llPreferredUnattached is the unattached model taken when LLMLab lists it
	// and it is not the chat model; otherwise the first other listed model.
	llPreferredUnattached = "Qwen/Qwen3.6-27B"
	// llUnlistedModel is a name no list shows.
	llUnlistedModel = "Unlisted/Not-A-Model"

	llMaxTokens = 8
	llProvider  = "llm-a"
	llPlatform  = "llmlab"
	llMDName    = "md-llm"
	llTimeout   = 150 * time.Second
)

// llUnknownValue is what a diagnostic prints for a value outside its closed
// set, whatever the value holds.
const llUnknownValue = "(outside the known set)"

// The closed sets of values a diagnostic may print.
var (
	llFinishes = []string{"stop", "length", "tool_calls", "content_filter", "function_call"}
	llSources  = []string{"engine", "front_events"}
	llEmitters = []string{"front", "provider", "instance"}
)

// llEnum returns v when it is in set, else llUnknownValue.
func llEnum(v string, set []string) string {
	for _, s := range set {
		if v == s {
			return v
		}
	}
	return llUnknownValue
}

// llEnumPtr is llEnum for an optional string; nil prints as null.
func llEnumPtr(p *string, set []string) string {
	if p == nil {
		return "null"
	}
	return llEnum(*p, set)
}

// llCredsKind names why the credentials file could not be read, from the
// error's kind alone: ReadManagedCredentials puts the file's path in its text.
func llCredsKind(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "it does not exist"
	case errors.Is(err, fs.ErrPermission):
		return "permission denied"
	case errors.Is(err, provider.ErrCredentials):
		return "it is not a valid managed credentials file (its type, mode, owner, size or content)"
	}
	return "an unclassified read error"
}

// llReply is the part of a chat reply, whole or one stream chunk, that the
// test reads.
type llReply struct {
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
		Details    *struct {
			Cached *int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// llSeen is what the client received of one chat request.
type llSeen struct {
	finish             string // the first non-null finish_reason, as received
	prompt, completion int64  // from the usage member
	cached             *int64 // from prompt_tokens_details, when present
	haveUsage          bool
}

// observe folds one reply (or chunk) into s and fails on an error member.
func (s *llSeen) observe(t *testing.T, label string, payload []byte) {
	t.Helper()
	var r llReply
	if err := json.Unmarshal(payload, &r); err != nil {
		t.Errorf("%s: a reply is not JSON (%s)", label, jsonKind(err))
		return
	}
	if len(r.Error) != 0 && string(r.Error) != "null" {
		t.Errorf("%s: the reply carries an error member", label)
	}
	for _, c := range r.Choices {
		if c.FinishReason != nil && *c.FinishReason != "" && s.finish == "" {
			s.finish = *c.FinishReason
		}
	}
	if r.Usage != nil && r.Usage.Prompt != nil && r.Usage.Completion != nil {
		s.haveUsage, s.prompt, s.completion = true, *r.Usage.Prompt, *r.Usage.Completion
		if r.Usage.Details != nil {
			s.cached = r.Usage.Details.Cached
		}
	}
}

// jsonKind names a decoding error without the bytes it choked on.
func jsonKind(err error) string {
	var se *json.SyntaxError
	if errors.As(err, &se) {
		return "syntax error at byte " + strconv.FormatInt(se.Offset, 10)
	}
	return "type error"
}

// llLive is a front whose one managed Provider reaches the real gateway.
type llLive struct {
	*harness
	key        string
	model      string // the chat model, which the one ModelDeployment carries
	unattached string // a listed model no ModelDeployment carries
	managedID  string // the catalog entry's managed ID, full form
	checked    int    // responses searched for the key
	leaked     bool   // the key was found: no later request is sent
}

// step runs one step of the test as a subtest, unless the key has been found.
func (l *llLive) step(t *testing.T, name string, f func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		if l.leaked {
			t.Skip("the upstream key was found in an earlier response; no further request is sent")
		}
		f(t)
	})
}

// leak stops the step at once with a fixed message. It names neither the place
// the key was found nor what surrounds it.
func (l *llLive) leak(t *testing.T) {
	t.Helper()
	l.leaked = true
	t.Fatal("the upstream key is in a response of the front (a header or the body); stopping")
}

// llCall sends one request through the front, reads the whole answer and
// searches its headers and body for the upstream key before anything else
// looks at them.
func (l *llLive) llCall(t *testing.T, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), llTimeout)
	defer cancel()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, l.url+path, rd)
	if err != nil {
		t.Fatal("building the request failed")
	}
	req.Header.Set("Authorization", "Bearer "+l.clientKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	// The errors below come from the client's reading of the front's reply,
	// whose text can quote that reply, so only their kind is printed.
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatalf("no answer from the front (%s)", llTransportKind(ctx, err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the front's answer failed (%s)", llTransportKind(ctx, err))
	}
	l.checked++
	if llHoldsKey(l.key, resp.Header, data) {
		l.leak(t)
	}
	return resp, data
}

// llHoldsKey reports whether the key is in a header's name or value, or in the
// body.
func llHoldsKey(key string, h http.Header, body []byte) bool {
	found := bytes.Contains(body, []byte(key))
	for name, vs := range h {
		found = found || strings.Contains(name, key)
		for _, v := range vs {
			found = found || strings.Contains(v, key)
		}
	}
	return found
}

// llTransportKind names a client error by its kind.
func llTransportKind(ctx context.Context, err error) string {
	switch {
	case ctx.Err() != nil:
		return "no answer within " + llTimeout.String()
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "the connection closed early"
	}
	return "transport error, not shown"
}

// llAttribution asserts the attribution headers of a response from a target
// (emitter provider, the Provider and the deployment ID's display form) or of
// the front alone (providerName and deployID empty), and returns the request
// ID. A mismatch names the header, from a fixed list, and never a value.
func llAttribution(t *testing.T, resp *http.Response, emitter, providerName, deployID string) string {
	t.Helper()
	var bad []string
	for _, c := range []struct{ name, want string }{
		{"hpcburst-emitter", emitter}, {"hpcburst-provider", providerName}, {"hpcburst-deployment-id", deployID},
		{"hpcburst-instance", ""}, {"hpcburst-instance-uid", ""},
	} {
		got := resp.Header.Values(c.name)
		if (c.want == "" && len(got) != 0) || (c.want != "" && (len(got) != 1 || got[0] != c.want)) {
			bad = append(bad, c.name)
		}
	}
	if len(bad) > 0 {
		t.Errorf("attribution headers that differ from the target's: %s", strings.Join(bad, ", "))
	}
	ids := resp.Header.Values("hpcburst-request-id")
	if len(ids) != 1 || ids[0] == "" {
		t.Fatal("hpcburst-request-id is not exactly one non-empty value")
	}
	return ids[0]
}

// llFrontError asserts a front-raised error: the status, the error type, code
// and detail, Retry-After, and the front alone in the attribution. A mismatch
// prints the status and which members differ, never their values.
func llFrontError(t *testing.T, resp *http.Response, data []byte, status int, code, detail, retryAfter string) {
	t.Helper()
	var e struct {
		Error struct {
			Type   string  `json:"type"`
			Code   string  `json:"code"`
			Detail *string `json:"detail"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &e); err != nil {
		t.Errorf("status %d: the error body is not JSON (%s)", resp.StatusCode, jsonKind(err))
		return
	}
	got := ""
	if e.Error.Detail != nil {
		got = *e.Error.Detail
	}
	var bad []string
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"status", resp.StatusCode == status}, {"type", e.Error.Type == "hpcburst_error"}, {"code", e.Error.Code == code},
		{"detail", got == detail}, {"Retry-After", resp.Header.Get("Retry-After") == retryAfter},
	} {
		if !c.ok {
			bad = append(bad, c.name)
		}
	}
	if len(bad) > 0 {
		t.Errorf("status %d: the error differs from HBEP-5's Errors table in: %s", resp.StatusCode, strings.Join(bad, ", "))
	}
	llAttribution(t, resp, "front", "", "")
}

// llRouted asserts the attribution and the usage record of a chat request that
// reached LLMLab through the managed Provider, against what the client
// received. The expected counts are the client's own copy of the usage member,
// parsed here; the record's come from the front's judge.
func (l *llLive) llRouted(t *testing.T, label string, stream bool, resp *http.Response, seen llSeen) {
	t.Helper()
	id := llAttribution(t, resp, "provider", llProvider, l.managedID[:12])
	rec := l.record(id)
	var bad []string
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"emitter", rec.Emitter == "provider"}, {"provider", rec.Provider == llProvider}, {"modelDeployment", rec.ModelDeployment == "ns/"+llMDName},
		{"deploymentID", rec.DeploymentID == l.managedID}, {"instance", rec.Instance == "" && rec.InstanceUID == ""},
		{"status", rec.Status == 200}, {"stream", rec.Stream == stream}, {"requestedModel", rec.RequestedModel == l.model},
		{"level", rec.Level == "model"}, {"clientKeyID", rec.ClientKeyID == "tester"}, {"path", rec.Path == "/v1/chat/completions"},
		{"cutBy", rec.CutBy == nil},
	} {
		if !c.ok {
			bad = append(bad, c.name)
		}
	}
	if len(bad) > 0 {
		t.Errorf("%s: the record's %s differ from the managed target's", label, strings.Join(bad, ", "))
	}
	if !seen.haveUsage {
		t.Errorf("%s: LLMLab sent no usage member, so the engine's counts cannot be compared", label)
	} else {
		if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, seen.prompt) || !ptrIs(rec.CompletionTokens, seen.completion) {
			t.Errorf("%s: record counts source %s prompt %s completion %s, want engine %d %d (the client's usage member)",
				label, llEnumPtr(rec.CountsSource, llSources), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens), seen.prompt, seen.completion)
		}
		if seen.cached != nil && !ptrIs(rec.CachedTokens, *seen.cached) {
			t.Errorf("%s: record cached %s, want %d", label, ptrStr(rec.CachedTokens), *seen.cached)
		}
		if seen.prompt <= 0 || seen.completion <= 0 || seen.completion > llMaxTokens {
			t.Errorf("%s: LLMLab counts prompt %d completion %d, want both above 0 and completion at most %d", label, seen.prompt, seen.completion, llMaxTokens)
		}
	}
	if seen.finish == "" {
		t.Errorf("%s: the client received no finish reason", label)
	}
	if rec.Finish == nil || *rec.Finish != seen.finish {
		t.Errorf("%s: record finish %s differs from the client's %s", label, llEnumPtr(rec.Finish, llFinishes), llEnum(seen.finish, llFinishes))
	}
	t.Logf("%s: status %d emitter %s stream %v finish %s prompt %s cached %s completion %s countsSource %s",
		label, rec.Status, llEnum(rec.Emitter, llEmitters), rec.Stream, llEnumPtr(rec.Finish, llFinishes), ptrStr(rec.PromptTokens),
		ptrStr(rec.CachedTokens), ptrStr(rec.CompletionTokens), llEnumPtr(rec.CountsSource, llSources))
}

// llFrontRecord asserts the record of a request the front answered itself.
func (l *llLive) llFrontRecord(t *testing.T, label string, resp *http.Response, status int) {
	t.Helper()
	rec := l.record(resp.Header.Get("Hpcburst-Request-Id"))
	if rec.Status != status || rec.Emitter != "front" || rec.Provider != "" || rec.DeploymentID != "" {
		t.Errorf("%s: the record's status, emitter, Provider or deployment ID differs from %d, front and no target", label, status)
	}
	t.Logf("%s: status %d emitter %s", label, rec.Status, llEnum(rec.Emitter, llEmitters))
}

// llSetup reads the credentials, lists the real catalog once, and starts a
// front on the wall clock whose managed Provider points at that file in place.
func llSetup(t *testing.T) *llLive {
	t.Helper()
	path := os.Getenv(llCredsEnv)
	if path == "" {
		t.Skipf("set %s to the absolute path of a managed credentials file (key and baseURL, mode 600) to send a few real requests through the front to LLMLab", llCredsEnv)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must be an absolute path: a test runs in its package directory", llCredsEnv)
	}
	// ReadManagedCredentials puts the path in its errors, so only the kind of
	// failure is reported.
	creds, err := provider.ReadManagedCredentials(path)
	if err != nil {
		t.Fatalf("the credentials file named by %s could not be read: %s", llCredsEnv, llCredsKind(err))
	}
	model := os.Getenv(llModelEnv)
	if model == "" {
		model = llDefaultModel
	}

	// One read of the gateway's list, as the Provider's controller makes it:
	// the catalog status (and its managed IDs) are the product's own code over
	// what LLMLab really lists. The client's errors are fixed categories that
	// hold no byte the gateway sent (internal/managed, Client).
	client, err := managed.NewClient(creds, nil)
	if err != nil {
		t.Fatalf("the credentials file named by %s is not usable as managed credentials", llCredsEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), llTimeout)
	defer cancel()
	listed, err := client.ListCatalog(ctx)
	if err != nil {
		t.Fatalf("reading LLMLab's model list failed: %v", err)
	}
	merged, err := provider.MergeCatalog(nil, listed, llPlatform, time.Now())
	if err != nil {
		t.Fatal("building the catalog status from LLMLab's list failed")
	}
	var managedID, unattached string
	for _, e := range merged.Catalog {
		switch {
		case !e.Listed:
		case e.Name == model:
			managedID = string(e.ManagedID)
		case unattached == "" || e.Name == llPreferredUnattached:
			unattached = e.Name
		}
	}
	if managedID == "" {
		t.Fatalf("LLMLab does not list the chat model (%d models listed); set %s to a listed chat model", len(listed), llModelEnv)
	}
	if unattached == "" {
		t.Fatalf("LLMLab lists no model other than the chat model (%d listed), so the check that a listed model without a ModelDeployment gets 503 model_inactive cannot run", len(listed))
	}
	t.Logf("the model list: status 200, %d models; the chat model and an unattached model are listed", len(listed))

	h := newHarnessReal(t, nil)
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.snap.Providers = append(h.snap.Providers, v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: llProvider, Labels: map[string]string{v1alpha1.LabelPlatform: llPlatform, v1alpha1.LabelOrganization: "cyfronet"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "llmlab"},
			CredentialsRef: &v1alpha1.CredentialsReference{File: path}},
		Status: v1alpha1.ProviderStatus{Catalog: merged.Catalog},
	})
	h.managedMD(llMDName, model, llPlatform, model, llProvider)
	for i := range h.snap.Deployments {
		if h.snap.Deployments[i].Spec.Model == unattached {
			t.Fatal("a ModelDeployment carries the unattached model")
		}
	}
	h.apply()
	return &llLive{harness: h, key: creds.Key, model: model, unattached: unattached, managedID: managedID}
}

// TestLLMLabThroughTheFront is the smoke test described above.
func TestLLMLabThroughTheFront(t *testing.T) {
	l := llSetup(t)

	l.step(t, "a chat completion", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions", chatRequest(l.model, false, false))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", resp.StatusCode)
		}
		var seen llSeen
		seen.observe(t, "chat", data)
		l.llRouted(t, "chat", false, resp, seen)
	})

	l.step(t, "a streamed chat completion", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions", chatRequest(l.model, true, true))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", resp.StatusCode)
		}
		var seen llSeen
		var payloads []string
		for _, line := range strings.Split(string(data), "\n") {
			if p, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "data:"); ok {
				payloads = append(payloads, strings.TrimSpace(p))
			}
		}
		if n := len(payloads); n < 2 || payloads[n-1] != "[DONE]" {
			t.Fatalf("the stream has %d data events and does not end with [DONE]", len(payloads))
		}
		for _, p := range payloads[:len(payloads)-1] {
			if p == "[DONE]" {
				t.Errorf("[DONE] before the end of the stream")
				continue
			}
			seen.observe(t, "stream", []byte(p))
		}
		if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			t.Errorf("the Content-Type is not text/event-stream")
		}
		l.llRouted(t, "stream", true, resp, seen)
	})

	l.step(t, "GET /v1/models", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodGet, "/v1/models", "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", resp.StatusCode)
		}
		var list struct {
			Object string `json:"object"`
			Data   []struct {
				ID       string `json:"id"`
				Object   string `json:"object"`
				Routable *bool  `json:"routable"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &list); err != nil {
			t.Fatalf("the list is not JSON (%s)", jsonKind(err))
		}
		if len(list.Data) != 1 || list.Data[0].ID != l.model || list.Data[0].Object != "model" || list.Data[0].Routable == nil || !*list.Data[0].Routable {
			t.Errorf("the list holds %d models, want exactly the one ModelDeployment's, routable true", len(list.Data))
		}
		llAttribution(t, resp, "front", "", "")
		l.llFrontRecord(t, "models", resp, 200)
	})

	l.step(t, "a listed model the front has no ModelDeployment for", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions", chatRequest(l.unattached, false, false))
		llFrontError(t, resp, data, 503, "model_inactive", "not_attached", "30")
		l.llFrontRecord(t, "unattached", resp, 503)
	})

	l.step(t, "a retrieve of an unlisted name", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodGet, "/v1/models/"+url.PathEscape(llUnlistedModel), "")
		llFrontError(t, resp, data, 404, "model_not_found", "", "")
		l.llFrontRecord(t, "retrieve", resp, 404)
	})

	l.step(t, "the usage files", func(t *testing.T) {
		recs := l.records()
		if len(recs) != 5 {
			t.Errorf("the usage file holds %d records, want 5, one per request", len(recs))
		}
		files, err := os.ReadDir(l.cfg.UsageDir)
		if err != nil {
			t.Fatal("listing the usage directory failed")
		}
		for _, f := range files {
			b, err := os.ReadFile(filepath.Join(l.cfg.UsageDir, f.Name()))
			if err != nil {
				t.Fatal("reading a usage file failed")
			}
			if bytes.Contains(b, []byte(l.key)) {
				l.leak(t)
			}
		}
		t.Logf("responses searched for the key: %d; usage records: %d", l.checked, len(recs))
	})
}

// chatRequest is a chat request of at most llMaxTokens tokens.
// includeUsage adds stream_options.include_usage, without which a stream
// carries no counts (HBEP-5, Usage records).
func chatRequest(model string, stream, includeUsage bool) string {
	req := map[string]any{
		"model":       model,
		"max_tokens":  llMaxTokens,
		"temperature": 0,
		"stream":      stream,
		"messages":    []map[string]string{{"role": "user", "content": "Say hello in one word."}},
	}
	if includeUsage {
		req["stream_options"] = map[string]bool{"include_usage": true}
	}
	b, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// TestLLMLabLiveDiagnosticsAreSafe checks the live test's helpers without the
// network, so CI runs them: the credentials failure names its kind and never
// the path, the key search finds the key wherever it sits, and a value outside
// a closed set never prints. The positive controls: the library's own error
// does hold the path, and the search does fire.
func TestLLMLabLiveDiagnosticsAreSafe(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, mode os.FileMode, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, c := range []struct{ name, path, kind string }{
		{"a missing file", filepath.Join(dir, "no-such-file.json"), "it does not exist"},
		{"a file open to others", write("open.json", 0o644, `{"key":"k","baseURL":"https://x/v1"}`), "it is not a valid managed credentials file"},
		{"a file with no JSON", write("bad.json", 0o600, "not json"), "it is not a valid managed credentials file"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := provider.ReadManagedCredentials(c.path)
			if err == nil {
				t.Fatal("the file was accepted")
			}
			if !strings.Contains(err.Error(), c.path) {
				t.Fatal("the library's error no longer holds the path; the control for this test is gone")
			}
			got := llCredsKind(err)
			if strings.Contains(got, dir) || !strings.HasPrefix(got, c.kind) {
				t.Errorf("the kind %q must name %q and no path", got, c.kind)
			}
		})
	}

	const key = "llmlab-fake-key-for-helper-test-0001"
	body := []byte(`{"ok":true}`)
	for _, c := range []struct {
		name string
		h    http.Header
		body []byte
		want bool
	}{
		{"nowhere", http.Header{"X-A": {"v"}}, body, false},
		{"in the body", http.Header{}, []byte(`{"m":"` + key + `"}`), true},
		{"in a header value", http.Header{"X-A": {"Bearer " + key}}, body, true},
		{"in a header name", http.Header{"X-" + key: {"v"}}, body, true},
	} {
		if got := llHoldsKey(key, c.h, c.body); got != c.want {
			t.Errorf("key %s: llHoldsKey = %v, want %v", c.name, got, c.want)
		}
	}

	hostile := "sk-" + key
	if got := llEnum(hostile, llFinishes); got != llUnknownValue || strings.Contains(got, key) {
		t.Errorf("a value outside the set printed as %q", got)
	}
	if got := llEnum("length", llFinishes); got != "length" {
		t.Errorf("a known finish reason printed as %q", got)
	}
	if got := llEnumPtr(nil, llFinishes); got != "null" {
		t.Errorf("a nil value printed as %q", got)
	}
	if got := llEnumPtr(&hostile, llSources); got != llUnknownValue {
		t.Errorf("a value outside the set printed as %q", got)
	}
}
