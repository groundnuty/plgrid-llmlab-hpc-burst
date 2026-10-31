package front

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
// What the redaction check is worth. The test searches every response header
// and body for the upstream key. The real gateway does not echo the key, so
// this search cannot fire here: it is an absence check, and it would stay
// green with the front's redaction removed. The evidence that the front
// redacts is the positive-control tests, which make an upstream echo the key
// and read the client's bytes: TestRedactErrorBodyAndHeaders,
// TestRedactSSEErrorEvent and redact_all_test.go (PR #95). Here the check
// guards against the front forwarding a credential of its own making.
const (
	llCredsEnv = "HPCBURST_LLMLAB_CREDENTIALS"
	llModelEnv = "HPCBURST_LLMLAB_MODEL"

	// llDefaultModel is a small instruct model that LLMLab lists.
	llDefaultModel = "Qwen/Qwen3-VL-8B-Instruct"
	// llUnattachedModel is a model LLMLab lists and no ModelDeployment of this
	// test carries: HBEP-5, Test plan, Keys, "plain model returns 503
	// model_inactive, although LLMLab serves that model".
	llUnattachedModel = "Qwen/Qwen3.6-27B"
	// llUnlistedModel is a name no list shows.
	llUnlistedModel = "Unlisted/Not-A-Model"

	llMaxTokens = 8
	llProvider  = "llm-a"
	llPlatform  = "llmlab"
	llMDName    = "md-llm"
	llTimeout   = 150 * time.Second
)

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
	finish             string // the first non-null finish_reason
	prompt, completion int64  // from the usage member
	cached             *int64 // from prompt_tokens_details, when present
	haveUsage          bool
}

// observe folds one reply (or chunk) into s and fails on an error member.
func (s *llSeen) observe(t *testing.T, label string, payload []byte) {
	t.Helper()
	var r llReply
	if err := json.Unmarshal(payload, &r); err != nil {
		t.Errorf("%s: a reply is not JSON: %v", label, jsonKind(err))
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
	if se, ok := err.(*json.SyntaxError); ok {
		return "syntax error at byte " + strconv.FormatInt(se.Offset, 10)
	}
	return "type error"
}

// llLive is a front whose one managed Provider reaches the real gateway.
type llLive struct {
	*harness
	key       string
	managedID string // the catalog entry's managed ID, full form
	checked   int    // responses searched for the key
}

// llCall sends one request through the front, reads the whole answer and
// searches its headers and body for the upstream key. Failure messages never
// carry a body.
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
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+l.clientKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: no answer from the front: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: reading the answer: %v", method, path, err)
	}
	l.checked++
	if bytes.Contains(data, []byte(l.key)) {
		t.Errorf("%s %s: the body holds the upstream key", method, path)
	}
	for name, vs := range resp.Header {
		for _, v := range vs {
			if strings.Contains(v, l.key) || strings.Contains(name, l.key) {
				t.Errorf("%s %s: header %s holds the upstream key", method, path, name)
			}
		}
	}
	return resp, data
}

// llFrontError asserts a front-raised error: the status, the error code and
// detail, Retry-After, and the front as emitter. A mismatch prints the status
// and the codes, never the body.
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
	if resp.StatusCode != status || e.Error.Code != code || got != detail || e.Error.Type != "hpcburst_error" {
		t.Errorf("status %d type %q code %q detail %q, want %d hpcburst_error %q %q", resp.StatusCode, e.Error.Type, e.Error.Code, got, status, code, detail)
	}
	if ra := resp.Header.Get("Retry-After"); ra != retryAfter {
		t.Errorf("Retry-After = %q, want %q", ra, retryAfter)
	}
	checkAttribution(t, resp, efFrontOnly)
}

// llRouted asserts the attribution and the usage record of a chat request that
// reached LLMLab through the managed Provider, against what the client
// received. The expected counts are the client's own copy of the usage member,
// parsed here; the record's come from the front's judge.
func (l *llLive) llRouted(t *testing.T, label, model string, stream bool, resp *http.Response, seen llSeen) {
	t.Helper()
	id := checkAttribution(t, resp, wantAttr{emitter: "provider", provider: llProvider, deployID: l.managedID[:12]})
	rec := l.record(id)
	if rec.Emitter != "provider" || rec.Provider != llProvider || rec.ModelDeployment != "ns/"+llMDName ||
		rec.DeploymentID != l.managedID || rec.Instance != "" || rec.InstanceUID != "" {
		t.Errorf("%s: record names emitter %q provider %q deployment %q deploymentID %q instance %q, want the managed Provider and ID",
			label, rec.Emitter, rec.Provider, rec.ModelDeployment, rec.DeploymentID, rec.Instance)
	}
	if rec.Status != 200 || rec.Stream != stream || rec.RequestedModel != model || rec.Level != "model" || rec.ClientKeyID != "tester" || rec.Path != "/v1/chat/completions" {
		t.Errorf("%s: record status %d stream %v level %q path %q, want 200 stream %v level model", label, rec.Status, rec.Stream, rec.Level, rec.Path, stream)
	}
	if !seen.haveUsage {
		t.Errorf("%s: LLMLab sent no usage member, so the engine's counts cannot be compared", label)
	} else {
		if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, seen.prompt) || !ptrIs(rec.CompletionTokens, seen.completion) {
			t.Errorf("%s: record counts source %s prompt %s completion %s, want engine %d %d (the client's usage member)",
				label, strStr(rec.CountsSource), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens), seen.prompt, seen.completion)
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
		t.Errorf("%s: record finish %s, want %q, the client's", label, strStr(rec.Finish), seen.finish)
	}
	if rec.CutBy != nil {
		t.Errorf("%s: record cutBy %s, want null", label, *rec.CutBy)
	}
	t.Logf("%s: status %d emitter %s stream %v finish %s prompt %s cached %s completion %s countsSource %s",
		label, rec.Status, rec.Emitter, rec.Stream, strStr(rec.Finish), ptrStr(rec.PromptTokens), ptrStr(rec.CachedTokens), ptrStr(rec.CompletionTokens), strStr(rec.CountsSource))
}

// llFrontRecord asserts the record of a request the front answered itself.
func (l *llLive) llFrontRecord(t *testing.T, label string, resp *http.Response, status int) {
	t.Helper()
	rec := l.record(resp.Header.Get("Hpcburst-Request-Id"))
	if rec.Status != status || rec.Emitter != "front" || rec.Provider != "" || rec.DeploymentID != "" {
		t.Errorf("%s: record status %d emitter %q provider %q deploymentID %q, want %d, front and no target", label, rec.Status, rec.Emitter, rec.Provider, rec.DeploymentID, status)
	}
	t.Logf("%s: status %d emitter %s", label, rec.Status, rec.Emitter)
}

// llSetup reads the credentials, lists the real catalog once, and starts a
// front on the wall clock whose managed Provider points at that file in place.
func llSetup(t *testing.T) (*llLive, string) {
	t.Helper()
	path := os.Getenv(llCredsEnv)
	if path == "" {
		t.Skipf("set %s to the absolute path of a managed credentials file (key and baseURL, mode 600) to send a few real requests through the front to LLMLab", llCredsEnv)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must be an absolute path: a test runs in its package directory", llCredsEnv)
	}
	creds, err := provider.ReadManagedCredentials(path)
	if err != nil {
		t.Fatalf("%s: %v", llCredsEnv, err)
	}
	model := os.Getenv(llModelEnv)
	if model == "" {
		model = llDefaultModel
	}

	// One read of the gateway's list, as the Provider's controller makes it:
	// the catalog status (and its managed IDs) are the product's own code over
	// what LLMLab really lists.
	client, err := managed.NewClient(creds, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), llTimeout)
	defer cancel()
	listed, err := client.ListCatalog(ctx)
	if err != nil {
		t.Fatalf("reading LLMLab's model list: %v", err)
	}
	merged, err := provider.MergeCatalog(nil, listed, llPlatform, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var managedID string
	unattachedListed := false
	for _, e := range merged.Catalog {
		switch e.Name {
		case model:
			managedID = string(e.ManagedID)
		case llUnattachedModel:
			unattachedListed = true
		}
	}
	if managedID == "" {
		t.Fatalf("LLMLab does not list the model %q (%d models listed); set %s to a listed chat model", model, len(listed), llModelEnv)
	}
	t.Logf("the model list: status 200, %d models, %q listed, %q listed %v", len(listed), model, llUnattachedModel, unattachedListed)

	h := newHarnessReal(t, nil)
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.snap.Providers = append(h.snap.Providers, v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: llProvider, Labels: map[string]string{v1alpha1.LabelPlatform: llPlatform, v1alpha1.LabelOrganization: "cyfronet"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "llmlab"},
			CredentialsRef: &v1alpha1.CredentialsReference{File: path}},
		Status: v1alpha1.ProviderStatus{Catalog: merged.Catalog},
	})
	h.managedMD(llMDName, model, llPlatform, model, llProvider)
	h.apply()
	return &llLive{harness: h, key: creds.Key, managedID: managedID}, model
}

// TestLLMLabThroughTheFront is the smoke test described above.
func TestLLMLabThroughTheFront(t *testing.T) {
	l, model := llSetup(t)

	t.Run("a chat completion", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions",
			chatRequest(model, false, false))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", resp.StatusCode)
		}
		var seen llSeen
		seen.observe(t, "chat", data)
		l.llRouted(t, "chat", model, false, resp, seen)
	})

	t.Run("a streamed chat completion", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions",
			chatRequest(model, true, true))
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
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
			t.Errorf("Content-Type = %q, want text/event-stream", ct)
		}
		l.llRouted(t, "stream", model, true, resp, seen)
	})

	t.Run("GET /v1/models", func(t *testing.T) {
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
		if len(list.Data) != 1 || list.Data[0].ID != model || list.Data[0].Object != "model" || list.Data[0].Routable == nil || !*list.Data[0].Routable {
			t.Errorf("the list holds %d models, want exactly the one ModelDeployment's, routable true", len(list.Data))
		}
		checkAttribution(t, resp, efFrontOnly)
		l.llFrontRecord(t, "models", resp, 200)
	})

	t.Run("a model the front does not know", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodPost, "/v1/chat/completions", chatRequest(llUnattachedModel, false, false))
		llFrontError(t, resp, data, 503, "model_inactive", "not_attached", "30")
		l.llFrontRecord(t, "unattached", resp, 503)
	})

	t.Run("a retrieve of an unlisted name", func(t *testing.T) {
		resp, data := l.llCall(t, http.MethodGet, "/v1/models/"+url.PathEscape(llUnlistedModel), "")
		llFrontError(t, resp, data, 404, "model_not_found", "", "")
		l.llFrontRecord(t, "retrieve", resp, 404)
	})

	t.Run("the usage files", func(t *testing.T) {
		recs := l.records()
		if len(recs) != 5 {
			t.Errorf("the usage file holds %d records, want 5, one per request", len(recs))
		}
		files, err := os.ReadDir(l.cfg.UsageDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			b, err := os.ReadFile(filepath.Join(l.cfg.UsageDir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(b, []byte(l.key)) {
				t.Errorf("the usage file %s holds the upstream key", f.Name())
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
