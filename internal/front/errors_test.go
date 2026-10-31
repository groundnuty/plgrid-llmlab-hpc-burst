package front

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The rows of HBEP-5's Errors table that the front raises before it chooses a
// target: 401, the 400s and the 503s. Each test checks the row's status,
// code, detail, Retry-After and the attribution (the front only, with no
// target headers). The rows that name a target are in errors_upstream_test.go
// and errors_stream_test.go.

// TestErrorInvalidAPIKey is row 1: no client key, or a wrong one, is 401
// invalid_api_key and names the front.
func TestErrorInvalidAPIKey(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	body := errChatBody(efModel, false, "hello there")
	for name, hdr := range map[string]map[string]string{
		"no Authorization header": {"Content-Type": "application/json"},
		"a wrong key":             {"Authorization": "Bearer not-the-client-key", "Content-Type": "application/json"},
		"an empty bearer":         {"Authorization": "Bearer ", "Content-Type": "application/json"},
		"the Instance key":        {"Authorization": "Bearer " + s.key, "Content-Type": "application/json"},
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.errPostRaw("/v1/chat/completions", body, hdr)
			got := readAll(t, resp)
			s.checkFrontError(t, resp, got, wantError{status: 401, code: "invalid_api_key"})
		})
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests of a client with no valid key, want 0", n)
	}
}

// TestErrorInvalidRequest is row 2: a body that is not JSON, or has no string
// model, is 400 invalid_request.
func TestErrorInvalidRequest(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	for name, body := range map[string]string{
		"not JSON":              "this is not json",
		"an empty body":         "",
		"a JSON array":          `[{"model": "example-27b"}]`,
		"no model member":       `{"messages": [{"role": "user", "content": "hi"}]}`,
		"a number as the model": `{"model": 27}`,
		"a null model":          `{"model": null}`,
		"an object as model":    `{"model": {"name": "example-27b"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.post("/v1/chat/completions", body)
			got := readAll(t, resp)
			s.checkFrontError(t, resp, got, wantError{status: 400, code: "invalid_request"})
		})
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for bodies the front rejected, want 0", n)
	}
}

// TestErrorInvalidModelNameGrammar is row 3, detail grammar: a name that
// breaks HBEP-3's grammar is 400 invalid_model_name and never falls back to a
// plain model.
func TestErrorInvalidModelNameGrammar(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	for name, model := range map[string]string{
		"an empty provider":            efModel + "@",
		"an empty model":               "@helios-a",
		"a deployment ID too short":    efModel + "@helios-a/0123456789a",
		"a deployment ID too long":     efModel + "@helios-a/0123456789abc",
		"a deployment ID not hex":      efModel + "@helios-a/0123456789xy",
		"a deployment ID in capitals":  efModel + "@helios-a/0123456789AB",
		"a trailing slash":             efModel + "@helios-a/",
		"an empty provider with an ID": efModel + "@/" + efDeployDisplay,
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			got := readAll(t, resp)
			s.checkFrontError(t, resp, got, wantError{status: 400, code: "invalid_model_name", detail: "grammar"})
		})
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for names that break the grammar, want 0 (no fallback to the plain model)", n)
	}
}

// TestErrorInvalidModelNameUnknownProvider is row 3, detail unknown_provider:
// the name is well formed but names no Provider.
func TestErrorInvalidModelNameUnknownProvider(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	for name, model := range map[string]string{
		"provider level": efModel + "@no-such-provider",
		"pinned level":   efModel + "@no-such-provider/" + efDeployDisplay,
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			got := readAll(t, resp)
			s.checkFrontError(t, resp, got, wantError{status: 400, code: "invalid_model_name", detail: "unknown_provider"})
		})
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for an unknown provider, want 0 (no fallback)", n)
	}
}

// TestErrorModelNotAttached is row 4: no ModelDeployment has the model name.
// It is 503 model_inactive with detail not_attached and Retry-After 30, and
// it names the front only.
func TestErrorModelNotAttached(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	for name, model := range map[string]string{
		"a plain unknown model":      "no-such-model",
		"an unknown model, provider": "no-such-model@helios-a",
		"an unknown model, pinned":   "no-such-model@helios-a/" + efDeployDisplay,
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			got := readAll(t, resp)
			s.checkFrontError(t, resp, got, wantError{status: 503, code: "model_inactive", detail: "not_attached", retryAfter: "30"})
		})
	}
}

// TestErrorModelNoReadyTarget is row 5: the model is attached, with no
// eligible target at the requested level. It is 503 model_inactive with
// detail no_ready_target and Retry-After 30, and never widens to a broader
// level (Request path step 3).
func TestErrorModelNoReadyTarget(t *testing.T) {
	t.Run("an Instance that is not ready", func(t *testing.T) {
		h := newHarness(t, nil)
		h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
		h.batchProvider("helios-a", "helios-gh200")
		h.servedMD("md", efModel, 1, "helios-a")
		key := newKey(t)
		e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
		h.instance("md", "md-0", "helios-a", e, key, instanceOpts{phase: v1alpha1.InstanceStarting, deployID: efDeployID})
		h.apply()
		resp := h.errPost("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		got := readAll(t, resp)
		h.checkFrontError(t, resp, got, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
		if n := len(e.Requests()); n != 0 {
			t.Errorf("the engine saw %d requests, want 0", n)
		}
	})
	t.Run("a ready Instance without a registered transport", func(t *testing.T) {
		h := newHarness(t, nil)
		h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
		h.batchProvider("helios-a", "helios-gh200")
		h.servedMD("md", efModel, 1, "helios-a")
		h.instance("md", "md-0", "helios-a", nil, newKey(t), instanceOpts{deployID: efDeployID})
		h.apply()
		resp := h.errPost("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		got := readAll(t, resp)
		h.checkFrontError(t, resp, got, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	})
	t.Run("a level with no ready target", func(t *testing.T) {
		s := newErrFix(t, nil, instanceOpts{}, "")
		// A second Provider with no Instance, and a pinned ID no Instance has.
		s.batchProvider("helios-b", "helios-gh200")
		s.apply()
		for name, model := range map[string]string{
			"provider level with no Instance there": efModel + "@helios-b",
			"pinned, a Provider with no Instance":   efModel + "@helios-b/" + efDeployDisplay,
			"pinned, another deployment ID":         efModel + "@helios-a/abcdef012345",
		} {
			t.Run(name, func(t *testing.T) {
				resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
				got := readAll(t, resp)
				s.checkFrontError(t, resp, got, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
			})
		}
		if n := len(s.e.Requests()); n != 0 {
			t.Errorf("the engine saw %d requests, want 0: a level never widens to a broader one", n)
		}
		// The same fixture serves the plain name and the right pinned name, so
		// the 503s above are about the level, not about the fixture.
		for _, model := range []string{efModel, efModel + "@helios-a", efModel + "@helios-a/" + efDeployDisplay} {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			got := readAll(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s: status %d, want 200: %s", model, resp.StatusCode, got)
			}
		}
	})
}

// TestErrorCapacity is row 6: every candidate at its cap past queueTimeout is
// 503 capacity with Retry-After 10. It names the front and the
// ModelDeployment: no target was chosen, so the headers are the front's only,
// and the ModelDeployment is in the message and the usage record.
func TestErrorCapacity(t *testing.T) {
	s := newErrFix(t, func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }, instanceOpts{maxNumSeqs: 1}, "")
	hold := make(chan struct{})
	s.e.Set(enginetest.Behaviour{Hold: hold})
	first := make(chan *http.Response, 1)
	go func() { first <- s.post("/v1/chat/completions", errChatBody(efModel, false, "first request")) }()
	errWaitFor(t, "the first request to reach the engine", func() bool { return s.e.Open() == 1 })

	start := time.Now()
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "second request"))
	got := readAll(t, resp)
	checkError(t, resp, got, wantError{status: 503, code: "capacity", retryAfter: "10"})
	id := checkAttribution(t, resp, efFrontOnly)
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Errorf("the request got its 503 after %v, want it to wait for queueTimeout (300 ms) first", waited)
	}
	if !strings.Contains(got, "ns/md") {
		t.Errorf("the error does not name the ModelDeployment ns/md: %s", got)
	}
	if rec := s.record(id); rec.ModelDeployment != "ns/md" || rec.Status != 503 || rec.Emitter != "front" {
		t.Errorf("usage record: modelDeployment %q status %d emitter %q, want ns/md 503 front", rec.ModelDeployment, rec.Status, rec.Emitter)
	}
	if n := len(s.e.Requests()); n != 1 {
		t.Errorf("the engine saw %d requests, want 1: the queued one never reached it", n)
	}

	close(hold)
	r := <-first
	if body := readAll(t, r); r.StatusCode != http.StatusOK {
		t.Errorf("the first request: status %d, want 200: %s", r.StatusCode, body)
	}
}
