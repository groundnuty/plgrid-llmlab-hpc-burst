package agent_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// What reaches a worker through the router, with the real Envoy 1.40.2: no x-envoy-* header (the
// router filter's suppress_envoy_headers), and the key and the session id as the front sent them. The
// controls are the tests' own, never the agent's.

// envoyHeaders is the names of the request headers that start with x-envoy-, in any case.
func envoyHeaders(h http.Header) []string {
	var names []string
	for k := range h {
		if strings.HasPrefix(strings.ToLower(k), "x-envoy-") {
			names = append(names, strings.ToLower(k))
		}
	}
	return names
}

func TestEnvoySuppressesItsOwnHeaders(t *testing.T) {
	send := func(t *testing.T, r *router) http.Header {
		t.Helper()
		resp, cancel, err := getVia(r.client(t), "/v1/models", time.Minute, http.Header{"X-Session-Id": {"conv-1"}})
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		reqs := r.worker.Requests()
		if resp.StatusCode != http.StatusOK || len(reqs) != 1 {
			t.Fatalf("status %d with %d requests at the worker, want a 200 and 1", resp.StatusCode, len(reqs))
		}
		return reqs[0].Header
	}
	t.Run("the agent's config", func(t *testing.T) {
		r := agentRouter(t, workerSpec{})
		h := send(t, r)
		if names := envoyHeaders(h); len(names) != 0 {
			t.Errorf("the worker saw Envoy's own headers %v, want none (all of them: %v)", names, h)
		}
		if h.Get("X-Request-Id") == "" {
			t.Errorf("the worker saw no x-request-id: the request did not go through Envoy (%v)", h)
		}
	})
	t.Run("control: suppress_envoy_headers false", func(t *testing.T) {
		r := controlRouter(t, workerSpec{}, func(m map[string]any) {
			at(t, hcmOf(t, m), "http_filters", 0, "typed_config").(map[string]any)["suppress_envoy_headers"] = false
		})
		h := send(t, r)
		if got := h.Get("X-Envoy-Expected-Rq-Timeout-Ms"); got == "" {
			t.Errorf("the worker saw no x-envoy-expected-rq-timeout-ms (Envoy's headers: %v): the test could not see Envoy's own headers", envoyHeaders(h))
		}
	})
}

// The values the front sends a worker: the Instance's key and the conversation's key, with mixed case
// and '=' padding, which a normalising proxy would change.
const (
	testKey     = "Ab12CdEf34GhIj56KlMn78OpQr=="
	testSession = "Conv-aB3xY9zQ=="
)

// passthroughProblems drives the router with the key and the session id and returns what it finds
// wrong: nothing, when the worker saw both exactly as sent, an x-request-id from Envoy, the worker's own
// 401 without the key and the worker's 200 with it.
func passthroughProblems(t *testing.T, r *router, viaEnvoy bool) []string {
	t.Helper()
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	c := r.client(t)
	if !viaEnvoy {
		c = unixClient(workerSock(r.dir))
		t.Cleanup(c.Transport.(*http.Transport).CloseIdleConnections)
	}
	do := func(auth string) (int, string, http.Header) {
		t.Helper()
		h := http.Header{"X-Session-Id": {testSession}}
		if auth != "" {
			h.Set("Authorization", auth)
		}
		resp, cancel, err := getVia(c, "/v1/chat/completions", time.Minute, h)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		reqs := r.worker.Requests()
		return resp.StatusCode, string(body), reqs[len(reqs)-1].Header
	}

	status, body, h := do("Bearer " + testKey)
	if status != http.StatusOK || body != workerBody {
		add("with the key: status %d, body %q; want the worker's 200 %q", status, body, workerBody)
	}
	if got := h.Values("Authorization"); len(got) != 1 || got[0] != "Bearer "+testKey {
		add("the worker saw Authorization %q, want exactly %q", got, "Bearer "+testKey)
	}
	if got := h.Values("X-Session-Id"); len(got) != 1 || got[0] != testSession {
		add("the worker saw x-session-id %q, want exactly %q", got, testSession)
	}
	if h.Get("X-Request-Id") == "" {
		add("the worker saw no x-request-id: the request did not go through Envoy")
	}

	// Without the key, or with the key in another case, the worker's own 401 comes back through Envoy.
	for name, auth := range map[string]string{"no key": "", "the key in lower case": "Bearer " + strings.ToLower(testKey)} {
		status, body, _ := do(auth)
		if status != http.StatusUnauthorized || body != workerUnauthorized+"\n" {
			add("%s: status %d, body %q; want the worker's 401 %q", name, status, body, workerUnauthorized)
		}
	}
	return problems
}

func TestEnvoyPassesAuthorizationAndSessionIDUnchanged(t *testing.T) {
	spec := workerSpec{Key: testKey}
	t.Run("the agent's config", func(t *testing.T) {
		r := agentRouter(t, spec)
		for _, p := range passthroughProblems(t, r, true) {
			t.Error(p)
		}
	})
	// Two controls show the checks can fail: a request sent to the worker's socket directly never goes
	// through Envoy, so it has no x-request-id; and a config that makes Envoy overwrite x-session-id
	// (one field added to the route) changes what the worker sees.
	t.Run("control: a request that skips Envoy", func(t *testing.T) {
		r := agentRouter(t, spec)
		problems := passthroughProblems(t, r, false)
		if !hasProblem(problems, "x-request-id") {
			t.Errorf("a request straight to the worker passed the check for Envoy's x-request-id: %q", problems)
		}
	})
	t.Run("control: Envoy told to overwrite x-session-id", func(t *testing.T) {
		r := controlRouter(t, spec, func(m map[string]any) {
			route := at(t, hcmOf(t, m), "route_config", "virtual_hosts", 0, "routes", 0).(map[string]any)
			route["request_headers_to_add"] = []any{map[string]any{
				"header":        map[string]any{"key": "x-session-id", "value": "rewritten"},
				"append_action": "OVERWRITE_IF_EXISTS_OR_ADD",
			}}
		})
		problems := passthroughProblems(t, r, true)
		if !hasProblem(problems, "x-session-id") {
			t.Errorf("a config that rewrites x-session-id passed the byte-for-byte check: %q", problems)
		}
	})
}

func hasProblem(problems []string, what string) bool {
	for _, p := range problems {
		if strings.Contains(p, what) {
			return true
		}
	}
	return false
}
