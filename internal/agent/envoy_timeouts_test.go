package agent_test

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The router's timeouts against Envoy's defaults, with the real Envoy 1.40.2 (HBEP-5, lines 117-122 and
// 474-478). The agent sets each of the route's timeout and idle_timeout and the connection manager's
// stream_idle_timeout to the front's maximum request duration plus 1 h (10,800 s), and Envoy's
// upstream connections idle out after 50 s. Each test runs the agent's own config, through StartEnvoy,
// and then a control: Envoy's real default for the setting the agent changes, which the test shows to
// cut what the agent's config lets through. The controls are the tests' own, never the agent's.

// readStream reads a server-sent-event body line by line: how many data events came, and whether the
// closing [DONE] did. err is what ended the read: nil at the body's end, otherwise a cut stream.
func readStream(body io.Reader) (events int, done bool, err error) {
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		switch line := sc.Text(); {
		case line == "data: [DONE]":
			done = true
		case strings.HasPrefix(line, "data: {"):
			events++
		}
	}
	return events, done, sc.Err()
}

// A stream of 20 events one second apart runs 20 s: past Envoy's default 15 s route timeout, far inside
// the agent's 10,800 s.
func TestEnvoyRouteTimeoutOutlivesTheDefault(t *testing.T) {
	spec := workerSpec{Events: 20, Every: time.Second}
	stream := func(t *testing.T, r *router) (events int, done bool, err error) {
		t.Helper()
		resp, cancel, err := getVia(r.client(t), "/v1/chat/completions", 2*time.Minute, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want the worker's 200", resp.StatusCode)
		}
		return readStream(resp.Body)
	}
	t.Run("the agent's config", func(t *testing.T) {
		t.Parallel()
		r := agentRouter(t, spec)
		events, done, err := stream(t, r)
		if events != 20 || !done || err != nil {
			t.Errorf("the stream gave %d events, [DONE]=%v, error %v; want all 20, [DONE] and no error", events, done, err)
		}
		if out, ok := r.worker.waitOutcome(0, 10*time.Second); !ok || out != "answered" {
			t.Errorf("the worker's outcome is %q (settled=%v), want it to have finished the stream", out, ok)
		}
	})
	t.Run("control: the route timeout deleted, so Envoy's own 15 s", func(t *testing.T) {
		t.Parallel()
		r := controlRouter(t, spec, func(m map[string]any) { delete(routeOf(t, m), "timeout") })
		events, done, err := stream(t, r)
		if events >= 20 || done {
			t.Errorf("the stream gave %d events, [DONE]=%v; want it cut short, with fewer than 20 and no [DONE]", events, done)
		}
		if err == nil {
			t.Errorf("the cut stream ended cleanly; want an error from the reset")
		}
		// Envoy ended it, not the client: the worker saw its request end before it finished.
		if out, ok := r.worker.waitOutcome(0, 30*time.Second); !ok || out != "cut" {
			t.Errorf("the worker's outcome is %q (settled=%v), want %q: Envoy reset the stream", out, ok, "cut")
		}
		t.Logf("the control's stream: %d events, error %v", events, err)
	})
}

// The worker writes nothing for 310 s, which is past Envoy's default 5 min stream idle timeout and far
// inside the agent's 10,800 s. A long prefill before the first token looks exactly like this. Both
// halves run at once, about 5.5 minutes in all, so the test belongs to hack/go-test.sh's envoy-timeouts
// group, on a timeout of its own.
func TestEnvoyStreamIdleOutlivesTheDefault(t *testing.T) {
	const silent = 310 * time.Second
	spec := workerSpec{First: silent}
	t.Run("the agent's config", func(t *testing.T) {
		t.Parallel()
		r := agentRouter(t, spec)
		resp, cancel, err := getVia(r.client(t), "/v1/chat/completions", 8*time.Minute, nil)
		if err != nil {
			t.Fatalf("after %v of silence from the worker: %v", silent, err)
		}
		defer cancel()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != workerBody || err != nil {
			t.Errorf("after %v of silence: status %d, body %q, error %v; want the worker's 200 and %q", silent, resp.StatusCode, body, err, workerBody)
		}
	})
	t.Run("control: both idle timeouts deleted, so Envoy's own 5 min", func(t *testing.T) {
		t.Parallel()
		// The route's idle_timeout overrides the connection manager's stream_idle_timeout, so both go
		// for Envoy's default to apply.
		r := controlRouter(t, spec, func(m map[string]any) {
			delete(routeOf(t, m), "idle_timeout")
			delete(hcmOf(t, m), "stream_idle_timeout")
		})
		resp, cancel, err := getVia(r.client(t), "/v1/chat/completions", 8*time.Minute, nil)
		if err == nil {
			defer cancel()
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK {
				t.Errorf("the control got the 200 (%q); want Envoy to cut the stream at its default 5 min, before the worker's 310 s", body)
			}
			t.Logf("the control got status %d, body %q", resp.StatusCode, body)
		} else {
			t.Logf("the control got an error: %v", err)
		}
		// Envoy ended the request while the worker was still silent: the worker never answered.
		if out, ok := r.worker.waitOutcome(0, 30*time.Second); !ok || out != "cut" {
			t.Errorf("the worker's outcome is %q (settled=%v), want %q: Envoy ended the request before the worker answered", out, ok, "cut")
		}
	})
}

// closedAfterIdle sends one request through the router and returns how long the worker's connection
// stayed open after its response: from its StateIdle event to its StateClosed one. Both events are the
// worker's own, so which side closed shows in the interval: the worker's IdleTimeout, or Envoy's.
func closedAfterIdle(t *testing.T, r *router) time.Duration {
	t.Helper()
	resp, cancel, err := getVia(r.client(t), "/v1/models", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != workerBody {
		t.Fatalf("the first request: status %d, body %q", resp.StatusCode, body)
	}
	conn := r.worker.Requests()[0].Conn
	idle, ok := r.worker.waitConnState(conn, http.StateIdle, 10*time.Second)
	if !ok {
		t.Fatalf("the worker's connection %d never went idle", conn)
	}
	closed, ok := r.worker.waitConnState(conn, http.StateClosed, 150*time.Second)
	if !ok {
		t.Fatalf("the worker's connection %d was not closed within 150 s of its response", conn)
	}
	return closed.At.Sub(idle.At)
}

// Envoy keeps an idle connection to a worker 50 s, and the worker (vLLM's keep-alive) 120 s, so Envoy,
// which reuses the connection, closes it first and never sends a request into a connection the worker
// is closing (HBEP-5; L-2.5). Here the worker's IdleTimeout is 120 s.
func TestEnvoyClosesIdleUpstreamConnectionsFirst(t *testing.T) {
	t.Run("the agent's config", func(t *testing.T) {
		t.Parallel()
		r := agentRouter(t, workerSpec{IdleTimeout: 120 * time.Second})
		d := closedAfterIdle(t, r)
		if d < 45*time.Second || d >= 120*time.Second {
			t.Errorf("the worker's connection closed %v after its response, want at least 45 s and under 120 s: Envoy's 50 s, before the worker's 120 s", d)
		}
		t.Logf("the connection closed %v after its response", d)
		// The next request goes through on a new connection.
		resp, cancel, err := getVia(r.client(t), "/v1/models", time.Minute, nil)
		if err != nil {
			t.Fatalf("the second request: %v", err)
		}
		defer cancel()
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		reqs := r.worker.Requests()
		if resp.StatusCode != http.StatusOK || string(body) != workerBody || len(reqs) != 2 || reqs[1].Conn == reqs[0].Conn {
			t.Errorf("the second request: status %d, body %q, worker requests %+v; want a 200 on a connection other than the first's", resp.StatusCode, body, reqs)
		}
	})
	t.Run("control: the worker's IdleTimeout 5 s, so the worker closes first", func(t *testing.T) {
		t.Parallel()
		// The control changes the worker, not Envoy's config: it runs through the agent's own start.
		r := agentRouter(t, workerSpec{IdleTimeout: 5 * time.Second})
		d := closedAfterIdle(t, r)
		if d >= 45*time.Second {
			t.Errorf("the worker's connection closed %v after its response, want under 45 s: the worker's own 5 s", d)
		}
		t.Logf("the connection closed %v after its response", d)
	})
}
