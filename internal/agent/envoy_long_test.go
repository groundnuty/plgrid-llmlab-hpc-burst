package agent_test

// HBEP-5's Test plan, Beta, the router, Deadlines, as written: the 7,000-token stream at one token a
// second (about 117 minutes) completes through the agent's Envoy, and so does a request whose first
// token comes after 1,100 s, as in S2 phase A. These runs take two hours, so they run once, on a hosted
// throwaway, before beta's graduation (the science agent's ruling on #429, Q9), and are skipped
// elsewhere; envoy_timeouts_test.go holds the same timeouts against Envoy's defaults in minutes, in CI.

import (
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestEnvoyCarriesHBEP5sLongReplies(t *testing.T) {
	if os.Getenv("HPCBURST_ROUTER_LONG") != "1" {
		t.Skip("about two hours; set HPCBURST_ROUTER_LONG=1 to run it (a hosted throwaway runs it once, #429, Q9)")
	}
	t.Run("7,000 tokens at one a second", func(t *testing.T) {
		t.Parallel()
		r := agentRouter(t, workerSpec{Events: 7000, Every: time.Second})
		start := time.Now()
		resp, cancel, err := getVia(r.client(t), "/v1/chat/completions", 150*time.Minute, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		defer resp.Body.Close()
		events, done, err := readStream(resp.Body)
		if resp.StatusCode != http.StatusOK || events != 7000 || !done || err != nil {
			t.Errorf("after %v: status %d, %d events, [DONE]=%v, error %v; want 200, all 7,000 and [DONE]", time.Since(start).Round(time.Second), resp.StatusCode, events, done, err)
		}
		t.Logf("7,000 tokens crossed the router in %v", time.Since(start).Round(time.Second))
	})
	t.Run("a first token after 1,100 s", func(t *testing.T) {
		t.Parallel()
		r := agentRouter(t, workerSpec{First: 1100 * time.Second})
		start := time.Now()
		resp, cancel, err := getVia(r.client(t), "/v1/chat/completions", 30*time.Minute, nil)
		if err != nil {
			t.Fatalf("after %v: %v", time.Since(start).Round(time.Second), err)
		}
		defer cancel()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != workerBody || err != nil {
			t.Errorf("after %v: status %d, body %q, error %v; want the worker's 200", time.Since(start).Round(time.Second), resp.StatusCode, body, err)
		}
		t.Logf("the reply after 1,100 s of silence crossed the router in %v", time.Since(start).Round(time.Second))
	})
}
