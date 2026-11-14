package agent_test

// A worker dies behind the real Envoy 1.40.2, with ring_hash's bounded load on (issue #561). A dead worker
// that Envoy still holds healthy has no request active, so the bounded load never finds it overloaded:
// once its sibling carries more than its share (from its sixth request in flight, with two workers), the
// bounded load's walk sends requests there, and the retry's host selection lands there again on every pick.
// HBEP-5 as #564 amends it: a connection failure ejects the worker at once, and the retry waits a back-off
// of 100 ms base and 250 ms maximum, so it follows the ring's update.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// hostStats are each worker's numeric stats on Envoy's admin /clusters (rq_active, cx_connect_fail, …),
// and its health flags that are set, as "health:<flag>" = 1.
func hostStats(t *testing.T, runDir string) map[string]map[string]int {
	t.Helper()
	c := unixClient(filepath.Join(runDir, agent.EnvoyAdminSocket))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := c.Get("http://admin/clusters?format=json")
	if err != nil {
		t.Fatalf("Envoy's /clusters: %v", err)
	}
	defer resp.Body.Close()
	var doc struct {
		Clusters []struct {
			Hosts []struct {
				Hostname string `json:"hostname"`
				Stats    []struct {
					Name  string          `json:"name"`
					Value json.RawMessage `json:"value"`
				} `json:"stats"`
				Health map[string]any `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("Envoy's /clusters: %v", err)
	}
	out := map[string]map[string]int{}
	for _, cl := range doc.Clusters {
		for _, h := range cl.Hosts {
			m := map[string]int{}
			for _, st := range h.Stats {
				var s string
				if json.Unmarshal(st.Value, &s) == nil {
					if v, err := strconv.Atoi(s); err == nil {
						m[st.Name] = v
					}
				}
			}
			for k, v := range h.Health {
				if b, ok := v.(bool); ok && b {
					m["health:"+k] = 1
				}
			}
			out[h.Hostname] = m
		}
	}
	return out
}

// heldOut reports whether Envoy holds the worker out: failed by its active check, or ejected.
func heldOut(st map[string]int) bool {
	return st["health:failed_active_health_check"] == 1 || st["health:failed_outlier_check"] == 1
}

// chatIn sends a chat completion of exactly tokens tokens in session, and returns its status and body.
func chatIn(h *harness, key, session string, tokens int) (int, string) {
	c := unixClient(agent.ServingSocket(h.runDir))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	n := strconv.Itoa(tokens)
	body := `{"model":"` + hServed + `","messages":[{"role":"user","content":"hello"}],"max_tokens":` + n + `,"min_tokens":` + n + `}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://instance/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-session-id", session)
	resp, err := c.Do(req)
	if err != nil {
		return -1, err.Error()
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

// TestALiveWorkersOwnSessionNeverReachesItsDeadSibling: worker-1 is killed while Envoy still holds it
// healthy; then 8 long requests in worker-0's OWN session, each sent once the one before is in flight or
// has ended. From the sixth, worker-0 is over its bounded share, and the bounded load's walk sends the
// request to worker-1, which holds no request.
//
// It discriminates by MECHANISM, from Envoy's own counters, which are deterministic given the kill: the
// walked request's first attempt is worker-1's first connection failure, which ejects it at once. Only a
// retry that beat the ring's update can add a second, and nothing reaches an ejected worker after. So
// worker-1 ends ejected with 1 or 2 connection failures. On the config before #561 a connection failure
// counted as a 5xx, and 3 in a row ejected: worker-1 ends with 3 or more, or not ejected. Measured on the
// shipped config: 3 to 5, in all 20 runs; with the first-failure ejection: 1, in all 40. (The admin's one allowed path, /clusters, shows each
// worker's counters and health flags, but not the cluster's stats, such as the ejections by type.)
//
// The outcome is checked with an allowance of one 503. On the fix only the walked request is exposed, to a
// retry whose 0-100 ms back-off beats the ring's update, so two losses need two exposures, which this run
// doesn't have. Measured: 0 losses in 23 runs of this case and 0 in 1,600 requests of the wave. The
// allowance can't tell the shipped config, which lost exactly one request in 26 of its 27 failing runs (5
// of 32 lost none): the mechanism does.
func TestALiveWorkersOwnSessionNeverReachesItsDeadSibling(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=200ms"})
	h.set(d)
	r := h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	s0, s1 := sessionOn(t, h, "worker-0"), sessionOn(t, h, "worker-1")
	killAt := killInWindow(t, h, &d, 1)
	before := hostStats(t, h.runDir)
	if heldOut(before["worker-1"]) {
		t.Fatalf("Envoy held worker-1 out before the load began (%v): the test proves nothing", before["worker-1"])
	}
	began := time.Since(killAt)
	const n = 8
	codes, bodies := make([]int, n), make([]string, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ended, peak := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, b := chatIn(h, key, s0, 20) // 4 s at 200 ms a token
			mu.Lock()
			codes[i], bodies[i] = c, b
			ended++
			mu.Unlock()
		}()
		// Each request is sent once every request before it is in flight or has ended.
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			st := hostStats(t, h.runDir)
			mu.Lock()
			peak = max(peak, st["worker-0"]["rq_active"])
			done := st["worker-0"]["rq_active"]+st["worker-1"]["rq_active"]+ended >= i+1
			mu.Unlock()
			if done {
				break
			}
		}
	}
	code, body := chatIn(h, key, s1, 1)
	wg.Wait()
	after := hostStats(t, h.runDir)
	fails := after["worker-1"]["cx_connect_fail"] - before["worker-1"]["cx_connect_fail"]
	t.Logf("the load began %s after the kill; worker-0 held up to %d at once; worker-1 failed %d connection(s), ejected %v",
		began.Round(time.Millisecond), peak, fails, after["worker-1"]["health:failed_outlier_check"] == 1)
	if peak < 5 || fails == 0 {
		t.Fatalf("worker-0 held up to %d requests at once, and worker-1 failed %d connections: no request was walked to the dead worker, so the test proves nothing", peak, fails)
	}
	// The mechanism.
	if after["worker-1"]["health:failed_outlier_check"] != 1 || fails > 2 {
		t.Errorf("worker-1 ends with %d connection failure(s), ejected %v: want it ejected by its first (1, or 2 if a retry beat the ring's update); 3 or more is the 5xx count of the config before #561",
			fails, after["worker-1"]["health:failed_outlier_check"] == 1)
	}
	if after["worker-0"]["cx_connect_fail"] != before["worker-0"]["cx_connect_fail"] {
		t.Errorf("worker-0 failed %d connections: the live worker refused", after["worker-0"]["cx_connect_fail"]-before["worker-0"]["cx_connect_fail"])
	}
	// The outcome, with its allowance of one.
	lost := 0
	for i := range codes {
		if codes[i] != http.StatusOK {
			lost++
			t.Logf("request %d of %d in worker-0's own session, worker-1 dead: %d %.160q", i+1, n, codes[i], bodies[i])
		}
	}
	if code != http.StatusOK {
		lost++
		t.Logf("a request in worker-1's session after the load: %d %.160q", code, body)
	}
	if lost > 1 {
		t.Errorf("%d of %d requests failed with worker-1 dead; the allowance is one (a retry that beat the ring's update)", lost, n+1)
	}
}

// TestAWaveAtAWorkersDeathGetsNoServiceUnavailable: worker-1 is killed while Envoy still holds it healthy;
// then 40 requests at once over 40 sessions, about half hashed to worker-1, and at least one must reach it.
// The first connection failure ejects worker-1, and each retry, after its back-off, follows the ring's
// update to worker-0. A wave is a race (many first attempts reach worker-1 before the ejection lands), so
// its counters can't show the mechanism; TestALiveWorkersOwnSessionNeverReachesItsDeadSibling does.
//
// It allows two 503s. Measured on the fix: 0 losses in 1,600 requests (40 waves). The 95% upper bound on the
// loss rate is then 3 in 1,600 (the rule of three), so a wave of 40 expects at most 0.075 losses, and
// P(3 or more) <= 1 - e^-0.075 (1 + 0.075 + 0.075^2/2) = 7e-5. Measured on the config before #561: 3 or more
// losses in 3 of 52 waves (6%); with only the first-failure ejection and Envoy's 25 ms back-off, in 1 of 40.
// It guards the end-to-end outcome against a gross regression, and isn't the discriminator.
func TestAWaveAtAWorkersDeathGetsNoServiceUnavailable(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=50ms"})
	h.set(d)
	r := h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	before := hostStats(t, h.runDir)
	killAt := killInWindow(t, h, &d, 1)
	const n = 40
	codes, bodies := make([]int, n), make([]string, n)
	var start, wg sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			codes[i], bodies[i] = chatIn(h, key, "wave-"+strconv.Itoa(i), 20)
		}()
	}
	start.Done()
	wg.Wait()
	after := hostStats(t, h.runDir)
	if got := after["worker-1"]["cx_connect_fail"] - before["worker-1"]["cx_connect_fail"]; got == 0 {
		t.Fatalf("no request of the wave reached the dead worker-1: the test proves nothing")
	}
	failed := 0
	for i := range codes {
		if codes[i] != http.StatusOK {
			failed++
			if failed <= 3 {
				t.Logf("request %d of the wave, %s after worker-1's kill: %d %.160q", i, time.Since(killAt).Round(time.Millisecond), codes[i], bodies[i])
			}
		}
	}
	if failed > 2 {
		t.Errorf("%d of %d requests failed; the allowance is two (P(3 or more) <= 7e-5 a wave on the fix's measured rate)", failed, n)
	}
}
