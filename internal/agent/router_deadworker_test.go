package agent_test

// A worker dies behind the real Envoy 1.40.2, ring_hash with no bounded load (issue #614; the science agent's
// ruling, which reverses #518's provisional bound). A dead worker that Envoy still holds healthy has no request
// active, so a bounded load never finds it overloaded: with the bound on, from a live worker's sixth request in
// flight (two workers), the bound's walk sent requests there, and the retry's host selection landed there again
// (#561). With no bound a request in a live worker's session stays on it, and only a session that hashes to
// the dead worker reaches it, once. HBEP-5 as #564 amends it: a connection failure ejects the worker at once,
// and the retry waits a back-off of 100 ms base and 250 ms maximum, so it follows the ring's update.

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
	out, err := hostStatsErr(runDir)
	if err != nil {
		t.Fatalf("Envoy's /clusters: %v", err)
	}
	return out
}

// hostStatsErr is hostStats returning its error, for a reader that is not the test's goroutine.
func hostStatsErr(runDir string) (map[string]map[string]int, error) {
	c := unixClient(filepath.Join(runDir, agent.EnvoyAdminSocket))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := c.Get("http://admin/clusters?format=json")
	if err != nil {
		return nil, err
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
		return nil, err
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
	return out, nil
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
// has ended, and a request in worker-1's own session. With the bound on, worker-0 was over its share from
// the sixth request, and the bound's walk sent that request to worker-1, which holds no request (#561): the
// test was written to catch that. With no bound (#614) the 8 stay on worker-0, and only worker-1's own
// session's request reaches the dead worker.
//
// It checks the outcome with NO allowance, and the mechanism from Envoy's counters: worker-0 held up to
// 5 or more at once (the load at which the bound walked), worker-1 failed a connection, and ended
// ejected by its first or its second (a retry that beat the ring's update), not by the third of the 5xx
// count of the config before #561. It cannot say that none of the 8 reached worker-1: the agent's probe
// through the router, every 100 ms here and in no session, lands on either worker and fails the first
// connection itself, so that the mechanism's failures are not tied to the requests. The test that a session
// never leaves its worker is TestAHeavySessionNeverSpillsToTheIdleSibling, and the config's own pins
// (TestTheRenderedRingHasNoBoundedLoad, the guard) hold the bound out.
//
// Measured on this config, pinned to 2 CPUs beside 2 spinners: worker-0 held 8 at once, and worker-1 failed
// exactly 1 connection and ended ejected, in all 50 runs, with no request lost. On the config with the
// bound, 0 losses in 23 runs of this case, and the allowance, which was one, could not tell it from the
// shipped config before #561 (1 loss in 26 of its 27 failing runs).
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
		t.Fatalf("worker-0 held up to %d requests at once, and worker-1 failed %d connections: the load never built, or nothing reached the dead worker (its own session's request, or the agent's probe, is the first to), so the test proves nothing", peak, fails)
	}
	// The mechanism.
	if after["worker-1"]["health:failed_outlier_check"] != 1 || fails > 2 {
		t.Errorf("worker-1 ends with %d connection failure(s), ejected %v: want it ejected by its first (1, or 2 if a retry beat the ring's update); 3 or more is the 5xx count of the config before #561",
			fails, after["worker-1"]["health:failed_outlier_check"] == 1)
	}
	if after["worker-0"]["cx_connect_fail"] != before["worker-0"]["cx_connect_fail"] {
		t.Errorf("worker-0 failed %d connections: the live worker refused", after["worker-0"]["cx_connect_fail"]-before["worker-0"]["cx_connect_fail"])
	}
	// The outcome, with no allowance (#614): no request in worker-0's session has a walk to the dead worker to
	// lose to, and the one request that reaches it is retried to worker-0 after the ring's update.
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
	if lost > 0 {
		t.Errorf("%d of %d requests failed with worker-1 dead and worker-0 healthy; the allowance is none (HBEP-5: no 503 while a sibling is healthy)", lost, n+1)
	}
}

// TestAWaveAtAWorkersDeathGetsNoServiceUnavailable: worker-1 is killed while Envoy still holds it healthy;
// then 40 requests at once over 40 sessions, about half hashed to worker-1, and at least one must reach it.
// The first connection failure ejects worker-1, and each retry, after its back-off, follows the ring's
// update to worker-0. A wave is a race (many first attempts reach worker-1 before the ejection lands), so
// its counters can't show the mechanism; TestALiveWorkersOwnSessionNeverReachesItsDeadSibling does.
//
// It allows NO 503 (HBEP-5: no 503 while a sibling is healthy; the science agent's ruling on #614). The
// allowance was two, on #561's measure of 0 losses in 1,600 requests, taken with the CPUs free and with
// ring_hash's bounded load on. Pinned to 2 CPUs beside 2 spinners, with the bound on, 100 waves lost 14 of
// 4,000 requests (10 waves, up to 3 at one time), 14 of 1,266 retries going back to the dead worker: the bound's
// walk steers a retry past a worker over its share, to the dead one, until the ejection reaches Envoy's worker
// threads (median 24 ms after the wave began; the retry's back-off is 0 to 100 ms). With no bound, the same
// 100 waves lost none (0 of 1,005 retries failed). TestWaveMeasure repeats the wave and records it per wave.
// Measured on the config before #561: 3 or more losses in 3 of 52 waves (6%); with only the first-failure
// ejection and Envoy's 25 ms back-off, in 1 of 40.
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
				// The time is when this loop ran, after the whole wave: it says nothing of when the request failed
				// (#614's CI log read three failures at one such moment as one event; TestWaveMeasure times each).
				t.Logf("request %d of the wave, %s after worker-1's kill when this was printed: %d %.160q", i, time.Since(killAt).Round(time.Millisecond), codes[i], bodies[i])
			}
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d requests failed with worker-0 healthy; the allowance is none (HBEP-5: no 503 while a sibling is healthy)", failed, n)
	}
}

// TestBothWorkersFailingLeaveOneInTheRing: both workers are killed while Envoy still holds them healthy, and
// both then fail a connection. Outlier detection ejects only ONE of them: with two workers its
// max_ejection_percent is 50 (⌈100·(n−1)/n⌉; the science agent's ruling on #565), so it never takes the
// last worker out. The active check then takes the other out (failed twice, at 5 s), and with no panic mode
// the router answers 503 "no healthy upstream". At 100 (the value before #565) both would be ejected on
// their first failures.
func TestBothWorkersFailingLeaveOneInTheRing(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	h.set(d)
	r := h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	s0, s1 := sessionOn(t, h, "worker-0"), sessionOn(t, h, "worker-1")
	killInWindow(t, h, &d, 0)
	if _, open := killWorker(t, h, "worker-1", d.Instance.Workers[1].Generation); !open {
		t.Fatal("Envoy's check held worker-1 out before a request could reach it: the test proves nothing")
	}
	before := hostStats(t, h.runDir)
	for _, w := range []string{"worker-0", "worker-1"} {
		if heldOut(before[w]) {
			t.Fatalf("Envoy held %s out before the requests (%v): the test proves nothing", w, before[w])
		}
	}
	// Both dead: each request gets a 503. They only make each worker fail a connection.
	c0, _ := chatIn(h, key, s0, 1)
	c1, _ := chatIn(h, key, s1, 1)
	after := hostStats(t, h.runDir)
	ejected := 0
	for _, w := range []string{"worker-0", "worker-1"} {
		fails := after[w]["cx_connect_fail"] - before[w]["cx_connect_fail"]
		if fails == 0 {
			t.Fatalf("%s failed no connection (the requests got %d and %d): the test proves nothing", w, c0, c1)
		}
		if after[w]["health:failed_outlier_check"] == 1 {
			ejected++
		}
		t.Logf("%s: %d connection failure(s), ejected %v", w, fails, after[w]["health:failed_outlier_check"] == 1)
	}
	if ejected != 1 {
		t.Fatalf("%d workers ejected after both failed: want exactly one, so outlier detection never empties the Instance", ejected)
	}
	// The active check takes the other out. The ring without it reaches Envoy's worker threads a moment
	// after /clusters shows it, so the 503 for no healthy worker is waited for, within a bound.
	h.waitReport("Envoy holding neither worker healthy", time.Minute, routerHolds(map[string]bool{"worker-0": false, "worker-1": false}))
	code, body := 0, ""
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if code, body = chatIn(h, key, s0, 1); code == http.StatusServiceUnavailable && strings.Contains(body, "no healthy upstream") {
			return
		}
	}
	t.Errorf("a request with both workers out, for 10 s: %d %.160q, want Envoy's 503 no healthy upstream", code, body)
}
