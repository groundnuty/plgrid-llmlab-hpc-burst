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
// healthy; then 8 long requests in worker-0's OWN session, each sent once the one before is in flight. From
// the sixth, worker-0 is over its bounded share, and the bounded load's walk sends the request to worker-1,
// which holds no request. Each must still get worker-0's 200: worker-1 ejected by that first connection
// failure, the retry, after its back-off, sent to worker-0. On the config before #561 the sixth got a 503,
// "retried and the latest reset reason: remote connection failure", in 22 of 24 runs.
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
	if st := hostStats(t, h.runDir)["worker-1"]; heldOut(st) {
		t.Fatalf("Envoy held worker-1 out before the load began (%v): the test proves nothing", st)
	}
	began := time.Since(killAt)
	const n = 8
	codes, bodies := make([]int, n), make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			codes[i], bodies[i] = chatIn(h, key, s0, 20) // 4 s at 200 ms a token
		}()
		// Each request is sent once the one before is in flight, or has ended.
		deadline := time.Now().Add(10 * time.Second)
	wait:
		for time.Now().Before(deadline) {
			st := hostStats(t, h.runDir)
			if st["worker-0"]["rq_active"]+st["worker-1"]["rq_active"] >= i+1 {
				break
			}
			select {
			case <-done:
				break wait
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	loaded := hostStats(t, h.runDir)["worker-0"]["rq_active"]
	code, body := chatIn(h, key, s1, 1)
	wg.Wait()
	t.Logf("the load began %s after the kill; worker-0 held %d at once", began.Round(time.Millisecond), loaded)
	if loaded < 6 {
		t.Fatalf("worker-0 held %d requests at once, fewer than the six that put it over its bounded share: the test proves nothing", loaded)
	}
	for i := range codes {
		if codes[i] != http.StatusOK {
			t.Errorf("request %d of %d in worker-0's own session, worker-1 dead: %d %.160q; want worker-0's 200", i+1, n, codes[i], bodies[i])
		}
	}
	if code != http.StatusOK {
		t.Errorf("a request in worker-1's session after the load: %d %.160q; want worker-0's 200 through the retry", code, body)
	}
	if st := hostStats(t, h.runDir)["worker-0"]; st["cx_connect_fail"] != 0 {
		t.Errorf("worker-0 failed %d connections: the live worker refused", st["cx_connect_fail"])
	}
}

// TestAWaveAtAWorkersDeathGetsNoServiceUnavailable: worker-1 is killed while Envoy still holds it healthy;
// then 40 requests at once over 40 sessions, about half hashed to worker-1. Each must get worker-0's 200:
// the first connection failure ejects worker-1, and every retry follows the ring's update after its
// back-off. With Envoy's default 25 ms back-off a wave lost requests to retries that beat the update (8 of
// 1,600 in 40 waves); on the config before #561, 24 of 1,600.
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
				t.Errorf("request %d of the wave, %s after worker-1's kill: %d %.160q", i, time.Since(killAt).Round(time.Millisecond), codes[i], bodies[i])
			}
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d requests failed; want each answered by worker-0", failed, n)
	}
}
