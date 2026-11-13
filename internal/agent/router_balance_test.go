package agent_test

// Bounded loads inside an Instance (HBEP-5, Inside an Instance; the science agent's decision on #518):
// Envoy's ring_hash runs with hash_balance_factor 125. Envoy's bound, as measured on the pinned 1.40.2
// with two workers, gives each host ceil(ceil((active + 1) × 1.25) / 2) slots, where active counts the
// cluster's requests in flight, and a host above its slots takes no new request, which moves on round
// the ring. So with every heavy session hashed to worker-1, sessions 1 to 5 stay on it (5 active: 7
// slots of the cluster's, 4 of them worker-1's, and 4 is not above 4), and the 6th spills to the idle
// worker-0 (5 active, 8 and 4 slots, and 5 is above 4). Measured twice, the same both times; the 7th
// then goes to worker-1 and the 8th to worker-0. Without the bound every session would stay on
// worker-1, filling it while its sibling idles.

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

// hostStat is one of Envoy's per-worker counters (rq_active, rq_total, …), from its admin.
func hostStat(t *testing.T, runDir, stat string) map[string]int {
	t.Helper()
	c := unixClient(filepath.Join(runDir, agent.EnvoyAdminSocket))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := c.Get("http://admin/clusters?format=json")
	if err != nil {
		t.Fatalf("Envoy's admin: %v", err)
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
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("Envoy's /clusters?format=json: %v", err)
	}
	out := map[string]int{}
	for _, cl := range doc.Clusters {
		for _, h := range cl.Hosts {
			for _, s := range h.Stats {
				if s.Name == stat {
					out[h.Hostname], _ = strconv.Atoi(strings.Trim(string(s.Value), `"`))
				}
			}
		}
	}
	return out
}

// sessionsOn finds n sessions that Envoy's ring hash places on worker w, while nothing else is in
// flight, so the bound plays no part: each is sent alone, and only w's own rq_total rises.
func sessionsOn(t *testing.T, h *harness, w string, n int) []string {
	t.Helper()
	var out []string
	for i := 0; i < 256 && len(out) < n; i++ {
		s := "heavy-" + strconv.Itoa(i)
		before := hostStat(t, h.runDir, "rq_total")
		if code, body := send(t, h, s); code != http.StatusOK {
			t.Fatalf("session %s: %d %q", s, code, body)
		}
		after := hostStat(t, h.runDir, "rq_total")
		if after[w] == before[w]+1 && after["worker-0"]+after["worker-1"] == before["worker-0"]+before["worker-1"]+1 {
			out = append(out, s)
		}
	}
	if len(out) < n {
		t.Fatalf("only %d of 256 sessions hash to %s, want %d", len(out), w, n)
	}
	return out
}

// chat sends one chat request in session s, of tokens tokens at least, and returns its status.
func chat(t *testing.T, h *harness, key, s string, tokens int) int {
	c := unixClient(agent.ServingSocket(h.runDir))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	body := `{"model":"` + hServed + `","messages":[{"role":"user","content":"hello"}],"max_tokens":` + strconv.Itoa(tokens) + `,"min_tokens":` + strconv.Itoa(tokens) + `}`
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://instance/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-session-id", s)
	resp, err := c.Do(req)
	if err != nil {
		t.Errorf("a request in session %s: %v", s, err)
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestAHeavySessionPastTheBoundSpillsToTheIdleSibling(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=50ms"})
	h.set(d)
	r := h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	s := sessionsOn(t, h, "worker-1", 6)

	// Six heavy sessions, about 15 s each, all hashed to worker-1, each sent once Envoy shows the one
	// before it active. Judged by Envoy's own active requests, which the agent's short probes through the
	// router, hashed on no session, do not move.
	//
	// The agent's own probe through the router, a short request, is now and then in flight at a pick and
	// counts in the bound, which can move the first spill one session earlier; so sessions 1 to 4 must
	// stay on worker-1, the first spill must come at the 5th or 6th, and by the 6th worker-0 must have one.
	// Each read waits until exactly the sessions sent are active, so a probe is never counted as one.
	var wg sync.WaitGroup
	codes := make([]int, len(s))
	firstSpill := 0
	for i := range s {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = chat(t, h, key, s[i], 300)
		}(i)
		deadline := time.Now().Add(30 * time.Second)
		var a map[string]int
		for a = hostStat(t, h.runDir, "rq_active"); a["worker-0"]+a["worker-1"] != i+1; a = hostStat(t, h.runDir, "rq_active") {
			if time.Now().After(deadline) {
				t.Fatalf("heavy session %d never showed active: %v", i+1, a)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("heavy session %d, hashed to worker-1: Envoy shows %v active", i+1, a)
		if a["worker-0"] > 0 && firstSpill == 0 {
			firstSpill = i + 1
		}
	}
	if firstSpill < 5 {
		t.Errorf("the first spill came at heavy session %d; want none before the 5th, under the bound", firstSpill)
	}
	if firstSpill == 0 {
		t.Errorf("none of %d heavy sessions hashed to worker-1 spilled to the idle worker-0: no bound", len(s))
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusOK {
			t.Errorf("heavy session %d: %d", i+1, c)
		}
	}
}
