package agent_test

// Several workers behind the real Envoy 1.40.2 (HBEP-7, Several workers): the agent renders Envoy's
// configuration with every worker's socket as an endpoint, health-checks each through the readiness port
// by its name, and Envoy routes to each verified worker and to no unverified one. The counts are Envoy's
// own, per host, from its admin's /clusters?format=json.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// workersRouterState is workersState behind Envoy, every worker verified at its generation 0.
func workersRouterState(h *harness, n int) agentapi.DesiredState {
	d := h.workersState(n, "")
	d.Instance.Generation = 1
	d.Instance.Router = &agentapi.DesiredRouter{Kind: agentapi.RouterEnvoy}
	for i := range d.Instance.Workers {
		v := int64(0)
		d.Instance.Workers[i].VerifiedGeneration = &v
	}
	return d
}

// hostRequests is Envoy's rq_total for each worker, from its admin's /clusters?format=json.
func hostRequests(t *testing.T, runDir string) map[string]int {
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
			Name  string `json:"name"`
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
		if cl.Name != "workers" {
			continue
		}
		for _, h := range cl.Hosts {
			out[h.Hostname] = 0
			for _, s := range h.Stats {
				if s.Name != "rq_total" {
					continue
				}
				if v := strings.Trim(string(s.Value), `"`); v != "" && v != "null" {
					n, err := strconv.Atoi(v)
					if err != nil {
						t.Fatalf("rq_total %s: %v", s.Value, err)
					}
					out[h.Hostname] = n
				}
			}
		}
	}
	return out
}

// sendSessions sends n requests through the agent's serving socket, each with its own x-session-id, which
// Envoy's ring hash places.
func sendSessions(t *testing.T, h *harness, from, n int) {
	t.Helper()
	c := unixClient(agent.ServingSocket(h.runDir))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	for i := from; i < from+n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("x-session-id", "session-"+strconv.Itoa(i))
		resp, err := c.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("request %d through the serving socket: %v", i, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: %d %q, want a worker's 200", i, resp.StatusCode, b)
		}
	}
}

// routerHolds reports whether the router's latest read holds each named worker healthy (true) or failing
// its active check (false).
func routerHolds(want map[string]bool) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		rr := routerOf(r)
		if rr == nil || rr.Phase != agentapi.PartRunning || rr.ClustersReadAt == nil {
			return false
		}
		seen := 0
		for _, w := range rr.Workers {
			if h, ok := want[w.Name]; ok {
				if w.Healthy != h || (!h && !w.FailedActiveCheck) {
					return false
				}
				seen++
			}
		}
		return seen == len(want)
	}
}

func TestTwoWorkersBehindTheRouterEachTakeTrafficOnlyWhileVerified(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	h.set(d)
	h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	before := hostRequests(t, h.runDir)
	sendSessions(t, h, 0, 40)
	after := hostRequests(t, h.runDir)
	for _, w := range []string{"worker-0", "worker-1"} {
		if after[w] <= before[w] {
			t.Errorf("Envoy sent %s %d requests, then %d, of 40 sessions: both verified workers should take traffic", w, before[w], after[w])
		}
	}

	// worker-1 unverified: Envoy holds it failing, and the next 40 sessions all go to worker-0.
	d.Instance.Workers[1].VerifiedGeneration = nil
	h.set(d)
	h.waitReport("Envoy holding worker-1 failing", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": false}))
	before = hostRequests(t, h.runDir)
	sendSessions(t, h, 40, 40)
	after = hostRequests(t, h.runDir)
	if after["worker-1"] != before["worker-1"] {
		t.Errorf("Envoy sent the unverified worker-1 %d requests", after["worker-1"]-before["worker-1"])
	}
	if got := after["worker-0"] - before["worker-0"]; got < 40 {
		t.Errorf("worker-0 took %d of the 40 sessions, want all of them", got)
	}
}
