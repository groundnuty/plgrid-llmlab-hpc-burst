package agent_test

// A worker behind the real Envoy 1.40.2 dies (the science agent's ruling on #439, PR 4's finding;
// HBEP-5, Worker health; HBEP-7, Several workers). Until Envoy's next active check, up to 5 s later, Envoy
// still holds the dead worker healthy:
//   - a request it sends there fails to connect, and the route's retry sends it once to another worker,
//     so with a healthy sibling no client sees a 503;
//   - the worker's restarted process starts only once Envoy holds the worker unhealthy, so no request
//     reaches it before its verification. The seam is the agent's restart hook, which runs just before
//     the new process starts and reads Envoy's own view of the worker there.
// Both at two workers and at one behind the router. The engine is killed through a pidfd.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// lastHealth is the health flags of envoyHolds' last read, for a failure's message.
var lastHealth string

// envoyHolds reads, from Envoy's admin, whether Envoy holds worker w out of its routing, and the worker's
// rq_total.
func envoyHolds(runDir, w string) (failing bool, requests int, err error) {
	c := unixClient(filepath.Join(runDir, agent.EnvoyAdminSocket))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := c.Get("http://admin/clusters?format=json")
	if err != nil {
		return false, 0, err
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
				Health struct {
					FailedActive  bool `json:"failed_active_health_check"`
					FailedOutlier bool `json:"failed_outlier_check"`
					PendingActive bool `json:"pending_active_hc"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return false, 0, err
	}
	for _, cl := range doc.Clusters {
		for _, h := range cl.Hosts {
			if h.Hostname != w {
				continue
			}
			for _, s := range h.Stats {
				if s.Name == "rq_total" {
					requests, _ = strconv.Atoi(strings.Trim(string(s.Value), `"`))
				}
			}
			// Envoy routes to it only while none of its active check failing, its outlier ejection or its
			// first check still pending holds (as the agent reads it, parseClusters).
			lastHealth = fmt.Sprintf("failed_active %v, failed_outlier %v, pending_active %v", h.Health.FailedActive, h.Health.FailedOutlier, h.Health.PendingActive)
			return h.Health.FailedActive || h.Health.FailedOutlier || h.Health.PendingActive, requests, nil
		}
	}
	return false, 0, fmt.Errorf("Envoy's /clusters names no %s", w)
}

// send sends one request through the serving socket in the session given and returns its status.
func send(t *testing.T, h *harness, session string) (int, string) {
	t.Helper()
	c := unixClient(agent.ServingSocket(h.runDir))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-session-id", session)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("a request in session %s: %v", session, err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

// sessionOn finds a session that Envoy's ring hash places on worker w, by sending sessions one by one and
// reading w's own request count.
func sessionOn(t *testing.T, h *harness, w string) string {
	t.Helper()
	for i := 0; i < 64; i++ {
		s := "probe-" + strconv.Itoa(i)
		_, before, err := envoyHolds(h.runDir, w)
		if err != nil {
			t.Fatal(err)
		}
		if code, body := send(t, h, s); code != http.StatusOK {
			t.Fatalf("session %s: %d %q", s, code, body)
		}
		if _, after, _ := envoyHolds(h.runDir, w); after > before {
			return s
		}
	}
	t.Fatalf("no session of 64 reached %s", w)
	return ""
}

// engineOf is the engine process of the worker whose leader (the fake Apptainer) is leader: its child that
// runs as the engine.
func engineOf(t *testing.T, leader int) agent.ProcessID {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// The parent PID is the second field after the command, which ends at the last ')'.
		f := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if len(f) < 2 || f[1] != strconv.Itoa(leader) {
			continue
		}
		env, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err != nil || !bytes.Contains(env, []byte("HB_TEST_ROLE=engine\x00")) {
			continue
		}
		id, err := procgroup.IDOf(pid)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	t.Fatalf("no engine process under the leader %d", leader)
	return agent.ProcessID{}
}

// leaderOf is worker w's process in the agent's last report, running at generation gen.
func leaderOf(h *harness, w string, gen int64) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		if r.Instance == nil {
			return false
		}
		for _, wr := range r.Instance.Workers {
			if wr.Name == w && wr.Generation == gen && wr.Phase == agentapi.PartRunning && wr.PID > 0 {
				return true
			}
		}
		return false
	}
}

func pidOf(r agentapi.Report, w string) int {
	for _, wr := range r.Instance.Workers {
		if wr.Name == w {
			return wr.PID
		}
	}
	return 0
}

// restartSeen is what the restart hook saw, just before the restarted process started: whether Envoy held
// the worker failing then, and its request count.
type restartSeen struct {
	mu       sync.Mutex
	at       time.Time
	failing  bool
	requests int
	err      error
	health   string
}

func hookRestart(t *testing.T, h *harness, w string) *restartSeen {
	t.Helper()
	seen := &restartSeen{}
	t.Cleanup(agent.SetRestartStartHookForTest(func(name string) {
		if name != w {
			return
		}
		f, n, err := envoyHolds(h.runDir, w)
		seen.mu.Lock()
		seen.at, seen.failing, seen.requests, seen.err, seen.health = time.Now(), f, n, err, lastHealth
		seen.mu.Unlock()
	}))
	return seen
}

func (s *restartSeen) get() restartSeen {
	s.mu.Lock()
	defer s.mu.Unlock()
	return restartSeen{at: s.at, failing: s.failing, requests: s.requests, err: s.err, health: s.health}
}

// killWorker kills worker w's engine through a pidfd once it runs at generation gen, and waits until the
// agent has seen it exit, while Envoy still routes to it: the window this file is about. It returns when
// the kill was sent.
func killWorker(t *testing.T, h *harness, w string, gen int64) time.Time {
	t.Helper()
	r := h.waitReport(w+" running", time.Minute, leaderOf(h, w, gen))
	pidfdKillID(t, engineOf(t, pidOf(r, w)))
	killAt := time.Now()
	// Once the agent has seen the exit, the engine's connections are closed and Envoy has dropped its
	// pooled ones, so a request opens a new connection, which fails. A request written onto a pooled
	// connection in the milliseconds the engine dies is reset after it was sent, which the retry does not
	// cover by design: nothing written to a worker is retried.
	h.waitReport(w+" exited", time.Minute, func(r agentapi.Report) bool {
		for _, wr := range r.Instance.Workers {
			if wr.Name == w && wr.Phase == agentapi.PartExited {
				return true
			}
		}
		return false
	})
	if out, _, err := envoyHolds(h.runDir, w); err != nil || out {
		t.Fatalf("Envoy already holds %s out of its routing %s after its kill (%s, %v): the test would not reach the window it is about", w, time.Since(killAt).Round(time.Millisecond), lastHealth, err)
	}
	return killAt
}

// TestADeadWorkersRequestsGoToItsSibling: two workers; worker-1's engine is killed. Requests in a session
// the ring hash places on worker-1, sent before Envoy's next check, are each answered by worker-0 through
// the route's retry: no client sees a 503.
func TestADeadWorkersRequestsGoToItsSibling(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	h.set(workersRouterState(h, 2))
	h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	session := sessionOn(t, h, "worker-1")
	killAt := killWorker(t, h, "worker-1", 0)
	for i := 0; i < 10; i++ {
		if code, body := send(t, h, session); code != http.StatusOK {
			t.Fatalf("request %d in worker-1's session, %s after its kill: %d %q, want worker-0's answer through the retry", i, time.Since(killAt).Round(time.Millisecond), code, body)
		}
	}
}

// TestARestartedWorkersProcessWaitsForEnvoy: two workers; worker-1's engine is killed and the controller
// restarts it at once, before any request reaches it. Its new process starts only once Envoy holds it out
// of its routing; meanwhile and until it is verified again, its session's requests go to worker-0 and
// none reaches worker-1; verified again, it takes them once more.
func TestARestartedWorkersProcessWaitsForEnvoy(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	h.set(d)
	h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	session := sessionOn(t, h, "worker-1")
	seen := hookRestart(t, h, "worker-1")
	killAt := killWorker(t, h, "worker-1", 0)
	d.Instance.Workers[1].Generation, d.Instance.Workers[1].VerifiedGeneration = 1, nil
	h.set(d)
	h.waitReport("worker-1's new process running", time.Minute, leaderOf(h, "worker-1", 1))
	s := seen.get()
	if s.at.IsZero() || s.err != nil {
		t.Fatalf("the restart hook saw nothing of worker-1's restart (%v)", s.err)
	}
	if !s.failing {
		t.Errorf("worker-1's new process started %s after its kill while Envoy still held it healthy (%s): a request could reach it before its verification", s.at.Sub(killAt).Round(time.Millisecond), s.health)
	}
	for i := 0; i < 10; i++ {
		if code, body := send(t, h, session); code != http.StatusOK {
			t.Fatalf("request %d in worker-1's session while it rejoins: %d %q", i, code, body)
		}
	}
	if _, n, _ := envoyHolds(h.runDir, "worker-1"); n != s.requests {
		t.Errorf("Envoy sent worker-1 %d requests after its new process started, before its verification", n-s.requests)
	}
	v := int64(1)
	d.Instance.Workers[1].VerifiedGeneration = &v
	h.set(d)
	h.waitReport("Envoy holding both workers healthy again", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	_, before, _ := envoyHolds(h.runDir, "worker-1")
	if code, body := send(t, h, session); code != http.StatusOK {
		t.Fatalf("worker-1's session once it is verified: %d %q", code, body)
	}
	if _, after, _ := envoyHolds(h.runDir, "worker-1"); after <= before {
		t.Error("worker-1's session did not reach worker-1 once it was verified again")
	}
}

// TestARestartedWorkerBehindTheRouterTakesNoRequestBeforeItsVerification: one worker behind the router,
// as main's router runs it. Its restarted process starts only once Envoy holds it failing, and takes no
// request until it is verified again.
func TestARestartedWorkerBehindTheRouterTakesNoRequestBeforeItsVerification(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0"}})
	d := workersRouterState(h, 1)
	h.set(d)
	h.waitReport("Envoy holding worker-0 healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true}))
	seen := hookRestart(t, h, "worker-0")
	killAt := killWorker(t, h, "worker-0", 0)
	d.Instance.Workers[0].Generation, d.Instance.Workers[0].VerifiedGeneration = 1, nil
	h.set(d)
	h.waitReport("worker-0's new process running", time.Minute, leaderOf(h, "worker-0", 1))
	s := seen.get()
	if s.at.IsZero() || s.err != nil {
		t.Fatalf("the restart hook saw nothing of worker-0's restart (%v)", s.err)
	}
	if !s.failing {
		t.Errorf("worker-0's new process started %s after its kill while Envoy still held it healthy (%s)", s.at.Sub(killAt).Round(time.Millisecond), s.health)
	}
	for i := 0; i < 5; i++ {
		send(t, h, "session-"+strconv.Itoa(i)) // Envoy answers 503: no worker is healthy
	}
	if _, n, _ := envoyHolds(h.runDir, "worker-0"); n != s.requests {
		t.Errorf("Envoy sent worker-0 %d requests after its new process started, before its verification", n-s.requests)
	}
}
