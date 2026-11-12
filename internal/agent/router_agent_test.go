package agent_test

// The agent runs the router (router.go; #429, beta, opt-in): a desired Instance with a router gets
// the real Envoy 1.40.2 in front of its worker, the forwarder serves the Instance's path to Envoy's
// socket, the worker gets the 120 s keep-alive, and the router is restarted only when its generation
// rises. Without a router, the agent is alpha's: the pins below hold what it runs to main's own output,
// with the router's runs as their positive controls.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// routerState runs the harness's Instance with Envoy in front of its worker, whose generation the
// controller has verified (DesiredWorker.VerifiedGeneration), so the router may route to it.
func routerState(h *harness, instGen, workerGen, routerGen int64) agentapi.DesiredState {
	d := h.runState("", instGen, workerGen, nil)
	d.Instance.Router = &agentapi.DesiredRouter{Kind: agentapi.RouterEnvoy, Generation: routerGen}
	verified := workerGen
	d.Instance.Workers[0].VerifiedGeneration = &verified
	return d
}

// routerServing: worker-0 and the router run, the router at gen, and the router's own /clusters, read
// after worker-0's process started, holds worker-0 healthy.
func routerServing(gen int64) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		if !routerRunning(gen)(r) {
			return false
		}
		rr, w := routerOf(r), worker0(r)
		if rr.ClustersReadAt == nil || w.StartTime == nil || !rr.ClustersReadAt.After(*w.StartTime) {
			return false
		}
		for _, rw := range rr.Workers {
			if rw.Name == agentapi.WorkerName(0) && rw.Healthy {
				return true
			}
		}
		return false
	}
}

func routerOf(r agentapi.Report) *agentapi.RouterReport {
	if r.Instance == nil {
		return nil
	}
	return r.Instance.Router
}

func routerRunning(gen int64) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		rr := routerOf(r)
		return running(r) && rr != nil && rr.Phase == agentapi.PartRunning && rr.Generation == gen
	}
}

// serve sends GET path to the agent's serving socket, as frpc's proxy does, and returns the status
// and body.
func serve(t *testing.T, h *harness, path string) (int, string) {
	t.Helper()
	c := unixClient(agent.ServingSocket(h.runDir))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET %s through the serving socket: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// envoyRequests is how many requests Envoy has sent worker-0, from its admin's /clusters?format=json:
// the count shows a request went through Envoy, which a forwarder aimed at the worker would not.
func envoyRequests(t *testing.T, runDir string) int {
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
	for _, cl := range doc.Clusters {
		for _, h := range cl.Hosts {
			if cl.Name != "workers" || h.Hostname != agentapi.WorkerName(0) {
				continue
			}
			for _, s := range h.Stats {
				if s.Name == "rq_total" {
					// A zero counter's value is absent or null; a uint64 is a string in Envoy's JSON.
					v := strings.Trim(string(s.Value), `"`)
					if v == "" || v == "null" {
						return 0
					}
					n, err := strconv.Atoi(v)
					if err != nil {
						t.Fatalf("rq_total %s: %v", s.Value, err)
					}
					return n
				}
			}
			return 0 // Envoy leaves a zero counter out
		}
	}
	t.Fatalf("Envoy's /clusters names no worker-0 in workers: %+v", doc)
	return 0
}

// pidfdKill sends SIGKILL to the process pid through a pidfd (pidfd_open, then pidfd_send_signal), after
// checking that pid is Envoy. Envoy is a child of this test process (the agent runs in it) and its
// process group's leader stays unreaped until the agent reaps it, so the PID names Envoy while the
// descriptor is open.
func pidfdKill(t *testing.T, pid int) {
	t.Helper()
	fd, _, errno := syscall.Syscall(434, uintptr(pid), 0, 0) // pidfd_open
	if errno != 0 {
		t.Fatalf("pidfd_open %d: %v", pid, errno)
	}
	defer syscall.Close(int(fd))
	// The agent runs Envoy as /proc/self/fd/3 with argv[0] "envoy" (StartEnvoy), so its command line,
	// not its comm, names it.
	if cmd, err := os.ReadFile("/proc/" + itoa(pid) + "/cmdline"); err != nil || !strings.HasPrefix(string(cmd), "envoy\x00") {
		t.Fatalf("process %d runs %q (%v), not Envoy", pid, cmd, err)
	}
	if _, _, errno := syscall.Syscall6(424, fd, uintptr(syscall.SIGKILL), 0, 0, 0, 0); errno != 0 { // pidfd_send_signal
		t.Fatalf("pidfd_send_signal %d: %v", pid, errno)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

func TestTheAgentRunsTheRouterInFrontOfItsWorker(t *testing.T) {
	bin := testenv.EnvoyBinary(t)
	h := newHarness(t, harnessOptions{envoy: bin})
	h.set(routerState(h, 1, 0, 0))
	r := h.waitReport("worker-0 behind a router that holds it healthy", time.Minute, routerServing(0))
	rr := routerOf(r)
	if rr.Kind != agentapi.RouterEnvoy || rr.Socket != filepath.Join(h.runDir, "router.sock") || rr.PID <= 0 || rr.StartTime == nil {
		t.Errorf("the router's report: %+v", rr)
	}
	if r.Instance.ServingEndpoint != "unix:"+agent.ServingSocket(h.runDir) {
		t.Errorf("serving endpoint %q", r.Instance.ServingEndpoint)
	}
	// What runs is the pinned binary, with the config the agent wrote.
	exe, err := os.Open("/proc/" + itoa(rr.PID) + "/exe")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.New()
	_, _ = io.Copy(sum, exe)
	exe.Close()
	if got := hex.EncodeToString(sum.Sum(nil)); got != "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc" && got != "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf" {
		t.Errorf("the router runs a binary with sha256 %s, not Envoy 1.40.2's", got)
	}
	cmdline, _ := os.ReadFile("/proc/" + itoa(rr.PID) + "/cmdline")
	if !strings.Contains(string(cmdline), filepath.Join(h.runDir, "envoy.json")) {
		t.Errorf("the router's command line %q does not name the agent's config", strings.ReplaceAll(string(cmdline), "\x00", " "))
	}
	// The serving path goes through Envoy to the worker. The agent's own probe of the router goes
	// through Envoy too, every probe interval, so the count is checked against 50 requests sent at once:
	// a serving path that skipped Envoy would add only the few probes of that moment.
	const sent = 50
	before := envoyRequests(t, h.runDir)
	for i := 0; i < sent; i++ {
		if status, body := serve(t, h, "/health"); status != http.StatusOK {
			t.Fatalf("GET /health through the serving socket: %d %q, want the worker's 200", status, body)
		}
	}
	if after := envoyRequests(t, h.runDir); after < before+sent {
		t.Errorf("Envoy sent worker-0 %d requests, then %d, for %d sent: the serving path did not go through it", before, after, sent)
	}
}

// Alpha's agent runs no router: no Envoy file or socket in the run directory, no router in the report,
// and the serving path goes straight to the worker. TestTheAgentRunsTheRouterInFrontOfItsWorker is its
// positive control.
func TestAlphaAgentStartsNoRouter(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: "/nonexistent/envoy"})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("worker-0 running", time.Minute, running)
	if r.Instance.Router != nil {
		t.Errorf("an alpha Instance's report has a router: %+v", r.Instance.Router)
	}
	if status, _ := serve(t, h, "/health"); status != http.StatusOK {
		t.Errorf("GET /health through the serving socket: %d, want the worker's 200", status)
	}
	for _, f := range []string{"router.sock", "envoy-admin.sock", "envoy.json", "envoy.log"} {
		if _, err := os.Lstat(filepath.Join(h.runDir, f)); !os.IsNotExist(err) {
			t.Errorf("alpha's run directory holds %s (%v)", f, err)
		}
	}
	for _, ex := range h.execs() {
		for _, kv := range ex.Env {
			if strings.Contains(kv, "VLLM_HTTP_TIMEOUT_KEEP_ALIVE") {
				t.Errorf("alpha's worker got %s", kv)
			}
		}
	}
}

// The worker's environment and arguments without a router are main's (3fc6001d), captured once from
// main's own WorkerExec for these inputs; with a router the environment differs by exactly the
// keep-alive, at 120 s, replacing a launch's own value.
func TestAlphaWorkerEnvIsUnchanged(t *testing.T) {
	ex := agent.WorkerExec{
		Apptainer: "/opt/apptainer", Image: "/staging/images/img.sif", CheckpointDir: "/staging/checkpoints/org/m/rev",
		RunDir: "/run/job", CacheDir: "/alloc/cache",
		Launch: agentapi.Launch{
			Image: "docker.io/vllm/vllm-openai", ImageDigest: "sha256:1111", Command: []string{"vllm", "serve", "/hpcburst/model", "--max-num-seqs=256"},
			Env: []agentapi.EnvVar{{Name: "EXAMPLE_VAR", Value: "1"}, {Name: "VLLM_HTTP_TIMEOUT_KEEP_ALIVE", Value: "5"}}, KeyEnv: "VLLM_API_KEY",
		},
		Key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", BaseEnv: []string{"PATH=/usr/bin", "CUDA_VISIBLE_DEVICES=0", "APPTAINERENV_STRAY=x"},
	}
	mainEnv := []string{"PATH=/usr/bin", "CUDA_VISIBLE_DEVICES=0", "APPTAINERENV_EXAMPLE_VAR=1", "APPTAINERENV_VLLM_HTTP_TIMEOUT_KEEP_ALIVE=5", "APPTAINERENV_CUDA_VISIBLE_DEVICES=0", "APPTAINERENV_XDG_CACHE_HOME=/hpcburst/cache/xdg", "APPTAINERENV_VLLM_CACHE_ROOT=/hpcburst/cache/vllm", "APPTAINERENV_TORCHINDUCTOR_CACHE_DIR=/hpcburst/cache/torchinductor", "APPTAINERENV_TRITON_CACHE_DIR=/hpcburst/cache/triton", "APPTAINERENV_FLASHINFER_WORKSPACE_BASE=/hpcburst/cache/flashinfer", "APPTAINERENV_CUDA_CACHE_PATH=/hpcburst/cache/cuda", "APPTAINERENV_VLLM_API_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	mainArgs := []string{"exec", "--nv", "--cleanenv", "--no-home", "--bind", "/staging/checkpoints/org/m/rev:/hpcburst/model:ro", "--bind", "/run/job:/hpcburst/run", "--bind", "/alloc/cache:/hpcburst/cache", "/staging/images/img.sif", "vllm", "serve", "/hpcburst/model", "--max-num-seqs=256"}
	env, err := ex.Env()
	if err != nil || !reflect.DeepEqual(env, mainEnv) {
		t.Errorf("alpha's environment changed (%v):\n got %q\nwant %q", err, env, mainEnv)
	}
	if args := ex.Args(); !reflect.DeepEqual(args, mainArgs) {
		t.Errorf("alpha's arguments changed:\n got %q\nwant %q", args, mainArgs)
	}
	ex.RouterKeepAlive = true
	routed, err := ex.Env()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, kv := range mainEnv {
		if kv != "APPTAINERENV_VLLM_HTTP_TIMEOUT_KEEP_ALIVE=5" {
			want = append(want, kv)
		}
	}
	want = append(want, "APPTAINERENV_VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120")
	if !reflect.DeepEqual(routed, want) {
		t.Errorf("behind a router:\n got %q\nwant %q", routed, want)
	}
	if args := ex.Args(); !reflect.DeepEqual(args, mainArgs) {
		t.Errorf("the router changed the arguments:\n got %q\nwant %q", args, mainArgs)
	}
}

// The real worker the agent starts behind the router gets the keep-alive.
func TestAWorkerBehindTheRouterGetsTheKeepAlive(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t)})
	h.set(routerState(h, 1, 0, 0))
	h.waitReport("the router running", time.Minute, routerRunning(0))
	execs := h.execs()
	if len(execs) == 0 {
		t.Fatal("no worker was started")
	}
	// The fake Apptainer records the container's environment, as the engine sees it.
	n := 0
	for _, kv := range execs[len(execs)-1].Env {
		if strings.HasPrefix(kv, "VLLM_HTTP_TIMEOUT_KEEP_ALIVE=") {
			n++
			if kv != "VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120" {
				t.Errorf("the worker got %s, want 120", kv)
			}
		}
	}
	if n != 1 {
		t.Errorf("the worker's environment names the keep-alive %d times, want once", n)
	}
}

// A router that exits is reported exited and is not started again until its generation rises
// (HBEP-7: the agent never restarts a part by itself). Meanwhile the forwarder answers 503 itself and
// never sends the path to the worker, which would skip the router. A router restart does not touch
// the worker.
func TestTheAgentNeverRestartsTheRouterByItself(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t)})
	h.set(routerState(h, 1, 0, 0))
	r := h.waitReport("the router serving", time.Minute, routerServing(0))
	pid, workerPID := routerOf(r).PID, worker0(r).PID
	pidfdKill(t, pid)
	r = h.waitReport("the router exited", 30*time.Second, func(r agentapi.Report) bool {
		rr := routerOf(r)
		return rr != nil && rr.Phase == agentapi.PartExited
	})
	if rr := routerOf(r); rr.ExitCode == nil || rr.PID != pid {
		t.Errorf("the exited router's report: %+v", rr)
	}
	if r.Instance.ServingEndpoint != "" {
		t.Errorf("serving endpoint %q while the router is down, want none", r.Instance.ServingEndpoint)
	}
	status, body := serve(t, h, "/health")
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "not serving") {
		t.Errorf("GET /health with the router down: %d %q, want the forwarder's own 503: the worker must not be reached past the router", status, body)
	}
	// Five more reports, by their sequence numbers, and the router has not been started again.
	seq := r.Seq
	r = h.waitReport("five more reports", 30*time.Second, func(r agentapi.Report) bool { return r.Seq >= seq+5 })
	if rr := routerOf(r); rr.Phase != agentapi.PartExited || rr.PID != pid {
		t.Fatalf("the agent restarted the router by itself: %+v", rr)
	}
	// The controller's router rung raises its generation: only then does it start again.
	h.set(routerState(h, 1, 0, 1))
	r = h.waitReport("the router serving at generation 1", time.Minute, routerServing(1))
	if rr := routerOf(r); rr.PID == pid {
		t.Errorf("the router at generation 1 has the old PID %d", pid)
	}
	if w := worker0(r); w.PID != workerPID || w.Generation != 0 {
		t.Errorf("the router's restart touched the worker: %+v, was PID %d", w, workerPID)
	}
	if status, body := serve(t, h, "/health"); status != http.StatusOK {
		t.Errorf("GET /health after the router's restart: %d %q, want the worker's 200", status, body)
	}
}

// A worker restart leaves the router running: its config names the worker's socket, which a
// restarted worker listens on again, so nothing is rendered or restarted.
func TestAWorkerRestartLeavesTheRouterRunning(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t)})
	h.set(routerState(h, 1, 0, 0))
	r := h.waitReport("the router running", time.Minute, routerRunning(0))
	pid, workerPID := routerOf(r).PID, worker0(r).PID
	cfg, err := os.ReadFile(filepath.Join(h.runDir, "envoy.json"))
	if err != nil {
		t.Fatal(err)
	}
	h.set(routerState(h, 1, 1, 0))
	r = h.waitReport("worker-0 at generation 1, held healthy again", time.Minute, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Generation == 1 && w.Phase == agentapi.PartRunning && w.PID != workerPID && routerServing(0)(r)
	})
	if rr := routerOf(r); rr.PID != pid || rr.Phase != agentapi.PartRunning {
		t.Errorf("a worker restart restarted the router: %+v, was PID %d", rr, pid)
	}
	if again, _ := os.ReadFile(filepath.Join(h.runDir, "envoy.json")); string(again) != string(cfg) {
		t.Error("a worker restart rendered the router's config again")
	}
	if status, _ := serve(t, h, "/health"); status != http.StatusOK {
		t.Errorf("GET /health after the worker's restart: %d, want 200", status)
	}
}

// A router start that fails (here: no Envoy binary) is reported exited with its reason, and the
// forwarder, in router mode, answers 503 itself rather than reach the worker past the router.
func TestARouterThatCannotStartIsReportedAndBypassesNothing(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: ""})
	h.set(routerState(h, 1, 0, 0))
	r := h.waitReport("the router's start refused", time.Minute, func(r agentapi.Report) bool {
		rr := routerOf(r)
		return running(r) && rr != nil && rr.Phase == agentapi.PartExited
	})
	rr := routerOf(r)
	if rr.ExitCode == nil || *rr.ExitCode != 127 || !strings.Contains(rr.LogTail, "no Envoy binary") {
		t.Errorf("the refused router's report: %+v", rr)
	}
	status, body := serve(t, h, "/health")
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "not serving") {
		t.Errorf("GET /health with no router: %d %q, want the forwarder's 503: never the worker past the router", status, body)
	}
}

// The forwarder in router mode: the router's socket is the target, and the workers' SetTarget, which
// alpha's code calls as a worker starts and stops, changes nothing until router mode ends.
func TestTheForwarderInRouterModeIgnoresTheWorkers(t *testing.T) {
	f, err := agent.NewForwarder(filepath.Join(shortDir(t), "serve.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	step := func(what, want string) {
		t.Helper()
		if got := f.Target(); got != want {
			t.Errorf("after %s the target is %q, want %q", what, got, want)
		}
	}
	f.SetTarget("worker.sock")
	step("alpha's SetTarget", "worker.sock")
	f.RouteTo("router.sock")
	step("RouteTo", "router.sock")
	f.SetTarget("worker.sock")
	step("a worker's SetTarget in router mode", "router.sock")
	f.SetTarget("")
	step("a worker's SetTarget(\"\") in router mode", "router.sock")
	f.RouteTo("")
	step("RouteTo(\"\")", "")
	f.SetTarget("worker.sock")
	step("a worker's SetTarget with the router down", "")
	if !f.Routed() {
		t.Error("not routed after RouteTo")
	}
	f.LeaveRouter()
	f.SetTarget("worker.sock")
	step("LeaveRouter and a worker's SetTarget", "worker.sock")
}

// The agent's report carries the router's own view of its worker, read from the running Envoy's
// /clusters, and the agent's probe through the router. Taking the worker's verification away makes
// Envoy hold it failing (the readiness endpoint answers 503): the report then shows the failing check,
// counted in a row, and the probe failing too, since no worker is healthy. Giving it back clears both.
func TestTheReportCarriesTheRoutersViewOfItsWorker(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t)})
	h.set(routerState(h, 1, 0, 0))
	r := h.waitReport("the router serving", time.Minute, routerServing(0))
	if p := routerOf(r).Probe; p == nil || !p.OK {
		t.Errorf("with worker-0 healthy, the router's probe is %+v, want OK", p)
	}
	unverified := routerState(h, 1, 0, 0)
	unverified.Instance.Workers[0].VerifiedGeneration = nil
	h.set(unverified)
	r = h.waitReport("Envoy holding worker-0 failing for 2 reads, and the probe failing", time.Minute, func(r agentapi.Report) bool {
		rr := routerOf(r)
		if rr == nil || rr.Probe == nil || rr.Probe.OK || rr.Probe.ConsecutiveFailures < 2 {
			return false
		}
		for _, w := range rr.Workers {
			if w.Name == agentapi.WorkerName(0) && !w.Healthy && w.FailedActiveCheck && w.ConsecutiveFailedChecks >= 2 {
				return true
			}
		}
		return false
	})
	if w := worker0(r); w.Phase != agentapi.PartRunning {
		t.Errorf("worker-0 is %s: the test needs it running and only unverified", w.Phase)
	}
	h.set(routerState(h, 1, 0, 0))
	r = h.waitReport("worker-0 healthy again", time.Minute, func(r agentapi.Report) bool {
		rr := routerOf(r)
		if !routerServing(0)(r) || rr.Probe == nil || !rr.Probe.OK {
			return false
		}
		for _, w := range rr.Workers {
			if w.Name == agentapi.WorkerName(0) && w.ConsecutiveFailedChecks == 0 {
				return true
			}
		}
		return false
	})
}

// routerStateModes is routerState with the engine double's modes.
func routerStateModes(h *harness, modes string, instGen, workerGen, routerGen int64) agentapi.DesiredState {
	d := h.runState(modes, instGen, workerGen, nil)
	d.Instance.Router = &agentapi.DesiredRouter{Kind: agentapi.RouterEnvoy, Generation: routerGen}
	verified := workerGen
	d.Instance.Workers[0].VerifiedGeneration = &verified
	return d
}

// The whole-Instance rung (HBEP-7, rung 2) on an Instance behind a router: every part stops, the
// staged files are checked again, and the worker and the router start again at the new generation.
// The worker ignores TERM until its KILL after the stop grace, so Envoy is gone first, as with a real
// engine: a router started again before the restage would be a live part never stopped, and the
// restage would never come (#442's review). A worker that exits at once on TERM could win that race
// and prove nothing.
func TestAWholeInstanceRestartRestagesBehindTheRouter(t *testing.T) {
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t)})
	h.set(routerStateModes(h, "ignore-term", 1, 0, 0))
	r := h.waitReport("worker-0 and the router running", time.Minute, routerRunning(0))
	oldWorker, oldRouter := worker0(r).PID, routerOf(r).PID
	h.set(routerStateModes(h, "ignore-term", 2, 0, 0))
	r = h.waitReport("worker-0 and the router running again at Instance generation 2", 2*time.Minute, func(r agentapi.Report) bool {
		w, rr := worker0(r), routerOf(r)
		return r.Instance != nil && r.Instance.Generation == 2 && w != nil && w.Phase == agentapi.PartRunning && w.PID != oldWorker &&
			rr != nil && rr.Phase == agentapi.PartRunning && rr.PID != oldRouter
	})
	h.waitReport("the router holding the restarted worker healthy", time.Minute, routerServing(0))
	if status, body := serve(t, h, "/health"); status != http.StatusOK {
		t.Errorf("GET /health after the whole-Instance restart: %d %q, want the worker's 200", status, body)
	}
}

// pidfdExited reports whether the process behind a pidfd has exited, waiting up to within for it.
func pidfdExited(t *testing.T, fd int, within time.Duration) bool {
	t.Helper()
	fds := []struct {
		fd      int32
		events  int16
		revents int16
	}{{fd: int32(fd), events: 0x1}} // POLLIN: the process has exited
	ts := syscall.NsecToTimespec(int64(within))
	n, _, errno := syscall.Syscall6(syscall.SYS_PPOLL, uintptr(unsafe.Pointer(&fds[0])), 1, uintptr(unsafe.Pointer(&ts)), 0, 0, 0)
	if errno != 0 {
		t.Fatalf("ppoll on the pidfd: %v", errno)
	}
	return n == 1 && fds[0].revents&0x1 != 0
}

// An Envoy whose start ends while the agent ends is stopped, never left running (#442's review): at
// the stage "started" the agent has already ended when the start tries to hand Envoy over, so the
// start stops it; at "held" the hand-over went to a loop that has ended, so shutdown stops it. Each
// case opens a pidfd on that Envoy and requires it to fire once the agent's Run has returned.
func TestAnEnvoyStartedWhileTheAgentEndsIsStopped(t *testing.T) {
	bin := testenv.EnvoyBinary(t)
	for _, stage := range []string{"started", "held"} {
		t.Run(stage, func(t *testing.T) {
			h := newHarness(t, harnessOptions{envoy: bin})
			fired := make(chan int, 1)
			reset := agent.SetRouterStartHookForTest(func(s string, p *agent.EnvoyProc) {
				if s != stage || p == nil {
					return
				}
				fd, _, errno := syscall.Syscall(434, uintptr(p.Group.PID()), 0, 0) // pidfd_open
				if errno != 0 {
					t.Errorf("pidfd_open: %v", errno)
					return
				}
				h.cancel() // the agent's context ends during the router's start
				// Wait for the agent's shutdown to begin, an observable state, before the hand-over.
				for deadline := time.Now().Add(30 * time.Second); !agent.RouterHoldDownForTest(h.agent); {
					if time.Now().After(deadline) {
						t.Errorf("the agent's shutdown did not begin within 30 s")
						break
					}
					time.Sleep(time.Millisecond)
				}
				select {
				case fired <- int(fd):
				default:
				}
			})
			defer reset()
			h.set(routerState(h, 1, 0, 0))
			var fd int
			select {
			case fd = <-fired:
			case <-time.After(time.Minute):
				t.Fatal("the router's start never reached the stage")
			}
			defer syscall.Close(fd)
			h.stop() // waits for Run to return
			if !pidfdExited(t, fd, 10*time.Second) {
				t.Errorf("Envoy is still running after the agent ended (stage %s)", stage)
			}
		})
	}
}
