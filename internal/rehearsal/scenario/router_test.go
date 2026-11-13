package scenario_test

// The whole stack with Instances behind Envoy (#454): hpcburst up --instance-router envoy, Envoy's
// build placed beside the agent's, and a direct-mode run on the fake cluster. The controller records
// the router in the Instance's launch; the Provider's controller stages Envoy by its pinned digest; the
// agent runs it bare from that staged file, as it runs frpc; and a request through the front crosses
// it, which Envoy's own request counter shows.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// portsRouter: #454's block is 30360-30379; hpcburst up takes base to base+12.
var portsRouter = stack.Ports{Base: 30360, ClientAddr: "127.0.0.1:30373", DialHomeListen: "127.0.0.1:30374"}

// pinnedEnvoy is Envoy 1.40.2's published release sum for this machine's architecture, written out as
// envoy-pins.txt carries it.
func pinnedEnvoy(t *testing.T) string {
	t.Helper()
	sum := map[string]string{
		"amd64": "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc",
		"arm64": "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf",
	}[runtime.GOARCH]
	if sum == "" {
		t.Fatalf("no Envoy is pinned for %s", runtime.GOARCH)
	}
	return sum
}

// stagedEnvoy is the Envoy file the controller staged on the cluster (<staging dir>/envoy/<digest>/envoy,
// agentapi.StagedEnvoyPath), resolved.
func (r *rig) stagedEnvoy() string {
	r.t.Helper()
	var found []string
	_ = filepath.WalkDir(r.cl.Scratch, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && d.Name() == agentapi.EnvoyBinaryName && filepath.Base(filepath.Dir(filepath.Dir(p))) == "envoy" {
			found = append(found, p)
		}
		return nil
	})
	if len(found) != 1 {
		r.t.Fatalf("%d staged Envoy files under %s (%v), want 1", len(found), r.cl.Scratch, found)
	}
	p, err := filepath.EvalSymlinks(found[0])
	if err != nil {
		r.t.Fatal(err)
	}
	return p
}

// envoyRequests is Envoy's own count of requests sent to worker-0, from its admin socket.
func envoyRequests(t *testing.T, admin string) int {
	t.Helper()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", admin)
	}}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Get("http://admin/clusters?format=json")
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
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("Envoy's /clusters?format=json: %v", err)
	}
	for _, c := range doc.Clusters {
		for _, h := range c.Hosts {
			if h.Hostname != agentapi.WorkerName(0) {
				continue
			}
			for _, s := range h.Stats {
				if s.Name == "rq_total" {
					n, _ := strconv.Atoi(strings.Trim(string(s.Value), `"`))
					return n
				}
			}
			return 0
		}
	}
	t.Fatalf("Envoy's /clusters names no worker-0: %.300s", b)
	return 0
}

// TestRehearsalInstanceRouter is the whole stack with the Instance behind Envoy.
//
//livecheck:none CI's rehearsal job runs it (hack/rehearsal.sh)
func TestRehearsalInstanceRouter(t *testing.T) {
	r := startRigWith(t, rigSetup{mode: "direct", ports: portsRouter, instanceRouter: true})
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	in := r.stepStart()
	if in.Spec.Launch.Router != v1alpha1.InstanceRouterEnvoy {
		t.Errorf("the Instance's launch.router is %q, want envoy: up's --instance-router did not reach the controller", in.Spec.Launch.Router)
	}

	// Staged by its pinned digest, with exactly the pinned bytes, mode 700.
	staged, sum := r.stagedEnvoy(), pinnedEnvoy(t)
	if filepath.Base(filepath.Dir(staged)) != sum {
		t.Errorf("Envoy is staged at %s, want under envoy/%s/", staged, sum)
	}
	b, err := os.ReadFile(staged)
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(b); hex.EncodeToString(got[:]) != sum {
		t.Errorf("the staged Envoy hashes to %x, want the pin %s", got, sum)
	}
	if fi, err := os.Stat(staged); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("the staged Envoy: %v, %v; want mode 700", fi, err)
	}

	// The agent runs it bare, from that file: a job process whose executable is the staged Envoy, with
	// the agent's config in the job's run directory.
	id, err := strconv.Atoi(in.Status.JobID)
	if err != nil {
		t.Fatalf("job ID %q: %v", in.Status.JobID, err)
	}
	var runDir string
	stack.WaitFor(t, stack.WaitDeadline, "the job to run the staged Envoy", func() bool {
		pids, err := r.cl.Plat.Processes(id)
		if err != nil {
			return false
		}
		for _, p := range pids {
			if stack.ProcessExe(p) != staged {
				continue
			}
			args, err := os.ReadFile("/proc/" + strconv.Itoa(p) + "/cmdline")
			if err != nil {
				continue
			}
			argv := strings.Split(strings.TrimRight(string(args), "\x00"), "\x00")
			for i := 0; i+1 < len(argv); i++ {
				if argv[i] == "-c" && filepath.Base(argv[i+1]) == "envoy.json" {
					runDir = filepath.Dir(argv[i+1])
					return true
				}
			}
		}
		return false
	})

	// A request through the front crosses Envoy: its own request counter rises by it.
	admin := filepath.Join(runDir, "envoy-admin.sock")
	before := envoyRequests(t, admin)
	key := r.fc.KeyFor(in)
	reply := r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 32, 8, false, ""), key)
	if reply.Status != http.StatusOK {
		t.Fatalf("a request through the front: %d %.200q", reply.Status, reply.Body)
	}
	if after := envoyRequests(t, admin); after <= before {
		t.Errorf("Envoy's requests to worker-0 went from %d to %d: the request did not cross the router", before, after)
	}

	// One worker behind Envoy (HBEP-7, Several workers, Test plan): a worker restart leaves ready for
	// verifying and returns, as in alpha. The engine is killed through a pidfd.
	engines := r.engineProcesses(id)
	if len(engines) != 1 {
		t.Fatalf("job %d runs %d engines %v, want one", id, len(engines), engines)
	}
	pidfdKill(t, engines[0], "--uds=")
	killAt := time.Now()
	stack.WaitFor(t, 3*stack.WaitDeadline, "worker-0 restarted and the Instance ready again", func() bool {
		now, ok := r.instance(in.Name)
		return ok && now.Status.Phase == v1alpha1.InstanceReady && len(now.Status.Restarts) == 1 &&
			now.Annotations[v1alpha1.RestartGenerationAnnotation(agentapi.WorkerName(0))] == "1"
	})
	if now, _ := r.instance(in.Name); now.Status.Restarts[0].Part != agentapi.WorkerName(0) || now.Status.Restarts[0].Signal != v1alpha1.ReasonPartExited {
		t.Errorf("the restart is %+v, want worker-0's for PartExited", now.Status.Restarts[0])
	}
	r.syncWatch(stack.KindInstances, "the Instance's phases after worker-0's kill")
	left, ok := r.watch.First(in.UID, killAt, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceVerifying })
	if !ok {
		t.Fatal("the watch never saw the Instance verifying after worker-0's kill: with one worker, a worker restart leaves ready, as in alpha")
	}
	if _, back := r.watch.First(in.UID, left.At, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceReady }); !back {
		t.Error("the watch never saw the Instance ready again after its worker's restart")
	}
	if reply := r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 32, 8, false, ""), key); reply.Status != http.StatusOK {
		t.Errorf("a request after the worker's restart: %d %.200q", reply.Status, reply.Body)
	}
}
