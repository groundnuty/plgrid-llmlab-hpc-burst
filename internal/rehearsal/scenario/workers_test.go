package scenario_test

// The whole stack with an Instance of two workers behind Envoy (HBEP-7, Several workers, its Test plan;
// #439): hpcburst up --instance-router envoy, a partition of two GPUs per node, workersPerInstance 2,
// the real agent and the engine double on the fake cluster. Each step is one of the Test plan's items.
// What would prove nothing is ruled out step by step: each worker's device is read from its own engine
// process and must be one device, its own; a worker is killed while a stream runs on the other; and a
// controller restart is judged by both workers taking requests after it, not by the phase.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// portsWorkers: #439's block is 28400-28419; hpcburst up takes base to base+12.
var portsWorkers = stack.Ports{Base: 28400, ClientAddr: "127.0.0.1:28413", DialHomeListen: "127.0.0.1:28414"}

// twoWorkersStartTimeout is the two-worker test's health.startTimeout: shorter than rehearsalTiming's
// 4m, so step (e2), which waits one out, stays short; a start here reaches ready about a minute after
// its start instruction, the tunnel's 46 s hold included.
const twoWorkersStartTimeout = 150 * time.Second

// twoWorkersLease is the two-worker test's leaseLimit: its steps, (e2)'s timeout included, outlast
// rehearsalTiming's 7m before the Instance's drain.
const twoWorkersLease = "20m"

// twoGPUObjects is the rehearsal's profile with two GPUs per node and twoWorkersStartTimeout, and its
// Provider.
func twoGPUObjects(r *rig, arch, creds string) []client.Object {
	p := stack.BatchProfile(arch, rehearsalTiming)
	p.Spec.Partitions[0].GPUsPerNode = stack.Ptr(int32(2))
	p.Spec.Health.StartTimeout = stack.Dur(twoWorkersStartTimeout.String())
	return []client.Object{p, stack.BatchProvider(creds)}
}

// engineProc is one worker's engine process, as the container runs it.
type engineProc struct {
	pid    int
	worker string   // from its --uds socket
	cvd    []string // every CUDA_VISIBLE_DEVICES in its environment
}

// engineWorkers returns the job's engine processes by worker.
func (r *rig) engineWorkers(jobID int) map[string]engineProc {
	r.t.Helper()
	out := map[string]engineProc{}
	for _, pid := range r.engineProcesses(jobID) {
		env, err1 := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
		cmd, err2 := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
		if err1 != nil || err2 != nil {
			continue
		}
		e := engineProc{pid: pid}
		for _, kv := range bytes.Split(env, []byte{0}) {
			if v, ok := strings.CutPrefix(string(kv), "CUDA_VISIBLE_DEVICES="); ok {
				e.cvd = append(e.cvd, v)
			}
		}
		// The controller's launch passes --uds and the socket as two arguments; --uds=<socket> is read too.
		args := strings.Split(string(cmd), "\x00")
		for i, a := range args {
			sock, ok := strings.CutPrefix(a, "--uds=")
			if !ok && a == "--uds" && i+1 < len(args) {
				sock, ok = args[i+1], true
			}
			if ok {
				e.worker = strings.TrimSuffix(filepath.Base(sock), ".sock")
			}
		}
		if e.worker == "" {
			r.t.Fatalf("engine process %d names no --uds socket: %q", pid, args)
		}
		out[e.worker] = e
	}
	return out
}

// envoyOf finds the job's Envoy process, run from the staged file, and its run directory.
func (r *rig) envoyOf(jobID int) (pid int, runDir string) {
	r.t.Helper()
	staged := r.stagedEnvoy()
	stack.WaitFor(r.t, stack.WaitDeadline, "the job to run the staged Envoy", func() bool {
		pids, err := r.cl.Plat.Processes(jobID)
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
					pid, runDir = p, filepath.Dir(argv[i+1])
					return true
				}
			}
		}
		return false
	})
	return pid, runDir
}

// hostStats is Envoy's per-worker counters, rq_total and rq_active, from its admin socket.
func hostStats(t *testing.T, admin string) map[string]map[string]int {
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
				Health struct {
					FailedActive bool `json:"failed_active_health_check"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("Envoy's /clusters?format=json: %v", err)
	}
	out := map[string]map[string]int{}
	for _, c := range doc.Clusters {
		for _, h := range c.Hosts {
			m := map[string]int{}
			for _, s := range h.Stats {
				m[s.Name], _ = strconv.Atoi(strings.Trim(string(s.Value), `"`))
			}
			// failed_active is 1 while Envoy's active check holds the worker failing.
			if h.Health.FailedActive {
				m["failed_active"] = 1
			}
			out[h.Hostname] = m
		}
	}
	return out
}

// pidfdKill sends SIGKILL to pid through a pidfd (pidfd_open, then pidfd_send_signal), after checking
// its command line names want: never by a bare PID.
func pidfdKill(t *testing.T, pid int, want string) {
	t.Helper()
	fd, _, errno := syscall.Syscall(434, uintptr(pid), 0, 0) // pidfd_open
	if errno != 0 {
		t.Fatalf("pidfd_open %d: %v", pid, errno)
	}
	defer syscall.Close(int(fd))
	if cmd, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline"); err != nil || !strings.Contains(string(cmd), want) {
		t.Fatalf("process %d runs %q (%v), not one naming %q", pid, cmd, err, want)
	}
	if _, _, errno := syscall.Syscall6(424, fd, uintptr(syscall.SIGKILL), 0, 0, 0, 0); errno != 0 { // pidfd_send_signal
		t.Fatalf("pidfd_send_signal %d: %v", pid, errno)
	}
}

func restartParts(in v1alpha1.Instance) []string {
	var out []string
	for _, rs := range in.Status.Restarts {
		out = append(out, rs.Part+"/"+rs.Signal)
	}
	return out
}

// sendSome sends n short requests through the front, each in a session of its own (x-session-id, on
// which Envoy's ring hash places a request), and requires each to be served. With one Instance, every
// session reaches it.
func (r *rig) sendSome(key string, n int, what string) {
	r.t.Helper()
	for i := 0; i < n; i++ {
		sessions++
		if rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key+"-s"+strconv.Itoa(sessions)); rp.Status != http.StatusOK {
			r.t.Fatalf("%s: request %d: %d %.200q", what, i, rp.Status, rp.Body)
		}
	}
}

// headerInstanceUID is the front's attribution header naming the Instance that answered (HBEP-5,
// Attribution headers; internal/front, HeaderInstanceUID).
const headerInstanceUID = "Hpcburst-Instance-Uid"

// sendOn sends one short request in conversation key and requires it served by Instance in, as the
// front's attribution header names it.
func (r *rig) sendOn(in *v1alpha1.Instance, key, what string) {
	r.t.Helper()
	rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key)
	if rp.Status != http.StatusOK {
		r.t.Fatalf("%s: a request in %s: %d %.200q", what, key, rp.Status, rp.Body)
	}
	if got := rp.Header.Get(headerInstanceUID); got != string(in.UID) {
		r.t.Fatalf("%s: a request in %s was answered by Instance %q, not %s: the key is not pinned to it", what, key, got, in.UID)
	}
}

// keyOn finds a conversation key that the front's ring sends to Instance in (the answer's
// Hpcburst-Instance-Uid) and that its Envoy's ring hash sends to worker w (Envoy's own rq_success for w,
// and only for w, rises by one). The front passes the key on as the x-session-id Envoy hashes, so both
// mappings hold for the run. Never a guess: each is read from the answer and from Envoy.
func (r *rig) keyOn(in *v1alpha1.Instance, admin, w, prefix string) string {
	r.t.Helper()
	for i := 0; i < 128; i++ {
		key := prefix + "-" + strconv.Itoa(i)
		before := hostStats(r.t, admin)
		rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 1, 0, false, ""), key)
		if rp.Status != http.StatusOK {
			r.t.Fatalf("a key search request in %s: %d %.200q", key, rp.Status, rp.Body)
		}
		if rp.Header.Get(headerInstanceUID) != string(in.UID) {
			continue
		}
		after := hostStats(r.t, admin)
		on := ""
		for h := range after {
			if after[h]["rq_success"] == before[h]["rq_success"]+1 {
				on += h
			}
		}
		if on == w {
			return key
		}
	}
	r.t.Fatalf("no key of 128 maps to Instance %s and, on its Envoy, to %s", in.Name, w)
	return ""
}

// sessions numbers sendSome's sessions, so none repeats within a run.
var sessions int

// TestRehearsalTwoWorkers is the whole stack with an Instance of two workers behind Envoy.
//
//livecheck:none CI's rehearsal-workers job runs it (hack/rehearsal.sh)
func TestRehearsalTwoWorkers(t *testing.T) {
	r := startRigWith(t, rigSetup{mode: "direct", ports: portsWorkers, instanceRouter: true, objects: twoGPUObjects, engineGate: true})
	// The launch sets its own CUDA_VISIBLE_DEVICES: each worker's slice replaces it (HBEP-7, Several
	// workers; the Test plan's item on a launch that sets the variable).
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}, {Name: "CUDA_VISIBLE_DEVICES", Value: "7"}}
	r.mdWorkers, r.mdLease = 2, twoWorkersLease
	in := r.stepStart()
	jobID, err := strconv.Atoi(in.Status.JobID)
	if err != nil {
		t.Fatalf("job ID %q: %v", in.Status.JobID, err)
	}
	key := r.fc.KeyFor(in)

	// (a) Two workers on two GPUs, each on its own device and socket, each verified on its own; ready
	// once both have passed and the probe through Envoy has, and each takes requests.
	if w := in.Spec.Launch.Workers; w == nil || *w != 2 || in.Spec.Launch.Router != v1alpha1.InstanceRouterEnvoy {
		t.Fatalf("the Instance's launch: workers %v, router %q; want 2 and envoy", w, in.Spec.Launch.Router)
	}
	if len(in.Status.Workers) != 2 {
		t.Fatalf("status.workers once ready: %+v, want two entries", in.Status.Workers)
	}
	for i, e := range in.Status.Workers {
		want := "ver-" + string(in.UID) + "-" + v1alpha1.WorkerName(i) + "-1"
		if e.Record != want || e.Verdict != string(v1alpha1.VerdictPass) || !e.WarmedUp {
			t.Errorf("%s's entry once ready: %+v, want record %s, passed, warmed up", e.Name, e, want)
		}
	}
	engines := r.engineWorkers(jobID)
	for w, want := range map[string]string{"worker-0": "0", "worker-1": "1"} {
		if e := engines[w]; !slices.Equal(e.cvd, []string{want}) {
			t.Errorf("%s's engine process sees CUDA_VISIBLE_DEVICES %q, want exactly %q: workers never share a device", w, e.cvd, want)
		}
	}
	if len(engines) != 2 {
		t.Fatalf("job %d runs engines %+v, want one for each of the two workers", jobID, engines)
	}
	envoyPID, runDir := r.envoyOf(jobID)
	admin := filepath.Join(runDir, "envoy-admin.sock")
	before := hostStats(t, admin)
	r.sendSome(key, 20, "step (a)")
	after := hostStats(t, admin)
	for _, w := range []string{"worker-0", "worker-1"} {
		if after[w]["rq_total"] <= before[w]["rq_total"] {
			t.Errorf("step (a): Envoy sent %s %d requests, then %d, of 20: both workers should take traffic", w, before[w]["rq_total"], after[w]["rq_total"])
		}
	}

	// (b) A count whose GPUs exceed the node's is refused, not capped: three workers on two GPUs.
	three := stack.ServedDeploymentAs(stack.Served{Name: "md-three", Model: "model-three", Provider: stack.ProviderName, MaxModelLen: "4096"}, rehearsalTiming)
	n3 := int32(3)
	three.Spec.WorkersPerInstance = &n3
	stack.Create(t, r.c, three)
	stack.WaitFor(t, stack.WaitDeadline, "the three-worker deployment refused", func() bool {
		var md v1alpha1.ModelDeployment
		if !r.get(&md, "md-three", stack.Namespace) {
			return false
		}
		for _, c := range md.Status.Conditions {
			if c.Type == v1alpha1.ConditionDegraded && c.Reason == v1alpha1.ReasonSpecUnsupported && strings.Contains(c.Message, "workersPerInstance 3") {
				return true
			}
		}
		return false
	})
	for _, a := range r.allocations() {
		if a.Labels[v1alpha1.LabelModelDeployment] == "md-three" {
			t.Errorf("step (b): Allocation %s for the refused deployment", a.Name)
		}
	}
	ctx, cancel := stack.CallCtx(t)
	if err := r.c.Delete(ctx, three); err != nil {
		t.Errorf("delete md-three: %v", err)
	}
	cancel()

	// (c) A worker killed while a stream runs on the other: it restarts at its own rung alone, the
	// Instance stays ready and the stream ends uncut, healthyWorkers goes 2, 1, 2, and the restarted
	// worker takes requests only once verified again.
	// The stream's key is the primary's (KeyFor); which worker Envoy gives it is read from Envoy, and the
	// victim is the other. The probes go in a key pinned to the primary and, on its Envoy, to the victim
	// (keyOn), so each probe is one the dead or restarted victim would get without the fix: the front's
	// ring would otherwise send a share to the handover replacement, and Envoy a share to the sibling
	// (#510's review).
	before = hostStats(t, admin)
	r.sendOn(in, key, "step (c)")
	after = hostStats(t, admin)
	var streamOn string
	for w := range after {
		if after[w]["rq_success"] > before[w]["rq_success"] {
			streamOn = w
		}
	}
	victim := map[string]string{"worker-0": "worker-1", "worker-1": "worker-0"}[streamOn]
	if victim == "" {
		t.Fatalf("step (c): Envoy's counters name no worker for the stream's key: %v, then %v", before, after)
	}
	probeKey := r.keyOn(in, admin, victim, "victim")
	first, done := r.fc.StartStream(stack.ChatBody(stack.Model, 1500, 1500, true, ""), key)
	<-first
	stack.WaitFor(t, stack.WaitDeadline, "Envoy to show the stream on "+streamOn, func() bool { return hostStats(t, admin)[streamOn]["rq_active"] > 0 })
	pidfdKill(t, engines[victim].pid, "--uds")
	killAt := time.Now()
	// From the kill on, strictly: a request is served, Envoy's route retrying one it sends the dead worker
	// to its sibling, and none reaches the restarted process before its verification, which waits until
	// Envoy holds the worker out of its routing (HBEP-5, Worker health; HBEP-7, Several workers; the
	// science agent's ruling on #439, PR 4's finding).
	var counts []int32
	stack.WaitFor(t, 3*stack.WaitDeadline, victim+" back, verified again and admitted", func() bool {
		now, ok := r.instance(in.Name)
		if !ok || now.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("step (c): Instance %s is %s after %s's kill, want ready on %s", in.Name, now.Status.Phase, victim, streamOn)
		}
		if parts := restartParts(now); len(parts) > 0 && !slices.Equal(parts, []string{victim + "/" + v1alpha1.ReasonPartExited}) {
			t.Fatalf("step (c): restarts %v, want %s's exit alone", parts, victim)
		}
		if h := now.Status.HealthyWorkers; h != nil && (len(counts) == 0 || counts[len(counts)-1] != *h) {
			counts = append(counts, *h)
		}
		var e v1alpha1.InstanceWorker
		for _, x := range now.Status.Workers {
			if x.Name == victim {
				e = x
			}
		}
		verified := func(x v1alpha1.Instance) bool {
			for _, e := range x.Status.Workers {
				if e.Name == victim {
					return e.Record == "ver-"+string(in.UID)+"-"+victim+"-2" && e.WarmedUp && e.VerifiedGeneration != nil && *e.VerifiedGeneration == 1
				}
			}
			return false
		}
		if !verified(now) {
			// Not yet verified again: a request in the victim's key is answered, by the sibling through
			// Envoy's retry while the victim is dead, and never by the victim's restarted process. A
			// successful answer from the victim counts only while the status, read after the request,
			// still shows it unverified.
			pre := hostStats(t, admin)[victim]
			r.sendOn(in, probeKey, "step (c), while "+victim+" rejoins")
			post := hostStats(t, admin)[victim]
			if later, _ := r.instance(in.Name); post["rq_success"] != pre["rq_success"] && !verified(later) {
				t.Fatalf("step (c): %s after the kill, %s's restarted process answered a request before its verification: before %v, after %v; restarts %v",
					time.Since(killAt).Round(10*time.Millisecond), victim, pre, post, restartParts(later))
			}
			return false
		}
		_ = e
		return now.Status.HealthyWorkers != nil && *now.Status.HealthyWorkers == 2 && e.RouterAdmittedGeneration != nil && *e.RouterAdmittedGeneration == 1
	})
	streamed := <-done
	if body := string(streamed.Body); streamed.Status != http.StatusOK || !strings.Contains(body, "[DONE]") || strings.Contains(body, "stream_cut") {
		t.Errorf("step (c): the stream on %s: %d, ending %.200q; want it uncut, with [DONE]", streamOn, streamed.Status, body[max(0, len(body)-200):])
	}
	if !slices.Contains(counts, 1) || counts[len(counts)-1] != 2 {
		t.Errorf("step (c): healthyWorkers went %v, want 2, 1, 2", counts)
	}
	r.syncWatch(stack.KindInstances, "step (c): the Instance's phases after the kill")
	if o, left := r.watch.First(in.UID, killAt, func(o stack.Observation) bool { return o.Phase != v1alpha1.InstanceReady }); left {
		t.Errorf("step (c): the watch saw the Instance %s at %s after %s's kill, want ready throughout", o.Phase, o.At.Format(time.RFC3339Nano), victim)
	}
	before = hostStats(t, admin)
	r.sendSome(key, 20, "step (c), once back")
	if after := hostStats(t, admin); after[victim]["rq_total"] <= before[victim]["rq_total"] {
		t.Errorf("step (c): %s took no request once back", victim)
	}

	// (d) A controller restarted while every worker is verified keeps every worker taking requests,
	// from status.workers: the step's own checks hold the Instance ready, registered and unrestarted,
	// and both workers take requests after it.
	r.stepControllerRestart(in, key)
	before = hostStats(t, admin)
	r.sendSome(key, 20, "step (d)")
	after = hostStats(t, admin)
	for _, w := range []string{"worker-0", "worker-1"} {
		if after[w]["rq_total"] <= before[w]["rq_total"] {
			t.Errorf("step (d): after the controller's restart Envoy sent %s %d requests, then %d, of 20", w, before[w]["rq_total"], after[w]["rq_total"])
		}
	}
	if now, _ := r.instance(in.Name); now.Status.HealthyWorkers == nil || *now.Status.HealthyWorkers != 2 {
		t.Errorf("step (d): healthyWorkers %v after the controller's restart, want 2", now.Status.HealthyWorkers)
	}

	// (e) Envoy killed is restarted at the router rung: healthyWorkers goes to 0 at once and returns once
	// Envoy holds a worker healthy, while the phase stays ready.
	restartsBefore := len(func() v1alpha1.Instance { i, _ := r.instance(in.Name); return i }().Status.Restarts)
	pidfdKill(t, envoyPID, "envoy")
	sawZero := false
	stack.WaitFor(t, 3*stack.WaitDeadline, "healthyWorkers 0, then 2 after the router's restart", func() bool {
		now, ok := r.instance(in.Name)
		if !ok || now.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("step (e): Instance %s is %s after Envoy's kill, want ready", in.Name, now.Status.Phase)
		}
		h := now.Status.HealthyWorkers
		if h != nil && *h == 0 {
			sawZero = true
		}
		return sawZero && h != nil && *h == 2
	})
	if now, _ := r.instance(in.Name); len(now.Status.Restarts) <= restartsBefore || now.Status.Restarts[len(now.Status.Restarts)-1].Part != v1alpha1.PartRouter {
		t.Errorf("step (e): restarts %v, want the router's last", restartParts(now))
	}

	// (e2) A failed rejoin is one failed attempt at that worker's rung (the science agent's ruling on
	// #439): the worker that served (c)'s stream, which has not restarted yet, is killed with the engine
	// gate armed, so its restart is held past health.startTimeout; the timeout restarts it again, at its
	// own rung, while the Instance stays ready on its sibling, whose stream runs uncut through that
	// restart. Then the gate lets it start, and it rejoins.
	r.stepFailedRejoin(in, key, admin, streamOn, jobID, engines)

	// (f) A count changed: every Instance running keeps its two workers, the primary and its handover
	// replacement (which a 20m lease brings at once: its drain start, less the class's default 30m
	// start-to-ready, is past), and the next Instance, from a scale-up, takes the new count, one.
	existing := map[types.UID]bool{}
	for _, x := range r.instances() {
		existing[x.UID] = true
	}
	var md v1alpha1.ModelDeployment
	if !r.get(&md, stack.MDName, stack.Namespace) {
		t.Fatal("step (f): no ModelDeployment")
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.WorkersPerInstance, md.Spec.Instances = nil, md.Spec.Instances+1
	ctx, cancel = stack.CallCtx(t)
	if err := r.c.Patch(ctx, &md, patch); err != nil {
		t.Fatalf("step (f): patch: %v", err)
	}
	cancel()
	var got v1alpha1.ModelDeployment
	if !r.get(&got, stack.MDName, stack.Namespace) || got.Spec.WorkersPerInstance != nil {
		t.Fatalf("step (f): the deployment after the patch: workersPerInstance %v, want absent", got.Spec.WorkersPerInstance)
	}
	var added v1alpha1.Instance
	for end := time.Now().Add(3 * stack.WaitDeadline); ; time.Sleep(2 * time.Second) {
		var seen []string
		found := false
		for _, x := range r.instances() {
			seen = append(seen, x.Name+" "+string(x.Status.Phase)+" "+x.Status.EndReason+" workers "+strconv.Itoa(int(v1alpha1.LaunchWorkers(x.Spec.Launch))))
			if !existing[x.UID] && x.Status.Phase == v1alpha1.InstanceReady {
				added, found = x, true
			}
		}
		if found {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("step (f): no Instance of the scale-up ready within %s of the patch: %v;%s", 3*stack.WaitDeadline, seen, r.describe())
		}
	}
	if w := added.Spec.Launch.Workers; w != nil {
		t.Errorf("step (f): the scale-up's Instance %s has launch.workers %d, want absent, one worker", added.Name, *w)
	}
	for _, x := range r.instances() {
		if w := x.Spec.Launch.Workers; existing[x.UID] && x.Status.Phase != v1alpha1.InstanceEnded && (w == nil || *w != 2) {
			t.Errorf("step (f): Instance %s, running before the change, has launch.workers %v, want 2 kept", x.Name, w)
		}
	}
}

// stepFailedRejoin is step (e2): worker w is killed with the engine gate armed, its restart held past
// health.startTimeout (twoWorkersStartTimeout); the timeout restarts it at its own rung, its second attempt,
// while its sibling serves a stream that straddles that restart, uncut; then the gate is released and w
// rejoins.
func (r *rig) stepFailedRejoin(in *v1alpha1.Instance, key, admin, w string, jobID int, engines map[string]engineProc) {
	r.t.Helper()
	if r.cl.gate == nil {
		r.t.Fatal("step (e2) needs the engine gate (rigSetup.engineGate)")
	}
	before, _ := r.instance(in.Name)
	sibling := map[string]string{"worker-0": "worker-1", "worker-1": "worker-0"}[w]
	siblingGen := before.Annotations[v1alpha1.RestartGenerationAnnotation(sibling)]
	// The stream's key is pinned to this Instance and, on its Envoy, to the sibling (keyOn), read while
	// both workers serve (#510's review).
	sibKey := r.keyOn(in, admin, sibling, "sibling")
	if err := r.cl.gate.arm(); err != nil {
		r.t.Fatal(err)
	}
	// The engine processes now: w's may have been replaced since step (a).
	pid := r.engineWorkers(jobID)[w].pid
	if pid == 0 {
		r.t.Fatalf("step (e2): no engine process for %s (step (a)'s were %+v)", w, engines)
	}
	pidfdKill(r.t, pid, "--uds")
	killAt := time.Now()
	var restartAt time.Time
	stack.WaitFor(r.t, stack.WaitDeadline, w+"'s restart after its kill, held at the gate", func() bool {
		now, _ := r.instance(in.Name)
		if rs := now.Status.Restarts; len(rs) > len(before.Status.Restarts) && rs[len(rs)-1].Part == w {
			restartAt = rs[len(rs)-1].Time.Time
		}
		_, held := r.cl.gate.held(jobID)
		return !restartAt.IsZero() && held
	})
	// The sibling's stream must straddle the timeout's restart, due at restartAt + twoWorkersStartTimeout.
	// It starts 25 s before that and lasts about 80 s (4,000 tokens at tokenDelay, within
	// routing.maxRequestDuration's 90 s); whether it straddled is then checked on state: Envoy shows it
	// active on the sibling when the restart is seen, and it has not ended by then.
	deadline := restartAt.Add(twoWorkersStartTimeout)
	for time.Until(deadline) > 25*time.Second {
		now, _ := r.instance(in.Name)
		if now.Status.Phase != v1alpha1.InstanceReady {
			r.t.Fatalf("step (e2): Instance %s is %s while %s is held, want ready on %s", in.Name, now.Status.Phase, w, sibling)
		}
		time.Sleep(2 * time.Second)
	}
	first, done := r.fc.StartStream(stack.ChatBody(stack.Model, 4000, 4000, true, ""), sibKey)
	<-first
	stack.WaitFor(r.t, stack.WaitDeadline, "Envoy to show the stream on "+sibling, func() bool { return hostStats(r.t, admin)[sibling]["rq_active"] > 0 })
	var streamed *stack.Streamed
	stack.WaitFor(r.t, 2*stack.WaitDeadline, w+"'s restart for its start timeout, with the stream on "+sibling, func() bool {
		select {
		case s := <-done:
			streamed = &s
			r.t.Fatalf("step (e2): the stream ended at %s, before %s's restart for its start timeout: it does not straddle the failed rejoin", s.EndAt.Format(time.RFC3339Nano), w)
		default:
		}
		now, _ := r.instance(in.Name)
		if now.Status.Phase != v1alpha1.InstanceReady {
			r.t.Fatalf("step (e2): Instance %s is %s, want ready on %s", in.Name, now.Status.Phase, sibling)
		}
		rs := now.Status.Restarts
		return len(rs) > 0 && rs[len(rs)-1].Part == w && rs[len(rs)-1].Signal == v1alpha1.ReasonStartTimeout
	})
	if a := hostStats(r.t, admin)[sibling]["rq_active"]; a == 0 {
		r.t.Errorf("step (e2): Envoy shows no request active on %s when %s's failed rejoin is seen: the stream is not on the sibling then", sibling, w)
	}
	s := <-done
	streamed = &s
	if body := string(streamed.Body); streamed.Status != http.StatusOK || !strings.Contains(body, "[DONE]") || strings.Contains(body, "stream_cut") {
		r.t.Errorf("step (e2): %s's stream: %d, ending %.200q; want it uncut through %s's failed rejoin", sibling, streamed.Status, body[max(0, len(body)-200):], w)
	}
	if got := streamed.Header.Get(headerInstanceUID); got != string(in.UID) {
		r.t.Errorf("step (e2): the stream was answered by Instance %q, not %s", got, in.UID)
	}
	// Open the gate for good: a later start, step (f)'s new Instance, must not wait at it.
	if err := r.cl.gate.open(); err != nil {
		r.t.Fatal(err)
	}
	stack.WaitFor(r.t, 3*stack.WaitDeadline, w+" back after the gate's release", func() bool {
		now, _ := r.instance(in.Name)
		if now.Status.Phase != v1alpha1.InstanceReady {
			r.t.Fatalf("step (e2): Instance %s is %s, want ready", in.Name, now.Status.Phase)
		}
		gen := now.Annotations[v1alpha1.RestartGenerationAnnotation(w)]
		for _, e := range now.Status.Workers {
			if e.Name == w && e.WarmedUp && e.VerifiedGeneration != nil && strconv.FormatInt(*e.VerifiedGeneration, 10) == gen {
				return now.Status.HealthyWorkers != nil && *now.Status.HealthyWorkers == 2
			}
		}
		return false
	})
	after, _ := r.instance(in.Name)
	var parts []string
	for _, rs := range after.Status.Restarts[len(before.Status.Restarts):] {
		parts = append(parts, rs.Part+"/"+rs.Signal)
	}
	if want := []string{w + "/" + v1alpha1.ReasonPartExited, w + "/" + v1alpha1.ReasonStartTimeout}; !slices.Equal(parts, want) {
		r.t.Errorf("step (e2): restarts %v since the kill, want %v: the failed rejoin is one attempt at %s's own rung", parts, want, w)
	}
	if g := after.Annotations[v1alpha1.RestartGenerationAnnotation(sibling)]; g != siblingGen {
		r.t.Errorf("step (e2): %s's restart generation went from %q to %q: only %s restarts", sibling, siblingGen, g, w)
	}
	r.syncWatch(stack.KindInstances, "step (e2): the Instance's phases after the kill")
	if o, left := r.watch.First(in.UID, killAt, func(o stack.Observation) bool { return o.Phase != v1alpha1.InstanceReady }); left {
		r.t.Errorf("step (e2): the watch saw the Instance %s at %s, want ready throughout %s's failed rejoin", o.Phase, o.At.Format(time.RFC3339Nano), w)
	}
}
