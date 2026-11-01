package scenario_test

// The scenario of issue #146, criterion 2, steps a-g, in direct dial-home
// mode; ssh mode runs once in TestRehearsalSSH (criterion 3).

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

// rehearsalTiming: a 6 min lease whose drain window is 90 s + 30 s (the
// explicit drainWindow, 10 min, is longer), so a handover happens in minutes
// and steps (b), (d), (e) and (f) fit before the primary's drain start; the
// agent's stop grace is short, so scancel's TERM finishes before the fake's
// KillWait.
var rehearsalTiming = timing{lease: "6m", maxRequest: "90s", drainMargin: "30s", drainWindow: "10m", stopGrace: "5s", agentTimeout: "20s"}

// rehearsalAgentTimeout is rehearsalTiming's agentTimeout: how long a
// controller waits for a silent agent before it judges it unreachable.
const rehearsalAgentTimeout = 20 * time.Second

// rehearsalDrainWindow is rehearsalTiming's drain window as the brief and
// HBEP-7 define it, from the spec alone: routing.maxRequestDuration (90 s)
// plus the profile's drainMargin (30 s), shorter than its drainWindow (10 min).
const rehearsalDrainWindow = 90*time.Second + 30*time.Second

// maxRequestDuration is rehearsalTiming's routing.maxRequestDuration: the
// front cuts a request at it, so no stream of the rehearsal may be longer,
// and a request that gets no answer within it and a margin hangs.
const maxRequestDuration = 90 * time.Second

// tokenDelay is the engine double's token-delay mode in the direct run
// (HB_ENGINE_DOUBLE, through spec.source.served.env): each generated token
// takes this long, so a stream of n tokens lasts at least n times it. 20 ms
// keeps a warm-up round (about 512 sequential tokens) near 10 s, and a 50 s
// stream within max-model-len 4096.
const tokenDelay = 20 * time.Millisecond

// drainStartOf is an Instance's drain start from its recorded lease end:
// status.leaseEnd minus rehearsalDrainWindow. It is computed here, not with
// v1alpha1.DrainStart, which the controller and the front both use.
func drainStartOf(in *v1alpha1.Instance) (time.Time, bool) {
	if in.Status.LeaseEnd == nil {
		return time.Time{}, false
	}
	return in.Status.LeaseEnd.Add(-rehearsalDrainWindow), true
}

// rig is one running rehearsal: the fake cluster, the host and the API.
type rig struct {
	t   *testing.T
	b   binaries
	cl  *cluster
	h   *host
	hub *hub
	c   client.Client
	// engineEnv is the served deployment's engine env: the engine double's
	// modes (direct run only).
	engineEnv []v1alpha1.EnvVar
	// watch records every Instance version the API sends (startWatch).
	watch *recorder
	// proxyEnds is when frps was first seen to list each ended Instance's
	// proxy offline (proxyEnded), for the TunnelOrphaned window.
	proxyEnds map[types.UID]time.Time
}

func startRig(t *testing.T, mode string) *rig {
	t.Helper()
	b := requireRehearsal(t)
	dir := shortDir(t)
	r := &rig{t: t, b: b, hub: startHub(t)}
	// The job's environment has no HF_ENDPOINT: the agent learns the Hub
	// from agent.json, which the controller writes from its own (ruling of 1
	// Nov 2026 on #156; step (a) checks the downloads reached the double).
	r.cl = startCluster(t, dir, nil)
	creds := filepath.Join(dir, "provider-ssh")
	if err := r.cl.login.WriteCredentials(creds); err != nil {
		t.Fatal(err)
	}
	dh := dialHome{mode: "direct", addr: dialHomeAddr}
	if mode == "ssh" {
		dh = r.startGateway(dir)
	}
	// The controller lists the checkpoint from the Hub double through
	// HF_ENDPOINT, which up passes on to it.
	r.h = startHost(t, b, dir, dh, cleanEnv("HF_ENDPOINT="+r.hub.url))
	arch := cpuArch(t)
	r.h.placeAgentBuild(arch)
	r.c = kubeClient(t, r.h.state)
	create(t, r.c, batchProfile(arch, rehearsalTiming), batchProvider(creds))
	return r
}

func (r *rig) get(obj client.Object, name, ns string) bool {
	return r.c.Get(context.Background(), client.ObjectKey{Name: name, Namespace: ns}, obj) == nil
}

// providerReady waits until the Provider is Ready from a fresh queue read
// over the login node (v1alpha1.ProviderReady).
func (r *rig) providerReady() {
	r.t.Helper()
	var p v1alpha1.Provider
	waitFor(r.t, waitDeadline, "the Provider to be Ready over the login node", func() bool {
		return r.get(&p, providerName, "") && v1alpha1.ProviderReady(&p, time.Now())
	})
	r.assertRealSSH()
}

// assertRealSSH checks that the controller reached the login node with the
// system's ssh client: its ssh master, a child process, runs the ssh found on
// PATH, never this test binary, which the engine double's fakes link to.
// Otherwise the "real wire" would be a fake that every step still passes.
func (r *rig) assertRealSSH() {
	r.t.Helper()
	want, err := exec.LookPath("ssh")
	if err == nil {
		want, err = filepath.EvalSymlinks(want)
	}
	self, serr := os.Executable()
	if err != nil || serr != nil {
		r.t.Fatalf("the system ssh (%v) or this binary (%v) is unknown", err, serr)
	}
	pid, found := r.statusPID("controller"), false
	for _, c := range childrenOf(pid) {
		switch exe := processExe(c); exe {
		case want:
			found = true
		case self:
			r.t.Errorf("the controller's child %d runs this test binary, a fake, not the system ssh", c)
		}
	}
	if !found {
		r.t.Errorf("the controller (pid %d) has no child running %s: its ssh master to the login node is not the system ssh", pid, want)
	}
}

// allocations lists the deployment's Allocations.
func (r *rig) allocations() []v1alpha1.Allocation {
	var l v1alpha1.AllocationList
	if err := r.c.List(context.Background(), &l, client.InNamespace(namespace)); err != nil {
		r.t.Fatalf("list allocations: %v", err)
	}
	return l.Items
}

func (r *rig) instances() []v1alpha1.Instance {
	var l v1alpha1.InstanceList
	if err := r.c.List(context.Background(), &l, client.InNamespace(namespace)); err != nil {
		r.t.Fatalf("list instances: %v", err)
	}
	return l.Items
}

// describe is a one-line view of the deployment's objects, for a wait's
// failure message.
func (r *rig) describe() string {
	var md v1alpha1.ModelDeployment
	s := ""
	if r.get(&md, mdName, namespace) {
		for _, c := range md.Status.Conditions {
			s += fmt.Sprintf(" md:%s=%s(%s: %.160s)", c.Type, c.Status, c.Reason, c.Message)
		}
	}
	for _, a := range r.allocations() {
		s += fmt.Sprintf(" alloc:%s phase=%s job=%s held=%v", a.Name, a.Status.AtProvider.Phase, a.Status.AtProvider.JobID, a.Status.AtProvider.Held)
	}
	for _, in := range r.instances() {
		s += fmt.Sprintf(" inst:%s phase=%s end=%s", in.Name, in.Status.Phase, in.Status.EndReason)
	}
	return s
}

// stepStart is step (a): an Allocation submitted held and released, the
// agent in the job, and an Instance that reaches ready only once its tunnel
// is connected.
func (r *rig) stepStart() *v1alpha1.Instance {
	r.t.Helper()
	r.providerReady()
	create(r.t, r.c, servedDeployment(rehearsalTiming, r.engineEnv...))
	var alloc v1alpha1.Allocation
	waitFor(r.t, waitDeadline, "an Allocation with a job ID", func() bool {
		for _, a := range r.allocations() {
			if a.Status.AtProvider.JobID != "" {
				alloc = a
				return true
			}
		}
		r.t.Logf("waiting:%s", r.describe())
		return false
	})
	id, err := strconv.Atoi(alloc.Status.AtProvider.JobID)
	if err != nil {
		r.t.Fatalf("job ID %q: %v", alloc.Status.AtProvider.JobID, err)
	}
	// Submitted held, then released: the job ran only after scontrol release.
	waitFor(r.t, waitDeadline, "the job to start in the fake cluster", func() bool {
		j, ok := r.cl.plat.Job(id)
		return ok && j.PID > 0
	})
	if n := r.cl.plat.Count("scontrol"); n < 1 {
		r.t.Errorf("the job started with no scontrol release (%d calls): it was not submitted held", n)
	}
	// The primary is the Instance with no predecessor: its replacement is
	// created at its start when no queue wait is observed (HBEP-7), so two
	// Instances start, and either may be ready first.
	var ready *v1alpha1.Instance
	waitFor(r.t, waitDeadline, "the primary Instance to be ready", func() bool {
		for _, in := range r.instances() {
			if in.Status.Phase == v1alpha1.InstanceReady && in.Annotations[v1alpha1.AnnotationPredecessor] == "" {
				in := in
				ready = &in
				return true
			}
		}
		return false
	})
	// The agent staged from the Hub double, whose address reached the job only
	// through agent.json. The LFS file shows it: the controller downloads only
	// the files outside LFS, to hash them, and takes an LFS file's SHA-256 from
	// the listing, so only the agent fetches the weights.
	agentFetched := false
	for _, p := range r.hub.resolved() {
		for _, f := range hubFiles {
			agentFetched = agentFetched || f.lfs && f.path == p
		}
	}
	if !agentFetched {
		r.t.Errorf("the agent never downloaded the LFS weights from the Hub double (downloads %v): agent.json's hubEndpoint did not reach it", r.hub.resolved())
	}
	if !conditionTrue(ready.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
		r.t.Errorf("Instance %s is ready without TunnelConnected True: %v", ready.Name, ready.Status.Conditions)
	}
	return ready
}

// TestRehearsalDirect is criterion 2 in direct mode. Steps (b), (d), (e) and
// (f) run on the primary before its drain start, then the handover (c), then
// the scale to 0 (g). The replacement is ready from the start (HBEP-7: with
// no queue wait observed it is created at the primary's start), so the
// requests carry a conversation key the ring maps to the primary.
func TestRehearsalDirect(t *testing.T) {
	r := startRig(t, "direct")
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	r.startWatch()
	done := r.stepTimer("a")
	in := r.stepStart()
	done()
	r.replacementReady(in)
	key := r.keyFor(in)
	done = r.stepTimer("b")
	r.beforeDrain("b", in)
	r.stepRequests(in, key)
	done()
	r.stepWorkerRestart(in, key)
	r.stepTunnelLoss(in, key)
	r.stepControllerRestart(in, key)
	r.stepHandover(in)
	done = r.stepTimer("g")
	r.stepScaleToZero(true)
	done()
}

// startGateway starts the control-plane host's ssh server for ssh mode, with
// HBEP-6's restricted key for the agent endpoint (base+3) and the front's
// dial-home listener (base+7), and puts the key and known_hosts where the
// agent looks, the cluster user's ~/.hpcburst/dial-home.
func (r *rig) startGateway(dir string) dialHome {
	r.t.Helper()
	gwDir := filepath.Join(dir, "gw")
	if err := os.Mkdir(gwDir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	gw, err := rehearsal.StartGateway(rehearsal.GatewayConfig{Dir: gwDir, AgentPort: portBase + 3, DialHomePort: portBase + 7})
	if err != nil {
		r.t.Fatalf("the dial-home gateway: %v", err)
	}
	r.t.Cleanup(func() {
		if err := gw.Close(); err != nil {
			r.t.Errorf("the gateway's Close: %v", err)
		}
	})
	dhDir := filepath.Join(r.cl.home, ".hpcburst", "dial-home")
	if err := os.MkdirAll(filepath.Dir(dhDir), 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := gw.WriteDialHome(dhDir); err != nil {
		r.t.Fatalf("the dial-home files: %v", err)
	}
	return dialHome{mode: "ssh", addr: gw.Addr(), keyPath: filepath.Join(dhDir, "key")}
}

// jobSSHForwards lists the agent's ssh processes in its job: the system ssh
// running a local forward. In ssh mode the agent reaches this host through
// them (HBEP-6, Dial-home modes).
func (r *rig) jobSSHForwards(jobID int) []int {
	r.t.Helper()
	want, _ := exec.LookPath("ssh")
	want, _ = filepath.EvalSymlinks(want)
	pids, err := r.cl.plat.Processes(jobID)
	if err != nil {
		r.t.Fatalf("job %d's processes: %v", jobID, err)
	}
	var out []int
	for _, p := range pids {
		args, _ := os.ReadFile("/proc/" + strconv.Itoa(p) + "/cmdline")
		if processExe(p) == want && strings.Contains(string(args), "-L\x00") {
			out = append(out, p)
		}
	}
	return out
}

// TestRehearsalSSH is criterion 3's ssh dial-home mode, run once: the agent
// reaches the agent endpoint and the dial-home listener through two ssh
// forwards to the gateway, and nothing of them outlives its job.
func TestRehearsalSSH(t *testing.T) {
	r := startRig(t, "ssh")
	in := r.stepStart()
	jobID, _ := strconv.Atoi(in.Status.JobID)
	fwd := r.jobSSHForwards(jobID)
	if len(fwd) < 2 {
		t.Errorf("the agent of job %d has %d ssh forwards (%v), want 2: the agent endpoint and the dial-home listener", jobID, len(fwd), fwd)
	}
	r.stepScaleToZero(false)
	for _, p := range fwd {
		if alive(p) {
			t.Errorf("the agent's ssh forward %d outlived its job (HBEP-6: no ssh process behind)", p)
		}
	}
}
