package scenario_test

// The scenario of issue #146, criterion 2, steps a-g, in direct dial-home
// mode; ssh mode runs once in TestRehearsalSSH (criterion 3).

import (
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
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// rehearsalTiming: a 7 min lease whose drain window is 90 s + 30 s (the
// explicit drainWindow, 10 min, is longer), so a handover happens in minutes
// and steps (b), (d), (e) and (f) fit before the primary's drain start, after
// step (a)'s tunnel hold (about 45 s) and the 30 s frpc backoff that the hold
// leaves for step (e); the agent's stop grace is short, so scancel's TERM
// finishes before the fake's KillWait. health.startTimeout is 4 min, not its
// 1 h default: HBEP-7 (Provisioning, A partition too short) skips a Provider
// whose time limit, here the 7 min leaseLimit, is not longer than the lead,
// the drain window plus handoverMargin plus health.startTimeout, so the lead
// is 2 min + 0 s + 4 min = 6 min. An Instance of the rehearsal is ready in
// under a minute. The profile sets drainWindow explicitly, longer than the
// sum, so the test computes the drain start from the spec alone (drainStartOf)
// rather than with the helper the controller and the front share.
var rehearsalTiming = stack.Timing{Lease: "7m", MaxRequest: "90s", DrainMargin: "30s", DrainWindow: "10m", StopGrace: "5s", AgentTimeout: "20s", StartTimeout: "4m"}

// rehearsalFrontTuning is the front's scaled probe and pause times (step d): a
// 2 s probe, and a 10 s pause after HBEP-5's three Instance-emitted 5xx in a row.
const rehearsalFrontTuning = `"probeInterval":"2s","pauseAfterFailures":3,"pauseDuration":"10s",`

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
	b   stack.Binaries
	cl  *rigCluster
	h   *stack.Host
	hub *stack.Hub
	c   client.Client
	// fc is the front's client side (stack.Client).
	fc *stack.Client
	// engineEnv is the served deployment's engine env: the engine double's
	// modes (direct run only).
	engineEnv []v1alpha1.EnvVar
	// watch records every Instance version the API sends.
	watch *stack.Recorder
	// proxyEnds is when frps was first seen to list each ended Instance's
	// proxy offline (proxyEnded), for the TunnelOrphaned window.
	proxyEnds map[types.UID]time.Time
	// ports are the stack's fixed listeners.
	ports stack.Ports
	// tokenDelay is the engine double's token-delay mode of the served
	// deployment: each generated token takes this long.
	tokenDelay time.Duration
	// orphanBase is the TunnelOrphaned check's first read of the raised-events
	// counter (absence_test.go), taken once the controller it covers serves
	// and before any Instance ends.
	orphanBase *absenceBase
	// onLed, when set, runs in a controller restart once the new controller
	// leads and answers /readyz: the earliest point the new process serves,
	// where a check takes its first read.
	onLed func()
	// dir is the test's short scratch directory.
	dir string
}

// rigSetup is what one rehearsal stack is started with. #146's tests take
// the zero value but for the mode; another issue's test names its own ports
// and, when it needs them, the fake cluster's QOSes and its own objects.
type rigSetup struct {
	mode  string // "direct" or "ssh"
	ports stack.Ports
	shape stack.ClusterShape
	// engineGate puts the engine gate (enginegate_test.go) in front of the
	// jobs' apptainer; it holds nothing until armed.
	engineGate bool
	// objects returns the PlatformProfiles and Providers to create once the
	// stack is up, given the rig, the host's CPU architecture and the batch
	// Provider's credentials file; nil creates #146's profile and Provider.
	objects func(r *rig, arch, creds string) []client.Object
	// noWatch starts no watch: the measurement of what a watch costs starts its
	// own (TestWatchOverhead).
	noWatch bool
}

func startRig(t *testing.T, mode string) *rig {
	t.Helper()
	return startRigWith(t, rigSetup{mode: mode, ports: ports146})
}

func startRigWith(t *testing.T, s rigSetup) *rig {
	t.Helper()
	b := requireRehearsal(t)
	dir := stack.ShortDir(t)
	r := &rig{t: t, b: b, hub: stack.StartHub(t), ports: s.ports, tokenDelay: tokenDelay, dir: dir}
	// The job's environment has no HF_ENDPOINT: the agent learns the Hub
	// from agent.json, which the controller writes from its own (ruling of 1
	// Nov 2026 on #156; step (a) checks the downloads reached the double).
	r.cl = startRigCluster(t, dir, s.shape, s.engineGate)
	creds := filepath.Join(dir, "provider-ssh")
	if err := r.cl.Login.WriteCredentials(creds); err != nil {
		t.Fatal(err)
	}
	dh := stack.DialHome{Mode: "direct", Addr: s.ports.DialHomeListen}
	if s.mode == "ssh" {
		dh = r.startGateway(dir)
	}
	// The controller lists the checkpoint from the Hub double through
	// HF_ENDPOINT, which up passes on to it.
	r.h = startRigHost(t, b, dir, dh, stack.CleanEnv("HF_ENDPOINT="+r.hub.URL), s.ports)
	r.fc = rigClient(t, r.h)
	arch := stack.CPUArch(t)
	r.h.PlaceAgentBuild(arch)
	r.c = stack.KubeClient(t, r.h.State)
	objs := []client.Object{stack.BatchProfile(arch, rehearsalTiming), stack.BatchProvider(creds)}
	if s.objects != nil {
		objs = s.objects(r, arch, creds)
	}
	stack.Create(t, r.c, objs...)
	if s.noWatch {
		return r
	}
	// Before the deployment exists, so the watch sees every Instance and
	// Allocation from its first version.
	r.watch = stack.StartWatch(r.t, r.h.State, stack.Namespace, nil)
	return r
}

// startRigHost is the stack's hpcburst up with the rehearsal's own choices: its
// front tuning, and a down past its bound that says so.
func startRigHost(t *testing.T, b stack.Binaries, dir string, dh stack.DialHome, env []string, ports stack.Ports) *stack.Host {
	t.Helper()
	return stack.StartHost(t, b, dir, dh, env, ports, rehearsalFrontTuning, stack.NameDownBound)
}

// rigClient is the rig's client of the front. The rehearsal reads no send times,
// so no request of its waits for the transport's write hook.
func rigClient(t *testing.T, h *stack.Host) *stack.Client {
	return &stack.Client{T: t, H: h, Model: stack.Model, MaxRequest: maxRequestDuration, NoWriteWait: true}
}

func (r *rig) get(obj client.Object, name, ns string) bool {
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	return r.c.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, obj) == nil
}

// providerReady waits until the Provider is Ready from a fresh queue read
// over the login node (v1alpha1.ProviderReady).
func (r *rig) providerReady() {
	r.t.Helper()
	var p v1alpha1.Provider
	stack.WaitFor(r.t, stack.WaitDeadline, "the Provider to be Ready over the login node", func() bool {
		return r.get(&p, stack.ProviderName, "") && v1alpha1.ProviderReady(&p, time.Now())
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
	pid, found := r.h.StatusPID("controller"), false
	for _, c := range stack.ChildrenOf(pid) {
		switch exe := stack.ProcessExe(c); exe {
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
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.List(ctx, &l, client.InNamespace(stack.Namespace)); err != nil {
		r.t.Fatalf("list allocations: %v", err)
	}
	return l.Items
}

func (r *rig) instances() []v1alpha1.Instance {
	var l v1alpha1.InstanceList
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.List(ctx, &l, client.InNamespace(stack.Namespace)); err != nil {
		r.t.Fatalf("list instances: %v", err)
	}
	return l.Items
}

// describe is a one-line view of the deployment's objects, for a wait's
// failure message.
func (r *rig) describe() string {
	var md v1alpha1.ModelDeployment
	s := ""
	if r.get(&md, stack.MDName, stack.Namespace) {
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

// TestRehearsalDirect is criterion 2 in direct mode. Steps (b), (d), (e) and
// (f) run on the primary before its drain start, then the handover (c), then
// the scale to 0 (g). The replacement is ready from the start (HBEP-7: with
// no queue wait observed it is created at the primary's start), so the
// requests carry a conversation key the ring maps to the primary.
//
//livecheck:none CI's rehearsal job runs it (hack/rehearsal.sh)
func TestRehearsalDirect(t *testing.T) {
	r := startRig(t, "direct")
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	done := r.stepTimer("a")
	in := r.stepStart()
	done()
	r.replacementReady(in)
	key := r.fc.KeyFor(in)
	done = r.stepTimer("b")
	r.beforeDrain("b", in)
	r.stepRequests(in, key)
	done()
	r.stepWorkerRestart(in, key)
	r.stepTunnelLoss(in, key)
	r.stepControllerRestart(in, key)
	// No Instance has ended yet; the restarted controller serves the steps
	// that end them, (c) and (g).
	r.orphanBase = r.baseline("after step (f)'s controller restart", keyTunnelOrphaned)
	r.stepHandover(in)
	done = r.stepTimer("g")
	r.stepScaleToZero("c, g")
	done()
}

// startGateway starts the control-plane host's ssh server for ssh mode, with
// HBEP-6's restricted key for the agent endpoint (base+3) and the front's
// dial-home listener (base+7), and puts the key and known_hosts where the
// agent looks, the cluster user's ~/.hpcburst/dial-home.
func (r *rig) startGateway(dir string) stack.DialHome {
	r.t.Helper()
	gwDir := filepath.Join(dir, "gw")
	if err := os.Mkdir(gwDir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	gw, err := rehearsal.StartGateway(rehearsal.GatewayConfig{Dir: gwDir, AgentPort: r.ports.Base + 3, DialHomePort: r.ports.Base + 7})
	if err != nil {
		r.t.Fatalf("the dial-home gateway: %v", err)
	}
	r.t.Cleanup(func() {
		if err := gw.Close(); err != nil {
			r.t.Errorf("the gateway's Close: %v", err)
		}
	})
	dhDir := filepath.Join(r.cl.Home, ".hpcburst", "dial-home")
	if err := os.MkdirAll(filepath.Dir(dhDir), 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := gw.WriteDialHome(dhDir); err != nil {
		r.t.Fatalf("the dial-home files: %v", err)
	}
	return stack.DialHome{Mode: "ssh", Addr: gw.Addr(), KeyPath: filepath.Join(dhDir, "key")}
}

// jobSSHForwards lists the agent's ssh processes in its job: the system ssh
// running a local forward. In ssh mode the agent reaches this host through
// them (HBEP-6, Dial-home modes).
func (r *rig) jobSSHForwards(jobID int) []int {
	r.t.Helper()
	want, _ := exec.LookPath("ssh")
	want, _ = filepath.EvalSymlinks(want)
	pids, err := r.cl.Plat.Processes(jobID)
	if err != nil {
		r.t.Fatalf("job %d's processes: %v", jobID, err)
	}
	var out []int
	for _, p := range pids {
		args, _ := os.ReadFile("/proc/" + strconv.Itoa(p) + "/cmdline")
		if stack.ProcessExe(p) == want && strings.Contains(string(args), "-L\x00") {
			out = append(out, p)
		}
	}
	return out
}

// TestRehearsalSSH is criterion 3's ssh dial-home mode, run once: the agent
// reaches the agent endpoint and the dial-home listener through two ssh
// forwards to the gateway, and nothing of them outlives its job.
//
//livecheck:none CI's rehearsal job runs it (hack/rehearsal.sh)
func TestRehearsalSSH(t *testing.T) {
	r := startRig(t, "ssh")
	in := r.stepStart()
	r.orphanBase = r.baseline("after step (a)", keyTunnelOrphaned)
	jobID, _ := strconv.Atoi(in.Status.JobID)
	fwd := r.jobSSHForwards(jobID)
	if len(fwd) < 2 {
		t.Errorf("the agent of job %d has %d ssh forwards (%v), want 2: the agent endpoint and the dial-home listener", jobID, len(fwd), fwd)
	}
	r.stepScaleToZero("g")
	for _, p := range fwd {
		if stack.Alive(p) {
			t.Errorf("the agent's ssh forward %d outlived its job (HBEP-6: no ssh process behind)", p)
		}
	}
}
