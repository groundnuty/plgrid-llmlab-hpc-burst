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
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// rehearsalTiming: a 6 min lease whose drain window is 90 s + 30 s, so a
// handover happens in minutes; the agent's stop grace is short, so scancel's
// TERM finishes before the fake's KillWait (30 s).
var rehearsalTiming = timing{lease: "6m", maxRequest: "90s", drainMargin: "30s", stopGrace: "5s", agentTimeout: "20s"}

// rehearsalAgentTimeout is rehearsalTiming's agentTimeout: how long a
// controller waits for a silent agent before it judges it unreachable.
const rehearsalAgentTimeout = 20 * time.Second

// rig is one running rehearsal: the fake cluster, the host and the API.
type rig struct {
	t   *testing.T
	b   binaries
	cl  *cluster
	h   *host
	hub *hub
	c   client.Client
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
	// The controller lists the checkpoint from the Hub double through
	// HF_ENDPOINT, which up passes on to it.
	r.h = startHost(t, b, dir, mode, cleanEnv("HF_ENDPOINT="+r.hub.url))
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
	create(r.t, r.c, servedDeployment(rehearsalTiming))
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
	var ready *v1alpha1.Instance
	waitFor(r.t, waitDeadline, "an Instance to be ready", func() bool {
		for _, in := range r.instances() {
			if in.Status.Phase == v1alpha1.InstanceReady {
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

func TestRehearsalDirect(t *testing.T) {
	r := startRig(t, "direct")
	in := r.stepStart()
	r.stepControllerRestart(in)
	r.stepScaleToZero()
}
