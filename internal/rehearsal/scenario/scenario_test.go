package scenario_test

// The scenario of issue #146, criterion 2, steps a-g, in direct dial-home
// mode; ssh mode runs once in TestRehearsalSSH (criterion 3).

import (
	"context"
	"fmt"
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
var rehearsalTiming = timing{lease: "6m", maxRequest: "90s", drainMargin: "30s", stopGrace: "5s"}

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
	r.cl = startCluster(t, dir, map[string]string{"HF_ENDPOINT": r.hub.url})
	creds := filepath.Join(dir, "provider-ssh")
	if err := r.cl.login.WriteCredentials(creds); err != nil {
		t.Fatal(err)
	}
	// The controller's checkpoint listing goes to the Hub double too, if the
	// controller honours HF_ENDPOINT (issue #146 finding F1).
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
	if !conditionTrue(ready.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
		r.t.Errorf("Instance %s is ready without TunnelConnected True: %v", ready.Name, ready.Status.Conditions)
	}
	return ready
}

func TestRehearsalDirect(t *testing.T) {
	r := startRig(t, "direct")
	r.stepStart()
}
