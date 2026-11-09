package providerctl

// The Provider controller's side of HBEP-6's host-dialed mode: what each
// observation tells the relay owner (syncRelays), read back from what the
// owner keeps (Controller.Relays). No relay process runs here: the fake
// Platform reads no fresh listing, so every start is refused (freshJobs fails
// closed), and the ssh client is /bin/false. The relays themselves are tested
// through the real path in internal/hostdial.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// hostDialed makes the env's controller host-dialed.
func hostDialed(e *env) {
	front := exampleFront(e.t, e.buildDir)
	front.Mode, front.Endpoint = agentapi.DialHomeHostDialed, ""
	dir, err := os.MkdirTemp("", "pr")
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = os.RemoveAll(dir) })
	e.opts.Front = front
	e.opts.Relays = &RelaySettings{
		Ports: map[agentapi.RelayChannel]int{agentapi.RelayControl: 1, agentapi.RelayServing: 2}, Slots: hostdial.NewSlots(0),
		ControlDir: dir, SSHBinary: "/bin/false", MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	}
}

// runRelays runs the controller's relay owner until the test ends.
func runRelays(e *env) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.ctl.RunRelays(ctx); close(done) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			e.t.Error("the relay owner did not stop")
		}
	})
}

// relaysOf is what the owner keeps for tag.
func relaysOf(e *env, tag string) []hostdial.RelayState {
	var out []hostdial.RelayState
	for _, r := range e.ctl.Relays() {
		if r.Tag == tag {
			out = append(out, r)
		}
	}
	return out
}

func waitRelays(t *testing.T, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("%s: not within 30 s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Through the reconciler: a submitted Allocation whose job runs gets its two
// relays, for its recorded job, the staged agent and the credentials'
// generation; each start is refused because the fake Platform reads no fresh
// listing, never from a cached one (N3); and the job's end stops them.
func TestARunningAllocationKeepsItsRelaysUntilItsJobEnds(t *testing.T) {
	e := newEnv(t, hostDialed)
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag, job := tagOf(a), a.Status.AtProvider.JobID
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || job == "" {
		t.Fatalf("the Allocation is %s with job %q, want Running", a.Status.AtProvider.Phase, job)
	}
	rs := relaysOf(e, tag)
	if len(rs) != 2 {
		t.Fatalf("the owner keeps %d relays for a running Allocation, want 2: %+v", len(rs), rs)
	}
	for _, r := range rs {
		if r.JobID != job || r.Gen != credentialsGen(e.profiles.v.SSH) || r.Gen == "" {
			t.Errorf("relay %+v: want job %s and the credentials' generation", r, job)
		}
	}
	waitRelays(t, "both starts refused", func() bool {
		for _, r := range relaysOf(e, tag) {
			if !strings.Contains(r.LastRefusal, "reads no fresh listing") {
				return false
			}
		}
		return true
	})
	if rs := relaysOf(e, tag); rs[0].Starts != 0 || rs[1].Starts != 0 {
		t.Errorf("a relay started on a Platform that reads no fresh listing: %+v", rs)
	}
	e.batch.CancelExternally(job)
	e.run("alloc-a", 3)
	if p := e.get("alloc-a").Status.AtProvider.Phase; p == v1alpha1.AllocationRunning {
		t.Fatalf("the job's end left the Allocation %s", p)
	}
	if rs := relaysOf(e, tag); len(rs) != 0 {
		t.Errorf("the job ended and the owner still keeps %d relays", len(rs))
	}
}

// Each of syncRelays' branches, from an Allocation whose relays are kept:
// being deleted, a job that is not running, no recorded job, a completing job
// and no batch parameters each stop them; a failed paths read and an ssh
// client that cannot be made leave them as they were; and changed
// credentials make a new generation of them (F1).
func TestSyncRelaysBranches(t *testing.T) {
	e := newEnv(t, hostDialed)
	runRelays(e)
	ext := &external{s: e.ctl.shared, v: e.profiles.v, p: e.platform}
	ctx := context.Background()
	const tag = "hb-example-0123456789ab"
	running := func() *v1alpha1.Allocation {
		a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "alloc-x", Namespace: testNS}}
		meta.SetExternalName(a, tag)
		a.Spec.ForProvider.Batch = &v1alpha1.BatchParameters{AgentDigest: e.digest}
		a.Status.AtProvider.Phase, a.Status.AtProvider.JobID = v1alpha1.AllocationRunning, "4242"
		return a
	}
	kept := func(what string) {
		t.Helper()
		waitRelays(t, what+": the owner keeps both relays", func() bool { return len(relaysOf(e, tag)) == 2 })
	}
	gone := func(what string) {
		t.Helper()
		if rs := relaysOf(e, tag); len(rs) != 0 {
			t.Errorf("%s: the owner still keeps %d relays", what, len(rs))
		}
	}
	ext.syncRelays(ctx, running(), tag)
	kept("a running Allocation")
	if r := relaysOf(e, tag)[0]; r.JobID != "4242" {
		t.Errorf("relay job %s, want 4242", r.JobID)
	}
	now := metav1.Now()
	for name, change := range map[string]func(*v1alpha1.Allocation){
		"being deleted":    func(a *v1alpha1.Allocation) { a.DeletionTimestamp = &now },
		"pending":          func(a *v1alpha1.Allocation) { a.Status.AtProvider.Phase = v1alpha1.AllocationPending },
		"ended":            func(a *v1alpha1.Allocation) { a.Status.AtProvider.Phase = v1alpha1.AllocationEnded },
		"no recorded job":  func(a *v1alpha1.Allocation) { a.Status.AtProvider.JobID = "" },
		"completing":       func(a *v1alpha1.Allocation) { a.Status.AtProvider.JobCompleting = true },
		"no batch request": func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch = nil },
	} {
		ext.syncRelays(ctx, running(), tag)
		kept("before " + name)
		a := running()
		change(a)
		ext.syncRelays(ctx, a, tag)
		gone(name)
	}
	// A failed read of the login node's paths changes nothing, either way.
	ext.syncRelays(ctx, running(), tag)
	e.platform.mu.Lock()
	e.platform.pathErr = errors.New("example: HOME unresolved")
	e.platform.mu.Unlock()
	ext.syncRelays(ctx, running(), tag)
	kept("a failed paths read")
	e.platform.mu.Lock()
	e.platform.pathErr = nil
	e.platform.mu.Unlock()
	// An ssh client that cannot be made (a credential value the transport
	// refuses) changes nothing either.
	bad := e.profiles.v
	bad.SSH.HostName = "login example"
	(&external{s: e.ctl.shared, v: bad, p: e.platform}).syncRelays(ctx, running(), tag)
	kept("an ssh client that cannot be made")
	if r := relaysOf(e, tag)[0]; r.Gen != credentialsGen(e.profiles.v.SSH) {
		t.Errorf("the relays moved to credentials that cannot be used")
	}
	// Rotated credentials: a new generation.
	rotated := e.profiles.v
	rotated.SSH.IdentityFile = "/nonexistent/key-2"
	(&external{s: e.ctl.shared, v: rotated, p: e.platform}).syncRelays(ctx, running(), tag)
	kept("rotated credentials")
	for _, r := range relaysOf(e, tag) {
		if r.Gen != credentialsGen(rotated.SSH) || r.Gen == credentialsGen(e.profiles.v.SSH) {
			t.Errorf("relay %s keeps generation %s after the credentials changed, want %s", r.Channel, r.Gen, credentialsGen(rotated.SSH))
		}
	}
}

// The generation changes with each credential and with nothing else.
func TestCredentialsGen(t *testing.T) {
	base := slurm.SSHConfig{HostName: "login.example.org", Port: 22, User: "example-user", IdentityFile: "/k", UserKnownHostsFile: "/kh"}
	seen := map[string]string{credentialsGen(base): "base"}
	for name, c := range map[string]slurm.SSHConfig{
		"user": {HostName: base.HostName, Port: 22, User: "other", IdentityFile: "/k", UserKnownHostsFile: "/kh"},
		"key":  {HostName: base.HostName, Port: 22, User: base.User, IdentityFile: "/k2", UserKnownHostsFile: "/kh"},
		"pin":  {HostName: base.HostName, Port: 22, User: base.User, IdentityFile: "/k", UserKnownHostsFile: "/kh2"},
		"port": {HostName: base.HostName, Port: 2222, User: base.User, IdentityFile: "/k", UserKnownHostsFile: "/kh"},
	} {
		g := credentialsGen(c)
		if other, dup := seen[g]; dup {
			t.Errorf("a changed %s has %s's generation", name, other)
		}
		seen[g] = name
	}
	if credentialsGen(base) != credentialsGen(base) {
		t.Error("the generation is not stable")
	}
}
