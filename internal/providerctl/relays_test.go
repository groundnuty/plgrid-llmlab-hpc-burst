package providerctl

// The Provider controller's side of HBEP-6's host-dialed mode: what each
// observation tells the relay owner (syncRelays), read back from what the
// owner keeps (Controller.Relays), and the events it raises. The ssh client
// is /bin/false, so every relay ends at once and is retried; the relays
// themselves are tested through the real path in internal/hostdial.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// hostDialed makes the env's controller host-dialed.
func hostDialed(e *env) { hostDialedOn(hostdial.NewHosts(), 1000)(e) }

// hostDialedOn makes the env's controller host-dialed, with the given
// one-Provider-per-host register and restart limit.
func hostDialedOn(hosts *hostdial.Hosts, restartLimit int) envOption {
	return func(e *env) { hostDialedWith(e, hosts, restartLimit) }
}

func hostDialedWith(e *env, hosts *hostdial.Hosts, restartLimit int) {
	front := exampleFront(e.t, e.buildDir)
	front.Mode, front.Endpoint = agentapi.DialHomeHostDialed, ""
	dir, err := os.MkdirTemp("", "pr")
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = os.RemoveAll(dir) })
	e.opts.Front = front
	e.opts.Relays = &RelaySettings{
		Ports: map[agentapi.RelayChannel]int{agentapi.RelayControl: 1, agentapi.RelayServing: 2}, Slots: hostdial.NewSlots(0), Hosts: hosts,
		ControlDir: dir, SSHBinary: "/bin/false", MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		RestartLimit: restartLimit, Epoch: func(context.Context) (int64, error) { return 3, nil },
	}
}

// runRelays enrolls the Provider for its login host, as its manager's start
// does (enrollRunnable), and runs the controller's relay owner until the test
// ends.
func runRelays(e *env) {
	e.t.Helper()
	if err := e.ctl.shared.relays.enroll(e.ctx, e.c); err != nil {
		e.t.Fatal(err)
	}
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
// relays, for its recorded job and the credentials' generation, started on
// the job the Provider's listing shows (the ssh, /bin/false, ends at once and
// is retried), with the Lease's epoch; and the job's end stops them.
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
	waitRelays(t, "both relays started on the listed job and retried", func() bool {
		for _, r := range relaysOf(e, tag) {
			if r.Starts < 2 || r.Epoch != 3 || !strings.HasPrefix(r.LastEnd, "retried") {
				return false
			}
		}
		return true
	})
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

// The emit site of RelayFailed (HBEP-6, What ends a relay; HBEP-8): a relay
// that ends for good for a reason other than superseded or the job's end, here
// more restarts than the limit, raises RelayFailed regarding its Allocation,
// naming the channel and the cause.
func TestARelayThatEndsForGoodRaisesRelayFailed(t *testing.T) {
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 2))
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	waitRelays(t, "RelayFailed raised", func() bool { return e.rec.has(v1alpha1.EventReasonRelayFailed) })
	tag := tagOf(e.get("alloc-a"))
	waitRelays(t, "both relays ended for good", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].Final && rs[1].Final
	})
	for _, r := range relaysOf(e, tag) {
		if !strings.Contains(r.FinalCause, "more than 2 restarts") {
			t.Errorf("relay %s ended %q, want more than 2 restarts", r.Channel, r.FinalCause)
		}
	}
}

// One host-dialed Provider per login host (HBEP-6, Capacity; #298, 56746eb):
// a Provider whose login host, as ssh -G resolves it, another host-dialed
// Provider of the process holds goes Ready False with LoginHostInUse, naming
// the host and the holder; it submits nothing even for an Allocation made
// before, keeps no relays, and raises no RelayFailed. The planner gives an
// unready Provider no Allocation (ProviderUnavailable, internal/lifecycle).
func TestASecondHostDialedProviderOnOneLoginHostIsRefused(t *testing.T) {
	hosts := hostdial.NewHosts()
	hosts.Enroll("another-provider", at(10, 0, 0)) // older than the env's Provider, made at 11:00
	if _, _, ok := hosts.Claim("login.example.org", "another-provider"); !ok {
		t.Fatal(err0)
	}
	e := newEnv(t, hostDialedOn(hosts, 1000))
	runRelays(e)
	ready := func() *metav1.Condition {
		t.Helper()
		if err := e.ctl.SyncStatus(e.ctx); err != nil {
			t.Fatal(err)
		}
		var prov v1alpha1.Provider
		if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
			t.Fatal(err)
		}
		return apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady)
	}
	c := ready()
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonLoginHostInUse ||
		!strings.Contains(c.Message, "login.example.org") || !strings.Contains(c.Message, "another-provider") {
		t.Fatalf("Ready is %+v; want False, LoginHostInUse, naming login.example.org and another-provider", c)
	}
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	if n := e.count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls for a Provider refused its login host, want 0", n)
	}
	if rs := relaysOf(e, tagOf(e.get("alloc-a"))); len(rs) != 0 {
		t.Errorf("a refused Provider keeps %d relays", len(rs))
	}
	if e.rec.has(v1alpha1.EventReasonRelayFailed) {
		t.Errorf("a refused login host raised RelayFailed; Ready carries it")
	}
	hosts.Release("another-provider")
	if c := ready(); c == nil || c.Reason == v1alpha1.ReasonLoginHostInUse {
		t.Errorf("Ready after the host was released: %+v, want it no longer LoginHostInUse", c)
	}
	e.run("alloc-a", 2)
	if n := e.count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls once the host was free, want 1", n)
	}
}

var err0 = errors.New("the register refused its first claim")

// A running job that a listing misses for a while keeps its relays (review
// of the final round): while it is absent, within the Provider's absence
// grace, each start is refused, not ended for good, and the Allocation stays
// Running, so its observation does not change; once it is listed again, the
// relays start again. A final end there would leave the Allocation without
// relays for the rest of its life.
func TestAJobMissingFromTheListingForAWhileKeepsItsRelays(t *testing.T) {
	e := newEnv(t, hostDialed)
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag, job := tagOf(a), a.Status.AtProvider.JobID
	waitRelays(t, "both relays started", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].Starts >= 1 && rs[1].Starts >= 1
	})
	e.batch.Vanish(job)
	waitRelays(t, "both starts refused while the job is not listed", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && strings.Contains(rs[0].LastRefusal, "not listed under tag") && strings.Contains(rs[1].LastRefusal, "not listed under tag")
	})
	e.run("alloc-a", 2)
	if p := e.get("alloc-a").Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
		t.Fatalf("the Allocation is %s within the absence grace, want Running", p)
	}
	starts := map[agentapi.RelayChannel]int{}
	for _, r := range relaysOf(e, tag) {
		if r.Final {
			t.Errorf("the %s relay ended for good (%s) while its job was missing from the listing", r.Channel, r.FinalCause)
		}
		starts[r.Channel] = r.Starts
	}
	e.batch.Reappear(job)
	waitRelays(t, "both relays started again once the job is listed", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].Starts > starts[rs[0].Channel] && rs[1].Starts > starts[rs[1].Channel]
	})
}

// After a restart the older Provider keeps its login host, whichever asks
// first (the delta review of PR 301, D4): a younger Provider, made at 12:00,
// claims the host before the env's Provider, made at 11:00, has enrolled; once
// it has, the older one is admitted and keeps its relays, and the younger is
// refused, naming it.
func TestAnOlderProviderKeepsItsLoginHostAfterARestart(t *testing.T) {
	hosts := hostdial.NewHosts()
	hosts.Enroll("helios-b", at(12, 0, 0))
	if _, _, ok := hosts.Claim("login.example.org", "helios-b"); !ok {
		t.Fatal("the younger Provider, alone, was refused")
	}
	e := newEnv(t, hostDialedOn(hosts, 1000))
	runRelays(e)
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	if c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady); c == nil || c.Reason == v1alpha1.ReasonLoginHostInUse {
		t.Fatalf("the older Provider's Ready is %+v; it must keep the login host", c)
	}
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	if n := e.count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls for the older Provider, want 1", n)
	}
	waitRelays(t, "the older Provider's relays", func() bool { return len(relaysOf(e, tagOf(e.get("alloc-a")))) == 2 })
	if holder, _, ok := hosts.Claim("login.example.org", "helios-b"); ok || holder != testProvider {
		t.Errorf("the younger Provider's claim after the older one's: %q, %v; want refused, naming %s", holder, ok, testProvider)
	}
}

// The cap of 2 Allocations is checked again in Create, from a fresh read,
// before it submits (the delta review of PR 301, D3): two Allocations that
// two leaders' planners each made past the other's count, beside one already
// submitted, are three, and the third's submission is refused with
// SubmitLimitReached; nothing is submitted for it.
func TestCreateRefusesAThirdAllocationOfAHostDialedProvider(t *testing.T) {
	e := newEnv(t, hostDialed)
	runRelays(e)
	for _, n := range []string{"alloc-a", "alloc-b"} {
		e.newAlloc(n)
		e.run(n, 4)
	}
	if n := e.count("sbatch"); n != 2 {
		t.Fatalf("%d sbatch calls for two Allocations, want 2", n)
	}
	e.newAlloc("alloc-c")
	e.run("alloc-c", 4)
	if n := e.count("sbatch"); n != 2 {
		t.Errorf("%d sbatch calls with a third Allocation, want still 2", n)
	}
	rec, err := readRecord(e.get("alloc-c"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached ||
		!strings.Contains(rec.sub.Reason, "at most 2 in host-dialed mode") {
		t.Errorf("the third Allocation's record %+v: want Rejected, SubmitLimitReached, naming host-dialed mode's 2", rec.sub)
	}
}
