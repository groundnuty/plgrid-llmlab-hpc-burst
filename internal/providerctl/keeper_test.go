package providerctl

// The controller's side of HBEP-6's relay keeper (#343, PR 4): what the
// leader sends a socket-level fake keeper (keeper_harness_test.go), and what
// it does with the keeper's answers. The expected values come from the
// HBEP's rules and the science agent's rulings on #343's questions, and
// from what each test writes itself, never from the code under test.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Alpha is unchanged and keeper mode is opt-in (#343's brief, binding):
// without a keeper socket the controller builds alpha's *hostdial.Owner from
// exactly the settings it is given, as before, with the process's shared
// login-host register; with one it builds no Owner and no ssh client, and
// keeps a register of its own. The keeper-mode env is the positive control.
func TestKeeperModeIsOptInAndAlphaBuildsItsOwner(t *testing.T) {
	alpha := newEnv(t, hostDialed)
	r := alpha.ctl.shared.relays
	if r == nil || r.keeper != nil {
		t.Fatalf("without a keeper socket: relays %v, keeper %v; want alpha's relay state and no keeper", r != nil, r != nil && r.keeper != nil)
	}
	var owner *hostdial.Owner = r.owner
	if owner == nil {
		t.Fatal("without a keeper socket the controller built no *hostdial.Owner")
	}
	want := *alpha.opts.Relays
	got := r.settings
	if got.ControlDir != want.ControlDir || got.SSHBinary != want.SSHBinary || got.Slots != want.Slots || got.Hosts != want.Hosts ||
		got.MinBackoff != want.MinBackoff || got.MaxBackoff != want.MaxBackoff || got.RestartLimit != want.RestartLimit ||
		len(got.Ports) != 2 || got.Ports[agentapi.RelayControl] != 1 || got.Ports[agentapi.RelayServing] != 2 || got.KeeperSocket != "" {
		t.Errorf("alpha's relay settings changed: got %+v, want %+v", got, want)
	}

	// Alpha's default register is the process's shared one.
	shared := newEnv(t, func(e *env) { hostDialedWith(e, nil, 1000) })
	if shared.ctl.shared.relays.hosts != hostdial.SharedHosts {
		t.Error("alpha with no register given does not use hostdial.SharedHosts")
	}

	k := startFakeKeeper(t)
	beta := newEnv(t, keeperMode(k, 3))
	kr := beta.ctl.shared.relays
	if kr == nil || kr.keeper == nil {
		t.Fatal("with a keeper socket the controller is not in keeper mode")
	}
	if kr.owner != nil || kr.ssh != nil {
		t.Errorf("keeper mode built alpha's relay owner (%v) or ssh client (%v)", kr.owner != nil, kr.ssh != nil)
	}
	if kr.hosts == hostdial.SharedHosts {
		t.Error("keeper mode uses the process's shared login-host register")
	}
	if beta.ctl.Relays() != nil {
		t.Error("keeper mode lists relays of its own")
	}
}

// A keeper socket that is not an absolute path within the 107-byte limit is
// refused at New.
func TestKeeperModeRefusesABadSocketPath(t *testing.T) {
	for _, p := range []string{"relaykeeper.sock", "/" + strings.Repeat("s", agentapi.MaxRelaySocketPath)} {
		e := newEnv(t, hostDialed)
		o := e.opts
		rs := *o.Relays
		rs.KeeperSocket = p
		o.Relays = &rs
		if _, err := New(e.c, e.c, e.c.Scheme(), o); err == nil {
			t.Errorf("socket %q was accepted", p)
		}
	}
}

// Fencing (HBEP-6; #343's brief): every set and every acknowledgement
// carries the sender's Lease epoch, leaseTransitions at its own acquisition.
// The test writes the Lease's leaseTransitions, 41, held by another copy;
// client-go's own elector, through the Provider manager's fence, takes it
// over, which raises the count by one, so the epoch is 42. The expected
// value is the test's own 41 plus client-go's rule, never read back through
// the fence's Epoch, which the sender uses.
//
// The same run then shows that a copy that loses the lead stops sending
// before it releases the Lease (HBEP-6, Fencing's beta half): its renewals
// fail, client-go releases the Lease, and by the time the release record is
// written the sender and acknowledger have returned; nothing is sent after it.
func TestKeeperSetsAndAcksCarryTheLeaseEpochAndStopBeforeTheRelease(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 0), func(e *env) { e.opts.Relays.Epoch = nil })
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	lease := &memLease{rec: &resourcelock.LeaderElectionRecord{
		HolderIdentity: "copy-old", LeaderTransitions: 41, LeaseDurationSeconds: 1,
		AcquireTime: metaTime(time.Now().Add(-time.Hour)), RenewTime: metaTime(time.Now().Add(-time.Hour)),
	}}
	ml := &memLock{id: "copy-new", store: lease}
	fence := NewRelayFence(ml)
	if err := wireRelayFence(fencedManager{fence: fence}, e.ctl); err != nil {
		t.Fatal(err)
	}
	var atRelease atomic.Int64
	var doneAtRelease atomic.Bool
	atRelease.Store(-1)
	lease.onRelease = func() {
		atRelease.Store(int64(k.requests()))
		done := e.ctl.shared.relays.keeper.doneChan()
		select {
		case <-done:
			doneAtRelease.Store(true)
		default:
		}
	}
	runDone := make(chan struct{})
	leading, stopped := make(chan struct{}), make(chan struct{})
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: fence, LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 200 * time.Millisecond,
		ReleaseOnCancel: true, Name: "provider-example",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				close(leading)
				_ = e.ctl.RunRelays(ctx)
				close(runDone)
			},
			OnStoppedLeading: func() { close(stopped) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go le.Run(context.Background())
	select {
	case <-leading:
	case <-time.After(30 * time.Second):
		t.Fatal("the elector did not lead")
	}
	// The Provider admits Allocations once an exchange succeeded under the lead.
	keeper := e.ctl.shared.relays.keeper
	waitRelays(t, "the keeper accepted a set of the lead", func() bool {
		_, pending, _ := keeper.claim()
		return keeper.unavailable(e.clock.Now()) == "" && pending == ""
	})
	a := e.running("alloc-a")
	end := wire.FinalEnd{UID: string(a.UID), Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayServing,
		Kind: wire.EndFailed, Cause: "the step limit is reached", At: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), Epoch: 1<<31 + 1, Seq: 3}
	k.script(func(k *fakeKeeper) { k.ends = []wire.FinalEnd{end} })
	set := k.waitSet(t, "a set naming alloc-a", 0, func(s wire.DesiredSet) bool { _, ok := entryOf(s, tagOf(a)); return ok })
	if set.LeaseEpoch != 42 {
		t.Errorf("the set's Lease epoch is %d, want 42: the test's 41, raised by one at the take-over", set.LeaseEpoch)
	}
	acks := k.waitAcks(t, "the end acknowledged", func(as []wire.Ack) bool { return len(as) > 0 })
	if acks[0].LeaseEpoch != 42 {
		t.Errorf("the ack's Lease epoch is %d, want 42", acks[0].LeaseEpoch)
	}
	// The positive control for the release check: sends flow while it leads.
	n := k.setCount()
	k.waitSet(t, "a later set while it leads", n, func(wire.DesiredSet) bool { return true })

	lease.mu.Lock()
	ml.failRenew = true
	lease.mu.Unlock()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("the elector did not stop leading after its renewals failed")
	}
	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the relay runnable did not return after the lead ended")
	}
	if !doneAtRelease.Load() {
		t.Error("the sender or the acknowledger still ran when the release record was written")
	}
	if got, final := atRelease.Load(), int64(k.requests()); got < 0 || final != got {
		t.Errorf("requests at the release: %d; after every sender returned: %d; want equal (nothing sent after the release)", got, final)
	}
}

// A copy whose lead has ended sends nothing, through the sender and the
// acknowledger's own steps called directly (no window to wait out): after
// StopRelays, the fence's release hook, returns, a set step and an ack step
// send nothing, and no goroutine of the lead is left. The positive control:
// the same steps send while it leads.
func TestKeeperSendsNothingOnceItsLeadEnds(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	a := e.running("alloc-a")
	k.waitSet(t, "a set while it leads", 0, func(wire.DesiredSet) bool { return true })
	end := wire.FinalEnd{UID: "uid-gone", Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayControl,
		Kind: wire.EndFailed, Cause: "the step limit is reached", At: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), Epoch: 1<<31 + 1, Seq: 1}
	r := e.ctl.shared.relays
	before := len(k.ackList()) // the sender runs too, so its sets are not counted here
	e.ctl.shared.ackEnd(r.leadContext(), end)
	if got := len(k.ackList()); got != before+1 {
		t.Fatalf("an ack step while it leads sent %d acks, want 1 (the positive control)", got-before)
	}

	e.ctl.StopRelays()
	select {
	case <-r.keeper.doneChan():
	default:
		t.Fatal("StopRelays returned while the sender or the acknowledger still ran")
	}
	before = k.requests()
	lead := r.leadContext()
	e.ctl.shared.sendSet(lead)
	e.ctl.shared.ackEnd(lead, end)
	e.run("alloc-a", 1)
	if got := k.requests(); got != before {
		t.Errorf("%d requests were sent after the lead ended", got-before)
	}
}

// Completeness (#343's Q6): a set is complete only once every Allocation of
// the Provider that is Running with a job ID has had an observation under
// this lead that reached a decision, and the keeper has answered the
// login-host claim. A new leader, as after a hand-over, has observed nothing
// under its lead: an unobserved Allocation, and one whose paths could not be
// resolved, make the set incomplete.
func TestKeeperSetIsCompleteOnlyOnceEveryRunningAllocationIsDecided(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	stopOld := keeperUp(e)
	a, b := e.running("alloc-a"), e.running("alloc-b")
	// The new leader: a controller with nothing in memory, whose lead begins
	// with both Allocations Running.
	stopOld()
	e.build()
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	from := k.setCount()
	runKeeperClient(e)
	// The first sets: claim unanswered, and no observation under this lead.
	first := k.waitSet(t, "the new leader's first set", from, func(wire.DesiredSet) bool { return true })
	if first.Complete {
		t.Error("the first set of the lead is complete, before any observation under it and before the keeper answered the claim")
	}
	// The claim is answered once a set was accepted; a still incomplete.
	n := k.setCount()
	k.waitSet(t, "a set after the first was accepted", n, func(wire.DesiredSet) bool { return true })

	e.run("alloc-a", 1)
	n = k.setCount()
	s := k.waitSet(t, "a set with alloc-a decided", n, func(s wire.DesiredSet) bool { _, ok := entryOf(s, tagOf(a)); return ok })
	if s.Complete {
		t.Error("the set is complete while alloc-b, Running, has had no observation under this lead")
	}

	// alloc-b's observation cannot resolve the login node's paths: no decision.
	e.platform.mu.Lock()
	e.platform.pathErr = errors.New("the login node's paths are not resolved yet")
	e.platform.mu.Unlock()
	e.run("alloc-b", 1)
	n = k.setCount()
	// n+1: a set built before the observation may arrive after the count.
	s = k.waitSet(t, "a set after alloc-b's unresolved observation", n+1, func(wire.DesiredSet) bool { return true })
	if s.Complete {
		t.Error("the set is complete although alloc-b's observation could not resolve the paths")
	}

	e.platform.mu.Lock()
	e.platform.pathErr = nil
	e.platform.mu.Unlock()
	e.run("alloc-b", 1)
	n = k.setCount()
	s = k.waitSet(t, "a complete set", n, func(s wire.DesiredSet) bool { return s.Complete })
	ea, okA := entryOf(s, tagOf(a))
	eb, okB := entryOf(s, tagOf(b))
	if !okA || !okB || len(s.Allocations) != 2 {
		t.Fatalf("the complete set names %+v, want alloc-a and alloc-b", s.Allocations)
	}
	for _, c := range []struct {
		got   wire.Entry
		alloc *v1alpha1.Allocation
	}{{ea, a}, {eb, b}} {
		tag := tagOf(c.alloc)
		want := wire.Entry{UID: string(c.alloc.UID), Tag: tag, JobID: c.alloc.Status.AtProvider.JobID,
			AllocDir:  "/scratch/example-user/stage/" + agentapi.AllocationDir(tag),
			AgentPath: "/scratch/example-user/stage/" + agentapi.StagedAgentPath(e.digest),
			Gen:       credentialsGen(e.profiles.v.SSH)}
		if c.got != want {
			t.Errorf("entry %+v, want %+v", c.got, want)
		}
	}
	if s.Provider != testProvider || s.ProviderUID != exampleProviderUID || !s.ProviderCreated.Equal(at(11, 0, 0)) {
		t.Errorf("the set names Provider %s %s created %v", s.Provider, s.ProviderUID, s.ProviderCreated)
	}
	if s.SSH != (wire.SSH{HostName: "login.example.org", User: "example-user", IdentityFile: "/nonexistent/key"}) {
		t.Errorf("the set's ssh settings are %+v", s.SSH)
	}
}

// The login-host claim (Q6): while the keeper refuses the Provider's sets for
// a host another Provider holds, no set is complete; once it accepts one, the
// next is.
func TestKeeperSetIsIncompleteUntilTheKeeperAnswersTheClaim(t *testing.T) {
	k := startFakeKeeper(t)
	refusing := atomic.Bool{}
	k.script(func(k *fakeKeeper) {
		k.onSet = func(s wire.DesiredSet) (wire.Kind, any) {
			if refusing.Load() {
				return wire.KindRefused, wire.Refused{Reason: wire.RefusedLoginHost, LoginHost: fakeLoginHost, Holder: "older-provider", Message: "held"}
			}
			return wire.KindReport, wire.Report{Provider: s.Provider, KeeperEpoch: 1<<31 + 1, LoginHost: fakeLoginHost}
		}
	})
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	e.running("alloc-a")
	refusing.Store(true)
	n := k.setCount()
	k.waitSet(t, "a set the keeper refuses", n, func(wire.DesiredSet) bool { return true })
	e.run("alloc-a", 1)
	n = k.setCount()
	if s := k.waitSet(t, "a set after the observation", n+1, func(wire.DesiredSet) bool { return true }); s.Complete {
		t.Error("a set is complete while the keeper refuses the claim")
	}
	refusing.Store(false)
	n = k.setCount()
	k.waitSet(t, "a set the keeper accepts", n, func(wire.DesiredSet) bool { return true })
	n = k.setCount()
	k.waitSet(t, "a complete set once the claim is answered", n, func(s wire.DesiredSet) bool { return s.Complete })
}

// An Allocation that carries RelayEndedForGood is sent with endedForGood
// (Q1 and #347), and one whose job is not shown running in the listing is
// not sent, and makes the set incomplete, so nothing is dropped (HBEP-4's
// absence grace).
func TestKeeperSetMarksEndedForGoodAndWaitsOnTheListing(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	a := e.running("alloc-a")
	if !setRelayEndedForGood(a, relayFailure{msg: "the serving relay ended for good: the step limit is reached", at: at(12, 0, 0)}) {
		t.Fatal("the test could not set the mark")
	}
	if err := e.c.Status().Update(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	e.run("alloc-a", 1)
	// Sets from before the mark name alloc-a without it; one built after it
	// must carry it.
	k.waitSet(t, "a complete set naming alloc-a with endedForGood", 0, func(s wire.DesiredSet) bool {
		en, ok := entryOf(s, tagOf(a))
		return ok && s.Complete && en.EndedForGood
	})

	// The job leaves the listing: the entry is not sent, and the set is
	// incomplete, so nothing is dropped, until an observation decides.
	e.batch.Vanish(a.Status.AtProvider.JobID)
	// One set may be built before the job vanished and arrive after this
	// count; the sender sends one at a time, so the next one after it was
	// built once the job had vanished.
	n := k.setCount()
	s := k.waitSet(t, "a set whose listing does not show the job", n+1, func(wire.DesiredSet) bool { return true })
	if _, ok := entryOf(s, tagOf(a)); ok || s.Complete {
		t.Errorf("with the job not listed the set names alloc-a (%v) or is complete (%v)", ok, s.Complete)
	}
}

// The listing's age and the validity (#343's Q5): the age at sending, from
// the Provider's latest listing by the controller's clock, and the validity,
// the poll interval in force plus the renew deadline (10 s): 30 s + 10 s,
// or 5 s + 10 s while the short interval is in force.
func TestKeeperSetCarriesTheListingAgeAndTheValidity(t *testing.T) {
	for _, c := range []struct {
		name   string
		maxAge time.Duration
		want   int64
	}{{"poll interval", 0, 40000}, {"busy interval", 5 * time.Second, 15000}} {
		t.Run(c.name, func(t *testing.T) {
			k := startFakeKeeper(t)
			e := newEnv(t, keeperMode(k, 5), func(e *env) {
				if c.maxAge > 0 {
					p := e.platform
					e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
						return maxAgePlatform{fakePlatform: p, maxAge: c.maxAge}, io.NopCloser(nil), nil
					}
				}
			})
			listed := e.clock.Now()
			if err := e.ctl.SyncStatus(e.ctx); err != nil {
				t.Fatal(err)
			}
			e.clock.Advance(7 * time.Second)
			runKeeperClient(e)
			s := k.waitSet(t, "a set", 0, func(wire.DesiredSet) bool { return true })
			if s.ListingAgeMS != 7000 {
				t.Errorf("listing age %d ms, want 7000 (the test's 7 s)", s.ListingAgeMS)
			}
			if s.ValidForMS != c.want {
				t.Errorf("validity %d ms, want %d", s.ValidForMS, c.want)
			}
			if !s.ListedAt.Equal(listed) {
				t.Errorf("listed at %v, want %v", s.ListedAt, listed)
			}
		})
	}
}

// The final ends (Q3, Q12; HBEP-6's Test plan): an end the keeper reports
// writes RelayEndedForGood, reason RelayFailed, raises RelayFailed once, and
// is acknowledged as marked with its own identity. Reported again (an ack
// that was lost, a crash between the mark and the ack), it is acknowledged
// again with no second event. A superseded end in beta is marked the same
// way, with its own message.
func TestKeeperFinalEndsAreMarkedAcknowledgedAndRaisedOnce(t *testing.T) {
	for _, c := range []struct {
		kind, cause, msg string
	}{
		{wire.EndFailed, "the step limit is reached", "the serving relay ended for good: the step limit is reached"},
		{wire.EndSuperseded, "superseded", "the serving relay ended for good: superseded, which is unexpected in beta"},
	} {
		t.Run(c.kind, func(t *testing.T) {
			k := startFakeKeeper(t)
			e := newEnv(t, keeperMode(k, 9))
			keeperUp(e)
			a := e.running("alloc-a")
			end := wire.FinalEnd{UID: string(a.UID), Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayServing,
				Kind: c.kind, Cause: c.cause, At: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), Epoch: 1<<31 + 2, Seq: 7}
			k.script(func(k *fakeKeeper) { k.ends, k.keepAcked = []wire.FinalEnd{end}, true })
			acks := k.waitAcks(t, "the end acknowledged twice (reported again after the first)", func(as []wire.Ack) bool { return len(as) >= 2 })
			want := wire.Ack{Provider: testProvider, LeaseEpoch: 9, UID: end.UID, JobID: end.JobID, Channel: end.Channel,
				Epoch: end.Epoch, Seq: end.Seq, Outcome: wire.OutcomeMarked}
			for _, got := range acks[:2] {
				if got != want {
					t.Errorf("ack %+v, want %+v", got, want)
				}
			}
			cond := e.get("alloc-a").GetCondition(v1alpha1.ConditionRelayEndedForGood)
			if cond.Status != "True" || cond.Reason != v1alpha1.ReasonRelayFailed || cond.Message != c.msg {
				t.Errorf("the mark is %s/%s %q, want True/RelayFailed %q", cond.Status, cond.Reason, cond.Message, c.msg)
			}
			if n := countReason(e.rec, v1alpha1.EventReasonRelayFailed); n != 1 {
				t.Errorf("RelayFailed was raised %d times, want once", n)
			}
		})
	}
}

// An end whose mark is refused is acknowledged as refused, with the reason,
// and raises nothing: the Allocation is gone (deleted), records another job,
// or no longer runs.
func TestKeeperRefusedMarksAreAcknowledgedWithTheirReason(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 9))
	keeperUp(e)
	a := e.running("alloc-a")
	b := e.running("alloc-b")
	b.Status.AtProvider.Phase = v1alpha1.AllocationEnded
	if err := e.c.Status().Update(e.ctx, b); err != nil {
		t.Fatal(err)
	}
	at0 := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	ends := []wire.FinalEnd{
		{UID: "uid-gone", Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayControl, Kind: wire.EndFailed, Cause: "x", At: at0, Epoch: 1<<31 + 1, Seq: 1},
		{UID: string(a.UID), Tag: tagOf(a), JobID: "999999", Channel: agentapi.RelayControl, Kind: wire.EndFailed, Cause: "x", At: at0, Epoch: 1<<31 + 1, Seq: 2},
		{UID: string(b.UID), Tag: tagOf(b), JobID: b.Status.AtProvider.JobID, Channel: agentapi.RelayServing, Kind: wire.EndFailed, Cause: "x", At: at0, Epoch: 1<<31 + 1, Seq: 3},
	}
	k.script(func(k *fakeKeeper) { k.ends = ends })
	acks := k.waitAcks(t, "an ack for each of the three ends", func(as []wire.Ack) bool {
		seqs := map[int64]bool{}
		for _, ak := range as {
			seqs[ak.Seq] = true
		}
		return len(seqs) == 3
	})
	reasons := map[int64]string{}
	for _, ak := range acks {
		if ak.Outcome != wire.OutcomeRefused {
			t.Errorf("ack %+v: want refused", ak)
		}
		reasons[ak.Seq] = ak.Reason
	}
	if reasons[1] != wire.AckDeleted || reasons[2] != wire.AckAnotherJob || reasons[3] != wire.AckNotRunning {
		t.Errorf("the refusals' reasons by end: %v; want 1 deleted, 2 another_job, 3 not_running", reasons)
	}
	for _, name := range []string{"alloc-a", "alloc-b"} {
		if marked(e.get(name)) {
			t.Errorf("%s was marked", name)
		}
	}
	if n := countReason(e.rec, v1alpha1.EventReasonRelayFailed); n != 0 {
		t.Errorf("RelayFailed was raised %d times for refused marks", n)
	}
}

// Q12, beta only: a mark whose write fails raises nothing and acknowledges
// nothing, the end is reported again, and once a write succeeds RelayFailed
// is raised once and the end acknowledged.
func TestKeeperFailedMarkWriteRaisesNothingUntilAWriteSucceeds(t *testing.T) {
	k := startFakeKeeper(t)
	var failing atomic.Bool
	var attempts atomic.Int32
	e := newEnv(t, keeperMode(k, 9), func(e *env) {
		e.c = &failingStatusClient{Client: e.c, failing: &failing, attempts: &attempts}
	})
	keeperUp(e)
	a := e.running("alloc-a")
	failing.Store(true)
	attempts.Store(0)
	end := wire.FinalEnd{UID: string(a.UID), Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayControl,
		Kind: wire.EndFailed, Cause: "the step limit is reached", At: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), Epoch: 1<<31 + 1, Seq: 2}
	k.script(func(k *fakeKeeper) { k.ends = []wire.FinalEnd{end} })
	waitRelays(t, "two failed writes of the mark", func() bool { return attempts.Load() >= 2 })
	// The second attempt comes from a later report, so the first one's
	// handling has returned: an ack or an event it made would show now.
	if acks := k.ackList(); len(acks) != 0 {
		t.Errorf("a failed write was acknowledged: %+v", acks)
	}
	if n := countReason(e.rec, v1alpha1.EventReasonRelayFailed); n != 0 {
		t.Errorf("RelayFailed was raised %d times before a write succeeded", n)
	}
	failing.Store(false)
	acks := k.waitAcks(t, "the end acknowledged once the write succeeds", func(as []wire.Ack) bool { return len(as) >= 1 })
	if acks[0].Outcome != wire.OutcomeMarked {
		t.Errorf("ack %+v, want marked", acks[0])
	}
	if !marked(e.get("alloc-a")) {
		t.Error("the Allocation is not marked")
	}
	if n := countReason(e.rec, v1alpha1.EventReasonRelayFailed); n != 1 {
		t.Errorf("RelayFailed was raised %d times, want once", n)
	}
}

// A refusal for a login host another Provider holds (HBEP-6, Capacity): the
// Provider's Ready turns False with LoginHostInUse, naming the host and its
// holder, and Create submits nothing. The positive control: once the keeper
// accepts the Provider's set, Ready is no longer LoginHostInUse.
func TestKeeperLoginHostRefusalMakesTheProviderUnready(t *testing.T) {
	k := startFakeKeeper(t)
	var refusing atomic.Bool
	k.script(func(k *fakeKeeper) {
		k.onSet = func(s wire.DesiredSet) (wire.Kind, any) {
			if refusing.Load() {
				return wire.KindRefused, wire.Refused{Reason: wire.RefusedLoginHost, LoginHost: fakeLoginHost, Holder: "older-provider", Message: "held"}
			}
			return wire.KindReport, wire.Report{Provider: s.Provider, KeeperEpoch: 1<<31 + 1, LoginHost: fakeLoginHost}
		}
	})
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	e.running("alloc-a")
	refusing.Store(true)
	n := k.setCount()
	k.waitSet(t, "a refused set", n, func(wire.DesiredSet) bool { return true })
	waitRelays(t, "the refusal kept", func() bool { _, inUse, _ := e.ctl.shared.relays.claimLogin(e.profiles.v.SSH); return inUse != "" })
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	ready := providerReady(t, e)
	if ready.Status != "False" || ready.Reason != v1alpha1.ReasonLoginHostInUse ||
		!strings.Contains(ready.Message, fakeLoginHost) || !strings.Contains(ready.Message, "older-provider") {
		t.Errorf("Ready is %s/%s %q; want False/LoginHostInUse naming %s and older-provider", ready.Status, ready.Reason, ready.Message, fakeLoginHost)
	}
	submits := e.count("sbatch")
	e.newAlloc("alloc-b")
	e.run("alloc-b", 3)
	if got := e.count("sbatch"); got != submits {
		t.Errorf("Create submitted %d jobs while the keeper refuses the login host", got-submits)
	}

	refusing.Store(false)
	waitRelays(t, "the refusal cleared", func() bool { _, inUse, _ := e.ctl.shared.relays.claimLogin(e.profiles.v.SSH); return inUse == "" })
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Reason == v1alpha1.ReasonLoginHostInUse {
		t.Errorf("Ready stays LoginHostInUse after the keeper accepted the set: %+v", r)
	}
}

func providerReady(t *testing.T, e *env) struct{ Status, Reason, Message string } {
	t.Helper()
	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady)
	if c == nil {
		t.Fatal("the Provider has no Ready condition")
	}
	return struct{ Status, Reason, Message string }{string(c.Status), c.Reason, c.Message}
}

// failingStatusClient fails the status patches while failing is set, and
// counts them.
type failingStatusClient struct {
	client.Client
	failing  *atomic.Bool
	attempts *atomic.Int32
}

func (c *failingStatusClient) Status() client.SubResourceWriter {
	return failingStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type failingStatusWriter struct {
	client.SubResourceWriter
	c *failingStatusClient
}

func (w failingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if _, ok := obj.(*v1alpha1.Allocation); ok {
		w.c.attempts.Add(1)
		if w.c.failing.Load() {
			return errors.New("the API server does not answer")
		}
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// doneChan is the current lead's done channel, or a closed one when no lead
// ran: a test reads whether the lead's goroutines are gone.
func (k *keeperState) doneChan() <-chan struct{} {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.done == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return k.done
}

// maxAgePlatform is a Platform that tells its poll interval in force, as
// slurm.Batch.ListingMaxAge does.
type maxAgePlatform struct {
	*fakePlatform
	maxAge time.Duration
}

func (p maxAgePlatform) ListingMaxAge() time.Duration { return p.maxAge }

// The keeper unreachable (HBEP-6, The relay keeper, as #360 amends it): with
// the keeper's socket missing, the Provider is Ready False with
// RelayKeeperUnavailable, while no exchange has succeeded under the lead, and
// later once none has for longer than a set's validity; no new Allocation is
// submitted meanwhile; at the first successful exchange the status loop, woken
// at once, makes it Ready again; and a running Allocation is left as it is
// throughout (its relays are the keeper's: Stage B shows them untouched).
func TestKeeperUnreachableMakesTheProviderUnreadyAndAdmitsNothing(t *testing.T) {
	k := newFakeKeeper(t) // its socket is missing until start
	// The status loop's ticker is the poll interval, 10 min here, so within
	// the test only the keeper's wake-up makes it write; the listing's
	// interval in force, which the validity uses, is 30 s.
	e := newEnv(t, keeperMode(k, 5), func(e *env) {
		e.opts.PollInterval = 10 * time.Minute
		p := e.platform
		e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
			return maxAgePlatform{fakePlatform: p, maxAge: 30 * time.Second}, io.NopCloser(nil), nil
		}
	})
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Status != "False" || r.Reason != v1alpha1.ReasonRelayKeeperUnavailable {
		t.Fatalf("before any exchange under a lead, Ready is %+v; want False/RelayKeeperUnavailable", r)
	}
	statusCtx, stopStatus := context.WithCancel(context.Background())
	statusDone := make(chan struct{})
	go func() { _ = e.ctl.status.Start(statusCtx); close(statusDone) }()
	t.Cleanup(func() { stopStatus(); <-statusDone })
	runKeeperClient(e)

	submits := e.count("sbatch")
	e.newAlloc("alloc-b")
	e.run("alloc-b", 3)
	if got := e.count("sbatch"); got != submits {
		t.Errorf("%d jobs were submitted while the keeper's socket is missing", got-submits)
	}
	if r := providerReady(t, e); r.Reason != v1alpha1.ReasonRelayKeeperUnavailable {
		t.Errorf("with the keeper's socket missing under the lead, Ready is %+v; want RelayKeeperUnavailable", r)
	}

	// The keeper comes: the first successful exchange wakes the status loop,
	// whose ticker is the poll interval, so Ready follows at once.
	k.start()
	waitRelays(t, "Ready again at the first successful exchange", func() bool {
		r := providerReady(t, e)
		return r.Status == "True"
	})
	e.run("alloc-b", 3)
	if got := e.count("sbatch"); got == submits {
		t.Error("no job was submitted once the keeper answered")
	}
	a := e.running("alloc-a")

	// The keeper goes, and stays gone past a set's validity (40 s by the
	// controller's clock): Ready False again, nothing submitted, and the
	// running Allocation is left as it is.
	k.stop()
	// A set in flight at the stop may still record its success: the clock
	// moves only once a send has failed after the stop, which the sender
	// makes after that success (one exchange at a time).
	keeper := e.ctl.shared.relays.keeper
	waitRelays(t, "a send failed after the keeper went", func() bool {
		keeper.mu.Lock()
		defer keeper.mu.Unlock()
		return keeper.lastErr != ""
	})
	e.clock.Advance(41 * time.Second)
	waitRelays(t, "Ready False once no exchange succeeded for longer than the validity", func() bool {
		r := providerReady(t, e)
		return r.Status == "False" && r.Reason == v1alpha1.ReasonRelayKeeperUnavailable
	})
	submits = e.count("sbatch")
	e.newAlloc("alloc-c")
	e.run("alloc-c", 3)
	if got := e.count("sbatch"); got != submits {
		t.Errorf("%d jobs were submitted after the keeper went", got-submits)
	}
	e.run("alloc-a", 2)
	if got := e.get("alloc-a"); got.Status.AtProvider.Phase != v1alpha1.AllocationRunning || got.Status.AtProvider.JobID != a.Status.AtProvider.JobID || marked(got) {
		t.Errorf("the running Allocation changed while the keeper was gone: %s, job %s, marked %v", got.Status.AtProvider.Phase, got.Status.AtProvider.JobID, marked(got))
	}

	k.start()
	waitRelays(t, "Ready again once the keeper answers", func() bool { return providerReady(t, e).Status == "True" })
}

// A pending claim (the review of PR 360): a keeper that answers but has
// accepted no set of this lead (here it refuses every set for its epoch,
// which it decided on, so the keeper counts as reachable) leaves the claim
// pending, as alpha's pending case does: Create submits nothing and Ready is
// False. The positive control: once it accepts a set, Ready is True and Create
// submits.
func TestKeeperAPendingClaimAdmitsNothing(t *testing.T) {
	k := startFakeKeeper(t)
	var accepting atomic.Bool
	k.script(func(k *fakeKeeper) {
		k.onSet = func(s wire.DesiredSet) (wire.Kind, any) {
			if !accepting.Load() {
				return wire.KindRefused, wire.Refused{Reason: wire.RefusedEpoch, HighestEpoch: 9, Message: "lower"}
			}
			return wire.KindReport, wire.Report{Provider: s.Provider, KeeperEpoch: 1<<31 + 1, LoginHost: fakeLoginHost}
		}
	})
	e := newEnv(t, keeperMode(k, 5))
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	runKeeperClient(e)
	keeper := e.ctl.shared.relays.keeper
	waitRelays(t, "an exchange the keeper decided on (an epoch refusal)", func() bool { return keeper.unavailable(e.clock.Now()) == "" })
	submits := e.count("sbatch")
	e.newAlloc("alloc-a")
	e.run("alloc-a", 3)
	if got := e.count("sbatch"); got != submits {
		t.Errorf("%d jobs were submitted while the keeper had accepted no set of the lead", got-submits)
	}
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Status != "False" || !strings.Contains(r.Message, "has not accepted") {
		t.Errorf("with the claim pending, Ready is %+v; want False, naming the pending claim", r)
	}

	accepting.Store(true)
	waitRelays(t, "the claim answered", func() bool { _, pending, _ := keeper.claim(); return pending == "" })
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Status != "True" {
		t.Errorf("once the keeper accepted a set, Ready is %+v; want True", r)
	}
	e.run("alloc-a", 3)
	if got := e.count("sbatch"); got == submits {
		t.Error("no job was submitted once the keeper accepted a set")
	}
}

// The listing's age at sending (#343's Q5; the review of PR 360): a set that
// waits to be sent behind an acknowledgement the keeper is slow to answer
// carries its listing's age when it is sent, the wait included, never when it
// was built. The keeper holds the ack until the test releases it; a set is
// built while it waits; the test then moves the controller's clock on 3 s and
// releases the ack: the set sent next is 3000 ms old, the listing being read
// at the test's start and the clock moving only by the test's hand.
func TestKeeperListingAgeCountsTheWaitBehindAnAck(t *testing.T) {
	k := startFakeKeeper(t)
	release, ackIn := make(chan struct{}), make(chan struct{}, 1)
	k.script(func(k *fakeKeeper) {
		k.onAck = func(a wire.Ack) (wire.Kind, any) {
			wake(ackIn)
			<-release
			return wire.KindAcked, wire.Acked{Provider: a.Provider, Decided: true}
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	a := e.running("alloc-a")
	keeper := e.ctl.shared.relays.keeper
	var built atomic.Int64
	keeper.mu.Lock()
	keeper.built = func() { built.Add(1) }
	keeper.mu.Unlock()

	end := wire.FinalEnd{UID: string(a.UID), Tag: tagOf(a), JobID: a.Status.AtProvider.JobID, Channel: agentapi.RelayServing,
		Kind: wire.EndFailed, Cause: "the step limit is reached", At: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), Epoch: 1<<31 + 1, Seq: 3}
	k.script(func(k *fakeKeeper) { k.ends = []wire.FinalEnd{end} })
	select {
	case <-ackIn:
	case <-time.After(30 * time.Second):
		t.Fatal("no acknowledgement reached the keeper")
	}
	b0 := built.Load()
	waitRelays(t, "a set built while the ack holds the exchange", func() bool { return built.Load() > b0 })
	n := k.setCount() // no set can be sent while the ack is held
	e.clock.Advance(3 * time.Second)
	close(release)
	s := k.waitSet(t, "the set sent once the ack was answered", n, func(wire.DesiredSet) bool { return true })
	if s.ListingAgeMS != 3000 {
		t.Errorf("the set's listing age is %d ms; want 3000, its age when sent, after the 3 s wait", s.ListingAgeMS)
	}
}

// A Provider deleted and created again under its name (the review of PR 360):
// the supervisor keeps a Provider's manager by name, so the old controller
// sees the new UID; it sends no set from then on, with either UID, admits
// nothing, and its relay runnable returns errProviderReplaced, which stops its
// manager. The next controller, a new one, sends the new UID and
// creationTimestamp, its claim pending until the keeper accepts one of its
// sets, so nothing of the old Provider's state is carried over.
func TestKeeperAProviderCreatedAgainStopsItsController(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ran := make(chan error, 1)
	go func() { ran <- e.ctl.RunRelays(ctx) }()
	k.waitSet(t, "a set of the first Provider", 0, func(s wire.DesiredSet) bool { return s.ProviderUID == exampleProviderUID })

	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	if err := e.c.Delete(e.ctx, &prov); err != nil {
		t.Fatal(err)
	}
	created := at(11, 30, 0)
	again := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: testProvider, UID: "uid-example-provider-again", CreationTimestamp: metav1.NewTime(created)}}
	if err := e.c.Create(e.ctx, again); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &got); err != nil || string(got.UID) != "uid-example-provider-again" {
		t.Fatalf("the re-created Provider reads back with UID %q (%v); the test needs the one it wrote", got.UID, err)
	}
	select {
	case err := <-ran:
		if !errors.Is(err, errProviderReplaced) {
			t.Fatalf("the old controller's relay runnable returned %v; want errProviderReplaced", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the old controller's relay runnable did not return after its Provider was created again")
	}
	k.mu.Lock()
	for _, s := range k.sets {
		if s.ProviderUID != exampleProviderUID {
			t.Errorf("the old controller sent a set with UID %q", s.ProviderUID)
		}
	}
	k.mu.Unlock()
	if _, inUse, _ := e.ctl.shared.relays.claimLogin(e.profiles.v.SSH); inUse == "" {
		t.Error("the old controller still grants the claim after its Provider was created again")
	}

	// The next controller, as the supervisor starts it again.
	from := k.setCount()
	e.build()
	keeperUp(e)
	s := k.waitSet(t, "a set of the new controller", from, func(wire.DesiredSet) bool { return true })
	if s.ProviderUID != "uid-example-provider-again" || !s.ProviderCreated.Equal(created) {
		t.Errorf("the new controller's set names UID %q created %v; want the re-created Provider's", s.ProviderUID, s.ProviderCreated)
	}
}

// The lead's end flags itself before it takes the exchange lock (the review of
// PR 360), so an exchange queued behind one in flight does not begin: with the
// flag set and the lead not yet ended, an exchange sends nothing. The positive
// control: with the flag clear, the same exchange is sent.
func TestKeeperAnExchangeQueuedBehindTheLeadsEndDoesNotBegin(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	keeperUp(e)
	r := e.ctl.shared.relays
	lead := r.leadContext()
	ack := wire.Ack{Provider: testProvider, LeaseEpoch: 5, UID: "uid-gone", JobID: "1", Channel: agentapi.RelayControl,
		Seq: 1, Outcome: wire.OutcomeRefused, Reason: wire.AckDeleted}
	var acked wire.Acked
	before := len(k.ackList())
	if err := r.keeper.exchange(lead, wire.KindAck, ack, &acked); err != nil || len(k.ackList()) != before+1 {
		t.Fatalf("the positive control: an exchange under the lead returned %v and sent %d acks, want 1", err, len(k.ackList())-before)
	}
	r.keeper.ending.Store(true)
	if lead.Err() != nil {
		t.Fatal("the lead ended; the test needs it live with the end flagged")
	}
	before = len(k.ackList())
	if err := r.keeper.exchange(lead, wire.KindAck, ack, &acked); !errors.Is(err, errLeadEnded) {
		t.Errorf("with the lead's end flagged, the exchange returned %v; want errLeadEnded", err)
	}
	if got := len(k.ackList()); got != before {
		t.Errorf("with the lead's end flagged, %d acks were sent", got-before)
	}
}
