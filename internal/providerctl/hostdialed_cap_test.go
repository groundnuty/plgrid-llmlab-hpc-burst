package providerctl

// HBEP-4, open question 17, amended 2026-11-20, and HBEP-6, Capacity: a host-dialed Provider's controller counts its
// Allocations again from a fresh read just before it submits, against the same cap as the planner:
// floor((transport.loginHostBudget - M) / 2), M being the stack's Providers on its login host, and 2 while the profile
// sets no budget and in relay-keeper mode. Every number is worked from that text.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// withBudget sets the profile's transport.loginHostBudget.
func withBudget(b int32) envOption {
	return func(e *env) {
		e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.Transport = &v1alpha1.TransportProfile{LoginHostBudget: &b} })
	}
}

func TestCreateHonoursTheProfilesBudget(t *testing.T) {
	// another is a younger Provider of the stack on the same login host: it holds a master there, so it counts in M, and
	// the env's Provider (older) keeps the host.
	withAnother := func() *hostdial.Hosts {
		h := hostdial.NewHosts()
		h.Enroll("another-provider", at(12, 0, 0))
		h.Claim("login.example.org", "another-provider")
		return h
	}
	for _, tc := range []struct {
		name  string
		hosts func() *hostdial.Hosts
		mods  func(hosts *hostdial.Hosts) []envOption
		cap   int
	}{
		{"budget unset: 2", hostdial.NewHosts, func(h *hostdial.Hosts) []envOption { return []envOption{hostDialedOn(h, 1000)} }, 2},
		{"budget 9, one Provider: 4", hostdial.NewHosts, func(h *hostdial.Hosts) []envOption { return []envOption{hostDialedOn(h, 1000), withBudget(9)} }, 4},
		{"budget 11, one Provider: 5", hostdial.NewHosts, func(h *hostdial.Hosts) []envOption { return []envOption{hostDialedOn(h, 1000), withBudget(11)} }, 5},
		{"budget 11, two Providers on the host: 4", withAnother, func(h *hostdial.Hosts) []envOption { return []envOption{hostDialedOn(h, 1000), withBudget(11)} }, 4},
		{"budget 17, one Provider: 8", hostdial.NewHosts, func(h *hostdial.Hosts) []envOption { return []envOption{hostDialedOn(h, 1000), withBudget(17)} }, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := tc.hosts()
			e := newEnv(t, tc.mods(hosts)...)
			runRelays(e)
			for i := range tc.cap {
				n := fmt.Sprintf("alloc-%d", i)
				e.newAlloc(n)
				e.run(n, 4)
			}
			if n := e.count("sbatch"); n != tc.cap {
				t.Fatalf("%d sbatch calls for %d Allocations under a cap of %d, want %d", n, tc.cap, tc.cap, tc.cap)
			}
			over := "alloc-over"
			e.newAlloc(over)
			e.run(over, 4)
			if n := e.count("sbatch"); n != tc.cap {
				t.Errorf("%d sbatch calls with one Allocation over the cap, want still %d", n, tc.cap)
			}
			rec, err := readRecord(e.get(over))
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("at most %d in host-dialed mode", tc.cap)
			if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(rec.sub.Reason, want) {
				t.Errorf("the Allocation over the cap has record %+v: want Rejected, SubmitLimitReached, naming %q", rec.sub, want)
			}
		})
	}
}

// In relay-keeper mode the cap stays 2 whatever the budget (K2: the keeper's wire carries no budget), without an error
// and without M: the keeper's claims, not the process's register, hold the login host there.
func TestHostDialedCapInRelayKeeperModeIsTwoWhateverTheBudget(t *testing.T) {
	spec := func(b int32) *v1alpha1.PlatformProfileSpec {
		return &v1alpha1.PlatformProfileSpec{Transport: &v1alpha1.TransportProfile{LoginHostBudget: &b}}
	}
	// The register holds the Provider's claim too, so a keeper-mode cap that read it would show the budget's 5 rather than
	// fail on a missing register.
	keeperHosts := hostdial.NewHosts()
	keeperHosts.Enroll("p", at(11, 0, 0))
	keeperHosts.Claim("login.example.org", "p")
	keeper := &relayState{keeper: &keeperState{}, hosts: keeperHosts, provider: "p"}
	for _, b := range []int32{5, 11, 17} {
		if c, known := keeper.hostDialedCap(spec(b)); c != 2 || !known {
			t.Errorf("relay-keeper mode, budget %d: cap %d, known %v; want 2, true", b, c, known)
		}
	}
	// The same budget in the process's own relay owner counts M from the register, so the contrast is the mode alone.
	hosts := hostdial.NewHosts()
	hosts.Enroll("p", at(11, 0, 0))
	hosts.Claim("login.example.org", "p")
	own := &relayState{hosts: hosts, provider: "p"}
	if c, known := own.hostDialedCap(spec(11)); c != 5 || !known {
		t.Errorf("own relay owner, budget 11: cap %d, known %v; want 5, true", c, known)
	}
	// A Provider that has not said which host it uses is not known: Create refuses, rather than guess M.
	own2 := &relayState{hosts: hostdial.NewHosts(), provider: "q"}
	if _, known := own2.hostDialedCap(spec(11)); known {
		t.Error("a Provider with no login host said is known")
	}
	if c, known := own2.hostDialedCap(&v1alpha1.PlatformProfileSpec{}); c != 2 || !known {
		t.Errorf("no budget and no login host said: cap %d, known %v; want 2, true (unset reads no M)", c, known)
	}
}

const loginHost = "login.example.org"

// setBudget changes the profile's budget the way an operator's edit does: the next read sees it.
func setBudget(e *env, b int32) {
	e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.Transport = &v1alpha1.TransportProfile{LoginHostBudget: &b} })
}

// restartRelays models a controller restart for the relays: the old owner stops, and a new controller is built with a
// register and slots of its own, empty as a new process's are, over the same objects. The returned owner runs until the
// test ends.
func restartRelays(e *env, stop func()) {
	e.t.Helper()
	stop()
	e.opts.Relays.Hosts, e.opts.Relays.Slots = hostdial.NewHosts(), hostdial.NewSlots(0)
	e.build()
	runRelays(e)
}

// stoppableRelays is runRelays with a stop for the test to call.
func stoppableRelays(e *env) (stop func()) {
	e.t.Helper()
	if err := e.ctl.shared.relays.enroll(e.ctx, e.c); err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.ctl.RunRelays(ctx); close(done) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			e.t.Error("the relay owner did not stop")
		}
	}
}

// pairsHeld reports, for the Allocations named, whether each holds its pair of relays and none waits for slots.
func pairsHeld(e *env, tags map[string]bool) bool {
	held := map[string]int{}
	for _, r := range e.ctl.Relays() {
		if r.Waiting {
			return false
		}
		held[r.Tag]++
	}
	for tag := range tags {
		if held[tag] != 2 {
			return false
		}
	}
	return true
}

// Through the reconciler: a cap of 5 gives each of five running Allocations its pair of relays on the login node, none
// waiting for slots (the relay limit is max(2 x cap, 2 x admitted): 10 at a budget of 11 beside one master, 2 x 5 + 1 <=
// 11). Without the limit the third to fifth would wait for slots for good, with no error. With no budget the limit is the
// default 4, as on main.
func TestEveryAdmittedAllocationHoldsItsPair(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mods   []envOption
		allocs int
		inUse  int
	}{
		{"budget 11: five Allocations each hold their pair", []envOption{hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11)}, 5, 10},
		{"budget 9: four", []envOption{hostDialedOn(hostdial.NewHosts(), 1000), withBudget(9)}, 4, 8},
		{"no budget: the default 4, two Allocations", []envOption{hostDialedOn(hostdial.NewHosts(), 1000)}, 2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, tc.mods...)
			runRelays(e)
			tags := map[string]bool{}
			for i := range tc.allocs {
				n := fmt.Sprintf("alloc-%d", i)
				e.newAlloc(n)
				e.run(n, 4)
				tags[tagOf(e.get(n))] = true
			}
			waitRelays(t, "every Allocation holds its pair, none waiting for slots", func() bool { return pairsHeld(e, tags) })
			if got := e.opts.Relays.Slots.InUse(loginHost); got != tc.inUse {
				t.Errorf("%d slots taken on the login node, want %d", got, tc.inUse)
			}
		})
	}
}

// THE REVIEW'S SCENARIO (PR 814, point 1): two Allocations run, a third is admitted (submitted, still queued), the budget
// is cut from 11 to 5 (a cap of 2), and then the third's job starts. Under a limit of 2 x cap alone it would wait for
// relays, miss its registration and be released; the limit counts the Provider's admitted Allocations too, so the third
// GETS its pair, and nothing is released.
func TestAnAdmittedAllocationStillGetsItsPairAfterTheBudgetIsCut(t *testing.T) {
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11))
	runRelays(e)
	tags := map[string]bool{}
	for _, n := range []string{"alloc-a", "alloc-b"} {
		e.newAlloc(n)
		e.run(n, 4)
		tags[tagOf(e.get(n))] = true
	}
	e.batch.Enqueue(fake.Script{QueueWait: 10 * time.Minute}) // the third job queues for ten minutes
	e.newAlloc("alloc-c")
	e.run("alloc-c", 4)
	if p := e.get("alloc-c").Status.AtProvider.Phase; p == v1alpha1.AllocationRunning {
		t.Fatalf("the third Allocation runs at once; the row needs it queued")
	}
	if n := e.count("sbatch"); n != 3 {
		t.Fatalf("%d sbatch calls, want 3: the third is admitted", n)
	}
	waitRelays(t, "the first two hold their pairs", func() bool { return pairsHeld(e, tags) })
	setBudget(e, 5) // the operator cuts the budget: a cap of floor((5 - 1) / 2) = 2
	e.run("alloc-a", 2)
	e.clock.Advance(11 * time.Minute) // the third job starts
	e.run("alloc-c", 4)
	a := e.get("alloc-c")
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the third Allocation is %s after its job started, want Running", a.Status.AtProvider.Phase)
	}
	tags[tagOf(a)] = true
	waitRelays(t, "the admitted third Allocation gets its pair after the cut, none waiting", func() bool { return pairsHeld(e, tags) })
	if got := e.opts.Relays.Slots.InUse(loginHost); got != 6 {
		t.Errorf("%d slots taken, want 6: three pairs, the held two untouched", got)
	}
	for _, n := range []string{"alloc-a", "alloc-b", "alloc-c"} {
		if p := e.get(n).Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
			t.Errorf("%s is %s, want Running: nothing is released", n, p)
		}
	}
}

// A restart with five Allocations serving under a budget that was cut meanwhile (a cap of 2): the new process's slots
// are empty, and every admitted Allocation takes its pair again, with no exemption: five admitted give a limit of 10.
// A sixth admission is refused at the cut cap by the final check before its submit, so it never asks for a pair.
func TestARestartUnderACutBudgetReadoptsEveryServingAllocation(t *testing.T) {
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11))
	stop := stoppableRelays(e)
	names := []string{"alloc-0", "alloc-1", "alloc-2", "alloc-3", "alloc-4"}
	tags := map[string]bool{}
	for _, n := range names {
		e.newAlloc(n)
		e.run(n, 4)
		tags[tagOf(e.get(n))] = true
	}
	waitRelays(t, "five pairs held", func() bool { return pairsHeld(e, tags) })
	setBudget(e, 5) // cut while serving: a cap of 2
	restartRelays(e, stop)
	for _, n := range names {
		e.run(n, 3)
	}
	waitRelays(t, "all five re-adopted, each with its pair, none waiting", func() bool { return pairsHeld(e, tags) })
	if got := e.opts.Relays.Slots.InUse(loginHost); got != 10 {
		t.Errorf("%d slots taken after the restart, want 10", got)
	}
	e.newAlloc("alloc-6")
	e.run("alloc-6", 4)
	if n := e.count("sbatch"); n != 5 {
		t.Errorf("%d sbatch calls, want still 5: the sixth is refused at the cut cap", n)
	}
	rec, err := readRecord(e.get("alloc-6"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(rec.sub.Reason, "at most 2 in host-dialed mode") {
		t.Errorf("the sixth Allocation's record %+v: want Rejected, SubmitLimitReached, naming the cut cap of 2", rec.sub)
	}
	if rs := relaysOf(e, tagOf(e.get("alloc-6"))); len(rs) != 0 {
		t.Errorf("the refused sixth Allocation has %d relays", len(rs))
	}
}

// A relay's reconnect keeps its pair after a reduction: the pair is taken once for the Allocation, and a relay that
// drops and starts again asks for none. Five Allocations hold their pairs; the budget is cut to 5; every relay starts
// again (the ssh double ends at once and is retried), and no pair is lost or waits.
func TestAReconnectKeepsItsPairAfterTheBudgetIsCut(t *testing.T) {
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11))
	runRelays(e)
	tags := map[string]bool{}
	for i := range 5 {
		n := fmt.Sprintf("alloc-%d", i)
		e.newAlloc(n)
		e.run(n, 4)
		tags[tagOf(e.get(n))] = true
	}
	waitRelays(t, "five pairs held", func() bool { return pairsHeld(e, tags) })
	setBudget(e, 5)
	e.run("alloc-0", 2)
	before := map[string]int{}
	for _, r := range e.ctl.Relays() {
		before[fmt.Sprintf("%s/%s", r.Tag, r.Channel)] = r.Starts
	}
	waitRelays(t, "every relay started again after the cut", func() bool {
		for _, r := range e.ctl.Relays() {
			if r.Starts < before[fmt.Sprintf("%s/%s", r.Tag, r.Channel)]+2 {
				return false
			}
		}
		return true
	})
	if !pairsHeld(e, tags) {
		t.Error("a pair was lost or waits after the reconnects")
	}
	if got := e.opts.Relays.Slots.InUse(loginHost); got != 10 {
		t.Errorf("%d slots taken after the reconnects, want 10", got)
	}
}

// BEFORE THE MASTER (PR 814, point 2): the Provider records its login host in the register before any command can open
// its ssh master, so the stack's other Providers count that master in M from the moment it can exist.
func TestAProviderClaimsItsLoginHostBeforeItsMasterCanOpen(t *testing.T) {
	hosts := hostdial.NewHosts()
	var claimedWhenMade, made bool
	e := newEnv(t, hostDialedOn(hosts, 1000), func(e *env) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			_, claimedWhenMade = hosts.OnHostOf(e.opts.Provider)
			made = true
			return inner(v)
		}
	})
	if _, _, err := e.ctl.shared.h.get(e.ctx); err != nil {
		t.Fatal(err)
	}
	if !made {
		t.Fatal("no Platform was made")
	}
	if !claimedWhenMade {
		t.Fatal("the Platform (so, soon, the master) was made before the Provider's login host was in the register")
	}
}

// M counts every live master (PR 814, point 2): at budget 5, a second Provider on the host counts in M as soon as it can
// have a master, before its first status claim and after a profile-read failure, so the first Provider's cap is computed
// with M = 2, not 1. A Provider that never made a Platform and cannot read its profile has no master, so it does not count.
func TestASecondProvidersOpenMasterCountsInM(t *testing.T) {
	hosts := hostdial.NewHosts()
	e1 := newEnv(t, hostDialedOn(hosts, 1000), withBudget(5))
	runRelays(e1)
	b5 := int32(5)
	spec := &v1alpha1.PlatformProfileSpec{Transport: &v1alpha1.TransportProfile{LoginHostBudget: &b5}}
	if _, _, err := e1.ctl.shared.h.get(e1.ctx); err != nil { // its login host is in the register before its master
		t.Fatal(err)
	}
	if c, known := e1.ctl.shared.relays.hostDialedCap(spec); !known || c != 2 {
		t.Fatalf("one Provider at budget 5: cap %d known %v, want 2 (M = 1)", c, known)
	}
	e2 := newEnv(t, hostDialedOn(hosts, 1000), withBudget(5), func(e *env) {
		e.opts.Provider = "second-provider"
		prov := &v1alpha1.Provider{}
		prov.Name = "second-provider"
		prov.CreationTimestamp = metav1.NewTime(at(12, 30, 0)) // younger: it is refused the host, and its master is open all the same
		if err := e.c.Create(e.ctx, prov); err != nil {
			e.t.Fatal(err)
		}
	})
	// Before its first status claim: the get that precedes any master has claimed.
	if _, _, err := e2.ctl.shared.h.get(e2.ctx); err != nil {
		t.Fatal(err)
	}
	if m, ok := hosts.OnHostOf(testProvider); !ok || m != 2 {
		t.Fatalf("M = %d (known %v) after the second Provider's get, want 2", m, ok)
	}
	if c, _ := e1.ctl.shared.relays.hostDialedCap(spec); c != 1 {
		t.Fatalf("the first Provider's cap is %d with a second master open, want floor((5 - 2) / 2) = 1", c)
	}
	// After a profile-read failure: its master is still open, so its claim stays.
	e2.profiles.mu.Lock()
	e2.profiles.err = errors.New("the profile cannot be read")
	e2.profiles.mu.Unlock()
	_ = e2.ctl.SyncStatus(e2.ctx)
	if m, ok := hosts.OnHostOf(testProvider); !ok || m != 2 {
		t.Fatalf("M = %d (known %v) after the second Provider's profile read failed with its master open, want 2", m, ok)
	}
	// A Provider that never made a Platform, and cannot read its profile, has no master: no claim.
	e3 := newEnv(t, hostDialedOn(hosts, 1000), func(e *env) {
		e.opts.Provider = "third-provider"
		e.profiles.err = errors.New("the profile cannot be read")
		prov := &v1alpha1.Provider{}
		prov.Name = "third-provider"
		prov.CreationTimestamp = metav1.NewTime(at(12, 45, 0))
		if err := e.c.Create(e.ctx, prov); err != nil {
			e.t.Fatal(err)
		}
	})
	_ = e3.ctl.SyncStatus(e3.ctx)
	if m, _ := hosts.OnHostOf(testProvider); m != 2 {
		t.Fatalf("M = %d after a Provider with no master failed to read its profile, want still 2", m)
	}
}

// THE FINAL CHECK READS THE PROFILE NOW (PR 814, point 3): the budget falls from 11 to 5 while the Allocation's files are
// staged, and the check right before the submit refuses the third submission against the cap of 2, not the cap of 5 the
// pass began with.
func TestTheFinalCheckReadsTheBudgetNotTheOneStagingBeganWith(t *testing.T) {
	cut := false
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11), func(e *env) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			p, c, err := inner(v)
			return stagingHook{Platform: p, onEnsure: func() {
				if cut {
					cut = false
					setBudget(e, 5) // cut while the third Allocation's files are staged
				}
			}}, c, err
		}
	})
	runRelays(e)
	for _, n := range []string{"alloc-a", "alloc-b"} {
		e.newAlloc(n)
		e.run(n, 4)
	}
	cut = true
	e.newAlloc("alloc-c")
	e.run("alloc-c", 4)
	if n := e.count("sbatch"); n != 2 {
		t.Errorf("%d sbatch calls, want 2: the third is refused by the cut cap", n)
	}
	rec, err := readRecord(e.get("alloc-c"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(rec.sub.Reason, "at most 2 in host-dialed mode") {
		t.Errorf("the third Allocation's record %+v: want Rejected, SubmitLimitReached, at most 2", rec.sub)
	}
}

// stagingHook is a Platform that runs onEnsure before each EnsureFile, the staging that precedes a submit.
type stagingHook struct {
	Platform
	onEnsure func()
}

func (p stagingHook) EnsureFile(ctx context.Context, f provider.File) error {
	p.onEnsure()
	return p.Platform.EnsureFile(ctx, f)
}

// A CAP OF 0 ENDS THE ALLOCATION (PR 814, point 4): the planner plans M = 1 until the Provider has claimed its host, so it
// can create an Allocation the final check then finds no room for (budget 5 with five Providers on the host: floor(0 / 2)
// = 0). That Allocation ends for good, phase Refused with one AllocationRefused event, and is never retried as
// Submitting, so its Instance ends and the planner, now knowing M, places the deployment elsewhere.
func TestACapOfZeroEndsTheAllocationForGood(t *testing.T) {
	hosts := hostdial.NewHosts()
	for i := range 4 { // four other Providers of the stack on the host
		n := fmt.Sprintf("other-%d", i)
		hosts.Enroll(n, at(12, 0, 0).Add(time.Duration(i)*time.Minute))
		hosts.Claim(loginHost, n)
	}
	e := newEnv(t, hostDialedOn(hosts, 1000), withBudget(5))
	runRelays(e)
	// The env's Provider is the oldest (11:00), so it holds the host; M = 5 with the four others.
	e.newAlloc("alloc-a")
	e.run("alloc-a", 6)
	if n := e.count("sbatch"); n != 0 {
		t.Fatalf("%d sbatch calls for a Provider whose cap is 0, want 0", n)
	}
	a := e.get("alloc-a")
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRefused {
		t.Fatalf("the Allocation is %s (%s), want Refused: a cap of 0 ends it, it never retries as Submitting", a.Status.AtProvider.Phase, a.Status.AtProvider.Reason)
	}
	if !strings.Contains(a.Status.AtProvider.Reason, "at most 0 in host-dialed mode") {
		t.Errorf("the reason %q does not name the cap of 0", a.Status.AtProvider.Reason)
	}
	if n := len(e.typed.of(v1alpha1.ReasonAllocationRefused)); n != 1 {
		t.Errorf("%d AllocationRefused events, want exactly 1", n)
	}
	e.run("alloc-a", 4)
	if n := e.count("sbatch"); n != 0 || e.get("alloc-a").Status.AtProvider.Phase != v1alpha1.AllocationRefused {
		t.Errorf("a later pass changed it: %d sbatch calls, phase %s", n, e.get("alloc-a").Status.AtProvider.Phase)
	}
}
