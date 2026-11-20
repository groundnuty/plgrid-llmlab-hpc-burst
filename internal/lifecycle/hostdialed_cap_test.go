package lifecycle

// HBEP-4, open question 17, amended 2026-11-20: a host-dialed Provider has at most floor((transport.loginHostBudget -
// M) / 2) Allocations, pending or running, M being the stack's Providers on its login host, and 2 while the profile sets
// no budget; in relay-keeper mode the cap stays 2 whatever the budget (K2). Every expected count below is worked from
// that text, never read from v1alpha1.HostDialedCap or hostDialedCapOf. As in hostdialed_test.go, the fake Slurm has no
// submit limit, so only the controller's own plan holds an Allocation back.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// withLoginHostBudget is a profile that sets transport.loginHostBudget (nil: none), with the controller told that m of
// the stack's Providers use the Provider's login host (m < 0: its controller has not said which host it uses).
func withLoginHostBudget(budget *int32, m int) []rigOption {
	return []rigOption{
		withHostDialed(),
		withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			if budget != nil {
				s.Transport = &v1alpha1.TransportProfile{LoginHostBudget: budget}
			}
		}),
		withConfig(func(c *Config) {
			c.LoginHostProviders = func(string) (int, bool) { return m, m >= 0 }
		}),
	}
}

// TestHostDialedCapFollowsTheBudget: n deployments of one Instance each share the rig's Provider; the first cap get an
// Allocation, and the next is SubmitLimitReached naming the cap.
func TestHostDialedCapFollowsTheBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []rigOption
		wantAt int // the cap
	}{
		{"unset: 2", withLoginHostBudget(nil, 1), 2},
		{"budget 5, one Provider: 2", withLoginHostBudget(i32(5), 1), 2},
		{"budget 9, one Provider: 4", withLoginHostBudget(i32(9), 1), 4},
		{"budget 11, one Provider: 5", withLoginHostBudget(i32(11), 1), 5},
		{"budget 11, two Providers on the host: 4", withLoginHostBudget(i32(11), 2), 4},
		{"budget 17, one Provider: 8", withLoginHostBudget(i32(17), 1), 8},
		{"budget 5, three Providers: 1", withLoginHostBudget(i32(5), 3), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, tc.opts...)
			rg.Step() // the Provider's first status: Ready
			n := tc.wantAt + 2
			for i := range n {
				rg.create(mdSpec(fmt.Sprintf("md-%d", i)))
				rg.reconcileAll()
			}
			for range 3 {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			if got := len(rg.allAllocations()); got != tc.wantAt {
				t.Fatalf("%d Allocations on one Provider, want its cap %d", got, tc.wantAt)
			}
			if got := rg.submissions(); got != tc.wantAt {
				t.Fatalf("%d sbatch calls, want %d: no attempt for Slurm to refuse", got, tc.wantAt)
			}
			last := fmt.Sprintf("md-%d", n-1)
			c := rg.condition(last, v1alpha1.ConditionDegraded)
			want := fmt.Sprintf("Provider %s has %d Allocations still holding a slot, at its maxSubmitPerUser %d (host-dialed mode, HBEP-6)", rigProvider, tc.wantAt, tc.wantAt)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, want) {
				t.Fatalf("%s Degraded = %s/%s %q, want SubmitLimitReached naming %q", last, c.Status, c.Reason, c.Message, want)
			}
		})
	}
}

// TestHostDialedCapStaysTwoInRelayKeeperMode: with the relay keeper, the cap is 2 whatever the budget (K2: the keeper's
// wire carries no budget), and nothing says so: the Provider stays Ready, no deployment gets another reason than the
// ordinary SubmitLimitReached on the third, and no Allocation is refused for the budget.
func TestHostDialedCapStaysTwoInRelayKeeperMode(t *testing.T) {
	opts := append(withLoginHostBudget(i32(11), 1), withConfig(func(c *Config) {
		c.RelayKeeper = true
		c.LoginHostProviders = func(string) (int, bool) { return 1, true }
	}))
	rg := newRig(t, opts...)
	rg.Step()
	for i := range 4 {
		rg.create(mdSpec(fmt.Sprintf("md-%d", i)))
		rg.reconcileAll()
	}
	for range 3 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if got := len(rg.allAllocations()); got != 2 {
		t.Fatalf("%d Allocations in relay-keeper mode with a budget of 11, want the cap 2", got)
	}
	c := rg.condition("md-3", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)") {
		t.Fatalf("md-3 Degraded = %s/%s %q, want the ordinary SubmitLimitReached at 2", c.Status, c.Reason, c.Message)
	}
	for i := range 4 {
		if r := rg.condition(fmt.Sprintf("md-%d", i), v1alpha1.ConditionDegraded).Reason; r == v1alpha1.ReasonProviderUnavailable {
			t.Fatalf("md-%d is ProviderUnavailable: the keeper's cap of 2 must not fail or hide the Provider", i)
		}
	}
	var p v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &p); err != nil {
		t.Fatal(err)
	}
	if !v1alpha1.ProviderReady(&p, rg.clock.Now()) {
		t.Fatalf("the Provider is not Ready in relay-keeper mode with a budget: %+v", p.Status.Conditions)
	}
	// The same budget without the keeper gives 5 (the mutant's contrast: a keeper that applied the budget).
	rg2 := newRig(t, withLoginHostBudget(i32(11), 1)...)
	rg2.Step()
	for i := range 4 {
		rg2.create(mdSpec(fmt.Sprintf("md-%d", i)))
		rg2.reconcileAll()
	}
	rg2.clock.Advance(10 * time.Second)
	rg2.Step()
	if got := len(rg2.allAllocations()); got != 4 {
		t.Fatalf("%d Allocations without the keeper and a budget of 11, want all 4 (cap 5)", got)
	}
}

// TestHostDialedCapOfZeroMakesTheProviderUnavailable: a budget that leaves no room beside the stack's masters (5 with 5
// Providers on the host: floor(0 / 2) = 0) is ProviderUnavailable (HBEP-7): no Allocation is made and none waits on it.
func TestHostDialedCapOfZeroMakesTheProviderUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []rigOption
		want string
	}{
		{"budget 5, five Providers on the host: cap 0", withLoginHostBudget(i32(5), 5), "leaves no room for an Allocation beside the 5 master(s)"},
		{"budget 5, six Providers: below zero reads 0", withLoginHostBudget(i32(5), 6), "so the host-dialed cap is 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, tc.opts...)
			rg.Step()
			rg.create(mdSpec("md"))
			for range 4 {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			if n := len(rg.allAllocations()); n != 0 {
				t.Fatalf("%d Allocations on a Provider whose cap is 0, want none", n)
			}
			c := rg.condition("md", v1alpha1.ConditionDegraded)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonProviderUnavailable || !strings.Contains(c.Message, tc.want) {
				t.Fatalf("Degraded = %s/%s %q, want ProviderUnavailable naming %q", c.Status, c.Reason, c.Message, tc.want)
			}
		})
	}
	// Unset, no host is read at all: the cap is 2 and the Provider is usable though the controller knows no host.
	rg := newRig(t, withLoginHostBudget(nil, -1)...)
	rg.Step()
	rg.create(mdSpec("md"))
	for range 3 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("%d Allocations with no budget and no login host said, want 1: unset reads no M", n)
	}
}

// TestHostDialedCapOfOneHandsOverInNoOverlapMode: a cap of 1 (budget 5, three Providers on the host) leaves no slot for
// the handover pair, so the deployment's primary drains at its drain start with no replacement beside it, in
// no-overlap mode (HBEP-7, Handover); the Degraded message names the host-dialed cap. nooverlap_test.go's one-slot
// path carries the rest of the mode; here the limit is the host-dialed cap, not a QOS's.
func TestHostDialedCapOfOneHandsOverInNoOverlapMode(t *testing.T) {
	rg := noOverlapRig(t, withLoginHostBudget(i32(5), 3)...)
	p := noOverlapStartK(rg, nil)
	before := func() {
		if n := len(rg.allAllocations()); n != 1 {
			t.Fatalf("at %s %d Allocations, want only the primary's before its drain start: the cap is 1", rg.clock.Now().Format(time.TimeOnly), n)
		}
		noReplacementLate(t, rg)
	}
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, before)
	hoStepTo(rg, hoT(9, 50, 10), 10*time.Second, before)
	hoStepTo(rg, hoT(9, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the primary is %s, want draining", ph)
	}
	c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonNoOverlapSlot || !strings.Contains(c.Message, "maxSubmitPerUser 1 (host-dialed mode, HBEP-6)") {
		t.Fatalf("at 09:50:20 Degraded = %s/%s %q, want NoOverlapSlot naming the host-dialed cap of 1", c.Status, c.Reason, c.Message)
	}
	if n := rg.submissions(); n != 1 {
		t.Fatalf("%d jobs at the drain start, want the primary's alone", n)
	}
}

// TestNoExtraInHostDialedModeAboveTheDefaultCap: in host-dialed mode no extra is made above the default cap of 2 (HBEP-7,
// Over-allocation, In host-dialed mode, as amended 2026-11-20: "no extra is made, whatever the Provider's cap": the cap's
// slots serve primaries and their handovers). A deliberate change on main, whose generic slot check would have made one at
// a cap of 5. A cap of 2 decides as before, so the row of extra_rig_test.go (an end time before the drain start reserves no
// slot, so the extra fits) is kept here too, and a Provider that is not host-dialed, with the same room, gets its extra, so
// the rows see the gate and not a lack of room.
func TestNoExtraInHostDialedModeAboveTheDefaultCap(t *testing.T) {
	endBefore := metav1.NewTime(hoT(14, 0, 0)) // before the primary's expected drain start, 17:50:10
	for _, tc := range []struct {
		name string
		opts []rigOption
		end  *metav1.Time
		want int // Allocations, the extra's included
	}{
		{"host-dialed, cap 5, k 1: no extra", withLoginHostBudget(i32(11), 1), nil, 1},
		{"host-dialed, cap 8, k 1: no extra", withLoginHostBudget(i32(17), 1), nil, 1},
		{"host-dialed, cap 3, k 1, an end time before the drain start: no extra", withLoginHostBudget(i32(7), 1), &endBefore, 1},
		{"host-dialed, cap 2 (budget 5), an end time before the drain start: the extra, as on main", withLoginHostBudget(i32(5), 1), &endBefore, 2},
		{"host-dialed, budget unset (cap 2), an end time before the drain start: the extra, as on main", withLoginHostBudget(nil, 1), &endBefore, 2},
		{"host-dialed, budget unset (cap 2), no end time: no extra, as on main", withLoginHostBudget(nil, 1), nil, 1},
		{"not host-dialed, QOS limit 5, k 1: the extra", qosOpts([]v1alpha1.QOSProfile{{Name: "example-s5", MaxSubmitPerUser: i32(5)}}, "", "example-s5"), nil, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, append(tc.opts, withMaxTime(12*time.Hour))...)
			md := withK(mdSpec(hoMD), ptrTo(int32(1)))
			md.Spec.EndTime = tc.end
			rg.create(md)
			for range 7 {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			rg.run(10*time.Minute, 10*time.Second)
			if n := len(rg.allAllocations()); n != tc.want {
				t.Fatalf("%d Allocations for a deployment of one Instance with k 1, want %d", n, tc.want)
			}
		})
	}
}

// TestControllerRestartWithABudgetSetChangesNothingWhileTheLoginHostIsUnknown: on a live stack whose profile sets a budget, a
// controller start leaves the Providers' controllers' register empty until each has claimed its login host (M unknown).
// The planner plans the Provider with M = 1 meanwhile (hostDialedCapOf), so nothing is made unavailable: with a ready
// primary, a restart and several passes with M unknown, then M known, no Allocation or Instance is created, deleted, ended
// or annotated, the deployment stays Ready and AsDesired, and no ProviderUnavailable is raised or counted.
func TestControllerRestartWithABudgetSetChangesNothingWhileTheLoginHostIsUnknown(t *testing.T) {
	known := true
	rg := newRig(t, withHostDialed(), withMaxTime(4*time.Hour),
		withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			s.Transport = &v1alpha1.TransportProfile{LoginHostBudget: i32(11)}
		}),
		withConfig(func(c *Config) { c.LoginHostProviders = func(string) (int, bool) { return 1, known } }))
	hoStart(rg, nil)
	snapshot := func() string {
		var out []string
		for _, a := range rg.allAllocations() {
			out = append(out, fmt.Sprintf("A %s %s %s del=%v ann=%v", a.Name, a.UID, a.Status.AtProvider.Phase, a.DeletionTimestamp, a.Annotations))
		}
		for _, in := range rg.instances(hoMD) {
			out = append(out, fmt.Sprintf("I %s %s %s del=%v ann=%v", in.Name, in.UID, in.Status.Phase, in.DeletionTimestamp, in.Annotations))
		}
		return strings.Join(out, "\n")
	}
	before := snapshot()
	if n := rg.readyCount(hoMD); n != 1 {
		t.Fatalf("%d ready before the restart, want 1", n)
	}
	check := func(what string) {
		t.Helper()
		if got := snapshot(); got != before {
			t.Fatalf("%s: the objects changed:\nbefore:\n%s\nafter:\n%s", what, before, got)
		}
		if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonAsDesired {
			t.Fatalf("%s: Degraded = %s/%s %q, want False/AsDesired", what, c.Status, c.Reason, c.Message)
		}
		if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonProviderUnavailable)); n != 0 {
			t.Fatalf("%s: %d ProviderUnavailable events, want none", what, n)
		}
	}
	known = false
	rg.restartController()
	for i := range 6 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		check(fmt.Sprintf("pass %d after the restart, the login host unknown", i+1))
	}
	known = true
	for i := range 3 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		check(fmt.Sprintf("pass %d with the login host known", i+1))
	}
}

// TestALoweredCapReleasesNothingThatRuns: a cap that falls below the Allocations that run (the budget lowered, or M
// raised by another Provider on the host) stops new Allocations and releases, replaces or ends none that runs: "lowering a
// live stack's share frees its connections only once its Allocations above the new cap have drained" (HBEP-4, open
// question 17). Five deployments of one Instance each run under a cap of 5; M becomes 6, a cap of 2; no object changes,
// the deployments stay AsDesired, and a sixth deployment waits at SubmitLimitReached.
func TestALoweredCapReleasesNothingThatRuns(t *testing.T) {
	m := 1
	rg := newRig(t, withHostDialed(),
		withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			s.Transport = &v1alpha1.TransportProfile{LoginHostBudget: i32(11)}
		}),
		withConfig(func(c *Config) { c.LoginHostProviders = func(string) (int, bool) { return m, true } }))
	rg.Step()
	names := []string{"md-0", "md-1", "md-2", "md-3", "md-4"}
	for _, n := range names {
		rg.create(mdSpec(n))
		rg.reconcileAll()
	}
	for range 8 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	snapshot := func() string {
		var out []string
		for _, a := range rg.allAllocations() {
			out = append(out, fmt.Sprintf("A %s %s %s del=%v ann=%v", a.Name, a.UID, a.Status.AtProvider.Phase, a.DeletionTimestamp, a.Annotations))
		}
		for _, n := range names {
			for _, in := range rg.instances(n) {
				out = append(out, fmt.Sprintf("I %s %s %s del=%v ann=%v", in.Name, in.UID, in.Status.Phase, in.DeletionTimestamp, in.Annotations))
			}
		}
		return strings.Join(out, "\n")
	}
	if got := len(rg.allAllocations()); got != 5 {
		t.Fatalf("%d Allocations under a cap of 5, want 5", got)
	}
	before := snapshot()
	m = 6 // the cap is now floor((11 - 6) / 2) = 2
	rg.create(mdSpec("md-5"))
	for i := range 6 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		if got := snapshot(); got != before {
			t.Fatalf("pass %d under the lowered cap: the objects changed:\nbefore:\n%s\nafter:\n%s", i+1, before, got)
		}
		for _, n := range names {
			if c := rg.condition(n, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
				t.Fatalf("%s Degraded = %s/%s %q under the lowered cap, want False", n, c.Status, c.Reason, c.Message)
			}
		}
	}
	if got := len(rg.allAllocations()); got != 5 {
		t.Fatalf("%d Allocations after the cap fell to 2, want the 5 that run, and none for md-5", got)
	}
	c := rg.condition("md-5", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)") {
		t.Fatalf("md-5 Degraded = %s/%s %q, want SubmitLimitReached at the cap of 2", c.Status, c.Reason, c.Message)
	}
}
