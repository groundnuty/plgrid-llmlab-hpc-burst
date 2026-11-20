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
// Providers on the host: floor(0 / 2) = 0), and a Provider whose controller has not said which host it uses while the
// profile sets a budget, are ProviderUnavailable (HBEP-7): no Allocation is made and none waits on it.
func TestHostDialedCapOfZeroMakesTheProviderUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []rigOption
		want string
	}{
		{"budget 5, five Providers on the host: cap 0", withLoginHostBudget(i32(5), 5), "leaves no room for an Allocation beside the 5 master(s)"},
		{"budget 5, six Providers: below zero reads 0", withLoginHostBudget(i32(5), 6), "so the host-dialed cap is 0"},
		{"a budget and no login host said yet", withLoginHostBudget(i32(11), -1), "has not said which login host it uses yet"},
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
