package providerctl

// HBEP-4, open question 17, amended 2026-11-20, and HBEP-6, Capacity: a host-dialed Provider's controller counts its
// Allocations again from a fresh read just before it submits, against the same cap as the planner:
// floor((transport.loginHostBudget - M) / 2), M being the stack's Providers on its login host, and 2 while the profile
// sets no budget and in relay-keeper mode. Every number is worked from that text.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
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
	keeper := &relayState{keeper: &keeperState{}}
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

// Through the reconciler: a cap of 5 gives each of five running Allocations its pair of relays on the login node, none
// waiting for slots. The node's relay limit follows the profile: 10 relays at a budget of 11 beside one master (2 x 5 + 1
// <= 11), and the default 4 while it sets no budget. Without the limit the third to fifth Allocations would wait for slots
// for good, with no error.
func TestRelayLimitFollowsTheCap(t *testing.T) {
	const login = "login.example.org"
	for _, tc := range []struct {
		name      string
		mods      []envOption
		allocs    int
		wantLimit int
	}{
		{"budget 11: five Allocations each hold their pair", []envOption{hostDialedOn(hostdial.NewHosts(), 1000), withBudget(11)}, 5, 10},
		{"budget 9: four", []envOption{hostDialedOn(hostdial.NewHosts(), 1000), withBudget(9)}, 4, 8},
		{"no budget: the default, two Allocations", []envOption{hostDialedOn(hostdial.NewHosts(), 1000)}, 2, hostdial.DefaultRelaysPerLogin},
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
			if got := e.opts.Relays.Slots.Limit(login); got != tc.wantLimit {
				t.Errorf("the login node's relay limit is %d, want %d", got, tc.wantLimit)
			}
			waitRelays(t, "every Allocation holds its pair, none waiting for slots", func() bool {
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
			})
			if got := e.opts.Relays.Slots.InUse(login); got != 2*tc.allocs {
				t.Errorf("%d slots taken on the login node, want %d: a pair for each of %d Allocations", got, 2*tc.allocs, tc.allocs)
			}
		})
	}
}
