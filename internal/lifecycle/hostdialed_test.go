package lifecycle

// HBEP-6, Host-dialed mode, Capacity: "in host-dialed mode a Provider has at
// most 2 Allocations, pending or running. That's a submit limit
// (`SubmitLimitReached`, HBEP-4), so the planner counts it, and the handover
// pair fits." The expected counts and texts below are written from that text,
// not read from HostDialedMaxAllocations or hostDialedLimitSource.
//
// The fake Slurm of these rigs has no submit limit (pfake.Config.MaxSubmitPerUser
// 0 is none), so only the controller's own plan can hold a third Allocation
// back: a test that passes on a rig where Slurm refuses it proves nothing.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// hostDialedMDs are the deployments of the tests below, one Instance each.
var hostDialedMDs = []string{"md-a", "md-b", "md-c"}

// TestHostDialedSubmitLimit: three deployments of one Instance each share the
// rig's Provider. The effective submit limit is the smaller of the QOS's, when
// it sets one, and 2 in host-dialed mode; with no host-dialed mode it is the
// QOS's alone, or none.
func TestHostDialedSubmitLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hostDialed bool
		qosLimit   *int32 // the Provider's QOS maxSubmitPerUser; nil: the QOS sets none
		wantAllocs int    // of the 3 deployments' Allocations, how many exist
		wantLimit  string // the limit md-c's Degraded message names; "" when none is refused
	}{
		{"host-dialed, QOS sets no limit: 2", true, nil, 2, "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)"},
		{"host-dialed, QOS limit 5: the cap of 2 binds", true, i32(5), 2, "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)"},
		{"host-dialed, QOS limit 1: 1 still binds", true, i32(1), 1, "maxSubmitPerUser 1 of QOS example-now"},
		{"direct or ssh mode, QOS sets no limit: no cap", false, nil, 3, ""},
		{"direct or ssh mode, QOS limit 5: 5 is no cap of 2", false, i32(5), 3, ""},
		{"direct or ssh mode, QOS limit 1: 1", false, i32(1), 1, "maxSubmitPerUser 1 of QOS example-now"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []rigOption
			if tc.qosLimit != nil {
				opts = qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxSubmitPerUser: tc.qosLimit}}, "", "example-now")
			}
			if tc.hostDialed {
				opts = append(opts, withHostDialed())
			}
			rg := newRig(t, opts...)
			rg.Step() // the Provider's first status: Ready
			for _, n := range hostDialedMDs {
				rg.create(mdSpec(n))
				rg.reconcileAll()
			}
			// Passes and time go on: a refused deployment waits, and nothing
			// changes the count.
			for range 3 {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			if n := len(rg.allAllocations()); n != tc.wantAllocs {
				t.Fatalf("%d Allocations on one Provider, want %d", n, tc.wantAllocs)
			}
			for i, n := range hostDialedMDs {
				want := 0
				if i < tc.wantAllocs {
					want = 1
				}
				if got := len(rg.allocations(n)); got != want {
					t.Fatalf("%s has %d Allocations, want %d (the first %d deployments get one)", n, got, want, tc.wantAllocs)
				}
			}
			last := hostDialedMDs[len(hostDialedMDs)-1]
			c := rg.condition(last, v1alpha1.ConditionDegraded)
			if tc.wantLimit == "" {
				if strings.Contains(c.Message, v1alpha1.ReasonSubmitLimitReached) {
					t.Fatalf("%s Degraded %q, want no SubmitLimitReached", last, c.Message)
				}
				return
			}
			wantAt := fmt.Sprintf("Provider %s has %d Allocations still holding a slot, at its %s", rigProvider, tc.wantAllocs, tc.wantLimit)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, wantAt) {
				t.Fatalf("%s Degraded = %s/%s %q, want SubmitLimitReached naming %q", last, c.Status, c.Reason, c.Message, wantAt)
			}
			if tc.wantLimit != "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)" && strings.Contains(c.Message, "host-dialed") {
				t.Fatalf("%s Degraded %q names host-dialed mode, but a QOS limit binds", last, c.Message)
			}
		})
	}
}

// TestHostDialedThirdAllocationOfOneDeployment: one deployment of three
// Instances, on a Provider whose QOS sets no submit limit, gets two Allocations
// in host-dialed mode, and its third Instance waits, with the deployment
// SubmitLimitReached. Nothing is refused by Slurm, and the controller stays
// quiet on the Provider.
func TestHostDialedThirdAllocationOfOneDeployment(t *testing.T) {
	rg := newRig(t, withHostDialed())
	rg.Step()
	md := mdSpec("md")
	md.Spec.Instances = 3
	rg.create(md)
	for range 4 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if n := len(rg.allocations("md")); n != 2 {
		t.Fatalf("%d Allocations of a deployment of 3 Instances, want 2", n)
	}
	if n := len(rg.instances("md")); n != 2 {
		t.Fatalf("%d Instances, want 2: the third waits for a slot, it is not created", n)
	}
	if n := rg.submissions(); n != 2 {
		t.Fatalf("%d sbatch calls, want 2: no third attempt for Slurm to refuse", n)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached ||
		!strings.Contains(c.Message, "Provider "+rigProvider+" has 2 Allocations still holding a slot, at its maxSubmitPerUser 2 (host-dialed mode, HBEP-6)") {
		t.Fatalf("Degraded = %s/%s %q, want SubmitLimitReached naming the Provider and the host-dialed cap of 2", c.Status, c.Reason, c.Message)
	}
}

// TestHostDialedLimitsLeaveTheirArgument: the capped limits are a new value; the
// QOS limits they came from, From included, are not changed, so the cap cannot
// leak into the limits another caller holds. Other limits pass through.
func TestHostDialedLimitsLeaveTheirArgument(t *testing.T) {
	in := v1alpha1.QOSLimits{
		MaxSubmitPerUser: i32(5), MaxGPUsPerUser: i32(1),
		From: map[string]string{v1alpha1.LimitMaxSubmitPerUser: "example-now", v1alpha1.LimitMaxGPUsPerUser: "example-now"},
	}
	out := hostDialedLimits(in)
	if *out.MaxSubmitPerUser != 2 || *out.MaxGPUsPerUser != 1 || out.From[v1alpha1.LimitMaxGPUsPerUser] != "example-now" {
		t.Fatalf("capped %d submit, %d GPUs, GPU source %q; want 2, 1, example-now", *out.MaxSubmitPerUser, *out.MaxGPUsPerUser, out.From[v1alpha1.LimitMaxGPUsPerUser])
	}
	if *in.MaxSubmitPerUser != 5 || in.From[v1alpha1.LimitMaxSubmitPerUser] != "example-now" {
		t.Fatalf("the argument changed to %d from %q, want 5 from example-now", *in.MaxSubmitPerUser, in.From[v1alpha1.LimitMaxSubmitPerUser])
	}
	pv := &providerView{limits: out}
	if got, want := submitLimitText(pv), "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)"; got != want {
		t.Fatalf("submitLimitText = %q, want %q", got, want)
	}
	// With no QOS limit at all, From is nil and the cap still names itself.
	pv = &providerView{limits: hostDialedLimits(v1alpha1.QOSLimits{})}
	if got, want := submitLimitText(pv), "maxSubmitPerUser 2 (host-dialed mode, HBEP-6)"; got != want {
		t.Fatalf("with no QOS limit submitLimitText = %q, want %q", got, want)
	}
}

// TestHostDialedLoginHostInUseProviderGetsNoAllocation: a host-dialed Provider
// that another holds its login host is Ready False with LoginHostInUse while
// its reads succeed (HBEP-6, Capacity, as #298 merged it), and the planner
// treats it as unusable: no Allocation, and the deployment's Degraded is
// ProviderUnavailable naming it not Ready. Its last successful read is fresh,
// so only the condition can make it unready: a Provider made unready by
// failing reads would pass this test whether the condition counts or not.
func TestHostDialedLoginHostInUseProviderGetsNoAllocation(t *testing.T) {
	rg := newRig(t, withHostDialed())
	rg.Step() // the Provider's first status: Ready, from a successful read
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	now := metav1.NewTime(rg.clock.Now())
	prov.Status.LastSuccessfulStatusRead = &now
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonLoginHostInUse,
		Message: "login host login.example.org is in use by host-dialed Provider another-provider", LastTransitionTime: now,
	})
	if err := rg.c.Status().Update(rg.ctx, &prov); err != nil {
		t.Fatal(err)
	}
	rg.create(mdSpec("md-a"))
	rg.reconcileAll()
	rg.reconcileAll()
	if n := len(rg.allAllocations()); n != 0 {
		t.Fatalf("%d Allocations on a Provider whose login host is in use, want 0", n)
	}
	c := rg.condition("md-a", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonProviderUnavailable || !strings.Contains(c.Message, "is not Ready") {
		t.Errorf("Degraded = %s/%s %q, want ProviderUnavailable naming the Provider not Ready", c.Status, c.Reason, c.Message)
	}
}
