package v1alpha1_test

import (
	"testing"

	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func specWithBudget(b int32) *v1alpha1.PlatformProfileSpec {
	return &v1alpha1.PlatformProfileSpec{Transport: &v1alpha1.TransportProfile{LoginHostBudget: ptr.To(b)}}
}

// HostDialedCap rows, written from HBEP-4's open question 17 as amended 2026-11-20: floor((budget - M) / 2), M the
// stack's Providers on the login host, and 2 while the budget is unset, in relay-keeper mode whatever the budget.
func TestHostDialedCap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   *v1alpha1.PlatformProfileSpec
		m      int
		keeper bool
		want   int32
	}{
		{"nil spec", nil, 1, false, 2},
		{"no transport", &v1alpha1.PlatformProfileSpec{}, 1, false, 2},
		{"a transport with no budget", &v1alpha1.PlatformProfileSpec{Transport: &v1alpha1.TransportProfile{}}, 3, false, 2},
		{"unset reads no M", &v1alpha1.PlatformProfileSpec{}, 0, false, 2},
		{"5, one Provider", specWithBudget(5), 1, false, 2},
		{"9, one Provider", specWithBudget(9), 1, false, 4},
		{"11, one Provider (Athena's measured 11)", specWithBudget(11), 1, false, 5},
		{"17, one Provider (the field's maximum)", specWithBudget(17), 1, false, 8},
		{"11, two Providers on the host", specWithBudget(11), 2, false, 4},
		{"5, three Providers: no room for a pair beside the masters", specWithBudget(5), 3, false, 1},
		{"5, five Providers: nothing left", specWithBudget(5), 5, false, 0},
		{"5, six Providers: below zero reads 0", specWithBudget(5), 6, false, 0},
		{"M of 0 reads as the Provider's own master", specWithBudget(11), 0, false, 5},
		{"relay-keeper mode: 2 whatever the budget", specWithBudget(11), 1, true, 2},
		{"relay-keeper mode, the maximum budget", specWithBudget(17), 1, true, 2},
		{"relay-keeper mode, a budget that leaves 0", specWithBudget(5), 5, true, 2},
		// Admission refuses these; the function stays inside the bound for a profile that did not pass it.
		{"above the maximum is clamped to the keeper's bound", specWithBudget(1000), 1, false, int32(v1alpha1.HostDialedCapMax)},
		{"below the minimum is the plain formula", specWithBudget(4), 1, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := v1alpha1.HostDialedCap(tc.spec, tc.m, tc.keeper); got != tc.want {
				t.Fatalf("HostDialedCap = %d, want %d", got, tc.want)
			}
		})
	}
}

// Each running Allocation holds len(RelayChannels) relays and each of the stack's Providers on the host a master, so
// at the cap the connections, 2 x cap + M, never pass the budget (HBEP-4, open question 17: "masters included"). Over
// every budget the field admits and every M that leaves a cap.
func TestHostDialedCapRelaysAndMastersStayWithinTheBudget(t *testing.T) {
	for b := int32(v1alpha1.LoginHostBudgetMin); b <= v1alpha1.LoginHostBudgetMax; b++ {
		for m := 1; m <= int(b); m++ {
			c := v1alpha1.HostDialedCap(specWithBudget(b), m, false)
			if used := len(agentapi.RelayChannels)*int(c) + m; used > int(b) {
				t.Errorf("budget %d, M %d: cap %d holds %d connections, over the budget", b, m, c, used)
			}
			// The cap is the most that fits: one more Allocation would pass the budget.
			if more := len(agentapi.RelayChannels)*int(c+1) + m; more <= int(b) {
				t.Errorf("budget %d, M %d: cap %d leaves room for another Allocation (%d <= %d)", b, m, c, more, b)
			}
		}
	}
	// Unset: the cap's 4 relays and the one master are the probe's 5 connections (HBEP-6, Capacity).
	if got := len(agentapi.RelayChannels)*int(v1alpha1.HostDialedDefaultAllocations) + 1; got != 5 {
		t.Errorf("the default cap holds %d connections, want 4 relays and a master", got)
	}
}

// The bounds' arithmetic: the least budget leaves the handover pair, and the most the cap HostDialedCapMax.
func TestLoginHostBudgetBoundsGiveTheirCaps(t *testing.T) {
	if got := v1alpha1.HostDialedCap(specWithBudget(v1alpha1.LoginHostBudgetMin), 1, false); got != v1alpha1.HostDialedDefaultAllocations {
		t.Errorf("the minimum budget gives a cap of %d, want the default %d (a handover pair)", got, v1alpha1.HostDialedDefaultAllocations)
	}
	if got := v1alpha1.HostDialedCap(specWithBudget(v1alpha1.LoginHostBudgetMax), 1, false); got != v1alpha1.HostDialedCapMax {
		t.Errorf("the maximum budget gives a cap of %d, want HostDialedCapMax %d", got, v1alpha1.HostDialedCapMax)
	}
	if v1alpha1.HostDialedCapMax != 8 {
		t.Errorf("HostDialedCapMax = %d, want 8: HBEP-4 states a cap of at most 8", v1alpha1.HostDialedCapMax)
	}
}
