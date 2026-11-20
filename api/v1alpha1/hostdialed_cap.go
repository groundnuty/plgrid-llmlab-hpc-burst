package v1alpha1

// The bounds of PlatformProfile's transport.loginHostBudget (HBEP-4, open
// question 17, amended 2026-11-20). The admission rules on the field repeat
// them as literals (markers cannot name a constant), so the e2e suite's
// TestLoginHostBudgetBounds holds the two together.
const (
	// LoginHostBudgetMin leaves a cap of 2 at one Provider on the host: a
	// master and a handover pair of Allocations (1 + 2 x 2).
	LoginHostBudgetMin = 5
	// LoginHostBudgetMax gives a cap of 8 at one Provider on the host, the
	// relay keeper's bound on one set of Allocations (wire.MaxAllocations;
	// HBEP-6, The relay keeper).
	LoginHostBudgetMax = 17
)

// HostDialedDefaultAllocations is the most Allocations, pending or running, a
// batch Provider has in host-dialed mode while its profile sets no
// transport.loginHostBudget, and in relay-keeper mode whatever the budget
// (HBEP-6, Capacity: "2 while its profile sets no budget"): the handover pair,
// whose two pairs of relays are the login node's 4. It was
// agentapi.HostDialedMaxAllocations, a name that stopped being true when the
// cap came to follow the budget (#813); it lives here because this package
// cannot import agentapi, whose tests import this one.
const HostDialedDefaultAllocations = 2

// HostDialedCapMax is the largest cap HostDialedCap returns: a host-dialed
// Provider's most Allocations at the field's maximum, with its own master the
// stack's only one there. A test holds it within wire.MaxAllocations.
const HostDialedCapMax = (LoginHostBudgetMax - 1) / 2

// HostDialedCap is the most Allocations, pending or running, a batch
// Provider has in host-dialed mode (HBEP-4, open question 17, amended
// 2026-11-20; HBEP-6, Capacity): floor((loginHostBudget - M) / 2), where
// loginHostBudget is the profile's transport.loginHostBudget and M is
// loginHostProviders, the stack's Providers on the Provider's login host, the
// Provider itself included, each holding its own master whatever its mode.
// Each running Allocation holds 2 relays (agentapi.RelayChannels), so the
// cap's relays and the masters, 2 x cap + M, never pass the budget.
//
// While the profile sets no budget the cap is
// HostDialedDefaultAllocations and M is not read, so a caller that
// does not know M yet asks with 0. Where the stack's relays are held by the
// relay keeper the cap stays that default whatever the budget: keeper is true
// (the keeper's wire carries no budget; HBEP-4, open question 17, K2), and no
// condition says so.
//
// A set budget needs M of at least 1, the Provider's own master: a smaller M
// reads as 1. The result is never below 0 (a cap of 0 leaves no room for an
// Allocation, so the planner treats the Provider as unavailable) and never
// above HostDialedCapMax, the keeper's bound on a set, which the field's
// admission rules keep a budget within already: the clamp holds for a profile
// that did not pass them (a fake client, a CRD not yet applied).
func HostDialedCap(spec *PlatformProfileSpec, loginHostProviders int, keeper bool) int32 {
	if keeper || spec == nil || spec.Transport == nil || spec.Transport.LoginHostBudget == nil {
		return HostDialedDefaultAllocations
	}
	c := 0
	if free := int(*spec.Transport.LoginHostBudget) - max(loginHostProviders, 1); free > 0 {
		c = free / 2
	}
	return int32(min(c, HostDialedCapMax))
}
