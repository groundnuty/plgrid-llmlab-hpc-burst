package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// HostDialedMaxAllocations is the most Allocations, pending or running, a
// batch Provider has in host-dialed mode: the control-plane host holds a pair
// of relays per running Allocation, and the login node's probed budget is 4
// relays, so two Allocations (the handover pair) fit and a third would not.
// HBEP-6, Host-dialed mode, Capacity: "in host-dialed mode a Provider has at
// most 2 Allocations, pending or running. That's a submit limit
// (`SubmitLimitReached`, HBEP-4), so the planner counts it, and the handover
// pair fits."
const HostDialedMaxAllocations int32 = agentapi.HostDialedMaxAllocations

// hostDialedLimitSource is what QOSLimits.From names as the source of a submit
// limit that the host-dialed cap sets, in place of a QOS. submitLimitText
// prints it as the reason. It is no name a profile's qos entry would carry, so
// it cannot be mistaken for the source of a real QOS limit.
const hostDialedLimitSource = "host-dialed mode, HBEP-6"

// hostDialedLimits returns limits with the submit limit capped at
// HostDialedMaxAllocations: the smaller of the QOS limit, when one is set, and
// the cap. When the cap is the smaller, or there is no QOS limit, From names
// hostDialedLimitSource; a QOS limit at or below the cap stays as it is, with
// its QOS. It returns a new value and leaves its argument, including From,
// untouched. The slot count, the no-overlap check and the status messages all
// read the capped value, so the cap is planned like any QOS limit (HBEP-7,
// Provisioning, Per-user limits).
func hostDialedLimits(limits v1alpha1.QOSLimits) v1alpha1.QOSLimits {
	if q := limits.MaxSubmitPerUser; q != nil && *q <= HostDialedMaxAllocations {
		return limits
	}
	out := limits
	capped := HostDialedMaxAllocations
	out.MaxSubmitPerUser = &capped
	out.From = make(map[string]string, len(limits.From)+1)
	for k, v := range limits.From {
		out.From[k] = v
	}
	out.From[v1alpha1.LimitMaxSubmitPerUser] = hostDialedLimitSource
	return out
}
