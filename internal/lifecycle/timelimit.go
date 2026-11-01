package lifecycle

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A partition too short (HBEP-7, Provisioning; science-agent decision,
// 1 Nov 2026). An Allocation whose time limit is not longer than the lead an
// Instance needs cannot host one: the lead is the Instance's drain window
// (drainWindow, or routing.maxRequestDuration plus drainMargin when that is
// shorter) plus handoverMargin plus health.startTimeout. Such a Provider is
// skipped, as for a GPU count it cannot host, and no Allocation is created on
// it. Time limits are whole minutes, so the boundary is in minutes.

// leadOf is the lead an Instance needs on the Provider, and the drain window
// it is made of.
func leadOf(pv *providerView, md *v1alpha1.ModelDeployment) (lead, drain time.Duration) {
	drain = drainWindowOf(pv, md)
	return drain + pv.settings.handoverMargin + pv.settings.startTimeout, drain
}

// checkTimeLimits marks every usable allowed Provider whose time limit is not
// longer than the lead an Instance needs, and degrades the deployment with
// PartitionTooShort naming the partition, the time limit, the bound that set
// it and the lead. It holds while the Provider is allowed, also when another
// Provider takes the deployment's Allocations. A Provider whose time limit
// cannot be worked out is left to partitionSizing, which reports it.
func (p *pass) checkTimeLimits() {
	for _, pv := range p.providers {
		if !pv.usable() {
			continue
		}
		lim, bound, err := allocationTimeLimit(pv, p.md.Spec.LeaseLimit)
		if err != nil {
			continue
		}
		lead, drain := leadOf(pv, p.md)
		if lim > lead {
			continue
		}
		pv.tooShort = fmt.Sprintf("partition %s of Provider %s: its time limit %s, set by %s, is not longer than the lead %s an Instance needs (drain window %s + handoverMargin %s + health.startTimeout %s), so no Allocation is created there (HBEP-7, Provisioning, A partition too short)",
			pv.partition.Name, pv.name, lim, bound, lead, drain, pv.settings.handoverMargin, pv.settings.startTimeout)
		p.degrade(v1alpha1.ReasonPartitionTooShort, "%s", pv.tooShort)
	}
}
