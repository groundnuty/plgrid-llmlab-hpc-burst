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
// Provider takes the deployment's Allocations. A limit that is set but under a
// minute (a maxWall of 30 s) is too short, named with its raw value; a Provider
// with no limit at all is left to partitionSizing, which reports it.
func (p *pass) checkTimeLimits() {
	for _, pv := range p.providers {
		if !pv.usable() {
			continue
		}
		// Under qosByGpus the time limit is the chosen QOS's: the plan's GPU count resolves it before the limit is
		// read, so a gpuN QOS's 3-day wall bounds it, not Sano's 7-day partition (HBEP-4, QOS limits; #745). With no
		// plan (an unsupported spec) or a count no QOS fits, sizing reports it, not this check.
		rv := pv
		if len(pv.partition.QOSByGPUs) > 0 {
			var err error
			if p.specErr != "" || p.plan.gpus*p.plan.workers < 1 {
				continue
			}
			if rv, err = pv.forGPUs(p.plan.gpus * p.plan.workers); err != nil {
				continue
			}
		}
		if why := p.tooShortText(pv, rv); why != "" {
			pv.tooShort = why
			p.degrade(v1alpha1.ReasonPartitionTooShort, "%s", pv.tooShort)
		}
	}
}

// tooShortText says why the time limit of an Allocation under the resolved view rv cannot host an Instance, or "" when it
// can: it is not longer than the lead the Instance needs, or is under a minute (HBEP-7, Provisioning, A partition too
// short). pv is the Provider's own view, whose settings and name the message carries.
func (p *pass) tooShortText(pv, rv *providerView) string {
	lim, bound, err := allocationTimeLimit(rv, p.md.Spec.LeaseLimit)
	if err != nil {
		return ""
	}
	lead, drain := leadOf(pv, p.md)
	if lim > lead && lim >= time.Minute {
		return "" // a known limit under a minute is too short whatever the lead (Slurm counts whole minutes)
	}
	return fmt.Sprintf("partition %s of Provider %s: its time limit %s, set by %s, is not longer than the lead %s an Instance needs (drain window %s + handoverMargin %s + health.startTimeout %s), so no Allocation is created there (HBEP-7, Provisioning, A partition too short)",
		pv.partition.Name, pv.name, lim, bound, lead, drain, pv.settings.handoverMargin, pv.settings.startTimeout)
}

// atLaunch is the Provider's view for an Instance whose Allocation is not made yet, judged at the Instance's OWN launch: where
// the partition sets qosByGpus, the time limit it will get is the wall of the QOS ITS GPU count chooses, which may not be the
// QOS the deployment's current plan chooses. An existing unit is charged at its own recorded size, a future one at the
// current spec (the ruling on #756's review). Any other Provider, and one the launch gives no QOS, keeps its view.
func (p *pass) atLaunch(pv *providerView, in *v1alpha1.Instance) *providerView {
	if pv == nil || p.md == nil || len(pv.partition.QOSByGPUs) == 0 {
		return pv
	}
	out := *pv
	out.tooShort = ""
	if rv, err := pv.forGPUs(launchGPUs(in)); err == nil {
		out.tooShort = p.tooShortText(pv, rv)
	}
	return &out
}
