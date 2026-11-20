package lifecycle

import (
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// hostDialedLimitSource is what QOSLimits.From names as the source of a submit
// limit that the host-dialed cap sets, in place of a QOS. submitLimitText
// prints it as the reason. It is no name a profile's qos entry would carry, so
// it cannot be mistaken for the source of a real QOS limit.
const hostDialedLimitSource = "host-dialed mode, HBEP-6"

// hostDialedLimits returns limits with the submit limit capped at cap, the
// Provider's host-dialed cap (hostDialedCapOf; HBEP-6, Capacity): the smaller
// of the QOS limit, when one is set, and the cap. When the cap is the smaller,
// or there is no QOS limit, From names hostDialedLimitSource; a QOS limit at
// or below the cap stays as it is, with its QOS. It returns a new value and
// leaves its argument, including From, untouched. The slot count, the
// no-overlap check and the status messages all read the capped value, so the
// cap is planned like any QOS limit (HBEP-7, Provisioning, Per-user limits):
// at a cap of 1 a primary's replacement finds no free slot, and the
// deployment hands over in no-overlap mode.
//
// Under qosByGpus (qosscope.go) a QOS's own submit limit counts that QOS's Allocations alone, but the ceiling counts
// every Allocation on the Provider, so providerView.forGPUs takes the QOS's own limit out of limits BEFORE this folds
// the ceiling in: left in, a QOS limit at or below the ceiling would stand for it, and the ceiling would count only
// that QOS's Allocations (#745).
func hostDialedLimits(limits v1alpha1.QOSLimits, cap int32) v1alpha1.QOSLimits {
	if q := limits.MaxSubmitPerUser; q != nil && *q <= cap {
		return limits
	}
	out := limits
	capped := cap
	out.MaxSubmitPerUser = &capped
	out.From = make(map[string]string, len(limits.From)+1)
	for k, v := range limits.From {
		out.From[k] = v
	}
	out.From[v1alpha1.LimitMaxSubmitPerUser] = hostDialedLimitSource
	return out
}

// hostDialedCapOf is the host-dialed cap of the Provider named n, whose
// profile spec is spec (v1alpha1.HostDialedCap), and why the Provider cannot
// be used when the cap says so: "" with the cap, or a problem of the view, in
// the words of providerView.problem. While the profile sets no budget the cap
// is 2 and no login host is read. In relay-keeper mode the cap is 2 whatever
// the budget, and no problem says so: the keeper's wire carries no budget
// (HBEP-4, open question 17, K2). Otherwise M, the stack's Providers on n's
// login host, comes from Config.LoginHostProviders: a Provider whose host is
// not known yet (its controller has not said, as after a restart) is not used
// this pass, and one whose cap is 0 is unavailable (HBEP-7): "so no
// Allocation waits on it".
func (r *Reconciler) hostDialedCapOf(n string, spec *v1alpha1.PlatformProfileSpec) (int32, string) {
	if r.Config.RelayKeeper || spec.Transport == nil || spec.Transport.LoginHostBudget == nil {
		return v1alpha1.HostDialedCap(spec, 0, r.Config.RelayKeeper), ""
	}
	m, known := 0, false
	if r.Config.LoginHostProviders != nil {
		m, known = r.Config.LoginHostProviders(n)
	}
	if !known {
		return 0, fmt.Sprintf("Provider %s: its controller has not said which login host it uses yet, so the host-dialed cap, which follows transport.loginHostBudget, is not known (HBEP-4, open question 17)", n)
	}
	c := v1alpha1.HostDialedCap(spec, m, false)
	if c == 0 {
		return 0, fmt.Sprintf("Provider %s: transport.loginHostBudget %d leaves no room for an Allocation beside the %d master(s) of the stack's Providers on its login host, so the host-dialed cap is 0 (HBEP-4, open question 17)", n, *spec.Transport.LoginHostBudget, m)
	}
	return c, ""
}
