package lifecycle

import (
	"sort"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// clearDialHome clears each Provider's DialHomeUnreachable for which an agent
// of this deployment's Allocations has a registration: an accepted
// registration proves the dial-home path works, whatever the phase of the
// Instance on it, or with none (HBEP-7, The allocation agent; HBEP-11). The
// apply is made on every pass while the condition is True, so one that failed
// is retried; setDialHome writes nothing once it is False.
func (p *pass) clearDialHome() error {
	providers := map[string]time.Time{}
	for _, a := range p.allocs {
		for _, reg := range p.r.Backend.registrations(a.UID) {
			if n := a.Labels[v1alpha1.LabelProvider]; reg.at.After(providers[n]) {
				providers[n] = reg.at
			}
		}
	}
	names := make([]string, 0, len(providers))
	for n := range providers {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := p.r.setDialHome(p.ctx, n, false, p.now, "an agent registered through the dial-home path", providers[n]); err != nil {
			return err
		}
	}
	return nil
}
