// Package providerctl is a stand-in for the real internal/providerctl: the map
// from reason to type the typed helper reads, the helper, and its call sites.
package providerctl

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

const (
	typeStarted = "started"
	typeReady   = "ready"
	typeFailing = "failing"
	typeEnded   = "ended"
)

const (
	reasonSubmitted = "JobSubmitted"
	reasonEnded     = "JobEnded"
)

// allocationEventType is the map the scan reads: each reason of a typed event
// and its type, all named constants.
var allocationEventType = map[string]string{
	v1alpha1.ReasonA: typeReady,
	v1alpha1.ReasonB: typeFailing,
	reasonSubmitted:  typeStarted,
	reasonEnded:      typeEnded,
}

// otherEventType is a map of another name, which no helper may take a type from.
var otherEventType = map[string]string{
	reasonSubmitted: typeFailing,
}

type external struct{ rec eventcount.IDRecorder }

// typed is the helper: a sink, which takes its type from allocationEventType,
// and a listed wrapper, whose EventfID call passes on the regarding object, the
// reason, and the type it looked up under the reason.
func (e *external) typed(a *v1alpha1.Allocation, reason, fact, note string, related eventcount.Object) {
	action, ok := allocationEventType[reason]
	if !ok {
		return
	}
	id := a.Name + "/" + reason + "/" + fact
	e.rec.EventfID(id, a, related, "Normal", reason, action, note)
}

// plain passes a constant of the api package.
func (e *external) plain(a *v1alpha1.Allocation) { e.typed(a, v1alpha1.ReasonA, "f", "n", nil) }

// own passes a constant of the package.
func (e *external) own(a *v1alpha1.Allocation) { e.typed(a, reasonSubmitted, "f", "n", nil) }

// forwarded passes its parameter, so its callers' arguments are the reasons.
func (e *external) forwarded(a *v1alpha1.Allocation, reason string) {
	e.typed(a, reason, "f", "n", nil)
}

func (e *external) callers(a *v1alpha1.Allocation) {
	e.forwarded(a, reasonEnded)
	e.forwarded(a, v1alpha1.ReasonB)
}
