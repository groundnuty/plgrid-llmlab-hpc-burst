package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Controller stands for the managed controller: record is a listed wrapper
// with a parameter that carries an event.
type Controller struct{ R EventRecorder }

func (c *Controller) record(prov Object, ev provider.ManagedEvent) {
	c.R.Eventf(prov, nil, ev.Type, ev.Reason, ev.Action, ev.Note)
}

// direct hands the event to the wrapper where it is built.
func (c *Controller) direct(in *Instance) {
	c.record(in, provider.NewManagedEvent("degraded", v1alpha1.ReasonA, "n"))
}

// viaVariable puts it in a variable first.
func (c *Controller) viaVariable(in *Instance) {
	ev := provider.NewManagedEvent("failing", v1alpha1.ReasonB, "n")
	c.record(in, ev)
}

// literalReason writes the reason as a literal.
func (c *Controller) literalReason(in *Instance) {
	c.record(in, provider.NewManagedEvent("degraded", "CtorLiteral", "n"))
}

// discarded builds an event and raises nothing.
func (c *Controller) discarded() {
	_ = provider.NewManagedEvent("degraded", v1alpha1.ReasonA, "n")
}

// stored hands the events over through a slice, which the scan does not follow.
func (c *Controller) stored(in *Instance) {
	evs := []provider.ManagedEvent{provider.NewManagedEvent("degraded", v1alpha1.ReasonC, "n")}
	for _, ev := range evs {
		c.record(in, ev)
	}
}

// notBuilt gets an event from its caller, which no constructor built here.
func (c *Controller) notBuilt(in *Instance, ev provider.ManagedEvent) { c.record(in, ev) }

// unreadReason takes the reason from a value it was handed.
func (c *Controller) unreadReason(in *Instance, r string) {
	c.record(in, provider.NewManagedEvent("degraded", r, "n"))
}

func (c *Controller) callUnreadReason(in *Instance) { c.unreadReason(in, v1alpha1.ReasonB) }
