// Package lifecycle is a stand-in for the real internal/lifecycle: a Recorder
// the scan knows, a degradedOrder, and one function for each way a reason
// reaches a call.
package lifecycle

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

type Instance struct{}
type ModelDeployment struct{}

// Object is what an event regards.
type Object interface{ GetName() string }

func (*Instance) GetName() string        { return "" }
func (*ModelDeployment) GetName() string { return "" }

// Recorder is the interface the scan knows by its full name.
type Recorder interface {
	Event(regarding, related Object, typ, code, message string)
}

var degradedOrder = []string{v1alpha1.ReasonB, v1alpha1.ReasonA}

const (
	eventFailing  = "failing"
	eventDegraded = "degraded"
	localReason   = "Local"
)

type pass struct {
	r    Recorder
	md   *ModelDeployment
	flag bool
}

// direct passes a constant of the api package.
func (p *pass) direct(in *Instance) { p.r.Event(in, nil, eventFailing, v1alpha1.ReasonA, "m") }

// local passes a constant of its own.
func (p *pass) local(in *Instance) { p.r.Event(in, nil, eventFailing, localReason, "m") }

// typed passes a constant of a named string type, converted.
func (p *pass) typed(in *Instance) {
	p.r.Event(in, nil, eventFailing, string(v1alpha1.ReasonTyped), "m")
}

// fail forwards its parameter, so its callers' arguments are the reasons.
func (p *pass) fail(in *Instance, reason string) { p.r.Event(in, nil, eventFailing, reason, "m") }

func (p *pass) callers(in *Instance) {
	p.fail(in, v1alpha1.ReasonB)
	p.fail(in, v1alpha1.EventReasonD)
}

// variable sets a variable twice, and a closure captures it, so it lives on the heap.
func (p *pass) variable(in *Instance) {
	reason := v1alpha1.ReasonA
	if p.flag {
		reason = v1alpha1.ReasonC
	}
	func() { _ = reason }()
	p.r.Event(in, nil, eventDegraded, reason, "m")
}

// guarded starts from the empty string, which the scan drops.
func (p *pass) guarded(in *Instance) {
	reason := ""
	if p.flag {
		reason = v1alpha1.ReasonC
	}
	if reason != "" {
		p.r.Event(in, nil, eventDegraded, reason, "m")
	}
}

func pick(f bool) (string, string) {
	if f {
		return v1alpha1.ReasonA, "x"
	}
	return v1alpha1.ReasonB, "y"
}

// fromCall takes its reason from the result of a call.
func (p *pass) fromCall(in *Instance) {
	reason, _ := pick(p.flag)
	p.r.Event(in, nil, eventFailing, reason, "m")
}

func (p *pass) subject(in *Instance) Object {
	if p.md != nil {
		return p.md
	}
	return in
}

// either regards whichever of two kinds the helper returns.
func (p *pass) either(in *Instance) {
	p.r.Event(p.subject(in), nil, eventDegraded, v1alpha1.ReasonA, "m")
}

type holder struct{ reason string }

// field takes its reason from a field, which the scan cannot resolve.
func (p *pass) field(in *Instance, h *holder) { p.r.Event(in, nil, eventFailing, h.reason, "m") }

// viaValue is used as a value below, so no caller list covers it.
func (p *pass) viaValue(in *Instance, reason string) {
	p.r.Event(in, nil, eventFailing, reason, "m")
}

var registry []func(*Instance, string)

func (p *pass) register() { registry = append(registry, p.viaValue) }
