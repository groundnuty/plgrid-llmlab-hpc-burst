package fake

import (
	"context"
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The three Backend methods, as FailNext and Calls name them.
const (
	MethodRegister = "Register"
	MethodDesired  = "Desired"
	MethodReport   = "Report"
)

// Registration is one registration a Backend holds.
type Registration struct {
	Caller  agentapi.Caller
	Request agentapi.RegisterRequest
}

// Backend is an in-memory agentapi.Backend that stands in for the lifecycle
// controller. It is safe for concurrent use, and keeps everything per
// Allocation UID (Caller.AllocationUID).
//
// A caller whose agent instance ID has no registration gets CodeNotRegistered from
// Desired and Report. A agent instance ID registered under an Allocation stays
// registered until Forget; a second agent instance ID registered under the same
// Allocation (a requeue) is accepted too, because judging a requeue is the
// controller's job and this double does not.
//
// It refuses what the real server and controller refuse, with CodeBadRequest:
// a caller whose agent instance ID is not of the pinned form
// (agentapi.CheckAgentInstanceID, which the server checks before it calls the
// Backend) and a Report whose InstanceReport.Key is set and is not of the pinned
// form (agentapi.CheckKey, which the controller checks before it stores the key).
// A refused call records nothing. The messages never repeat the ID or the key.
type Backend struct {
	mu      sync.Mutex
	desired map[string]agentapi.DesiredState
	regs    map[string][]Registration
	reports map[string][]agentapi.Report
	fail    map[string][]error
	calls   map[string]int
}

var _ agentapi.Backend = (*Backend)(nil)

// NewBackend returns an empty Backend.
func NewBackend() *Backend {
	return &Backend{
		desired: map[string]agentapi.DesiredState{},
		regs:    map[string][]Registration{},
		reports: map[string][]agentapi.Report{},
		fail:    map[string][]error{},
		calls:   map[string]int{},
	}
}

// SetDesired sets what the Allocation's agent should run. Desired serves it
// with its Version set from its other fields (DesiredState.WithVersion), so
// the Version in d is ignored. Until it is called, an Allocation is told
// ActionIdle.
func (b *Backend) SetDesired(allocUID string, d agentapi.DesiredState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.desired[allocUID] = cloneDesired(d)
}

// Forget drops every registration, as a controller restart does. The reports
// recorded so far stay, for inspection.
func (b *Backend) Forget() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.regs)
}

// FailNext makes the next call of the method (MethodRegister, MethodDesired or
// MethodReport) return err, once. Calls queue: two FailNext calls fail the next
// two calls. A failed call records nothing. It panics on an unknown method or a
// nil error, because either would make the test pass for the wrong reason.
func (b *Backend) FailNext(method string, err error) {
	if err == nil {
		panic("fake: FailNext with a nil error")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checkMethod(method)
	b.fail[method] = append(b.fail[method], err)
}

// Calls returns how many times the method has been called, including calls
// that failed.
func (b *Backend) Calls(method string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checkMethod(method)
	return b.calls[method]
}

// Registrations returns the Allocation's registrations in the order they were
// made, one per agent instance ID.
func (b *Backend) Registrations(allocUID string) []Registration {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Registration, 0, len(b.regs[allocUID]))
	for _, r := range b.regs[allocUID] {
		out = append(out, Registration{Caller: r.Caller, Request: cloneRegister(r.Request)})
	}
	return out
}

// Reports returns every report the Allocation's agents have sent, oldest first.
func (b *Backend) Reports(allocUID string) []agentapi.Report {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]agentapi.Report, 0, len(b.reports[allocUID]))
	for _, r := range b.reports[allocUID] {
		out = append(out, cloneReport(r))
	}
	return out
}

// LastReport returns the Allocation's latest report, and false when it has
// sent none.
func (b *Backend) LastReport(allocUID string) (agentapi.Report, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rs := b.reports[allocUID]
	if len(rs) == 0 {
		return agentapi.Report{}, false
	}
	return cloneReport(rs[len(rs)-1]), true
}

// Register records the caller's registration. Registering again with the same
// agent instance ID is accepted and changes nothing, so the first request stays.
func (b *Backend) Register(_ context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.begin(MethodRegister, c); err != nil {
		return agentapi.RegisterResponse{}, err
	}
	if !b.registered(c) {
		b.regs[c.AllocationUID] = append(b.regs[c.AllocationUID], Registration{Caller: c, Request: cloneRegister(r)})
	}
	return agentapi.RegisterResponse{ProtocolVersion: agentapi.ProtocolVersion}, nil
}

// Desired returns the Allocation's desired state with its Version set.
func (b *Backend) Desired(_ context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.begin(MethodDesired, c); err != nil {
		return agentapi.DesiredState{}, err
	}
	if !b.registered(c) {
		return agentapi.DesiredState{}, notRegistered(c)
	}
	d, ok := b.desired[c.AllocationUID]
	if !ok {
		d = agentapi.DesiredState{Action: agentapi.ActionIdle}
	}
	return cloneDesired(d).WithVersion(), nil
}

// Report appends the report to the Allocation's list.
func (b *Backend) Report(_ context.Context, c agentapi.Caller, r agentapi.Report) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.begin(MethodReport, c); err != nil {
		return err
	}
	if r.Instance != nil && r.Instance.Key != "" {
		if err := agentapi.CheckKey([]byte(r.Instance.Key)); err != nil {
			return agentapi.Errorf(agentapi.CodeBadRequest, "%s: the instance key: %v", MethodReport, err)
		}
	}
	if !b.registered(c) {
		return notRegistered(c)
	}
	b.reports[c.AllocationUID] = append(b.reports[c.AllocationUID], cloneReport(r))
	return nil
}

// begin counts a call, rejects a caller that names no Allocation or whose agent
// instance ID is not of the pinned form, and then returns the error a FailNext
// queued, if any. The caller holds b.mu.
func (b *Backend) begin(method string, c agentapi.Caller) error {
	b.calls[method]++
	if c.AllocationUID == "" {
		return agentapi.Errorf(agentapi.CodeBadRequest, "%s: the caller needs an Allocation UID", method)
	}
	if err := agentapi.CheckAgentInstanceID(c.AgentInstanceID); err != nil {
		return agentapi.Errorf(agentapi.CodeBadRequest, "%s: %v", method, err)
	}
	if q := b.fail[method]; len(q) > 0 {
		b.fail[method] = q[1:]
		return q[0]
	}
	return nil
}

func (b *Backend) registered(c agentapi.Caller) bool {
	for _, r := range b.regs[c.AllocationUID] {
		if r.Caller.AgentInstanceID == c.AgentInstanceID {
			return true
		}
	}
	return false
}

func (b *Backend) checkMethod(method string) {
	switch method {
	case MethodRegister, MethodDesired, MethodReport:
	default:
		panic(fmt.Sprintf("fake: unknown Backend method %q", method))
	}
}

func notRegistered(c agentapi.Caller) *agentapi.Error {
	return agentapi.Errorf(agentapi.CodeNotRegistered, "agent instance ID %q is not registered for Allocation %q", c.AgentInstanceID, c.AllocationUID)
}
