package agent_test

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// backendAPI is the agent's API over an agentapi.Backend, as the agent API's
// server presents one, without HTTP: each call goes to a Backend that the
// test's fake.Gate authenticated (Gate.As). Desired long-polls as the server
// does: it calls the Backend again every pollEvery until the Version differs
// from since or longPoll passes (agentapi's documentation, Long poll).
//
// Calls can be cut, as a lost path is: while cut, every call fails with a
// transport error at once and reaches no Backend; dropNextRun drops the
// answer to the next Desired whose state runs an Instance after the Backend
// served it, as a reply lost on the way.
type backendAPI struct {
	b         agentapi.Backend
	caller    agentapi.Caller
	pollEvery time.Duration
	longPoll  time.Duration

	mu          sync.Mutex
	cut         bool
	dropNextRun bool
	dropped     []agentapi.DesiredState
	served      []agentapi.DesiredState
	servedAt    []time.Time
}

var errPathDown = errors.New("dial: the path to the agent endpoint is down")

func (a *backendAPI) isCut() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cut
}

func (a *backendAPI) setCut(on bool) {
	a.mu.Lock()
	a.cut = on
	a.mu.Unlock()
}

func (a *backendAPI) Register(ctx context.Context, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	if a.isCut() {
		return agentapi.RegisterResponse{}, errPathDown
	}
	return a.b.Register(ctx, a.caller, r)
}

func (a *backendAPI) Desired(ctx context.Context, since string) (agentapi.DesiredState, error) {
	deadline := time.Now().Add(a.longPoll)
	for {
		if a.isCut() {
			return agentapi.DesiredState{}, errPathDown
		}
		d, err := a.b.Desired(ctx, a.caller)
		if err != nil {
			return d, err
		}
		if since == "" || d.Version != since || !time.Now().Before(deadline) {
			a.mu.Lock()
			drop := a.dropNextRun && d.Action == agentapi.ActionRun
			if drop {
				a.dropNextRun = false
				a.dropped = append(a.dropped, d)
			} else {
				a.served = append(a.served, d)
				a.servedAt = append(a.servedAt, time.Now())
			}
			a.mu.Unlock()
			if drop {
				return agentapi.DesiredState{}, errors.New("read: connection reset by peer")
			}
			return d, nil
		}
		t := time.NewTimer(a.pollEvery)
		select {
		case <-ctx.Done():
			t.Stop()
			return agentapi.DesiredState{}, ctx.Err()
		case <-t.C:
		}
	}
}

func (a *backendAPI) Report(ctx context.Context, r agentapi.Report) error {
	if a.isCut() {
		return errPathDown
	}
	if _, err := agentapi.EncodeBody(r); err != nil {
		return err
	}
	return a.b.Report(ctx, a.caller, r)
}

// servedStates returns the desired states delivered to the agent, in order,
// with the time each was handed over.
func (a *backendAPI) servedStates() ([]agentapi.DesiredState, []time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agentapi.DesiredState(nil), a.served...), append([]time.Time(nil), a.servedAt...)
}
