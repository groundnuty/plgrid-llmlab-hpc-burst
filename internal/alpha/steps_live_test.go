package alpha

// The live steps. They wait for the one-host rehearsal's scenario code (PR
// #161, internal/rehearsal/scenario): its rig, its steps and its watchers start
// the host and drive the stack against a login node, and a live run replaces that
// login node with the real cluster. Until it is wired, a live run fails at its
// first step with ErrNotWired, having used no cluster.

import (
	"context"
	"testing"
)

func liveSteps(t *testing.T, cfg Config) Steps {
	t.Helper()
	return notWired{}
}

type notWired struct{}

func (notWired) Handover(context.Context, Plan) (HandoverObs, error) {
	return HandoverObs{}, ErrNotWired
}
func (notWired) NoOverlap(context.Context, Plan) (NoOverlapObs, error) {
	return NoOverlapObs{}, ErrNotWired
}
func (notWired) TunnelHour(context.Context, Plan) (TunnelHourObs, error) {
	return TunnelHourObs{}, ErrNotWired
}
func (notWired) Ledger() Ledger              { return nil }
func (notWired) Close(context.Context) error { return nil }
