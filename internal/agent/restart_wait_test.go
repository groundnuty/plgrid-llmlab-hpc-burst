package agent

// The rule of a restarted worker's wait behind the router (restartReleased; the science agent's ruling on
// #439): it starts at once with no running router; behind one, once a read of the router's workers taken
// after the worker went down holds it out of Envoy's routing, or at one check interval plus its time-out
// after it went down. A read from before it went down says nothing of it.

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestARestartedWorkersWaitRule(t *testing.T) {
	down := time.Date(2026, 11, 13, 1, 0, 0, 0, time.UTC)
	read := func(at time.Time, healthy bool) *routerPart {
		a := at
		return &routerPart{phase: agentapi.PartRunning, clustersReadAt: &a, workers: []agentapi.RouterWorker{
			{Name: "worker-0", Healthy: true}, {Name: "worker-1", Healthy: healthy, FailedActiveCheck: !healthy},
		}}
	}
	for _, c := range []struct {
		name   string
		router *routerPart
		now    time.Time
		want   bool
	}{
		{"no router", nil, down.Add(time.Second), true},
		{"a router that is not running", &routerPart{phase: agentapi.PartStarting}, down.Add(time.Second), true},
		{"a read after the death holding it out", read(down.Add(time.Second), false), down.Add(2 * time.Second), true},
		{"a read after the death still holding it healthy", read(down.Add(time.Second), true), down.Add(2 * time.Second), false},
		{"a read from before the death holding it out", read(down.Add(-time.Second), false), down.Add(2 * time.Second), false},
		{"no read yet, before the deadline", &routerPart{phase: agentapi.PartRunning}, down.Add(6 * time.Second), false},
		{"the deadline, one interval plus the time-out", read(down.Add(time.Second), true), down.Add(envoyHCInterval + envoyHCTimeout), true},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // an armed wake-up ends at once
		now := c.now
		a := &Agent{o: Options{Now: func() time.Time { return now }}, ctx: ctx}
		w := &worker{name: "worker-1", downAt: down}
		if got := a.restartReleased(&instance{router: c.router}, w); got != c.want {
			t.Errorf("%s: released %v, want %v", c.name, got, c.want)
		}
		if !c.want && !w.waitArmed {
			t.Errorf("%s: held, with no wake-up armed for its deadline", c.name)
		}
		a.wg.Wait()
	}
}
