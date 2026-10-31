package fake

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A running process of an older generation cannot survive a Step through
// convergeWorkers, which stops it, so the Agent's own Steps never show the
// generation clauses of the gate. This test reaches them directly: the gate must
// refuse a process the restart condition would replace even if one were running.
func TestRunsForRefusesARunningProcessStartedForAnOlderGeneration(t *testing.T) {
	desired := agentapi.DesiredInstance{
		Generation: 3,
		Workers:    []agentapi.DesiredWorker{{Name: "worker-0", Generation: 2}},
	}
	process := func(phase agentapi.PartPhase, workerGen, instanceGen int64) *instance {
		return &instance{workers: []*worker{{name: "worker-0", phase: phase, generation: workerGen, instGen: instanceGen}}}
	}
	cases := []struct {
		name string
		in   *instance
		want bool
	}{
		{"running for the desired generations", process(agentapi.PartRunning, 2, 3), true},
		{"running for later generations", process(agentapi.PartRunning, 5, 4), true},
		{"running for an older worker generation", process(agentapi.PartRunning, 1, 3), false},
		{"running for an older Instance generation", process(agentapi.PartRunning, 2, 2), false},
		{"starting", process(agentapi.PartStarting, 2, 3), false},
		{"stopping", process(agentapi.PartStopping, 2, 3), false},
		{"stopped", process(agentapi.PartStopped, 2, 3), false},
		{"exited", process(agentapi.PartExited, 2, 3), false},
		{"no process", &instance{}, false},
	}
	for _, c := range cases {
		if got := c.in.runsFor(desired); got != c.want {
			t.Errorf("%s: runsFor = %v, want %v", c.name, got, c.want)
		}
	}

	two := agentapi.DesiredInstance{Generation: 1, Workers: []agentapi.DesiredWorker{{Name: "worker-0"}, {Name: "worker-1"}}}
	one := &instance{workers: []*worker{{name: "worker-0", phase: agentapi.PartRunning, instGen: 1}}}
	if one.runsFor(two) {
		t.Error("runsFor is true although desired worker-1 has no process")
	}
	one.workers = append(one.workers, &worker{name: "worker-1", phase: agentapi.PartStarting, instGen: 1})
	if one.runsFor(two) {
		t.Error("runsFor is true although desired worker-1 is only starting")
	}
	one.workers[1].phase = agentapi.PartRunning
	if !one.runsFor(two) {
		t.Error("runsFor is false although both desired workers run for the desired generations")
	}
}
