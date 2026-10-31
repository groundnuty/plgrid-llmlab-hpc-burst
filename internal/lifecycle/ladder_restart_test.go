package lifecycle

// The restart ladder across controller restarts (HBEP-7, Health and restarts:
// "The attempts per rung are counted from status.restarts, so they survive a
// controller restart").

import (
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestReviewLadderAcrossControllerRestarts covers HBEP-7, Health and restarts:
// repeated failures climb the ladder in order, worker, worker, instance,
// instance, then the Instance fails, and a controller that is killed and
// started again after every rung (a new Reconciler and Backend over the same
// store, every memory lost) neither forgets an attempt nor repeats a rung nor
// shortens the 30 s restart.backoff. Each crash comes as soon as the new
// process is live, as in TestHealthRepeatedFailuresClimbLadder.
//
// Without the restarts the expectations are the same; they fail if the attempts
// or the backoff were held in memory: a controller that forgot would answer
// every crash with WorkerRestart, and one that forgot the last restart's time
// would restart less than 30 s after the one before.
func TestReviewLadderAcrossControllerRestarts(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
	ag := rg.agentOf(alloc).agent
	var times []time.Time
	restarts := 0
	for k := 0; k < 5; k++ {
		starts := len(ag.Starts())
		ag.CrashWorker(healthWorker, 1)
		times = append(times, healthUntilEvent(rg, in.Name, k, 3*time.Minute, startTick))
		before := rg.r
		rg.restartController()
		if rg.r == before {
			t.Fatalf("rung %d: the controller was not replaced", k+1)
		}
		restarts++
		if k < 4 {
			for len(ag.Starts()) == starts {
				startStep(rg, "md", startTick)
			}
		}
	}
	if restarts != 5 {
		t.Fatalf("%d controller restarts, want one after each of the 5 rungs", restarts)
	}
	want := []string{"WorkerRestart", "WorkerRestart", "InstanceRestart", "InstanceRestart", v1alpha1.ReasonRestartLoop}
	if got := healthFailing(rg, in.Name); !slices.Equal(got, want) {
		t.Fatalf("failing events = %v, want %v", got, want)
	}
	// Lower bound only: an agent that registers again after a restart can stretch
	// a gap, but nothing may shorten it below restart.backoff.
	for k := 1; k < len(times); k++ {
		if gap := times[k].Sub(times[k-1]); gap < 30*time.Second {
			t.Errorf("event %d came %s after event %d, want at least 30 s (restart.backoff)", k+1, gap, k)
		}
	}
	got := rg.instance(in.Name)
	if got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonRestartLoop ||
		got.Annotations[v1alpha1.RestartGenerationAnnotation(healthWorker)] != "2" || got.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance)] != "2" {
		t.Errorf("Instance %s (%s), annotations %v; want ended/RestartLoop with restart-worker-0 2 and restart-instance 2", got.Status.Phase, got.Status.EndReason, got.Annotations)
	}
	startUntil(rg, "md", 3*time.Minute, "the failed Allocation released and a new one provisioned", func() bool {
		_, alive := rg.allocation(alloc)
		return !alive && len(rg.allocations("md")) == 1 && rg.submissions() == 2
	})
}
