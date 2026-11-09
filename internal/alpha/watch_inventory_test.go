package alpha

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// watchReaders classes every function of this package that reads the watch
// (issue #281), as the rehearsal's does (scenario/watch_inventory_test.go). Each
// catch-up here takes the row's context, so that it stops at the row's deadline
// (PR #292's review). This is an inventory of the source, not a dataflow proof
// (stack.WatchUses): a removed catch-up, a new reader or a context dropped fails
// it, a read moved before its catch-up does not.
var watchReaders = map[string]stack.WatchClass{
	"whyGone":       {Syncs: []string{"KindDeployments|KindInstances"}},
	"workerRestart": {Syncs: []string{"KindInstances"}},
	"Handover":      {Syncs: []string{"KindAllocations|KindInstances"}},
	"NoOverlap":     {Syncs: []string{"KindAllocations|KindDeployments|KindInstances"}},
	"endDeployment": {Syncs: []string{"KindAllocations"}},

	"attribution": {Why: "helper: Handover makes the catch-up before it calls this"},
	"tagOf":       {Why: "helper: Handover makes the catch-up before it calls this"},
	"last":        {Why: "helper: Handover makes the catch-up before it calls this"},
	"runTags":     {Why: "helper: each result makes the catch-up first; TunnelHour's follows endDeployment's, on the same stream"},

	"firstJobStarted": {Why: "waits itself: a wait whose condition is the watch"},
	"allocations":     {Why: "helper: firstJobStarted's read, classed here"},
}

func TestWatchInventory(t *testing.T) {
	stack.CheckWatchInventory(t, ".", watchReaders, "ctx")
}
