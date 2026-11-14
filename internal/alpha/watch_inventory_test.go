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
	// Row A (issue #376; now_flows_test.go).

	"attribution": {Why: "helper: Handover makes the catch-up before it calls this"},
	"tagOf":       {Why: "helper: Handover makes the catch-up before it calls this"},
	"last":        {Why: "helper: Handover makes the catch-up before it calls this"},
	"runTags":     {Why: "helper: each result makes the catch-up first; TunnelHour's follows endDeployment's, on the same stream"},

	"NowServe": {Why: "its restart count reads the Instances' last versions after workerRestart's catch-up of them, which follows the only restart the row makes"},
	// The keeper row (issue #521; betakeeper_flow_test.go).
	"BetaKeeper": {Syncs: []string{"KindInstances"}},
	// The router row (issue #521; betarouter_flow_test.go).
	"BetaRouter": {Why: "its restart count reads the Instances' last versions after workerRestart's catch-up of them, which follows the only restart the row makes"},

	"firstJobStarted": {Why: "waits itself: a wait whose condition is the watch"},
	// Row B (issue #376; now_nooverlap_flows_test.go).
	"NowNoOverlap":          {Syncs: []string{"KindAllocations|KindDeployments|KindInstances"}},
	"nowNoOverlapFromWatch": {Why: "helper: NowNoOverlap makes the catch-up before it calls this"},
	"nowPredecessorLeft":    {Why: "waits itself: a wait whose condition is the watch, bounded by the predecessor's lease end and EndMargin"},
	"nowSuccessorStart":     {Why: "waits itself: a wait whose condition is the watch and the queue, bounded by SuccessorAllocationWait and the start wait; a refusal it misses is read again after NowNoOverlap's catch-up (nowNoOverlapFromWatch)"},
	"nowThirdJobSign":       {Why: "successorOf's stop: a wait's condition on the watch, which only ends the wait early"},
	"allocations":           {Why: "helper: firstJobStarted's read, classed here"},
}

func TestWatchInventory(t *testing.T) {
	stack.CheckWatchInventory(t, ".", watchReaders, "ctx")
}
