package scenario_test

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// watchReaders classes every function of this package that reads the watch
// (issue #281): one that follows an API poll makes its own catch-up, with the
// kinds named; any other says why it needs none. This is an inventory of the
// source, not a dataflow proof (stack.WatchUses): a removed catch-up or a new
// reader fails it, a read moved before its catch-up does not.
var watchReaders = map[string]stack.WatchClass{
	"checkAllocations":   {Syncs: []string{"KindAllocations"}},
	"checkFirstEntry":    {Syncs: []string{"KindInstances"}},
	"stepWorkerRestart":  {Syncs: []string{"KindInstances"}},
	"checkWorkerRestart": {Syncs: []string{"KindInstances"}},
	"checkTunnelBack":    {Syncs: []string{"KindInstances"}},
	"checkDrained":       {Syncs: []string{"KindInstances"}},
	"requeue":            {Syncs: []string{"KindAllocations"}},
	"checkGap":           {Syncs: []string{"KindInstances"}},
	"checkNoOverlapSlot": {Syncs: []string{"KindDeployments|KindInstances"}},
	"checkOneSlot":       {Syncs: []string{"KindAllocations"}},

	"allocFirstSeen":         {Why: "helper: checkOneSlot makes the catch-up before it calls this"},
	"deploymentObservations": {Why: "helper: checkNoOverlapSlot makes the catch-up before it calls this"},

	"holdTunnel":     {Why: "waits itself: a loop with a deadline, whose condition is the watch"},
	"stepTunnelLoss": {Why: "waits itself: a WaitFor on TunnelConnected False, from the watch"},
	"stepHandover":   {Why: "waits itself: WaitFor the primary to drain, WaitState it to end, from the watch"},
	"waitAllEnded":   {Why: "waits itself: the First Ended of each Instance, from the watch"},

	"stepScaleToZero": {Why: "covered: unioned with the API's Instances, and waitAllEnded then waits on the watch for each"},
	"scaleToZero":     {Why: "covered: unioned with the API's Instances, and waitAllEnded then waits on the watch for each"},
	"checkShutdown":   {Why: "covered: runs after waitAllEnded saw each Instance Ended, and a stream is in order"},
	"createdAt":       {Why: "covered: for Instances waitAllEnded saw Ended; a stream is in order"},
	"firstSpec":       {Why: "covered: its first version, of an Instance waitAllEnded saw Ended; a stream is in order"},
	"instanceEnded":   {Why: "covered: the TunnelOrphaned check runs after waitAllEnded saw each Instance Ended"},
}

func TestWatchInventory(t *testing.T) {
	stack.CheckWatchInventory(t, ".", watchReaders, "")
}
