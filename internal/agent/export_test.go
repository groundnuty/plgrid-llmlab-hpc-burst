package agent

import (
	"context"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// FrpcEnvForTest is frpcEnv, for the tests outside the package.
var FrpcEnvForTest = frpcEnv

// SetExitOrderForTest installs the hooks of exitOrder and returns the function that removes them. before
// runs in the goroutine that posts a worker's leader exit, after the leader exited and before the event is
// posted (it may block); after runs on the loop once the end of the worker's group has been handled. Both
// get the leader's PID.
func SetExitOrderForTest(before, after func(pid int)) (reset func()) {
	exitOrder.Store(&exitOrderHook{beforeLeaderExited: before, afterGroupDone: after})
	return func() { exitOrder.Store(nil) }
}

// LogWaitingForSend and LogOpenSendAtLast are the messages the agent logs
// when the last report meets a periodic report still in flight (issue #220).
const (
	LogWaitingForSend = logWaitingForSend
	LogOpenSendAtLast = logOpenSendAtLast
)

// SetTimerForTest replaces the timers of the waits at the agent's end (the wait for a periodic send in flight,
// and the cut of the last report's tries at a job cancellation), so that a test controls the progress of time. It
// returns the function that restores the real ones.
func SetTimerForTest(f func(d time.Duration) (<-chan time.Time, func())) (reset func()) {
	timerHook.Store(&f)
	return func() { timerHook.Store(nil) }
}

// ClampAtTermForTest starts the watch that cuts the last report's tries at a job cancellation (clampAtTerm) and
// returns the function that ends it.
func ClampAtTermForTest(jobCtx context.Context, cancel context.CancelFunc) (stop func()) {
	return clampAtTerm(jobCtx, cancel)
}

// QueuedReportForTest says whether a periodic report waits in a's reporter, not yet taken to be sent.
func QueuedReportForTest(a *Agent) bool {
	a.rep.mu.Lock()
	defer a.rep.mu.Unlock()
	return a.rep.next != nil
}

// PeriodicClosedForTest says whether a's periodic reports have closed: the stop has begun.
func PeriodicClosedForTest(a *Agent) bool { return a.rep.closed() }

// SnapshotKeyForTest is snapshotKey: the snapshot without what changes with every report.
func SnapshotKeyForTest(r agentapi.Report) []byte { return snapshotKey(r) }

// SetRouterStartHookForTest sets the seam a router start's hand-over calls at each stage (router.go,
// routerStartSeam), and returns its reset.
func SetRouterStartHookForTest(f func(stage string, p *EnvoyProc)) (reset func()) {
	routerStartHook.Store(&f)
	return func() { routerStartHook.Store(nil) }
}

// RouterHoldDownForTest says whether a's shutdown has marked the routers' hand-over down.
func RouterHoldDownForTest(a *Agent) bool {
	a.routerHold.mu.Lock()
	defer a.routerHold.mu.Unlock()
	return a.routerHold.down
}

// SetEnvoyLoaderForTest makes the glibc check run loader for whatever loader Envoy names (envoyglibc.go),
// and returns its reset.
func SetEnvoyLoaderForTest(loader string) (reset func()) {
	old := envoyLoader
	envoyLoader = func(string) string { return loader }
	return func() { envoyLoader = old }
}
