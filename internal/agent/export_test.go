package agent

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
