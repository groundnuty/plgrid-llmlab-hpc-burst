package hostdial

// Test seams for the default path's guarded stop (issue #370), for the
// external tests: the group signal alpha's relays use, the /proc read behind
// the start-time guard, and the pause before a refused stop's one retry. Each
// is swapped before an Owner starts and restored after it stopped, so no
// goroutine of the Owner reads one while it is written.

import (
	"syscall"
	"time"
)

// KillGroup is the real group signal, for a test's recording sink to forward to.
var KillGroup = killGroup

// SwapGroupSink replaces the group signal of alpha's relays; call the result to
// restore it.
func SwapGroupSink(f func(pgid int, sig syscall.Signal) error) (restore func()) {
	old := groupSink
	groupSink = f
	return func() { groupSink = old }
}

// SwapGroupProcRead replaces the /proc read behind the start-time guard and
// the pin.
func SwapGroupProcRead(f func(name string) ([]byte, error)) (restore func()) {
	old := groupProcRead
	groupProcRead = f
	return func() { groupProcRead = old }
}

// SwapStopRetryPause replaces the pause before a refused stop's retry.
func SwapStopRetryPause(d time.Duration) (restore func()) {
	old := stopRetryPause
	stopRetryPause = d
	return func() { stopRetryPause = old }
}
