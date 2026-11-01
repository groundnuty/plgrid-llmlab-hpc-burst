package launcher

import "time"

// ControllerStopBudget is the controller's longest normal stop (issues #131, #160): the lifecycle
// manager's graceful shutdown, lifecycle.ShutdownGrace (30 s), within which its runnables stop, the
// Provider supervisors and their managers among them, each of which releases its provider-<name>
// Lease; then the two phases of the lifecycle Lease's release: the elector's release, retried within
// leaderlock.ReleaseBudget (1 s), and the release after the manager's Start has returned, for a
// release whose read failed, within leaderlock.AfterStartBudget (1 s). A SIGKILL before the end
// leaves that Lease held until it expires, and the next controller waits for it.
const ControllerStopBudget = controllerShutdownGrace + controllerReleaseBudget + controllerAfterStartBudget

// The parts of ControllerStopBudget. TestControllerStopBudget checks them against
// lifecycle.ShutdownGrace, leaderlock.ReleaseBudget and leaderlock.AfterStartBudget.
const (
	controllerShutdownGrace    = 30 * time.Second
	controllerReleaseBudget    = 1 * time.Second
	controllerAfterStartBudget = 1 * time.Second
)

// ControllerTermGrace is how long the supervisor lets the controller stop, from SIGTERM to
// SIGKILL: ControllerStopBudget and a margin of 2 s for the elector's first release attempt, a read
// and a write, which take milliseconds on a healthy API server. A read that fails by its client
// time-out (half the renew deadline, 5 s) instead uses up the margin, and a stop that also used its
// whole shutdown grace then meets SIGKILL before the release after Start, as every stop did before
// issue #160. The supervisor's general grace, DefaultTermGrace (10 s), was shorter than the
// controller's own stop (PR #159 review).
const ControllerTermGrace = ControllerStopBudget + 2*time.Second

// controllerTermGrace is the grace the supervisor gives the controller, ControllerTermGrace; a
// test sets it to 0 to give the controller the supervisor's general grace instead.
var controllerTermGrace = ControllerTermGrace
