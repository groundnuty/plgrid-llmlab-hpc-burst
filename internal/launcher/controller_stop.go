package launcher

import "time"

// ControllerStopBudget is the controller's longest normal stop (issue #131): the lifecycle
// manager's graceful shutdown, lifecycle.ShutdownGrace (30 s), within which its runnables stop, the
// Provider supervisors and their managers among them, each of which releases its provider-<name>
// Lease; then the release of the lifecycle Lease, retried within leaderlock.ReleaseBudget (2 s).
// A SIGKILL before the end leaves that Lease held until it expires, and the next controller waits
// for it.
const ControllerStopBudget = controllerShutdownGrace + controllerReleaseBudget

// The parts of ControllerStopBudget. TestControllerStopBudget checks them against
// lifecycle.ShutdownGrace and leaderlock.ReleaseBudget.
const (
	controllerShutdownGrace = 30 * time.Second
	controllerReleaseBudget = 2 * time.Second
)

// ControllerTermGrace is how long the supervisor lets the controller stop, from SIGTERM to
// SIGKILL: ControllerStopBudget and a margin of 2 s for the release's first read and write, which
// take milliseconds. The supervisor's general grace, DefaultTermGrace (10 s), was shorter than the
// controller's own stop (PR #159 review).
const ControllerTermGrace = ControllerStopBudget + 2*time.Second

// controllerTermGrace is the grace the supervisor gives the controller, ControllerTermGrace; a
// test sets it to 0 to give the controller the supervisor's general grace instead.
var controllerTermGrace = ControllerTermGrace
