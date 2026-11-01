package launcher

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
)

// TestControllerStopBudget: the supervisor's kill grace for the controller covers the controller's
// longest normal stop, its lifecycle manager's shutdown grace and the lifecycle Lease's release
// budget (issue #131), and each grace above it is longer than the one it contains, up to down's
// default. The parts are checked against the controller's own constants, so a controller that stops
// slower makes this fail instead of being killed before it releases its Lease (PR #159 review).
func TestControllerStopBudget(t *testing.T) {
	if controllerShutdownGrace != lifecycle.ShutdownGrace {
		t.Errorf("the budget counts a shutdown grace of %s, the lifecycle manager's is %s", controllerShutdownGrace, lifecycle.ShutdownGrace)
	}
	if controllerReleaseBudget != leaderlock.ReleaseBudget {
		t.Errorf("the budget counts a release budget of %s, leaderlock's is %s", controllerReleaseBudget, leaderlock.ReleaseBudget)
	}
	if want := lifecycle.ShutdownGrace + leaderlock.ReleaseBudget; ControllerStopBudget != want {
		t.Errorf("ControllerStopBudget %s, want %s: the lifecycle manager's shutdown grace and the Lease's release", ControllerStopBudget, want)
	}
	for _, c := range []struct {
		inner, outer       time.Duration
		innerName, outerOf string
	}{
		{ControllerStopBudget, ControllerTermGrace, "the controller's stop", "the supervisor's kill grace for it"},
		{ControllerTermGrace, SupervisorGrace, "the controller's kill grace", "the supervisor's least grace"},
		{FrontTermGrace, SupervisorGrace, "the front's kill grace", "the supervisor's least grace"},
		{SupervisorGrace, DefaultGrace, "the supervisor's least grace", "down's default grace"},
		{DefaultTermGrace, ControllerTermGrace, "the supervisor's general kill grace", "the controller's own"},
	} {
		if c.outer <= c.inner {
			t.Errorf("%s (%s) is not longer than %s (%s)", c.outerOf, c.outer, c.innerName, c.inner)
		}
	}
}

// The supervisor gives the controller its own kill grace, ControllerTermGrace, not the general one.
func TestTheControllerEntryHasItsOwnGrace(t *testing.T) {
	st := &State{
		Ports:       Ports{API: 29220, HTTPS: 29221, KCM: 29222, Agent: 29223, LB: 29225, Controller: 29226},
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
	}
	set, err := st.supervisedSet(layout{"/run/st"})
	if err != nil {
		t.Fatal(err)
	}
	if len(set) == 0 || set[0].Name != "controller" || set[0].TermGrace != ControllerTermGrace {
		t.Fatalf("the supervised set %+v: want the controller first, with TermGrace %s", set, ControllerTermGrace)
	}
}
