package main

import (
	"context"
	"fmt"
	"io"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// cmdSupervise is the supervisor's body, started by up as a detached process and not listed in the
// usage: it keeps the controllers running and restarts any that exits, until it gets SIGTERM.
func cmdSupervise(ctx context.Context, args []string, _, stderr io.Writer) int {
	fs := flags("supervise", stderr)
	var o launcher.SuperviseOptions
	var sd string
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	if code, done := parse(fs, args); done {
		return code
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	o.Log = stderr
	if err := launcher.Supervise(ctx, o); err != nil {
		return fail(stderr, err)
	}
	return exitOK
}

// printSupervision writes what status found about the supervisor and the processes it keeps.
func printSupervision(w io.Writer, r *launcher.StatusReport) {
	if r.Supervisor == nil {
		if r.Version != "" {
			fmt.Fprintln(w, "controllers: none (no controller binary was found when the host started)")
		}
		return
	}
	line := fmt.Sprintf("supervisor pid %d: running=%v", r.Supervisor.PID, r.Supervisor.Running)
	if r.Supervisor.Detail != "" {
		line += " (" + r.Supervisor.Detail + ")"
	}
	fmt.Fprintln(w, line)
	for _, c := range r.Controllers {
		line := fmt.Sprintf("supervised %s: running=%v pid %d, %s, restarts %d", c.Name, c.Running, c.PID, c.State, c.Restarts)
		if c.HealthzOK != nil {
			line += fmt.Sprintf(", GET /healthz on %s: %v", c.Health, *c.HealthzOK)
		}
		if c.LastExit != nil {
			line += fmt.Sprintf(", last exit %q (code %d) at %s", c.LastExit.Detail, c.LastExit.Code, c.LastExit.At.Format("15:04:05"))
		}
		fmt.Fprintln(w, line)
	}
}
