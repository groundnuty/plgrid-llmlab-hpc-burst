package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// cmdSupervise is the supervisor's body, started by up as a detached process and not listed in the
// usage: it keeps the controllers running and restarts any that exits, until it gets SIGTERM.
func cmdSupervise(ctx context.Context, args []string, _, stderr io.Writer) int {
	fs := flags("supervise", stderr)
	var o launcher.SuperviseOptions
	var sd string
	var handoffFD int
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.IntVar(&handoffFD, "handoff-fd", 0, "descriptor of the pipe on which up confirms that it has recorded this process (required; only up starts a supervisor)")
	if code, done := parse(fs, args); done {
		return code
	}
	if handoffFD == 0 {
		fmt.Fprintln(stderr, "supervise: --handoff-fd is required: only hpcburst up starts a supervisor")
		return exitUsage
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	if o.Handoff, err = launcher.OpenHandoff(handoffFD); err != nil {
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
		if c.ReadyzOK != nil {
			line += fmt.Sprintf(", GET /readyz: %v", *c.ReadyzOK)
			if len(c.ReadyzFailing) > 0 {
				line += " (failing: " + strings.Join(c.ReadyzFailing, ", ") + ")"
			}
		}
		if c.Degraded != "" {
			line += ", degraded: " + c.Degraded
		}
		if c.UsageState == launcher.UsageUnknown {
			line += ", usage state unknown"
		}
		if c.LastExit != nil {
			line += fmt.Sprintf(", last exit %q (code %d) at %s", c.LastExit.Detail, c.LastExit.Code, c.LastExit.At.Format("15:04:05"))
		}
		fmt.Fprintln(w, line)
	}
}
