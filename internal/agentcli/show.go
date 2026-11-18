package agentcli

import (
	"context"
	"io"
)

// cmdOperation is hpcburst operation <name>: the plan, the phase, the conditions and the per-target
// report.
func (rt *cli) cmdOperation(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, o := newFlags("operation", stderr, false)
	name, code, done := oneArg(fs, args, "Operation name")
	if done {
		return code
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	op, err := getOperation(ctx, cl.c, ns, name)
	if err != nil {
		return failf(stderr, exitFail, "%v", err)
	}
	v := operationViewOf(op, rt.now())
	if o.asJSON {
		return emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "operation": v}))
	}
	printOperation(stdout, v)
	return exitOK
}
