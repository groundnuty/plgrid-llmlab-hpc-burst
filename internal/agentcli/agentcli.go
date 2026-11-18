// Package agentcli is the command line of HBEP-10's Operations and approvals: hpcburst scale, end,
// release, confirm, approvals, approve and operation (issue #659, beta step 2), and the inventory
// (issue #662, beta step 5). docs/agent-cli.md is its contract: what each command sends, what it prints,
// and the shape of --json.
//
// Every command acts as the caller, with the caller's own kubeconfig and nothing else (caller.go). The
// commands write only Operations (create, and update to confirm) and Approvals (create), never a
// ModelDeployment, an Allocation or an Instance; hpcburst inventory writes nothing at all (inventory.go,
// and the lister and printer of internal/inventory). This package must not import internal/lifecycle: the
// lifecycle package's end-to-end test imports this one.
package agentcli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Exit codes: the same as the other hpcburst commands.
const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

// usage is what hpcburst prints for a mistake in these commands.
const usage = `usage: hpcburst scale <md> --instances N [flags]
       hpcburst end <md> [--delete] [flags]
       hpcburst release <allocation>... | -l <selector> [flags]
       hpcburst confirm <operation> --plan <id> [flags]
       hpcburst approvals [-A] [flags]
       hpcburst approve <operation> --plan <id> [flags]
       hpcburst operation <name> [flags]
       hpcburst inventory [-n <namespace> | -A] [--owner <user>] [flags]
(every command takes --kubeconfig, -n and --json; hpcburst <command> -h lists its flags)`

// Env is what the commands read of the process, handed in so that no part of the package reads the
// process's own environment or terminal.
//
// Only Kubeconfig is read: the credential is --kubeconfig, else this. Home and XDGStateHome name the
// places where a launcher's kubeconfigs and the default kubeconfig live; nothing here reads them, and
// nothing may (a test plants a kubeconfig in each and runs every command to show it). IsTerminal says
// whether the commands may ask a question: true only when stdin and stdout are both terminals.
//
// WrapTransport is a test seam with no production caller: when it is set, every request the commands make
// goes through the round tripper it returns. The end-to-end tests use it to act between a command's two
// calls to the API server. It has no flag and no environment variable, so nothing outside a test can set
// it; nil, which is what hpcburst passes, leaves the transport as the kubeconfig makes it.
type Env struct {
	Kubeconfig    string
	Home          string
	XDGStateHome  string
	IsTerminal    func() bool
	WrapTransport func(http.RoundTripper) http.RoundTripper
}

func (e Env) terminal() bool { return e.IsTerminal != nil && e.IsTerminal() }

// cli is what the commands use besides their arguments. Run builds the real one, and the tests
// replace the connection, the clock, the pause between two reads and the generated names.
type cli struct {
	env     Env
	connect func(ctx context.Context, kubeconfig string, stderr io.Writer) (*caller, error)
	now     func() time.Time
	pause   func(ctx context.Context, d time.Duration) error
	poll    time.Duration
	newName func() string
}

// pollInterval is how often a command reads an Operation while it waits for the controller.
const pollInterval = 500 * time.Millisecond

// Run executes one command line, args[0] being the command, and returns the exit code: 0 success, 1 a
// refusal or a failure, 2 a usage error.
func Run(ctx context.Context, args []string, env Env, stdin io.Reader, stdout, stderr io.Writer) int {
	rt := &cli{env: env, now: time.Now, pause: sleep, poll: pollInterval, newName: generatedName}
	rt.connect = func(ctx context.Context, kubeconfig string, stderr io.Writer) (*caller, error) {
		return loadCaller(ctx, kubeconfig, env, stderr)
	}
	return rt.run(ctx, args, stdin, stdout, stderr)
}

func (rt *cli) run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	rest := args[1:]
	switch args[0] {
	case "scale":
		return rt.cmdScale(ctx, rest, stdin, stdout, stderr)
	case "end":
		return rt.cmdEnd(ctx, rest, stdin, stdout, stderr)
	case "release":
		return rt.cmdRelease(ctx, rest, stdin, stdout, stderr)
	case "confirm":
		return rt.cmdConfirm(ctx, rest, stdout, stderr)
	case "approvals":
		return rt.cmdApprovals(ctx, rest, stdout, stderr)
	case "approve":
		return rt.cmdApprove(ctx, rest, stdout, stderr)
	case "operation":
		return rt.cmdOperation(ctx, rest, stdout, stderr)
	case "inventory":
		return rt.cmdInventory(ctx, rest, stdout, stderr)
	}
	fmt.Fprintf(stderr, "hpcburst: unknown command %q\n%s\n", args[0], usage)
	return exitUsage
}

// sleep waits d, or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// failf prints one line on stderr and returns the exit code.
func failf(stderr io.Writer, code int, format string, a ...any) int {
	fmt.Fprintf(stderr, "hpcburst: "+format+"\n", a...)
	return code
}
