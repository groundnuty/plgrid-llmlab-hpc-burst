package main

// The agent commands of HBEP-10's beta step 2 (issue #659): scale, end, release, confirm, approvals,
// approve and operation, and beta step 5's inventory (issue #662). internal/agentcli does the work and docs/agent-cli.md is its contract; this file
// hands it the process's kubeconfig variable and the terminal, which the package never reads itself.

import (
	"context"
	"io"
	"os"

	"golang.org/x/term"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentcli"
)

// cmdAgent runs one agent command; args[0] is the command.
func cmdAgent(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return agentcli.Run(ctx, args, agentEnv(stdout), os.Stdin, stdout, stderr)
}

// agentEnv is the process's side of agentcli.Env. The commands ask a question only when stdin and stdout
// are both terminals.
func agentEnv(stdout io.Writer) agentcli.Env {
	return agentcli.Env{
		Kubeconfig:   os.Getenv("KUBECONFIG"),
		Home:         os.Getenv("HOME"),
		XDGStateHome: os.Getenv("XDG_STATE_HOME"),
		IsTerminal:   func() bool { return bothTerminals(term.IsTerminal, os.Stdin, stdout) },
	}
}

// bothTerminals reports whether stdin and stdout are both terminals: only then may a command ask a question,
// since a prompt on a pipe or an answer read from one is not a person at a terminal. isTerm is the check on a
// file descriptor.
func bothTerminals(isTerm func(fd int) bool, stdin *os.File, stdout io.Writer) bool {
	out, ok := stdout.(*os.File)
	return ok && isTerm(int(stdin.Fd())) && isTerm(int(out.Fd()))
}
