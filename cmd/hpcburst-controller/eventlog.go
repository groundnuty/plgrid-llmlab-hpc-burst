package main

// The eventlog subcommand (issue #625, PR E1; HBEP-8, The durable log, Where it runs): HBEP-8's
// durable log is `hpcburst-controller eventlog`, an embedded nats-server with JetStream, so the
// launcher resolves one binary. The process is internal/eventlog/node.Run; this file is its command
// line, its signals and its exit codes.
//
// Usage (hpcburst up writes both files, and the supervisor passes both):
//
//	hpcburst-controller eventlog --config <state>/eventlog/nats.conf --caps-file <state>/eventlog/caps.json
//
// It exits 0 after SIGTERM or SIGINT once the server has stopped, 1 on a failure, 2 on a usage
// error, and eventlog.ExitRefused (3) when it refuses to serve because a setting is not what HBEP-8
// says: it then names every difference on stderr and in <state>/eventlog/refused.txt. The token is
// only in the configuration file, never on the command line.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node"
)

// eventlogCommand is the first argument that makes the binary the event log.
const eventlogCommand = "eventlog"

// eventlogFlags is the subcommand's command line, parsed.
type eventlogFlags struct{ config, caps string }

// parseEventlog parses the subcommand's arguments (without the subcommand's name). A failure is one
// line, the error itself; -h prints the usage to stderr and returns flag.ErrHelp.
func parseEventlog(args []string, stderr io.Writer) (eventlogFlags, error) {
	var f eventlogFlags
	fs := flag.NewFlagSet("hpcburst-controller eventlog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&f.config, "config", "", "nats.conf, which the launcher wrote (required)")
	fs.StringVar(&f.caps, "caps-file", "", "caps.json, which the launcher wrote: the two streams' caps in bytes (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(stderr)
			fmt.Fprintln(stderr, "usage: hpcburst-controller eventlog --config FILE --caps-file FILE")
			fs.PrintDefaults()
		}
		return eventlogFlags{}, err
	}
	switch {
	case fs.NArg() > 0:
		return eventlogFlags{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	case f.config == "":
		return eventlogFlags{}, errors.New("--config is required")
	case f.caps == "":
		return eventlogFlags{}, errors.New("--caps-file is required")
	}
	return f, nil
}

// eventlogMain is the subcommand, with the arguments after its name. It returns the exit code.
func eventlogMain(args []string, stdout, stderr io.Writer) int {
	f, err := parseEventlog(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "hpcburst-controller eventlog:", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	return runEventlog(ctx, f, stdout, stderr)
}

// runEventlog serves until ctx ends and returns the exit code.
func runEventlog(ctx context.Context, f eventlogFlags, stdout, stderr io.Writer) int {
	err := node.Run(ctx, node.Options{ConfigPath: f.config, CapsPath: f.caps, Stdout: stdout, Stderr: stderr})
	var refused *node.RefusedError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &refused):
		fmt.Fprintln(stderr, "hpcburst-controller eventlog:", err)
		return eventlog.ExitRefused
	}
	fmt.Fprintln(stderr, "hpcburst-controller eventlog:", err)
	return 1
}
