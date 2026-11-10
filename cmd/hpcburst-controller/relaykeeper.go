package main

// The relaykeeper subcommand (issue #343, PR 3; HBEP-6, The relay keeper; the
// science agent's ruling on #343): the relay keeper is `hpcburst-controller
// relaykeeper`, so the launcher resolves one binary. The keeper itself is
// relaykeeper.Run; this file is its command line, its logger, its signals and
// its exit codes.
//
// Usage (hpcburst up passes both, the same values it passes the controller):
//
//	hpcburst-controller relaykeeper --state-dir DIR --port-base PORT
//
// The ports of the relays' targets come from the same function the
// controller's relaySettings uses, agentapi.Host.RelayChannelPorts, so the
// two processes of one host agree on them.
//
// It exits 0 after SIGTERM or SIGINT once every relay's process is gone, 1 on
// a failure, and 2 on a usage error. The controller's own command line does
// not change: main asks runSubcommand first, and only the word "relaykeeper"
// as the first argument is the keeper's.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// relaykeeperCommand is the first argument that makes the binary the relay
// keeper.
const relaykeeperCommand = "relaykeeper"

// runSubcommand runs the subcommand args names, if it names one, and returns
// its exit code; ok is false when args are the controller's own, which main
// then parses as before. args is os.Args[1:].
func runSubcommand(args []string, stderr io.Writer) (code int, ok bool) {
	if len(args) > 0 && args[0] == relaykeeperCommand {
		return relaykeeperMain(args[1:], stderr), true
	}
	return 0, false
}

// keeperFlags is the subcommand's command line, parsed.
type keeperFlags struct {
	host agentapi.Host
}

// parseKeeper parses the subcommand's arguments (without the subcommand's
// name). A failure is one line, the error itself; -h prints the usage to
// stderr and returns flag.ErrHelp.
func parseKeeper(args []string, stderr io.Writer) (keeperFlags, error) {
	var f keeperFlags
	fs := flag.NewFlagSet("hpcburst-controller relaykeeper", flag.ContinueOnError)
	// The flag package would print its error and the usage on every failure;
	// a failure here is one line, printed by the caller.
	fs.SetOutput(io.Discard)
	fs.StringVar(&f.host.StateDir, "state-dir", "", "the launcher's state directory (required)")
	fs.IntVar(&f.host.PortBase, "port-base", 0, "the launcher's loopback port base (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(stderr)
			fmt.Fprintln(stderr, "usage: hpcburst-controller relaykeeper --state-dir DIR --port-base PORT")
			fs.PrintDefaults()
		}
		return keeperFlags{}, err
	}
	switch {
	case fs.NArg() > 0:
		return keeperFlags{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	case f.host.StateDir == "":
		return keeperFlags{}, errors.New("--state-dir is required")
	case f.host.PortBase == 0:
		return keeperFlags{}, errors.New("--port-base is required")
	// The same range as the controller's, whose block this port base names
	// (see parse).
	case f.host.PortBase < 1024 || f.host.PortBase > 65535-agentapi.ControllerMetricsPortOffset:
		return keeperFlags{}, fmt.Errorf("--port-base %d is out of range", f.host.PortBase)
	}
	return f, nil
}

// relaykeeperMain is the subcommand, with the arguments after its name. It
// returns the exit code. The keeper stops on SIGTERM or SIGINT.
func relaykeeperMain(args []string, stderr io.Writer) int {
	f, err := parseKeeper(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "hpcburst-controller relaykeeper:", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	return runKeeper(ctx, f, slog.New(slog.NewTextHandler(stderr, nil)), stderr)
}

// runKeeper runs the keeper until ctx ends and returns the exit code: 0 for a
// clean stop, 1 for a failure.
func runKeeper(ctx context.Context, f keeperFlags, log *slog.Logger, stderr io.Writer) int {
	err := relaykeeper.Run(ctx, relaykeeper.ProcessConfig{
		StateDir: f.host.StateDir,
		Ports:    f.host.RelayChannelPorts(),
		Logger:   log,
	})
	if err != nil {
		fmt.Fprintln(stderr, "hpcburst-controller relaykeeper:", err)
		return 1
	}
	return 0
}
