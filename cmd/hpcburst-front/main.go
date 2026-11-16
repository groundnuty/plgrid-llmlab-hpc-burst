// Command hpcburst-front is the LLMLab-compatible front (HBEP-5): the one
// endpoint a user points their client at. hpcburst up starts and supervises
// it on the single-user host (HBEP-11).
//
//	hpcburst-front --config FILE --state-dir DIR --port-base N [--mode direct|ssh|host-dialed] [--health-addr HOST:PORT] [--kubeconfig FILE]
//	hpcburst-front version
//
// "version" prints the binary's release from its build information, as "hpcburst-front <tag>
// <commit> <time>[ dirty]" (internal/version documents the format). It is a subcommand, like the
// other two host binaries', so that the three are asked the same way, and not a --version flag,
// which hpcburst already uses for the k3s release; the first argument "version" cannot be taken
// for one of the front's flags, which all begin with "--".
//
// Its settings are in the configuration file (HBEP-5, Parameters). Keys are
// read from mode-600 files at run time and never appear on its command line.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func main() {
	if code, ok := versionCommand(os.Args[1:], os.Stdout, os.Stderr); ok {
		os.Exit(code)
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-front:", err)
		var u usageError
		if errors.As(err, &u) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

type usageError struct{ error }

func run(args []string) error {
	fs := flag.NewFlagSet("hpcburst-front", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "the front's configuration file (required)")
	stateDir := fs.String("state-dir", "", "the launcher's state directory (required)")
	portBase := fs.Int("port-base", 0, "the launcher's port base (required)")
	mode := fs.String("mode", "", "the dial-home mode the launcher recorded: direct, ssh or host-dialed (the last has no front address: the host dials the jobs)")
	health := fs.String("health-addr", "", "loopback host:port for GET /healthz")
	kubeconfig := fs.String("kubeconfig", "", "the control plane's kubeconfig (default: <state-dir>/kubeconfig)")
	bulkPath := fs.Bool("bulk-path", false, "beta: the bulk path (HBEP-6): split the dial-home listener by TLS server name, and send each non-streamed request through the Instance's bulk transport while it is registered; the launcher passes it with up --bulk-path")
	eventLogTokenFile := fs.String("event-log-token-file", "", "the durable log's token file (beta; HBEP-8), which the launcher passes only while it runs the log; the address comes from --port-base. Accepted and stored, and not used yet: the usage forwarder that connects to the log is a later change")
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	switch {
	case *cfgPath == "" || *stateDir == "" || *portBase <= 0:
		return usageError{errors.New("--config, --state-dir and --port-base are required")}
	case *eventLogTokenFile != "" && !filepath.IsAbs(*eventLogTokenFile):
		return usageError{fmt.Errorf("--event-log-token-file %q must be an absolute path", *eventLogTokenFile)}
	case *eventLogTokenFile != "" && *portBase > 65535-agentapi.EventLogMonitorPortOffset:
		return usageError{fmt.Errorf("--port-base %d is out of range with --event-log-token-file: the block reaches base+%d", *portBase, agentapi.EventLogMonitorPortOffset)}
	case *mode != "" && !front.ValidMode(*mode):
		return usageError{fmt.Errorf("--mode %q: want %s, %s or %s", *mode, front.ModeDirect, front.ModeSSH, front.ModeHostDialed)}
	}
	cfg, err := front.LoadConfig(*cfgPath)
	if err != nil {
		return err
	}
	if *kubeconfig == "" {
		*kubeconfig = filepath.Join(*stateDir, "kubeconfig")
	}
	rest, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	dyn, err := dynamic.NewForConfig(rest)
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return err
	}
	// The record fetch's own clientset. It is not what keeps the record reads,
	// which any client key may ask for, from queueing the Lease's renewals: with
	// QPS unset, as here, client-go gives each API group's client a limiter of
	// its own, and the records (apps/v1) and the Lease (coordination/v1) are
	// different groups. It keeps them apart only if a QPS is set, which makes a
	// clientset share one limiter across its groups (#585, PR #587's review).
	records, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return err
	}
	rec, stopEvents, err := front.NewKubeRecorder(cs)
	if err != nil {
		return err
	}
	defer stopEvents()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	o := options(cfg, agentapi.Host{StateDir: *stateDir, PortBase: *portBase}, *mode, dyn, cs, records, rec, log, *health)
	o.BulkPath = *bulkPath
	o.EventLogTokenFile = *eventLogTokenFile
	return front.Run(ctx, o)
}

// options are Run's inputs in production. They set no Transports, so Run
// builds HBEP-6's Tunnel and its probe, and no PathEvents, so the probe's
// PathProbeFailing goes to the API server through rec (HBEP-6, Reporting).
func options(cfg front.Config, host agentapi.Host, mode string, dyn dynamic.Interface, cs, records kubernetes.Interface, rec front.Recorder, log *slog.Logger, health string) front.Options {
	return front.Options{
		Config: cfg, Host: host, Mode: mode,
		Clock: clock.Real{}, Dynamic: dyn, Kube: cs, Records: records, Recorder: rec, Log: log, HealthAddr: health,
	}
}
