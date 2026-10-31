// Command hpcburst-front is the LLMLab-compatible front (HBEP-5): the one
// endpoint a user points their client at. hpcburst up starts and supervises
// it on the single-user host (HBEP-11).
//
//	hpcburst-front --config FILE --state-dir DIR --port-base N [--mode direct|ssh] [--health-addr HOST:PORT] [--kubeconfig FILE]
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
	mode := fs.String("mode", "", "the dial-home mode the launcher recorded: direct or ssh")
	health := fs.String("health-addr", "", "loopback host:port for GET /healthz")
	kubeconfig := fs.String("kubeconfig", "", "the control plane's kubeconfig (default: <state-dir>/kubeconfig)")
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	switch {
	case *cfgPath == "" || *stateDir == "" || *portBase <= 0:
		return usageError{errors.New("--config, --state-dir and --port-base are required")}
	case *mode != "" && *mode != front.ModeDirect && *mode != front.ModeSSH:
		return usageError{fmt.Errorf("--mode %q: want direct or ssh", *mode)}
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
	rec, stopEvents, err := front.NewKubeRecorder(cs)
	if err != nil {
		return err
	}
	defer stopEvents()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	return front.Run(ctx, front.Options{
		Config: cfg, Host: agentapi.Host{StateDir: *stateDir, PortBase: *portBase}, Mode: *mode,
		Clock: clock.Real{}, Dynamic: dyn, Recorder: rec, Log: log, HealthAddr: *health,
	})
}
