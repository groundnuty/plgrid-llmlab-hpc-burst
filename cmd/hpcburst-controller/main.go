// Command hpcburst-controller is the framework's controller binary on the
// single-user host (docs/development.md, Process layout). hpcburst up starts
// and supervises it. It runs the lifecycle controller (HBEP-7) as a
// controller-runtime manager with the Lease v1alpha1.LeaseLifecycle in
// v1alpha1.ControllerNamespace.
//
// Not wired yet, because their packages are not on this branch: the
// per-Provider manager supervisor (internal/providerctl, issue #59), which
// main starts beside the lifecycle manager, and the agent API's HTTP server
// (internal/agentserver, issue #60), which serves the lifecycle controller's
// agentapi.Backend on loopback. Both plug in at extraRunnables.
//
// Usage:
//
//	hpcburst-controller --kubeconfig FILE --credentials-dir DIR --health-addr 127.0.0.1:PORT
//	    [--agent-builds-dir DIR] [--front-mode direct|ssh --front-addr HOST:PORT]
//	    [--leader-elect=true] [--workers 4]
//
// It exits 0 after SIGTERM or SIGINT once the manager has stopped, 1 on a
// failure, and 2 on a usage error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-logr/logr/funcr"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// flags are the command line, parsed.
type flags struct {
	kubeconfig     string
	credentialsDir string
	healthAddr     string
	agentBuildsDir string
	frontMode      string
	frontAddr      string
	leaderElect    bool
	workers        int
}

func parse(args []string, stderr io.Writer) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("hpcburst-controller", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "the control plane's kubeconfig (required)")
	fs.StringVar(&f.credentialsDir, "credentials-dir", "", "the controller's credentials directory, mode 700 (required)")
	fs.StringVar(&f.healthAddr, "health-addr", "", "loopback host:port of /healthz and /readyz (required)")
	fs.StringVar(&f.agentBuildsDir, "agent-builds-dir", "", "directory of the allocation agent's builds, hb-agent-<cpuArch>")
	fs.StringVar(&f.frontMode, "front-mode", "", "the recorded dial-home mode: direct or ssh")
	fs.StringVar(&f.frontAddr, "front-addr", "", "the recorded front address")
	fs.BoolVar(&f.leaderElect, "leader-elect", true, "take the lifecycle Lease, a guard against a stale second copy")
	fs.IntVar(&f.workers, "workers", lifecycle.DefaultWorkers, "work queue workers")
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	switch {
	case fs.NArg() > 0:
		return flags{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	case f.kubeconfig == "":
		return flags{}, errors.New("--kubeconfig is required")
	case f.credentialsDir == "":
		return flags{}, errors.New("--credentials-dir is required")
	case f.healthAddr == "":
		return flags{}, errors.New("--health-addr is required")
	case (f.frontMode == "") != (f.frontAddr == ""):
		return flags{}, errors.New("--front-mode and --front-addr go together")
	case f.frontMode != "" && f.frontMode != "direct" && f.frontMode != "ssh":
		return flags{}, fmt.Errorf("--front-mode %q: want direct or ssh", f.frontMode)
	case f.workers < 1:
		return flags{}, errors.New("--workers must be at least 1")
	}
	host, _, err := net.SplitHostPort(f.healthAddr)
	if err != nil {
		return flags{}, fmt.Errorf("--health-addr %q: %w", f.healthAddr, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return flags{}, fmt.Errorf("--health-addr %q: the controller listens on loopback only", f.healthAddr)
	}
	return f, nil
}

// checkCredentialsDir refuses a credentials directory that others can reach
// (HBEP-11, Credentials) and creates the subdirectories the controller writes.
func checkCredentialsDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("credentials directory: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("credentials directory %s is not a directory", dir)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("credentials directory %s has mode %v; it must be reachable by its owner only (0700)", dir, fi.Mode().Perm())
	}
	for _, sub := range []string{agentapi.AllocationCredentialPath(dir, "x"), agentapi.InstanceKeyPath(dir, "x")} {
		if err := os.MkdirAll(filepath.Dir(sub), 0o700); err != nil {
			return err
		}
	}
	return nil
}

// extraRunnables are the other components of the binary, added to the
// manager beside the lifecycle controller: #59's provider supervisor and #60's
// agent API server, once their packages land.
var extraRunnables []func(mgr manager.Manager, backend agentapi.Backend) (manager.Runnable, error)

func run(ctx context.Context, f flags) error {
	if err := checkCredentialsDir(f.credentialsDir); err != nil {
		return err
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", f.kubeconfig)
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	mgr, backend, err := lifecycle.NewManager(lifecycle.Options{
		Config: cfg, CredentialsDir: f.credentialsDir, AgentBuildsDir: f.agentBuildsDir,
		FrontConfigured: f.frontMode != "", HealthAddr: f.healthAddr, LeaderElection: f.leaderElect, Workers: f.workers,
	})
	if err != nil {
		return err
	}
	for _, add := range extraRunnables {
		r, err := add(mgr, backend)
		if err != nil {
			return err
		}
		if err := mgr.Add(r); err != nil {
			return err
		}
	}
	return mgr.Start(ctx)
}

func main() {
	f, err := parse(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-controller:", err)
		os.Exit(2)
	}
	ctrl.SetLogger(funcr.New(func(prefix, args string) {
		fmt.Fprintln(os.Stderr, prefix, args)
	}, funcr.Options{LogTimestamp: true}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, f); err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-controller:", err)
		os.Exit(1)
	}
}
