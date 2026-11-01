// Command hpcburst-controller is the framework's controller binary on the
// single-user host (docs/development.md, Process layout). hpcburst up starts
// and supervises it. It runs the lifecycle controller (HBEP-7) as a
// controller-runtime manager with the Lease v1alpha1.LeaseLifecycle in
// v1alpha1.ControllerNamespace.
//
// Beside the lifecycle controller, in its manager, it runs the components of
// components.go: the per-Provider manager supervisors for the batch and the
// managed Providers (internal/providerctl, #59; internal/managed, #69), which
// follow the lifecycle leader (HBEP-11, Controllers), and the agent API's HTTP
// server (internal/agentserver, #60), which serves the lifecycle controller's
// agentapi.Backend on the agent endpoint. Its log names its port base, which
// tells two copies on one host apart. frps's server
// plugin (#90) plugs in at extraRunnables once it merges.
//
// Usage (hpcburst up passes the first three and the front it recorded):
//
//	hpcburst-controller --state-dir DIR --port-base PORT --health-addr 127.0.0.1:PORT
//	    [--front-mode direct|ssh --front-addr HOST:PORT]
//	    [--kubeconfig FILE] [--leader-elect=true] [--workers 4]
//
// The state directory and the port base fill an agentapi.Host, which names
// the shared paths and ports (docs/development.md, Runtime conventions): the
// kubeconfig is <state>/kubeconfig unless --kubeconfig says otherwise, the
// credentials directory is Host.CredentialsDir() and the agent builds are in
// Host.AgentBuildsDir(); the agent API binds Host.AgentEndpointAddr().
//
// Environment: HF_ENDPOINT, when set, is the origin of the Hugging Face Hub
// that the lifecycle controller lists checkpoints from, as hb-agent downloads
// them from it (cmd/hb-agent); unset, it is https://huggingface.co.
//
// It exits 0 after SIGTERM or SIGINT once the manager has stopped, 1 on a
// failure (a Provider-manager scheme without apps/v1, and front settings that
// agent.json would refuse, are refused before anything starts), and 2 on a
// usage error.
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// flags are the command line, parsed.
type flags struct {
	host        agentapi.Host
	kubeconfig  string
	healthAddr  string
	frontMode   string
	frontAddr   string
	leaderElect bool
	workers     int
}

func parse(args []string, stderr io.Writer) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("hpcburst-controller", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.host.StateDir, "state-dir", "", "the launcher's state directory (required)")
	fs.IntVar(&f.host.PortBase, "port-base", 0, "the launcher's loopback port base (required)")
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "the control plane's kubeconfig (default <state-dir>/kubeconfig)")
	fs.StringVar(&f.healthAddr, "health-addr", "", "loopback host:port of /healthz and /readyz (required)")
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
	case f.host.StateDir == "":
		return flags{}, errors.New("--state-dir is required")
	// The block ends at the controller's metrics, base+12 (HBEP-8, Metrics).
	case f.host.PortBase < 1024 || f.host.PortBase > 65535-agentapi.ControllerMetricsPortOffset:
		return flags{}, fmt.Errorf("--port-base %d is out of range", f.host.PortBase)
	case f.healthAddr == "":
		return flags{}, errors.New("--health-addr is required")
	case (f.frontMode == "") != (f.frontAddr == ""):
		return flags{}, errors.New("--front-mode and --front-addr go together")
	case f.frontMode != "" && f.frontMode != "direct" && f.frontMode != "ssh":
		return flags{}, fmt.Errorf("--front-mode %q: want direct or ssh", f.frontMode)
	case f.workers < 1:
		return flags{}, errors.New("--workers must be at least 1")
	}
	if f.kubeconfig == "" {
		f.kubeconfig = filepath.Join(f.host.StateDir, "kubeconfig")
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

// extraRunnables are components still to come, added to the manager after
// mount's (components.go). The next is frps's server plugin (#90,
// internal/frpsplugin): a Runnable without leader election that serves its
// Handler at agentapi.FrpsPluginPath on host.FrpsPluginAddr() (base+8), with an
// Index fed from mgr's cache of Allocations and Instances and the credential
// files under host.CredentialsDir().
var extraRunnables []func(mgr manager.Manager, host agentapi.Host, backend agentapi.Backend) (manager.Runnable, error)

func run(ctx context.Context, f flags, d deps) error {
	scheme, err := d.providerScheme()
	if err != nil {
		return err
	}
	if err := checkScheme(scheme); err != nil {
		return err
	}
	if err := checkCredentialsDir(f.host.CredentialsDir()); err != nil {
		return err
	}
	front, err := frontSettings(f, d)
	if err != nil {
		return err
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", f.kubeconfig)
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	mgr, backend, err := lifecycle.NewManager(managerOptions(f, cfg, os.Getenv))
	if err != nil {
		return err
	}
	w := wiring{cfg: cfg, host: f.host, scheme: scheme, front: front, newPlatform: d.newPlatform, log: ctrl.Log.WithName("hpcburst-controller").WithValues("portBase", f.host.PortBase)}
	if err := mount(mgr, w, backend); err != nil {
		return err
	}
	for _, add := range extraRunnables {
		r, err := add(mgr, f.host, backend)
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
	if err := run(ctx, f, defaultDeps()); err != nil {
		fmt.Fprintln(os.Stderr, "hpcburst-controller:", err)
		os.Exit(1)
	}
}

// hubEndpointEnv names the Hub's origin, as for hb-agent.
const hubEndpointEnv = "HF_ENDPOINT"

// managerOptions are the lifecycle manager's options from the flags and the
// environment.
func managerOptions(f flags, cfg *rest.Config, getenv func(string) string) lifecycle.Options {
	return lifecycle.Options{
		Config: cfg, CredentialsDir: f.host.CredentialsDir(), AgentBuildsDir: f.host.AgentBuildsDir(),
		FrontConfigured: f.frontMode != "", HealthAddr: f.healthAddr, LeaderElection: f.leaderElect, Workers: f.workers,
		HubEndpoint: getenv(hubEndpointEnv),
	}
}
