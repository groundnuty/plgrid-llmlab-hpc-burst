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
// plugin and the writer of the Instances' TunnelConnected condition (#90,
// tunnel.go) plug in at extraRunnables.
//
// Usage (hpcburst up passes the first three and the front it recorded):
//
//	hpcburst-controller --state-dir DIR --port-base PORT --health-addr 127.0.0.1:PORT
//	    [--front-mode direct|ssh --front-addr HOST:PORT | --front-mode host-dialed]
//	    [--kubeconfig FILE] [--leader-elect=true] [--workers 4]
//
// --front-mode host-dialed takes no --front-addr, and --front-addr is refused with it: the host
// dials the jobs through the login node, so agent.json names no endpoint (HBEP-6, Dial-home
// modes; HBEP-11, open question 8, amended 9 Nov 2026). Direct and ssh need an address.
//
// The state directory and the port base fill an agentapi.Host, which names
// the shared paths and ports (docs/development.md, Runtime conventions): the
// kubeconfig is <state>/kubeconfig unless --kubeconfig says otherwise, the
// credentials directory is Host.CredentialsDir() and the agent builds are in
// Host.AgentBuildsDir(); the agent API binds Host.AgentEndpointAddr(). The
// lifecycle controller serves /metrics at Host.ControllerMetricsAddr(), on
// loopback at the port base + 12 (HBEP-8, Metrics).
//
// Environment: HF_ENDPOINT, when set, is the origin of the Hugging Face Hub
// that the lifecycle controller lists checkpoints from; it is written into
// each Allocation's agent.json (hubEndpoint), from which hb-agent downloads
// them, since a job does not inherit this host's environment. Unset, it is
// https://huggingface.co. A value agentapi.CheckHubEndpoint refuses stops the
// controller at start.
//
// It exits 0 after SIGTERM or SIGINT once the manager has stopped, 1 on a
// failure (a Provider-manager scheme without apps/v1, and front settings that
// agent.json would refuse, are refused before anything starts), and 2 on a
// usage error.
//
// With "relaykeeper" as its first argument the binary is the relay keeper
// instead (relaykeeper.go; HBEP-6, The relay keeper), and none of the flags
// above apply.
//
// With "version" as its first argument it prints its release from its build
// information and exits, as "hpcburst-controller <tag> <commit> <time>[ dirty]"
// (internal/version documents the format); it takes no flags.
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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
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
	// relayKeeperSocket is the relay keeper's socket (beta; HBEP-6, The
	// relay keeper; #343): set, host-dialed relays are the keeper's, and the
	// controller sends it desired sets; "" is alpha, the relays the
	// controller's own. The launcher passes the path; none is computed here.
	relayKeeperSocket string
	// instanceRouter is the router each new Instance runs in front of its
	// worker (beta, opt-in; HBEP-5, Inside an Instance; #429): "envoy", or ""
	// for none, alpha's shape. It is recorded in each Instance's launch at
	// creation, so a restart with another value changes no running Instance.
	instanceRouter string
}

func parse(args []string, stderr io.Writer) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("hpcburst-controller", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.host.StateDir, "state-dir", "", "the launcher's state directory (required)")
	fs.IntVar(&f.host.PortBase, "port-base", 0, "the launcher's loopback port base (required)")
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "the control plane's kubeconfig (default <state-dir>/kubeconfig)")
	fs.StringVar(&f.healthAddr, "health-addr", "", "loopback host:port of /healthz and /readyz (required)")
	fs.StringVar(&f.frontMode, "front-mode", "", "the recorded dial-home mode: direct, ssh or host-dialed")
	fs.StringVar(&f.frontAddr, "front-addr", "", "the recorded front address; direct and ssh need it, host-dialed takes none because the host dials the jobs")
	fs.BoolVar(&f.leaderElect, "leader-elect", true, "take the lifecycle Lease, a guard against a stale second copy")
	fs.IntVar(&f.workers, "workers", lifecycle.DefaultWorkers, "work queue workers")
	fs.StringVar(&f.relayKeeperSocket, "relay-keeper-socket", "", "the relay keeper's Unix socket, in host-dialed mode only (beta): the keeper holds the relays; unset, the controller does (alpha)")
	fs.StringVar(&f.instanceRouter, "instance-router", "", "the router each new Instance runs in front of its worker (beta): envoy; unset, none (alpha)")
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
	case f.frontMode == "" && f.frontAddr != "":
		return flags{}, errors.New("--front-mode and --front-addr go together")
	case f.frontMode != "" && !agentapi.ValidDialHomeMode(agentapi.DialHomeMode(f.frontMode)):
		return flags{}, fmt.Errorf("--front-mode %q: want %s, %s or %s", f.frontMode, agentapi.DialHomeDirect, agentapi.DialHomeSSH, agentapi.DialHomeHostDialed)
	// Host-dialed mode has no address, because the host dials the jobs (HBEP-6, Dial-home
	// modes; HBEP-11, open question 8, amended 9 Nov 2026); direct and ssh need one.
	case f.frontMode == string(agentapi.DialHomeHostDialed) && f.frontAddr != "":
		return flags{}, fmt.Errorf("--front-mode %s takes no --front-addr: the host dials the jobs", f.frontMode)
	case f.frontMode != "" && f.frontMode != string(agentapi.DialHomeHostDialed) && f.frontAddr == "":
		return flags{}, errors.New("--front-mode and --front-addr go together")
	case f.workers < 1:
		return flags{}, errors.New("--workers must be at least 1")
	// The keeper holds host-dialed relays only (HBEP-6, The relay keeper), at
	// a path the agent API's 107-byte socket limit allows.
	case f.relayKeeperSocket != "" && f.frontMode != string(agentapi.DialHomeHostDialed):
		return flags{}, fmt.Errorf("--relay-keeper-socket is for --front-mode %s only", agentapi.DialHomeHostDialed)
	case f.relayKeeperSocket != "" && (!filepath.IsAbs(f.relayKeeperSocket) || len(f.relayKeeperSocket) > agentapi.MaxRelaySocketPath):
		return flags{}, fmt.Errorf("--relay-keeper-socket must be an absolute path of at most %d bytes", agentapi.MaxRelaySocketPath)
	case f.instanceRouter != "" && f.instanceRouter != string(v1alpha1.InstanceRouterEnvoy):
		return flags{}, fmt.Errorf("--instance-router %q: want %s, or unset for no router (alpha)", f.instanceRouter, v1alpha1.InstanceRouterEnvoy)
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

// extraRunnables are components added to the manager after mount's
// (components.go). Each returns the Runnable run adds, and may add others to mgr
// itself. addTunnel (#90, tunnel.go) returns frps's server plugin, a Runnable
// without leader election that serves the frpsplugin Handler on
// host.FrpsPluginAddr() (base+8) with an Index fed from mgr's cache of
// Allocations and Instances and the credential files, and adds the leader
// component that writes each Instance's TunnelConnected condition. events is
// the copy's raised-events counter, which a component that raises events
// counts into through its recorder (HBEP-8, Raised events).
var extraRunnables = []func(mgr manager.Manager, host agentapi.Host, backend agentapi.Backend, listen func(addr string) (net.Listener, error), events *eventcount.Counter) (manager.Runnable, error){addTunnel}

func run(ctx context.Context, f flags, d deps) error {
	// The agent endpoint and frps's plugin are bound through ls (components.go),
	// and whatever it bound is closed when run returns.
	ls := newListeners(d)
	defer ls.closeAll()
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
	opts := managerOptions(f, cfg, os.Getenv)
	opts.Registry = d.registry
	opts.RelayKeeperSocket = f.relayKeeperSocket
	opts.InstanceRouter = v1alpha1.InstanceRouter(f.instanceRouter)
	// One raised-events counter for this copy: the lifecycle controller, its
	// tunnel-state reader and the Provider managers count into it, and the
	// lifecycle controller's serving collector exports it (HBEP-8, Raised
	// events).
	events := eventcount.New()
	opts.Events = events
	if opts.HubEndpoint != "" {
		if err := agentapi.CheckHubEndpoint(opts.HubEndpoint); err != nil {
			return fmt.Errorf("%s: %w", hubEndpointEnv, err)
		}
	}
	mgr, backend, err := lifecycle.NewManager(opts)
	if err != nil {
		return err
	}
	w := wiring{cfg: cfg, host: f.host, scheme: scheme, front: front, hubEndpoint: opts.HubEndpoint, newPlatform: d.newPlatform, listen: ls.Listen, events: events, log: ctrl.Log.WithName("hpcburst-controller").WithValues("portBase", f.host.PortBase)}
	w.keeperSocket = f.relayKeeperSocket
	if err := mount(mgr, w, backend); err != nil {
		return err
	}
	for _, add := range extraRunnables {
		r, err := add(mgr, f.host, backend, ls.Listen, events)
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
	// A subcommand runs before the controller's flags are parsed.
	if code, ok := versionCommand(os.Args[1:], os.Stdout, os.Stderr); ok {
		os.Exit(code)
	}
	if code, ok := runSubcommand(os.Args[1:], os.Stderr); ok {
		os.Exit(code)
	}
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
// environment; the caller adds the metrics registry.
func managerOptions(f flags, cfg *rest.Config, getenv func(string) string) lifecycle.Options {
	return lifecycle.Options{
		Config: cfg, CredentialsDir: f.host.CredentialsDir(), AgentBuildsDir: f.host.AgentBuildsDir(),
		FrontConfigured: f.frontMode != "", HostDialed: f.frontMode == string(agentapi.DialHomeHostDialed), HealthAddr: f.healthAddr, MetricsAddr: f.host.ControllerMetricsAddr(),
		LeaderElection: f.leaderElect, Workers: f.workers, HubEndpoint: getenv(hubEndpointEnv),
	}
}
