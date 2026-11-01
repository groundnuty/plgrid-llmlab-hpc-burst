package main

// The components the binary mounts beside the lifecycle controller, in its
// manager (docs/development.md, Process layout; HBEP-7, Controllers; HBEP-4,
// Two controllers):
//
//   - the per-Provider manager supervisor for the batch Providers
//     (internal/providerctl, #59), which runs providerctl.Setup over the Slurm
//     plugin in one manager per Provider, each with its own Lease;
//   - the same supervisor for the managed Providers, with managed.Setup (#69);
//   - the agent API's HTTP server (internal/agentserver, #60) on the agent
//     endpoint, agentapi.Host.AgentEndpointAddr() (base+3), which
//     authenticates each call from the manager's cache and the credential files
//     and then calls the lifecycle controller's Backend.

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/user"
	"path/filepath"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentserver"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// deps are the parts of run that a test replaces; main passes defaultDeps().
// Everything else run builds the one way, in tests too.
type deps struct {
	// providerScheme builds the scheme of every Provider manager.
	providerScheme func() (*runtime.Scheme, error)
	// newPlatform makes a batch Provider's Platform; nil means the production
	// one, a slurm.Batch over ssh (providerctl.SSHPlatform).
	newPlatform providerctl.PlatformFactory
}

func defaultDeps() deps { return deps{providerScheme: providerScheme} }

// providerScheme is the Provider managers' scheme: the built-in kinds, apps/v1
// among them, and the framework's.
func providerScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// checkScheme refuses, at start, a Provider-manager scheme that lacks a kind
// the Provider controllers read or write. apps/v1's ControllerRevision is the
// accounting record of a deleted Allocation (HBEP-8, Provider accounting;
// #113). providerctl.New refuses such a scheme too (#118), but it runs inside
// the supervisor, which only backs off and retries, so the binary would keep
// running with no Provider controller at all; here it exits instead.
func checkScheme(s *runtime.Scheme) error {
	var missing []error
	for _, gvk := range []schema.GroupVersionKind{
		appsv1.SchemeGroupVersion.WithKind("ControllerRevision"),
		v1alpha1.GroupVersion.WithKind("Allocation"),
		v1alpha1.GroupVersion.WithKind("Provider"),
		v1alpha1.GroupVersion.WithKind("PlatformProfile"),
	} {
		if !s.Recognizes(gvk) {
			missing = append(missing, fmt.Errorf("the Provider managers' scheme does not know %s (register %s)", gvk, gvk.GroupVersion()))
		}
	}
	return errors.Join(missing...)
}

// frpcDigests are the SHA-256 of the frpc binary per cpuArch that agent.json
// names (HBEP-4, Submission: Files first; providerctl.FrontSettings): frp
// v0.71.1's frpc for linux_amd64, hack/fetch-frp.sh's FRPC_SHA, which a test
// keeps equal to it. Only linux_amd64 is pinned, so a Create on an aarch64
// partition stops before any platform call with "no frpc digest is configured
// for cpuArch aarch64".
var frpcDigests = map[string]string{
	"x86_64": "35c6054d4549bbcf2f02875543895fd9e44c2aae2925f61cd8ff5a693703c28b",
}

// frontSettings are the batch Providers' front and agent-endpoint settings
// (providerctl.FrontSettings), from the front the launcher recorded; nil when
// it recorded none, and then nothing is submitted (HBEP-11, open question 8).
// The pin is the agent endpoint certificate's, which the launcher created; in
// ssh mode the user is the one who runs this binary, and the two forwarded
// ports are the Host's (agentapi.Host).
func frontSettings(f flags) (*providerctl.FrontSettings, error) {
	if f.frontMode == "" {
		return nil, nil
	}
	pin, err := certPin(f.host.AgentCertFile())
	if err != nil {
		return nil, err
	}
	fs := &providerctl.FrontSettings{
		Mode: agentapi.DialHomeMode(f.frontMode), Endpoint: f.frontAddr, CertPin: pin,
		FrpcDigests: maps.Clone(frpcDigests),
	}
	if fs.Mode == agentapi.DialHomeSSH {
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("the ssh dial-home user: %w", err)
		}
		fs.SSHUser, fs.AgentPort, fs.DialHomePort = u.Username, f.host.AgentEndpointPort(), f.host.DialHomePort()
	}
	return fs, nil
}

// certPin reads the agent endpoint's PEM certificate and returns its pin.
func certPin(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("the agent endpoint certificate: %w", err)
	}
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return "", fmt.Errorf("the agent endpoint certificate %s holds no PEM certificate", path)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return "", fmt.Errorf("the agent endpoint certificate %s: %w", path, err)
	}
	return agentapi.PinOf(c), nil
}

// sshControlDir is where the batch Providers' ssh masters keep their sockets.
// sshrun caps a socket's path at 90 bytes, so it is not under --state-dir but
// under $XDG_RUNTIME_DIR, or the system's temporary directory, named by the
// user and the port base so that two launchers on one host never share it.
// sshrun creates it with mode 700 and refuses one another user owns.
func sshControlDir(host agentapi.Host) string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if !filepath.IsAbs(dir) {
		dir = os.TempDir()
	}
	return filepath.Join(dir, fmt.Sprintf("hpcburst-ssh-%d-%d", os.Geteuid(), host.PortBase))
}

// wiring is what the components are built from.
type wiring struct {
	cfg         *rest.Config
	host        agentapi.Host
	scheme      *runtime.Scheme
	front       *providerctl.FrontSettings
	newPlatform providerctl.PlatformFactory
	log         logr.Logger
}

func (w wiring) newProviderManager(provider string) (manager.Manager, error) {
	return providerctl.NewProviderManager(w.cfg, w.scheme, provider, providerctl.ManagerOptions{})
}

// setupBatch registers a batch Provider's controller with its manager. Its
// recorder is crossplane-runtime's over the manager's, so the Allocation events
// reach the API; providerctl's default drops them (#90's wiring note; HBEP-8,
// Code that differs). It writes core/v1 Events with no action, which readers
// treat as type "other" (HBEP-8, open question 14).
func (w wiring) setupBatch(pm manager.Manager, provider string) error {
	return providerctl.Setup(pm, providerctl.Options{
		Provider:       provider,
		Profiles:       &slurm.ProfileReader{Reader: pm.GetClient()},
		NewPlatform:    w.newPlatform,
		CredentialsDir: w.host.CredentialsDir(),
		AgentBuildsDir: w.host.AgentBuildsDir(),
		Front:          w.front,
		Log:            logging.NewLogrLogger(pm.GetLogger().WithName("provider").WithValues("provider", provider)),
		Recorder:       event.NewAPIRecorder(pm.GetEventRecorderFor(v1alpha1.FieldManagerProvider)),
	})
}

// component is one part of the binary in the lifecycle controller's manager.
// It logs when it starts and stops, so the controller's log shows every part
// running, and none waits for the lifecycle Lease: each Provider's manager
// takes its own Lease, and the agent API answers through the Backend, which
// says CodeUnavailable while this replica does not lead.
type component struct {
	name  string
	log   logr.Logger
	start func(context.Context) error
}

var _ manager.LeaderElectionRunnable = component{}

func (c component) NeedLeaderElection() bool { return false }

func (c component) Start(ctx context.Context) error {
	c.log.Info("component starting")
	if err := c.start(ctx); err != nil {
		c.log.Error(err, "component failed")
		return fmt.Errorf("%s: %w", c.name, err)
	}
	c.log.Info("component stopped")
	return nil
}

// mount adds the components to the lifecycle controller's manager. Each reads
// through the manager's cache, which already watches the Providers,
// PlatformProfiles and Allocations for the lifecycle controller.
func mount(mgr manager.Manager, w wiring, backend agentapi.Backend) error {
	newPlatform := w.newPlatform
	if newPlatform == nil {
		newPlatform = providerctl.SSHPlatform(providerctl.SSHPlatformOptions{ControlDir: sshControlDir(w.host)})
	}
	w.newPlatform = newPlatform
	batch := &providerctl.Supervisor{
		Kind: v1alpha1.PlatformBatch, Client: mgr.GetClient(), NewManager: w.newProviderManager, Setup: w.setupBatch,
		Log: logging.NewLogrLogger(w.log.WithName("providers-batch")),
	}
	catalog := &providerctl.Supervisor{
		Kind: v1alpha1.PlatformManaged, Client: mgr.GetClient(), NewManager: w.newProviderManager, Setup: managed.Setup,
		Log: logging.NewLogrLogger(w.log.WithName("providers-managed")),
	}
	srv, err := agentserver.New(agentserver.Config{
		Lookup: agentserver.NewClientLookup(mgr.GetClient()), CredentialsDir: w.host.CredentialsDir(),
		Backend: backend, Host: w.host, Logger: slog.New(logr.ToSlogHandler(w.log.WithName("agent-api"))),
	})
	if err != nil {
		return err
	}
	named := func(name string, kv ...any) logr.Logger {
		return w.log.WithValues(append([]any{"component", name}, kv...)...)
	}
	for _, c := range []component{
		{name: "provider-supervisor-batch", log: named("provider-supervisor-batch"), start: batch.Start},
		{name: "provider-supervisor-managed", log: named("provider-supervisor-managed"), start: catalog.Start},
		{name: "agent-api", log: named("agent-api", "addr", w.host.AgentEndpointAddr()), start: srv.Start},
	} {
		if err := mgr.Add(c); err != nil {
			return err
		}
	}
	return nil
}
