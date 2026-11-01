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
//
// The two supervisors follow the lifecycle leader: they run only in the
// process that leads the Lease v1alpha1.LeaseLifecycle, or in the one process
// with --leader-elect=false (HBEP-11, Controllers: one manager per provider;
// `@hpc-burst-science-agent`, 1 Nov 2026, for #132). The agent API runs in
// every copy and answers CodeUnavailable where it does not lead.

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
	"slices"
	"time"

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
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
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
	// frpcDigests gives the frpc digest per cpuArch; nil means the pinned
	// ones, launcher.FrpcDigests of the release the launcher runs.
	frpcDigests func() (map[string]string, error)
	// username gives the ssh dial-home user; nil means the user who runs the
	// binary (user.Current).
	username func() (string, error)
}

func (d deps) digests() (map[string]string, error) {
	if d.frpcDigests != nil {
		return d.frpcDigests()
	}
	return launcher.FrpcDigests(launcher.DefaultFrpVersion)
}

func (d deps) user() (string, error) {
	if d.username != nil {
		return d.username()
	}
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return u.Username, nil
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
//
// The list kinds are checked too: the managers' typed informers list each
// kind they cache, so a scheme without, say, AllocationList would pass a check
// of Allocation alone and fail only after the start (review of PR 132: N1).
func checkScheme(s *runtime.Scheme) error {
	var missing []error
	for _, gvk := range []schema.GroupVersionKind{
		appsv1.SchemeGroupVersion.WithKind("ControllerRevision"),
		appsv1.SchemeGroupVersion.WithKind("ControllerRevisionList"),
		v1alpha1.GroupVersion.WithKind("Allocation"),
		v1alpha1.GroupVersion.WithKind("AllocationList"),
		v1alpha1.GroupVersion.WithKind("Provider"),
		v1alpha1.GroupVersion.WithKind("ProviderList"),
		v1alpha1.GroupVersion.WithKind("PlatformProfile"),
		v1alpha1.GroupVersion.WithKind("PlatformProfileList"),
	} {
		if !s.Recognizes(gvk) {
			missing = append(missing, fmt.Errorf("the Provider managers' scheme does not know %s (register %s)", gvk, gvk.GroupVersion()))
		}
	}
	return errors.Join(missing...)
}

// frontSettings are the batch Providers' front and agent-endpoint settings
// (providerctl.FrontSettings), from the front the launcher recorded; nil when
// it recorded none, and then nothing is submitted (HBEP-11, open question 8).
// The pin is the agent endpoint certificate's, which the launcher created; the
// frpc digest per cpuArch is the one internal/launcher/frp-pins.txt pins for
// the frp release the launcher runs (HBEP-4, Submission: Files first), for
// x86_64 and aarch64 (Helios GH200) alike; in ssh mode the user is the one who
// runs this binary, and the two forwarded ports are the Host's
// (agentapi.Host). The settings are checked as agent.json takes them
// (checkFront), so a value every submission would fail on stops the binary
// here, before anything listens.
func frontSettings(f flags, d deps) (*providerctl.FrontSettings, error) {
	if f.frontMode == "" {
		return nil, nil
	}
	pin, err := certPin(f.host.AgentCertFile())
	if err != nil {
		return nil, err
	}
	digests, err := d.digests()
	if err != nil {
		return nil, fmt.Errorf("the frpc digests: %w", err)
	}
	fs := &providerctl.FrontSettings{
		Mode: agentapi.DialHomeMode(f.frontMode), Endpoint: f.frontAddr, CertPin: pin,
		FrpcDigests: maps.Clone(digests),
	}
	if fs.Mode == agentapi.DialHomeSSH {
		u, err := d.user()
		if err != nil {
			return nil, fmt.Errorf("the ssh dial-home user: %w", err)
		}
		fs.SSHUser, fs.AgentPort, fs.DialHomePort = u, f.host.AgentEndpointPort(), f.host.DialHomePort()
	}
	if err := checkFront(fs); err != nil {
		return nil, fmt.Errorf("the front settings: %w", err)
	}
	return fs, nil
}

// sampleTag, sampleStaging and sampleDialHome stand in, in checkFront, for
// the values a Create fills from the Allocation and the platform.
var (
	sampleTag      = v1alpha1.JobTag("example", "example", "00000000-0000-0000-0000-000000000000")
	sampleStaging  = "/scratch/example/stage"
	sampleDialHome = filepath.Join("/home/example", agentapi.DialHomeDir)
)

// checkFront encodes, for each cpuArch, the agent.json a Create would write,
// through agentapi's own Encode and its validation, with the front's values
// and sample ones for the rest. Validation used to happen only at Encode,
// inside a Create and after the platform calls, so a front address without a
// usable port, a digest that is not 64 lowercase hex characters or a user
// name agentapi refuses let the binary start, report ready and fail every
// submission (review of PR 132: B3).
func checkFront(fs *providerctl.FrontSettings) error {
	if len(fs.FrpcDigests) == 0 {
		return errors.New("no frpc digest is configured for any cpuArch")
	}
	var errs []error
	for _, arch := range slices.Sorted(maps.Keys(fs.FrpcDigests)) {
		c := agentapi.AgentConfig{
			ProtocolVersion: agentapi.ProtocolVersion, Tag: sampleTag, Mode: fs.Mode, Endpoint: fs.Endpoint,
			CertSPKISHA256: fs.CertPin, FrpcDigest: fs.FrpcDigests[arch], StagingDir: sampleStaging, DialHomeDir: sampleDialHome,
		}
		if fs.Mode == agentapi.DialHomeSSH {
			c.SSH = &agentapi.SSHForward{User: fs.SSHUser, AgentPort: fs.AgentPort, DialHomePort: fs.DialHomePort}
		}
		if _, err := c.Encode(); err != nil {
			errs = append(errs, fmt.Errorf("agent.json for cpuArch %s: %w", arch, err))
		}
	}
	return errors.Join(errs...)
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

// providerGrace is each Provider manager's shutdown grace. The Provider
// managers run inside the lifecycle manager, whose grace is controller-runtime's
// default, 30 s (lifecycle.NewManager sets none), and hpcburst down sends
// SIGKILL 30 s after SIGTERM (launcher.DefaultGrace), so a longer outer grace
// would not help. At a stop the lifecycle manager first stops the agent API,
// then the lifecycle controller and the two supervisors together, and with the
// supervisors every Provider manager at once (providerctl.Supervisor), each for
// at most providerGrace; that leaves 10 s for the lifecycle controller, the
// caches, the health server and the Leases' release. With equal graces a
// Provider controller still busy at the stop (a submission's outcome is
// written for up to 30 s; providerctl's Create) used up the outer grace, and a
// normal SIGTERM became exit 1 (review of PR 132: N3).
//
// When this replica loses the lifecycle Lease instead, controller-runtime sets
// the lifecycle manager's grace to 0 (its OnStoppedLeading), so run returns at
// once with an error, without waiting for the Provider managers, and main
// exits 1. They get no graceful stop and their Leases are not released: each
// expires after its duration (controller-runtime's 15 s; providerctl's
// ManagerOptions), and then the new lifecycle leader's supervisors take it. No
// change is needed for that (review of PR 132: N3). That is the one hand-over
// in which two copies' Provider controllers can overlap, since a Lease does
// not fence (HBEP-11, Controllers). After a clean stop there is none: the old
// leader stops its Provider managers, and they release their Leases, before it
// releases the lifecycle Lease, and the new leader starts its supervisors only
// once it holds that.
const providerGrace = 20 * time.Second

func (w wiring) newProviderManager(provider string) (manager.Manager, error) {
	return providerctl.NewProviderManager(w.cfg, w.scheme, provider, providerctl.ManagerOptions{GracefulShutdownTimeout: providerGrace})
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
// running. A leader component starts only once this process leads the
// lifecycle Lease (or at once with --leader-elect=false): the two Provider
// supervisors, so that a copy that does not lead runs no Provider, with
// credentials and a front endpoint the leader never wrote; each Provider's
// manager still takes its own Lease, provider-<name> (HBEP-11, Controllers;
// `@hpc-burst-science-agent`, 1 Nov 2026, for #132; review of PR 132: B5).
// The agent API is not a leader component: it answers through the Backend,
// which says CodeUnavailable while this process does not lead.
type component struct {
	name   string
	log    logr.Logger
	leader bool
	start  func(context.Context) error
}

var _ manager.LeaderElectionRunnable = component{}

func (c component) NeedLeaderElection() bool { return c.leader }

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
		{name: "provider-supervisor-batch", log: named("provider-supervisor-batch"), leader: true, start: batch.Start},
		{name: "provider-supervisor-managed", log: named("provider-supervisor-managed"), leader: true, start: catalog.Start},
		{name: "agent-api", log: named("agent-api", "addr", w.host.AgentEndpointAddr()), start: srv.Start},
	} {
		if err := mgr.Add(c); err != nil {
			return err
		}
	}
	return nil
}
