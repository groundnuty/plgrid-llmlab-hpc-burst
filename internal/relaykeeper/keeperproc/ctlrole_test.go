package keeperproc

// The REAL controller role of Stage B (#343's PR 4: the relay keeper's
// controller side): this test binary re-run as one Provider's controller,
// running exactly what cmd/hpcburst-controller runs per batch Provider in
// keeper mode (components.go's newProviderManager and setupBatch):
// providerctl.NewProviderManager, its own provider-<name> Lease with the
// relays' fence, then providerctl.Setup in host-dialed keeper mode, with
// crossplane-runtime's API recorder over the manager's, until SIGTERM or the
// end of its standard input. Only these differ from the binary, each a
// setting the binary takes from its flags, files or the API server:
//   - short Lease timings, so hand-overs are quick (ManagerOptions), and the
//     matching renew deadline, poll and send intervals;
//   - a static ProfileSource: the world's login node's ssh credentials, and a
//     profile whose stagingDir is the world's staging directory;
//   - the Platform is providerctl.SSHPlatform, the binary's production
//     factory, a slurm.Batch over an ssh transport to the world's login node,
//     with a control directory of its own;
//   - the keeper's socket is the test's tap, and the front is host-dialed
//     with the test's example certificate and frpc digests.
//
// It logs to standard error, which the test keeps as it keeps PR 3's
// children's. The role is dispatched by an init in this file, before
// TestMain, so roles_test.go (PR 3's) is unchanged.
//
// Provenance: new, for #343's PR 4 (Stage B); the wiring follows
// cmd/hpcburst-controller/components.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// realControllerRole is the real controller role's first argument.
const realControllerRole = "keeperproc-realctl"

func init() {
	if len(os.Args) > 1 && os.Args[1] == realControllerRole {
		os.Exit(realControllerMain(os.Args[2:]))
	}
}

// ctlTimings are a real controller's timings, in milliseconds.
type ctlTimings struct {
	LeaseDurationMS, RenewDeadlineMS, RetryPeriodMS int64
	PollMS, BusyMS, SendMS, GraceMS                 int64
}

// The timings of Stage B's controllers. The renew deadline is above the
// 5 s that every set's age carries for its exchange (keeper.go,
// exchangeSet), so a set the keeper gets is unexpired for several seconds
// after its sender dies (validity: the 2 s poll plus the renew deadline); the
// Lease outlasts the renew deadline, so a killed leader's successor takes
// over about 16 s after the kill.
var stageBTimings = ctlTimings{LeaseDurationMS: 16_000, RenewDeadlineMS: 14_000, RetryPeriodMS: 1_000,
	PollMS: 2_000, BusyMS: 1_000, SendMS: 1_000, GraceMS: 20_000}

// realControllerConfig is the role's configuration, in a file the test
// writes.
type realControllerConfig struct {
	Kubeconfig string
	Provider   string
	// Socket is the relay keeper's socket (--relay-keeper-socket).
	Socket          string
	SSH             slurm.SSHConfig
	CredentialsFile string
	StagingDir      string
	Partition       v1alpha1.PartitionProfile
	ControlDir      string
	CredentialsDir  string
	AgentBuildsDir  string
	Control         int
	Serving         int
	CertPEM         []byte
	CertPin         string
	FrpcDigests     map[string]string
	Timings         ctlTimings
}

// staticProfile is a ProfileSource that reads one validated profile.
type staticProfile struct{ v slurm.Validated }

func (s staticProfile) Read(context.Context, string) (slurm.Validated, error) { return s.v, nil }

// realControllerMain is "<realControllerRole> <config file>".
func realControllerMain(args []string) int {
	if len(args) != 1 {
		return 2
	}
	b, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		return 2
	}
	var c realControllerConfig
	if err := json.Unmarshal(b, &c); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		return 2
	}
	lr := logr.FromSlogHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctrl.SetLogger(lr)
	kc, err := os.ReadFile(c.Kubeconfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		return 2
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		return 2
	}
	scheme, err := providerScheme()
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		return 2
	}
	ms := func(n int64) time.Duration { return time.Duration(n) * time.Millisecond }
	tm := c.Timings
	mgr, err := providerctl.NewProviderManager(cfg, scheme, c.Provider, providerctl.ManagerOptions{
		LeaseDuration: ms(tm.LeaseDurationMS), RenewDeadline: ms(tm.RenewDeadlineMS), RetryPeriod: ms(tm.RetryPeriodMS),
		GracefulShutdownTimeout: ms(tm.GraceMS),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller: the manager:", err)
		return 1
	}
	v := slurm.Validated{
		Profile: provider.Profile{Name: "example-profile", Spec: v1alpha1.PlatformProfileSpec{
			ProviderKind: v1alpha1.PlatformBatch, JSONDataParser: "v0.0.42", StagingDir: c.StagingDir,
			Partitions: []v1alpha1.PartitionProfile{c.Partition},
		}, Partition: c.Partition},
		Settings: provider.Settings{Provider: c.Provider, Account: "example-account", Partition: c.Partition.Name, CredentialsFile: c.CredentialsFile},
		SSH:      c.SSH,
	}
	err = providerctl.Setup(mgr, providerctl.Options{
		Provider:       c.Provider,
		Profiles:       staticProfile{v},
		NewPlatform:    providerctl.SSHPlatform(providerctl.SSHPlatformOptions{ControlDir: c.ControlDir, PollInterval: ms(tm.PollMS), BusyInterval: ms(tm.BusyMS)}),
		CredentialsDir: c.CredentialsDir,
		AgentBuildsDir: c.AgentBuildsDir,
		Front:          &providerctl.FrontSettings{Mode: agentapi.DialHomeHostDialed, CertPEM: c.CertPEM, CertPin: c.CertPin, FrpcDigests: c.FrpcDigests},
		Relays: &providerctl.RelaySettings{
			Ports:        map[agentapi.RelayChannel]int{agentapi.RelayControl: c.Control, agentapi.RelayServing: c.Serving},
			KeeperSocket: c.Socket, KeeperSendInterval: ms(tm.SendMS), RenewDeadline: ms(tm.RenewDeadlineMS),
		},
		PollInterval: ms(tm.PollMS),
		BusyInterval: ms(tm.BusyMS),
		Log:          logging.NewLogrLogger(lr.WithName("provider").WithValues("provider", c.Provider)),
		// As setupBatch: crossplane-runtime's recorder over the manager's,
		// so the Allocation events reach the API (eventcount's wrapper,
		// metrics only, is left out).
		Recorder: event.NewAPIRecorder(mgr.GetEventRecorderFor(v1alpha1.FieldManagerProvider)),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller: setup:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		// The end of standard input stops it too: the test's clean-up, or
		// the test process gone.
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	if err := mgr.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "controller: the manager stopped:", err)
		return 1
	}
	fmt.Fprintln(os.Stderr, "controller: stopped cleanly")
	return 0
}

// realCtl is a real controller process.
type realCtl struct {
	t    *testing.T
	name string
	c    *child
	log  *lineLog
}

// pid is the controller's process ID, for comparing with the tap's
// SO_PEERCRED; it is never signalled by number.
func (r *realCtl) pid() int { return r.c.pid }

// startController starts a real controller of Provider provider whose keeper
// socket is socket.
func (g *rig) startController(name, provider, socket string) *realCtl {
	t := g.t
	t.Helper()
	ctlDir, err := os.MkdirTemp("", "kc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	c := realControllerConfig{
		Kubeconfig: g.kubeconfig, Provider: provider, Socket: socket,
		SSH: g.w.creds, CredentialsFile: filepath.Join(g.w.base, "creds"), StagingDir: g.w.staging, Partition: g.partition,
		ControlDir: ctlDir, CredentialsDir: g.credsDir, AgentBuildsDir: g.buildsDir,
		Control: g.w.targets[control].port(), Serving: g.w.targets[serving].port(),
		CertPEM: g.certPEM, CertPin: g.certPin, FrpcDigests: g.frpcDigests, Timings: stageBTimings,
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(ctlDir, "controller.json")
	if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rc := &realCtl{t: t, name: name, log: &lineLog{}}
	t.Cleanup(func() {
		if t.Failed() || os.Getenv("HB_TEST_KEEPER_LOG") == "1" {
			t.Logf("controller %s's log:\n%s", name, tail(rc.log.all(), 200))
		}
	})
	cmd := exec.Command(exe, realControllerRole, cfgPath)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, rc.log, rc.log
	rc.c = startChild(t, "controller "+name, cmd, inW)
	_ = inR.Close()
	return rc
}

// The controller logs the tests wait on: client-go's elector's (with
// controller-runtime's logger or klog's) and controller-runtime's manager's.
const (
	logAcquiring     = "Attempting to acquire leader lease"
	logStopLeaderRun = "Stopping and waiting for leader election runnables"
	logStopCaches    = "Stopping and waiting for caches"
)

// waitLog waits until the controller has logged a line holding s.
func (r *realCtl) waitLog(s string) {
	r.t.Helper()
	r.log.waitLine(r.t, 0, "controller "+r.name+" logged "+strconv.Quote(s), s)
}

// logged reports whether the controller has logged a line holding s.
func (r *realCtl) logged(s string) bool { return len(r.log.since(0, s)) > 0 }

// term sends SIGTERM through the controller's pidfd and waits for its exit,
// which must be clean (exit 0).
func (r *realCtl) term() {
	r.t.Helper()
	if err := r.c.signal(syscall.SIGTERM); err != nil {
		r.t.Fatalf("TERM controller %s through its pidfd: %v", r.name, err)
	}
	r.waitClean()
}

// waitClean waits for the controller's exit, which must be clean (exit 0).
func (r *realCtl) waitClean() {
	r.t.Helper()
	if !r.c.wait(time.Minute) {
		r.t.Fatalf("controller %s did not exit within a minute of its TERM", r.name)
	}
	if code := r.c.cmd.ProcessState.ExitCode(); code != 0 {
		r.t.Fatalf("controller %s exited %d after its TERM, want 0", r.name, code)
	}
}

// kill sends SIGKILL through the controller's pidfd and waits for its exit.
func (r *realCtl) kill() { r.c.kill(r.t) }

// identity is the Lease's holder while this controller, and only it, holds
// it: the test reads it from the API server once the controller's first set
// went through the tap, so no other copy's could be there.
func (g *rig) identityOf(r *realCtl, provider string) string {
	g.t.Helper()
	l := g.api.lease(g.t, provider)
	if !l.found || l.holder == "" {
		g.t.Fatalf("Provider %s's Lease has no holder while controller %s leads", provider, r.name)
	}
	return l.holder
}

// noLine checks that the controller's log has no line holding every one of
// parts.
func (r *realCtl) noLine(what string, parts ...string) {
	r.t.Helper()
	if got := r.log.since(0, parts...); len(got) != 0 {
		r.t.Errorf("controller %s logged %s: %q", r.name, what, got)
	}
}
