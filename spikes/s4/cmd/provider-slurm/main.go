// Command provider-slurm runs the spike's Allocation controller as a plain
// process against any Kubernetes API server given by --kubeconfig (in the
// tests, an envtest kube-apiserver with no nodes).
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	controller "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/controller"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/slurm"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "provider-slurm:", err)
		os.Exit(1)
	}
}

type flags struct {
	kubeconfig      string
	mode            string
	deterministic   bool
	guardSucceeded  bool
	guardInflight   bool
	holdRelease     bool
	inflightWindow  time.Duration
	unknownBound    time.Duration
	lostGrace       time.Duration
	unresolvedPoll  time.Duration
	poll            time.Duration
	timeout         time.Duration
	grace           time.Duration
	maxBackoff      time.Duration
	leaderElect     bool
	leaseDuration   time.Duration
	renewDeadline   time.Duration
	retryPeriod     time.Duration
	identity        string
	namespace       string
}

func parseFlags() (flags, error) {
	var f flags
	// Own flag set: controller-runtime registers --kubeconfig on the global one.
	fl := flag.NewFlagSet("provider-slurm", flag.ContinueOnError)
	fl.StringVar(&f.kubeconfig, "kubeconfig", "", "kubeconfig of the API server (required; no in-cluster or default fallback)")
	fl.StringVar(&f.mode, "observe", controller.ModeTag, "observe mode: tag (design), squeue-only or jobid (controls)")
	fl.BoolVar(&f.deterministic, "deterministic-external-name", true, "crossplane-runtime WithDeterministicExternalName")
	fl.BoolVar(&f.guardSucceeded, "guard-succeeded", true, "never report an established job that is not found as missing (Lost after --lost-grace); off only in controls")
	fl.BoolVar(&f.guardInflight, "guard-inflight", true, "an unresolved submission attempt blocks a new one; off only in controls")
	fl.BoolVar(&f.holdRelease, "hold-release", true, "platform profile hold_release: submit with --hold and release only the established job; false = fallback, where an unknown outcome needs positive resolution")
	fl.DurationVar(&f.inflightWindow, "inflight-window", 2*time.Minute, "how long an attempt may stay unresolved before hold mode retries it (fallback: before InFlight becomes Unknown); must exceed --timeout")
	fl.DurationVar(&f.unknownBound, "unknown-bound", 10*time.Minute, "fallback mode: raise the operator condition and event after an attempt stays unresolved this long")
	fl.DurationVar(&f.lostGrace, "lost-grace", 2*time.Minute, "platform profile lost_grace: report an established job Lost only after it is absent from squeue and sacct this long")
	fl.DurationVar(&f.unresolvedPoll, "unresolved-poll", 5*time.Second, "poll interval while a submission is unresolved or the Allocation is being deleted")
	fl.DurationVar(&f.poll, "poll", time.Minute, "poll interval")
	fl.DurationVar(&f.timeout, "timeout", time.Minute, "reconcile timeout for external calls")
	fl.DurationVar(&f.grace, "creation-grace", 30*time.Second, "crossplane-runtime creation grace period")
	fl.DurationVar(&f.maxBackoff, "max-backoff", time.Minute, "maximum requeue backoff")
	fl.BoolVar(&f.leaderElect, "leader-elect", false, "elect one acting controller through a coordination.k8s.io Lease")
	fl.DurationVar(&f.leaseDuration, "lease-duration", 15*time.Second, "Lease duration")
	fl.DurationVar(&f.renewDeadline, "renew-deadline", 10*time.Second, "Lease renew deadline")
	fl.DurationVar(&f.retryPeriod, "retry-period", 2*time.Second, "Lease retry period")
	fl.StringVar(&f.identity, "identity", "", "reconciler identity for logs (defaults to hostname_pid)")
	fl.StringVar(&f.namespace, "namespace", "", "watch only this namespace (required; also holds the Lease)")
	if err := fl.Parse(os.Args[1:]); err != nil {
		return f, err
	}
	if f.namespace == "" {
		return f, errors.New("--namespace is required")
	}
	if f.kubeconfig == "" {
		return f, errors.New("--kubeconfig is required (refusing to fall back to a default cluster)")
	}
	switch f.mode {
	case controller.ModeTag, controller.ModeSqueueOnly, controller.ModeJobID:
	default:
		return f, fmt.Errorf("unknown --observe %q", f.mode)
	}
	if f.guardInflight && f.inflightWindow <= f.timeout {
		return f, fmt.Errorf("--inflight-window (%s) must exceed --timeout (%s)", f.inflightWindow, f.timeout)
	}
	if f.identity == "" {
		h, _ := os.Hostname()
		f.identity = fmt.Sprintf("%s_%d", h, os.Getpid())
	}
	return f, nil
}

func run() error {
	f, err := parseFlags()
	if err != nil {
		return err
	}
	zl := zap.New(zap.UseDevMode(true))
	ctrl.SetLogger(zl)
	log := logging.NewLogrLogger(zl.WithValues("identity", f.identity))

	cfg, err := clientcmd.BuildConfigFromFlags("", f.kubeconfig)
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	opts := manager.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"}, // default ":8080" binds every interface
		HealthProbeBindAddress:  "0",
		PprofBindAddress:        "0",
		LeaderElection:          f.leaderElect,
		LeaderElectionID:        "provider-slurm-spike",
		LeaderElectionNamespace: f.namespace,
		Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{f.namespace: {}}},
		LeaseDuration:           &f.leaseDuration,
		RenewDeadline:           &f.renewDeadline,
		RetryPeriod:             &f.retryPeriod,
	}
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	recorder := event.NewAPIRecorder(mgr.GetEventRecorderFor("provider-slurm"))
	copts := controller.Options{
		Mode:              f.mode,
		Identity:          f.identity,
		HoldRelease:       f.holdRelease,
		GuardSucceeded:    f.guardSucceeded,
		GuardInflight:     f.guardInflight,
		InflightWindow:    f.inflightWindow,
		UnknownBound:      f.unknownBound,
		LostGrace:         f.lostGrace,
		UnresolvedPoll:    f.unresolvedPoll,
		SacctMargin:       time.Hour,
		FaultBeforeSbatch: os.Getenv("S4_FAULT_BEFORE_SBATCH"),
		FaultAfterSbatch:  os.Getenv("S4_FAULT_AFTER_SBATCH"),
		Log:               log,
	}
	conn := &controller.Connector{
		Slurm:    &slurm.Client{R: slurm.ExecRunner{}},
		Kube:     mgr.GetClient(),
		Reader:   mgr.GetAPIReader(),
		Recorder: recorder,
		Opts:     copts,
	}
	ropts := []managed.ReconcilerOption{
		managed.WithExternalConnector(conn),
		managed.WithLogger(log),
		managed.WithRecorder(recorder),
		managed.WithCriticalAnnotationUpdater(controller.CriticalAnnotationUpdater{Kube: mgr.GetClient(), Reader: mgr.GetAPIReader()}),
		managed.WithPollIntervalHook(controller.PollIntervalHook(copts)),
		managed.WithPollInterval(f.poll),
		managed.WithTimeout(f.timeout),
		managed.WithCreationGracePeriod(f.grace),
		managed.WithDeterministicExternalName(f.deterministic),
		managed.WithManagementPolicies(),
		// The Allocation's policy set excludes Update: a submitted job is never edited.
		managed.WithReconcilerSupportedManagementPolicies([]sets.Set[xpv2.ManagementAction]{
			sets.New(xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize),
			sets.New(xpv2.ManagementActionObserve),
		}),
	}
	if f.mode != controller.ModeJobID {
		ropts = append(ropts, managed.WithInitializers(controller.TagInitializer{Client: mgr.GetClient()}))
	} else {
		ropts = append(ropts, managed.WithInitializers()) // classic: no external name until Create
	}
	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.AllocationGroupVersionKind), ropts...)

	err = ctrl.NewControllerManagedBy(mgr).
		Named("allocation").
		// Status-only writes do not re-trigger a reconcile, so a waiting
		// Observe cannot spin on its own status updates.
		For(&v1alpha1.Allocation{}, builder.WithPredicates(resource.DesiredStateChanged())).
		WithOptions(crcontroller.Options{
			MaxConcurrentReconciles: 2,
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](50*time.Millisecond, f.maxBackoff),
		}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	log.Info("starting", "observe", f.mode, "deterministic", f.deterministic, "holdRelease", f.holdRelease,
		"guardSucceeded", f.guardSucceeded, "guardInflight", f.guardInflight, "leaderElect", f.leaderElect)
	return mgr.Start(ctrl.SetupSignalHandler())
}

