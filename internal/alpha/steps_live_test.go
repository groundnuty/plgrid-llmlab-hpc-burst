package alpha

// The live steps (issue #245). They start the whole stack on this host with
// hpcburst up (k3s, the controller, frps and the front) through
// internal/rehearsal/stack, the one-host rehearsal's harness, create the row's
// Provider and its ModelDeployment, drive the front, and return what the API,
// the events, the front's usage records and frps's proxy list showed. A world
// says where the cluster is: the real one behind the owner's dial-home route
// (liveWorld, below), or the stand-in on this host, the fake Slurm in run mode
// behind the rehearsal's sshd (standin_test.go). The steps are the same code
// for both (live_flows_test.go); the dry run's stand-in steps (dry_test.go)
// are not used here.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
)

// What the live steps cannot wire from here; each names what is missing.
var (
	ErrNoDialHomeListen = errors.New("direct dial-home mode needs the front's local dial-home listener, the host:port where the owner's route arrives on this host: set LIVE_DIALHOME_LISTEN")
	ErrNoDeployment     = errors.New("the row's served deployment (examples/modeldeployment-qwen25-instruct.yaml) could not be read")
	ErrRequestTooLong   = errors.New("a request of the length the hour needs does not fit the deployment's max-model-len at the engine's measured rate")
)

// world is where the live steps run.
type world struct {
	name  string // "live" or "stand-in", for the log
	bins  stack.Binaries
	place func(h *stack.Host) // puts the agent and frpc builds where the controller reads them
	dh    stack.DialHome
	ports stack.Ports
	env   []string
	// frontTuning is extra members of the front's configuration (stack.StartHost).
	frontTuning string
	// objects are the PlatformProfile and the Provider.
	objects []client.Object
	// provider is the Provider's name, cluster the platform it is on, and qos
	// the Provider's spec.qos ("" for none).
	provider, cluster, qos string
	// md is the served deployment before the row's plan is applied.
	md *v1alpha1.ModelDeployment
	// profile is the platform profile's spec, for the drain window.
	profile v1alpha1.PlatformProfileSpec
	// controlDir and hostLock are the run's own ssh transport's.
	controlDir, hostLock string
	// matchEngine tells the engine among the job's processes (PickEngine):
	// MatchesCommand on a cluster, the stand-in's own for its fake Apptainer.
	matchEngine func(argv, command []string) bool
	// gpuModel is the gpu_model label of the Instance's info series, the row's
	// partition's (HBEP-8, Engine gauges per Instance), and notReported the
	// engine gauges the world's engine does not export (obscheck.GaugeView): a
	// real vLLM's none (obscheck.VLLMNotReported, HBEP-8:419), the stand-in's
	// engine double's two prefix-cache counters (obscheck.DoubleNotReported).
	gpuModel    string
	notReported map[string]bool
}

// livePorts are the live stack's ports on this host: the launcher's block
// 29700-29712, the client listener 29713, and the dial-home listener the
// settings name.
func livePorts(listen string) stack.Ports {
	return stack.Ports{Base: 29700, ClientAddr: "127.0.0.1:29713", DialHomeListen: listen}
}

// liveSteps are the live run's steps over the real cluster of the row.
func liveSteps(t *testing.T, cfg Config) Steps {
	t.Helper()
	w, err := liveWorld(cfg)
	if err != nil {
		return failedSteps{err}
	}
	return newLiveRun(t, cfg, w)
}

// liveWorld is the real cluster: the row's shipped profile, a Provider with
// the run's account and credentials, the builds hack/live-checks.sh made, and
// the owner's dial-home route.
func liveWorld(cfg Config) (world, error) {
	if cfg.DialHomeMode == "direct" && cfg.DialHomeListen == "" {
		return world{}, ErrNoDialHomeListen
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return world{}, err
	}
	var pp v1alpha1.PlatformProfile
	b, err := os.ReadFile(filepath.Join(repo, "profiles", cfg.Profile+".yaml"))
	if err == nil {
		err = yaml.UnmarshalStrict(b, &pp)
	}
	if err != nil {
		return world{}, fmt.Errorf("the row's platform profile %s: %w", cfg.Profile, err)
	}
	var md v1alpha1.ModelDeployment
	b, err = os.ReadFile(filepath.Join(repo, "examples", "modeldeployment-qwen25-instruct.yaml"))
	if err == nil {
		err = yaml.UnmarshalStrict(b, &md)
	}
	if err != nil {
		return world{}, fmt.Errorf("%w: %v", ErrNoDeployment, err)
	}
	md.Name, md.Namespace = "alpha", "default"
	// A request near the 5-minute deadline needs room for its tokens.
	for i, f := range md.Spec.Source.Served.Flags {
		if f.Name == "max-model-len" {
			md.Spec.Source.Served.Flags[i].Value = "32768"
		}
	}
	r := cfg.Row
	gpuModel := ""
	for _, part := range pp.Spec.Partitions {
		if part.Name == r.Partition {
			gpuModel = part.GPUModel
		}
	}
	prov := &v1alpha1.Provider{Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: pp.Name}, Account: cfg.Account, AllowedPartitions: []string{r.Partition},
		QOS: r.QOS, CredentialsRef: &v1alpha1.CredentialsReference{File: cfg.Credentials},
	}}
	prov.Name = r.Cluster
	builds := filepath.Join(cfg.BuildDir, "agent-builds")
	return world{
		name: "live",
		bins: stack.Binaries{HPCBurst: filepath.Join(cfg.BuildDir, "bin", "hpcburst"), Controller: filepath.Join(cfg.BuildDir, "bin", "hpcburst-controller"),
			Front:  filepath.Join(cfg.BuildDir, "bin", "hpcburst-front"),
			K3sDir: cfg.BinDir, Frps: filepath.Join(cfg.BinDir, "frps"), Frpc: filepath.Join(cfg.BinDir, "frpc"), Repo: repo},
		place: func(h *stack.Host) { copyBuilds(h, builds) },
		dh:    stack.DialHome{Mode: cfg.DialHomeMode, Addr: cfg.DialHomeAddr, KeyPath: cfg.DialHomeKey},
		ports: livePorts(cfg.DialHomeListen), env: stack.CleanEnv(),
		objects: []client.Object{&pp, prov}, provider: r.Cluster, cluster: r.Cluster, qos: r.QOS, md: &md, profile: pp.Spec,
		controlDir: cfg.ControlDir, hostLock: cfg.HostLock, matchEngine: MatchesCommand, gpuModel: gpuModel, notReported: obscheck.VLLMNotReported,
	}, nil
}

// copyBuilds puts every agent and frpc build hack/live-checks.sh made where the
// controller reads them (HBEP-4, <state>/agent-builds).
func copyBuilds(h *stack.Host, dir string) {
	h.T.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		h.T.Fatalf("the agent builds: %v", err)
	}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			h.T.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(h.State, "agent-builds", e.Name()), b, 0o700); err != nil {
			h.T.Fatal(err)
		}
	}
}

// failedSteps are the steps of a run that cannot start: each returns why.
type failedSteps struct{ err error }

func (f failedSteps) Handover(context.Context, Plan) (HandoverObs, error) {
	return HandoverObs{}, f.err
}
func (f failedSteps) NoOverlap(context.Context, Plan) (NoOverlapObs, error) {
	return NoOverlapObs{}, f.err
}
func (f failedSteps) TunnelHour(context.Context, Plan) (TunnelHourObs, error) {
	return TunnelHourObs{}, f.err
}
func (f failedSteps) NowServe(context.Context, Plan) (NowServeObs, error) {
	return NowServeObs{}, f.err
}
func (f failedSteps) NowNoOverlap(context.Context, Plan) (NowNoOverlapObs, error) {
	return NowNoOverlapObs{}, f.err
}
func (f failedSteps) Ledger() Ledger              { return &BatchLedger{} }
func (f failedSteps) Close(context.Context) error { return nil }
func (f failedSteps) WatchResets() int            { return 0 }

// liveRun is one run of the live steps in a world.
type liveRun struct {
	t   *testing.T
	cfg Config
	w   world
	tm  Timing

	// prefix begins every job tag of the run (TagPrefix): Close and the
	// guard find the run's jobs by it, whatever the recorder holds.
	prefix string
	// dw is the drain window as the controller and the front work it out
	// (v1alpha1.InstanceDrainWindow), and limit the jobs' time limit as the
	// controller sets it (v1alpha1.AllocationTimeLimit): the one source of
	// both for every end, drain start and wait of the run.
	dw, limit time.Duration

	since time.Time
	tags  *Recorder
	h     *stack.Host
	c     client.Client
	watch *stack.Recorder
	fc    *stack.Client
	batch *slurm.Batch
	tr    *sshrun.SSH

	// keepEvidence is set by Run when the run ended in a failure that is not a Fatalf of the steps (a failed check, a broken run): Close then
	// keeps the evidence of a failed run (evidence_live_test.go). A Fatalf marks the test failed itself.
	keepEvidence bool

	// created is this host's time just before the deployment was created,
	// from which the first clock runs, and clusterAtCreate the cluster's clock
	// read before it, from which its first end is placed.
	created, clusterAtCreate time.Time
	// oneEnd is a row of one job's deployment end, its Instance's drain start (oneJob).
	oneEnd time.Time

	// The two-job guard (TwoJobGuard), run from its own goroutine every
	// guardEvery (default GuardEvery). lookFn, setEndFn and fatalf are
	// look, endAt and t.Fatalf unless a test sets them.
	guard      *TwoJobGuard
	guardEvery time.Duration
	// resultWait bounds guardResult's wait for the successor's drain start
	// (default six looks).
	resultWait time.Duration
	lookFn     func(context.Context) (Look, error)
	setEndFn   func(time.Time) (time.Time, error)
	fatalf     func(format string, args ...any)
	// claimed says the run has read a job as one that never started, which Close checks through the cancel; claimedPred is the first
	// job of a run that read its second so, which has started and may.
	claimed     bool
	claimedPred string
	// slotTakenFn, when set, runs once row B's read of the slot has seen a job that is not the run's in it, before the run
	// ends its deployment: a stand-in orders the fake's held submission after it.
	slotTakenFn func()
	// endEvery is how often Close looks for the run's jobs to have ended; 0 is five seconds. A test sets it.
	endEvery time.Duration
	// wrapAPI, when a test sets it, wraps the run's API client as start makes it: a stand-in whose catch-up fails on demand (#303).
	wrapAPI func(client.Client) client.Client
	// stop, when a test sets it, is filled in by Close with the stack's own stop, apart from the run's error (PR #317's review): the
	// stand-ins whose scenario is meant to fail check it first. Only they set it, so its read of the stack before down and its look at
	// what is left after it (standin_stop_test.go) are no part of a live row's Close. That look ends in time for the cancel of the jobs
	// and the clean-ups after it (stopBy, stopReserve), or does not happen, which the stand-ins fail.
	stop *stackStop
	// agentLogFn, when a test sets it, is the command line that reads row A's agent log at the path, in place of
	// agentLogLine: the stand-in's fake login shell has tail -c and no grep (internal/slurm/fakeslurm, builtins.go).
	agentLogFn func(path string) string
	// allocsFn is the watch's Allocation versions, and boundedFn stack.BoundedDeadline, unless a test sets them.
	allocsFn   func() []stack.AllocObservation
	boundedFn  func(time.Duration) (time.Time, bool)
	guardQuit  chan struct{}
	guardDone  chan struct{}
	guardFail  chan struct{}
	guardOnce  sync.Once
	guardStop  sync.Once
	guardErr   error
	guardFinal bool
	guardEnd   time.Time

	mu      sync.Mutex
	tagErrs []error
	replies []http.Header
}

// newLiveRun is a run in the world w. The run's Provider and deployment are
// named after its run ID (RunName), so that every job of the run has the
// run's tag prefix (TagPrefix).
func newLiveRun(t *testing.T, cfg Config, w world) *liveRun {
	name := RunName(cfg.RunID)
	md := w.md.DeepCopy()
	md.Name = name
	objs := make([]client.Object, 0, len(w.objects))
	for _, o := range w.objects {
		if p, ok := o.(*v1alpha1.Provider); ok {
			p = p.DeepCopy()
			p.Name = name
			o = p
		}
		objs = append(objs, o)
	}
	w.md, w.objects, w.provider = md, objs, name
	return &liveRun{t: t, cfg: cfg, w: w, tm: cfg.Row.Clock(), prefix: TagPrefix(cfg.RunID), guardFail: make(chan struct{})}
}

// Close stops the guard and the stack, then ends the run's jobs, found by the
// run's tag prefix and its written tags in the queue, and checks none is left
// alive, then stops the run's ssh master. It runs on every exit of Run, a
// failed step, a first job that never started and a run past its deadline
// included, so it takes a time of its own rather than the run's, which may be
// over.
func (r *liveRun) Close(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CloseTimeout)
	defer cancel()
	r.stopGuard()
	var errs []error
	// A failed run keeps its evidence (issue #503): the agents' logs while the API and the jobs still answer, the stack's own once it
	// has stopped and written them out.
	keep := r.t.Failed() || r.keepEvidence
	if keep {
		r.keepAgentLogs(cctx)
	}
	if r.h != nil {
		r.stop.before(r.h)
		// First, so that no controller submits a job after the cancel.
		derr := r.h.DownErr()
		errs = append(errs, derr)
		r.stop.after(r.t, r.h, derr)
		if keep {
			r.keepStackLogs()
		}
	}
	if r.batch != nil {
		every := r.endEvery
		if every == 0 {
			every = 5 * time.Second
		}
		if err := EndJobs(cctx, r.batch, RunOwned(r.prefix, r.tags), every); err != nil {
			errs = append(errs, fmt.Errorf("the run's jobs: %w", err))
		} else {
			if r.cfg.Row.Name != "" {
				r.t.Logf("%s: the run's jobs are ended: none of its prefix or its tags is alive in the queue", r.cfg.Row.Name)
			}
			if r.claimed {
				errs = append(errs, r.noStartThroughTheCancel(cctx))
			}
		}
	}
	if r.tr != nil {
		errs = append(errs, r.tr.Close())
	}
	return errors.Join(errs...)
}

// noStartThroughTheCancel is the last word on a job the run read as one that never started (the delta review of PR 284, L3). A job can
// start after that read: the stack takes up to stack.DownTimeout to stop before the cancel comes. So once every job of the run has
// ended, it reads their history from the cluster, and a job that started, past the predecessor a two-job run already knows of, is a
// failure naming that, never NOT RUN. A read that fails tells nothing, and is a failure too.
func (r *liveRun) noStartThroughTheCancel(ctx context.Context) error {
	listed, err := r.batch.Queue(ctx)
	if err != nil {
		return fmt.Errorf("the run read a job as one that never started, and the queue could not be read after its cancel: %w", err)
	}
	owned := RunOwned(r.prefix, r.tags)
	names := map[string]bool{}
	for _, tag := range r.tags.Tags() {
		names[tag] = true
	}
	for _, j := range listed {
		if owned(j.Name) {
			names[j.Name] = true
		}
	}
	tags := make([]string, 0, len(names))
	for tag := range names {
		tags = append(tags, tag)
	}
	jobs, err := r.Ledger().Jobs(ctx, tags)
	if err != nil {
		return fmt.Errorf("the run read a job as one that never started, and its history could not be read after its cancel: %w", err)
	}
	for _, j := range jobs {
		if j.ID != r.claimedPred && jobStarted(j) {
			return errors.New("the run read a job as one that never started, and it had started by its cancel: this is no NOT RUN")
		}
	}
	return nil
}

// Ledger reads the cluster's record over the run's own ssh transport.
func (r *liveRun) Ledger() Ledger {
	if r.batch == nil {
		return &BatchLedger{}
	}
	return &BatchLedger{Finder: r.batch, Since: r.since.Add(-AccountingSlack)}
}

// WatchResets counts the resets of the run's watch.
func (r *liveRun) WatchResets() int { return r.watch.ResetCount() }

// start brings the stack up, creates the Provider, makes the run's own
// transport, and creates the row's deployment with the end that keeps it to
// its job count, every Allocation's tag written down as the watch first sees
// it. A row of two jobs starts its guard.
func (r *liveRun) start(p Plan) {
	t := r.t
	t.Helper()
	r.since, r.tags = time.Now(), p.Rec
	p.Logf("%s: starting the stack (%s world)", p.Row.Name, r.w.name)
	r.dw, r.limit = r.controllerWindow(), r.timeLimit()
	r.h = stack.StartHost(t, r.w.bins, stack.ShortDir(t), r.w.dh, r.w.env, r.w.ports, r.w.frontTuning)
	r.w.place(r.h)
	r.c = stack.KubeClient(t, r.h.State)
	if r.wrapAPI != nil {
		r.c = r.wrapAPI(r.c)
	}
	stack.Create(t, r.c, r.w.objects...)
	// Before the deployment exists, so the watch sees every Allocation from its first version.
	r.watch = stack.StartWatch(t, r.h.State, r.w.md.Namespace, r.tag)
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	b, tr, err := SSHBatch(ctx, r.c, r.w.provider, r.w.controlDir, r.w.hostLock)
	if err != nil {
		t.Fatalf("the run's batch client: %v", err)
	}
	r.batch, r.tr = b, tr
	var prov v1alpha1.Provider
	r.waitFor(stack.WaitDeadline, "the Provider to be Ready", func() bool {
		ctx, cancel := stack.CallCtx(t)
		defer cancel()
		return r.c.Get(ctx, client.ObjectKey{Name: r.w.provider}, &prov) == nil && v1alpha1.ProviderReady(&prov, time.Now())
	})
	md := r.deployment(p.Row)
	p.Logf("%s: the deployment's first end is %s after the cluster's clock read before its creation", p.Row.Name, md.Spec.EndTime.Sub(r.clusterAtCreate).Round(time.Second))
	r.created = time.Now()
	stack.Create(t, r.c, md)
	if r.guard != nil {
		r.startGuard()
	}
	r.fc = &stack.Client{T: t, H: r.h, Model: md.Spec.Model, MaxRequest: r.tm.MaxRequestDuration}
}

// tag writes an Allocation's job tag down the first time the watch sees the
// Allocation. A tag the watch delivers late, or that cannot be written, does
// not lose the job: it has the run's prefix, by which Close and the script's
// cleanup find it (review of PR 255, B3); Run fails a run with such a tag.
func (r *liveRun) tag(a *v1alpha1.Allocation) {
	tg := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tags.Tagged(tg) {
		return
	}
	if err := r.tags.Tag(tg); err != nil {
		r.tagErrs = append(r.tagErrs, err)
	}
}

// runTags are the job tags of every Allocation the watch saw, in the order
// they first appeared, written down or not (Run fails a run with a tag that
// was not), or the error of a tag that could not be written down.
func (r *liveRun) runTags() ([]string, error) {
	seen := map[string]bool{}
	var tags []string
	for _, o := range r.watch.Allocations() {
		tg := v1alpha1.JobTag(o.Allocation.Namespace, o.Allocation.Name, o.Allocation.UID)
		if !seen[tg] {
			seen[tg] = true
			tags = append(tags, tg)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return tags, errors.Join(r.tagErrs...)
}

// deployment is the world's deployment with the row's plan applied: the row's
// Provider alone, its lease, its request deadline, for the no-overlap row
// ReleaseWhenIdle with its idle limit, and the first end that keeps the row
// to its job count, from the cluster's clock read now, before the deployment
// exists (the review of PR 255, nit: the controller compares the end with the
// jobs' times on that clock, not on this host's).
func (r *liveRun) deployment(row Row) *v1alpha1.ModelDeployment {
	t := r.t
	t.Helper()
	md := r.w.md.DeepCopy()
	plan := row.Plan()
	md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{r.w.provider}}
	md.Spec.LeaseLimit = minutes(plan.LeaseLimit)
	md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: dur(plan.MaxRequestDuration)}
	if plan.ReleaseWhenIdle {
		md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(plan.IdleLimit)}
	}
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	now, err := r.clusterNow(ctx)
	if err != nil {
		t.Fatalf("the cluster's clock before the deployment: %v", err)
	}
	r.clusterAtCreate = now
	var end time.Time
	switch row.MostJobs {
	case 1:
		end = FirstEndOfOne(now, r.limit, r.dw)
	case 2:
		r.guard = &TwoJobGuard{TimeLimit: r.limit, DrainWindow: r.dw}
		if end, _, err = r.guard.Next(Look{Clock: now}); err != nil {
			t.Fatalf("the two-job guard's first end: %v", err)
		}
	default:
		t.Fatalf("%s: no end keeps a row of %d jobs to its count", row.Name, row.MostJobs)
	}
	e := metav1.NewTime(end)
	md.Spec.EndTime = &e
	return md
}

// controllerWindow is the drain window as the controller and the front work
// it out (v1alpha1.InstanceDrainWindow): the deployment's end must meet the
// controller's drain start to the second.
func (r *liveRun) controllerWindow() time.Duration {
	maxReq := v1alpha1.Duration(r.tm.MaxRequestDuration.String())
	dw, err := v1alpha1.InstanceDrainWindow(r.w.profile, &v1alpha1.Routing{MaxRequestDuration: &maxReq})
	if err != nil {
		r.t.Fatalf("the drain window: %v", err)
	}
	return dw
}

// timeLimit is the time limit the controller gives the row's jobs
// (internal/lifecycle, allocationTimeLimit): the shortest of the partition's
// maxTime, the QOS limits' maxWall and the lease, in whole minutes. A job's
// lease end is its start plus it, or later by the time its configuration took
// (TwoJobGuard.planned).
func (r *liveRun) timeLimit() time.Duration {
	t := r.t
	t.Helper()
	row := r.cfg.Row
	var part *v1alpha1.PartitionProfile
	for i := range r.w.profile.Partitions {
		if r.w.profile.Partitions[i].Name == row.Partition {
			part = &r.w.profile.Partitions[i]
		}
	}
	if part == nil {
		t.Fatalf("the profile has no partition %s", row.Partition)
	}
	var maxT *time.Duration
	if part.MaxTime != nil {
		d, err := part.MaxTime.AsDuration()
		if err != nil {
			t.Fatalf("partition %s maxTime: %v", part.Name, err)
		}
		maxT = &d
	}
	limits, err := v1alpha1.EffectiveQOSLimits(&r.w.profile, *part, r.w.qos)
	if err != nil {
		t.Fatalf("the QOS limits: %v", err)
	}
	lease := row.Plan().LeaseLimit.Truncate(time.Minute)
	lim, _, ok := v1alpha1.AllocationTimeLimit(maxT, limits.MaxWall, &lease)
	if !ok {
		t.Fatalf("the row's jobs have no time limit of a minute or more")
	}
	return lim
}

// clusterNow reads the cluster's clock on its login node through the run's
// own transport.
func (r *liveRun) clusterNow(ctx context.Context) (time.Time, error) {
	res, err := r.tr.Run(ctx, slurm.Command{Line: ClusterClockLine, Timeout: 30 * time.Second})
	if err != nil {
		return time.Time{}, fmt.Errorf("the cluster's clock: %w", err)
	}
	if res.ExitCode != 0 {
		return time.Time{}, fmt.Errorf("the cluster's clock: exit %d", res.ExitCode)
	}
	return ParseClusterClock(res.Stdout)
}

// oneJob keeps a row of one job (MostJobs 1, the tunnel rows) to one job. With
// no queue wait observed, the lifecycle controller creates a primary's
// replacement at the primary's start (HBEP-7, Handover), which such a row must
// not have: on Helios it would pend in the owner's one-job slot. A deployment
// that ends at or before a primary's drain start gets no replacement
// (internal/lifecycle/handover.go, replacements), so the deployment is created
// ending at the earliest drain start its first job can have (deployment), and,
// once the Instance's lease end is known, moved to a minute before the
// Instance's own drain start. The Instance drains at that end, which oneJob
// returns as its drain start. A failed move fails the step at once.
func (r *liveRun) oneJob(in v1alpha1.Instance) time.Time {
	t := r.t
	t.Helper()
	ds, ok := DrainStartOf(&in, r.dw)
	if !ok {
		t.Fatal("the Instance has no lease end")
	}
	end, err := r.endAt(EndOfOne(ds))
	if err != nil {
		t.Fatal(err)
	}
	return end
}

// endAt sets the run's deployment's end to t, and returns the end the API
// then holds, which must be t: a write the API did not keep is an error. It
// never fails the test itself: the guard calls it from its goroutine.
func (r *liveRun) endAt(t time.Time) (time.Time, error) {
	end := metav1.NewTime(t.Truncate(time.Second))
	var md v1alpha1.ModelDeployment
	ctx, cancel := context.WithTimeout(context.Background(), stack.APICallTimeout)
	defer cancel()
	if err := r.c.Get(ctx, client.ObjectKey{Name: r.w.md.Name, Namespace: r.w.md.Namespace}, &md); err != nil {
		return time.Time{}, fmt.Errorf("the run's deployment: %w", err)
	}
	if md.Spec.EndTime != nil && md.Spec.EndTime.Equal(&end) {
		return end.Time, nil
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.EndTime = &end
	if err := r.c.Patch(ctx, &md, patch); err != nil {
		return time.Time{}, fmt.Errorf("set the run's deployment's end: %w", err)
	}
	if md.Spec.EndTime == nil || !md.Spec.EndTime.Equal(&end) {
		return time.Time{}, errors.New("the API did not keep the run's deployment's end")
	}
	return end.Time, nil
}

func dur(d time.Duration) *v1alpha1.Duration { s := v1alpha1.Duration(d.String()); return &s }

// minutes is a lease as the API takes it, in whole minutes.
func minutes(d time.Duration) *v1alpha1.Duration {
	s := v1alpha1.Duration(strconv.Itoa(int(d/time.Minute)) + "m")
	return &s
}

// instances lists the deployment's namespace's Instances.
func (r *liveRun) instances() []v1alpha1.Instance {
	var l v1alpha1.InstanceList
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.List(ctx, &l, client.InNamespace(r.w.md.Namespace)); err != nil {
		r.t.Fatalf("list Instances: %v", err)
	}
	return l.Items
}

// runObjects reads the Allocations and the Instances of the run's namespace from the API, for the state a wait names when it gives up
// (EndWaitState). A read that fails is returned, never taken for an empty list; its text goes to the private log.
func (r *liveRun) runObjects() ([]v1alpha1.Allocation, []v1alpha1.Instance, error) {
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	var al v1alpha1.AllocationList
	var il v1alpha1.InstanceList
	err := r.c.List(ctx, &al, client.InNamespace(r.w.md.Namespace))
	if err == nil {
		err = r.c.List(ctx, &il, client.InNamespace(r.w.md.Namespace))
	}
	if err != nil {
		r.t.Logf("the run's Allocations and Instances could not be read for the wait's state: %v", err)
		return nil, nil, err
	}
	return al.Items, il.Items, nil
}

// firstJobStarted is the run's first clock (FirstClock): the first
// Allocation's job must start within it of the deployment's creation. Past it
// the run ends with a NotStartedError, and Close stops the stack and cancels
// the run's jobs by their prefix and tags, and checks that none is left; on a
// one-job slot that was the owner's GPU waiting. That is for a job a read at the
// bound shows waiting, and for nothing else (review of PR 284, B2): a version of
// an Allocation with no start time and a phase past waiting ends the wait at once,
// and a read that cannot call the job waiting (firstJobWaiting) ends it at the
// bound; both are failures that name what they found, never NOT RUN.
func (r *liveRun) firstJobStarted(p Plan) error {
	bound, pastEnd := FirstClock(p.Row, p.PendingMax, r.limit, r.dw)
	if pastEnd {
		p.Logf("%s: the first job may wait %s in the queue, a minute before the deployment's first end, not the %s allowed", p.Row.Name, bound, p.PendingMax)
	}
	deadline := r.created.Add(bound)
	for {
		r.checkGuard()
		for _, o := range r.allocations() {
			st := o.Allocation.Status.AtProvider
			if st.StartTime != nil {
				return nil
			}
			switch st.Phase {
			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
				return fmt.Errorf("a job of the run is %s (reason %q) with no start time to read, so it cannot be called one that never started", st.Phase, st.Reason)
			}
		}
		if !time.Now().Before(deadline) {
			if err := r.firstJobWaiting(); err != nil {
				return fmt.Errorf("the first clock of %s ran out, and the run's first job cannot be called one that never started: %w", bound, err)
			}
			r.claimed = true
			r.watch.ResetsDecideNothing() // this run ends on the scheduler's read, and judges nothing from the watch
			p.Logf("%s: the run's first job was still pending %s after the deployment's creation; the run ends and its jobs are cancelled", p.Row.Name, time.Since(r.created).Round(time.Second))
			return &NotStartedError{Bound: bound, PastEnd: pastEnd, StartWait: p.Row.StartWait > 0 && !pastEnd}
		}
		time.Sleep(time.Second)
	}
}

// firstJobWaiting is the read at the first clock's end: the run's jobs in a look made now, every one of them waiting.
// nil is the one way to NOT RUN. A look that fails, and a queue with no job of the run in it, tell nothing about a start.
func (r *liveRun) firstJobWaiting() error {
	l, err := r.lookNow()
	if err != nil {
		return fmt.Errorf("the queue could not be read: %w", err)
	}
	if len(l.Jobs) == 0 {
		return errors.New("the queue lists no job of the run")
	}
	for _, j := range l.Jobs {
		if !WaitingToStart(j) {
			return fmt.Errorf("a job of the run is %s (reason %q)", j.Phase, j.Reason)
		}
	}
	return nil
}

// allocations are the Allocation versions the watch saw.
func (r *liveRun) allocations() []stack.AllocObservation {
	if r.allocsFn != nil {
		return r.allocsFn()
	}
	return r.watch.Allocations()
}

// readyWait is how long the run waits, once its first job runs, for the
// Instance to become ready: health.startTimeout, and a margin.
func (r *liveRun) readyWait() time.Duration {
	return r.tm.StartTimeout + ReadyMargin
}

// firstReady waits for the first job to start, then for the deployment's
// first Instance (no predecessor) to be ready and registered at the front.
func (r *liveRun) firstReady(p Plan) (v1alpha1.Instance, error) {
	if err := r.firstJobStarted(p); err != nil {
		return v1alpha1.Instance{}, err
	}
	if p.Row.MostJobs == 1 {
		// At once: the deployment's first end holds only until a job that
		// waited the whole first clock could have moved it (FirstClock).
		var first v1alpha1.Instance
		r.waitFor(stack.WaitDeadline, "the first Instance's lease end", func() bool {
			for _, x := range r.instances() {
				if x.Annotations[v1alpha1.AnnotationPredecessor] == "" && x.Status.LeaseEnd != nil {
					first = x
					return true
				}
			}
			return false
		})
		r.oneEnd = r.oneJob(first)
	}
	var in v1alpha1.Instance
	r.waitFor(r.readyWait(), "the first Instance to be ready and registered at the front", func() bool {
		for _, x := range r.instances() {
			if x.Annotations[v1alpha1.AnnotationPredecessor] == "" && x.Status.Phase == v1alpha1.InstanceReady {
				if reg, known := stack.Registered(r.h.Ports, &x); known && reg {
					in = x
					return true
				}
			}
		}
		return false
	})
	p.Logf("%s: the first Instance is ready", p.Row.Name)
	return in, nil
}

// successorOf waits, within d, for the replacement of pred to be ready and
// registered at the front; ok is false when it was not. stop, when set, ends the
// wait early: the successor job's own bound (secondPastItsBound).
func (r *liveRun) successorOf(pred string, d time.Duration, stop func() bool) (v1alpha1.Instance, bool) {
	bounded := r.boundedFn
	if bounded == nil {
		bounded = func(d time.Duration) (time.Time, bool) { return stack.BoundedDeadline(r.t, d) }
	}
	deadline, _ := bounded(d)
	for time.Now().Before(deadline) {
		r.checkGuard()
		if stop != nil && stop() {
			return v1alpha1.Instance{}, false
		}
		for _, x := range r.instances() {
			if x.Annotations[v1alpha1.AnnotationPredecessor] != pred {
				continue
			}
			if x.Status.Phase == v1alpha1.InstanceReady {
				if reg, known := stack.Registered(r.h.Ports, &x); known && reg {
					return x, true
				}
			}
		}
		time.Sleep(time.Second)
	}
	return v1alpha1.Instance{}, false
}

// syncWatch makes the watch catch up with the API on the kinds before a read of
// it that follows something the API showed (stack.Recorder.CatchUp): the API
// leads the watch, so such a read can precede an event that is only late
// (issue #281). what names the read. The error says that an event was lost, or
// that it could not be told whether the watch had caught up. ctx is the row's:
// the catch-up stops at its deadline, and takes at most stack.CatchUpWorstCase
// of the row's time.
func (r *liveRun) syncWatch(ctx context.Context, kinds stack.Kinds, what string) error {
	r.t.Helper()
	return r.watch.CatchUp(ctx, r.t, r.c, r.w.md.Namespace, kinds, what).Err(what)
}

// tagOf is the job tag of an Instance's Allocation.
func (r *liveRun) tagOf(in v1alpha1.Instance) string {
	for _, o := range r.watch.Allocations() {
		if o.Name == in.Spec.AllocationRef.Name {
			return v1alpha1.JobTag(o.Allocation.Namespace, o.Allocation.Name, o.Allocation.UID)
		}
	}
	return ""
}

// last is the last version of an Instance the watch saw.
func (r *liveRun) last(in v1alpha1.Instance) v1alpha1.Instance {
	obs := r.watch.Of(in.UID)
	if len(obs) == 0 {
		return in
	}
	return *obs[len(obs)-1].Instance
}

// observedDeployment is what the API held for the run's deployment.
func (r *liveRun) observedDeployment() DeploymentSpec {
	var md v1alpha1.ModelDeployment
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(ctx, client.ObjectKey{Name: r.w.md.Name, Namespace: r.w.md.Namespace}, &md); err != nil {
		r.t.Fatalf("the run's deployment: %v", err)
	}
	d, err := DeploymentSpecOf(&md, map[string]string{r.w.provider: r.w.cluster})
	if err != nil {
		r.t.Fatalf("the run's deployment: %v", err)
	}
	return d
}

// keep records a reply's headers, for the attribution count.
func (r *liveRun) keep(h http.Header) {
	r.mu.Lock()
	r.replies = append(r.replies, h.Clone())
	r.mu.Unlock()
}

// attribution counts the replies the run kept and those that carried HBEP-5's
// headers for one of its Instances.
func (r *liveRun) attribution() (responses, attributed int) {
	uids := map[string]bool{}
	for _, u := range r.watch.UIDs() {
		uids[string(u)] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.replies {
		responses++
		if Attributed(h, uids) {
			attributed++
		}
	}
	return responses, attributed
}

// healthyPartExited reports whether an Instance version shows the exit the
// agent reported: Healthy False with PartExited.
func healthyPartExited(in *v1alpha1.Instance) bool {
	c := meta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionHealthy)
	return c != nil && c.Status == "False" && c.Reason == v1alpha1.ReasonPartExited
}
