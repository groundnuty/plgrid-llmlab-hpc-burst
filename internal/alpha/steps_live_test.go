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
	// provider is the Provider's name, and cluster the platform it is on.
	provider, cluster string
	// md is the served deployment before the row's plan is applied.
	md *v1alpha1.ModelDeployment
	// profile is the platform profile's spec, for the drain window.
	profile v1alpha1.PlatformProfileSpec
	// controlDir and hostLock are the run's own ssh transport's.
	controlDir, hostLock string
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
		objects: []client.Object{&pp, prov}, provider: r.Cluster, cluster: r.Cluster, md: &md, profile: pp.Spec,
		controlDir: cfg.ControlDir, hostLock: cfg.HostLock,
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
func (f failedSteps) Ledger() Ledger              { return &BatchLedger{} }
func (f failedSteps) Close(context.Context) error { return nil }

// liveRun is one run of the live steps in a world.
type liveRun struct {
	t   *testing.T
	cfg Config
	w   world
	tm  Timing

	since time.Time
	tags  *Recorder
	h     *stack.Host
	c     client.Client
	watch *stack.Recorder
	fc    *stack.Client
	batch *slurm.Batch
	tr    *sshrun.SSH

	mu      sync.Mutex
	tagErrs []error
	replies []http.Header
}

func newLiveRun(t *testing.T, cfg Config, w world) *liveRun {
	return &liveRun{t: t, cfg: cfg, w: w, tm: cfg.Row.Clock()}
}

// closeTimeout bounds Close: hpcburst down and the cancel of the run's jobs.
const closeTimeout = 5 * time.Minute

// Close stops the stack, then ends the run's jobs by their tags and checks
// none is left live, then stops the run's ssh master. It runs on every exit of
// Run, a failed step and a run past its deadline included, so it takes a time
// of its own rather than the run's, which may be over.
func (r *liveRun) Close(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	var errs []error
	if r.h != nil {
		// First, so that no controller submits a job after the cancel.
		errs = append(errs, r.h.DownErr())
	}
	if r.batch != nil && r.tags != nil {
		if err := EndJobs(cctx, r.batch, r.tags.Tags(), 5*time.Second); err != nil {
			errs = append(errs, fmt.Errorf("the run's jobs: %w", err))
		}
	}
	if r.tr != nil {
		errs = append(errs, r.tr.Close())
	}
	return errors.Join(errs...)
}

// Ledger reads the cluster's record over the run's own ssh transport.
func (r *liveRun) Ledger() Ledger {
	if r.batch == nil {
		return &BatchLedger{}
	}
	return &BatchLedger{Finder: r.batch, Since: r.since.Add(-AccountingSlack)}
}

// start brings the stack up, creates the Provider, makes the run's own
// transport, and creates the row's deployment with every Allocation's tag
// written down as the watch first sees it.
func (r *liveRun) start(p Plan) {
	t := r.t
	t.Helper()
	r.since, r.tags = time.Now(), p.Rec
	p.Logf("%s: starting the stack (%s world)", p.Row.Name, r.w.name)
	r.h = stack.StartHost(t, r.w.bins, stack.ShortDir(t), r.w.dh, r.w.env, r.w.ports, r.w.frontTuning)
	r.w.place(r.h)
	r.c = stack.KubeClient(t, r.h.State)
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
	stack.WaitFor(t, stack.WaitDeadline, "the Provider to be Ready", func() bool {
		ctx, cancel := stack.CallCtx(t)
		defer cancel()
		return r.c.Get(ctx, client.ObjectKey{Name: r.w.provider}, &prov) == nil && v1alpha1.ProviderReady(&prov, time.Now())
	})
	md := r.deployment(p.Row)
	stack.Create(t, r.c, md)
	r.fc = &stack.Client{T: t, H: r.h, Model: md.Spec.Model, MaxRequest: r.tm.MaxRequestDuration}
}

// tag writes an Allocation's job tag down the first time the watch sees the
// Allocation. The Provider's controller submits only after it has set the tag
// as the Allocation's external name, a write the watch delivers too, so the
// tag is down before the job in all but the window of one watch delivery.
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
// Provider alone, its lease, its request deadline, and for the no-overlap row
// ReleaseWhenIdle with its idle limit.
func (r *liveRun) deployment(row Row) *v1alpha1.ModelDeployment {
	md := r.w.md.DeepCopy()
	plan := row.Plan()
	md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{r.w.provider}}
	md.Spec.LeaseLimit = minutes(plan.LeaseLimit)
	md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: dur(plan.MaxRequestDuration)}
	if plan.ReleaseWhenIdle {
		md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(plan.IdleLimit)}
	}
	if row.MostJobs == 1 {
		// The earliest drain start any first job can have (oneJob).
		end := metav1.NewTime(time.Now().Add(row.Lease - r.drainWindow()).Truncate(time.Second))
		md.Spec.EndTime = &end
	}
	return md
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
// returns as its drain start.
func (r *liveRun) oneJob(in v1alpha1.Instance) time.Time {
	t := r.t
	t.Helper()
	cur := r.current(in)
	ds, ok := DrainStartOf(&cur, r.drainWindow())
	if !ok {
		t.Fatal("the Instance has no lease end")
	}
	end := metav1.NewTime(ds.Add(-time.Minute).Truncate(time.Second))
	var md v1alpha1.ModelDeployment
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := r.c.Get(ctx, client.ObjectKey{Name: r.w.md.Name, Namespace: r.w.md.Namespace}, &md); err != nil {
		t.Fatalf("the run's deployment: %v", err)
	}
	if md.Spec.EndTime != nil && !end.After(md.Spec.EndTime.Time) {
		return md.Spec.EndTime.Time
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.EndTime = &end
	if err := r.c.Patch(ctx, &md, patch); err != nil {
		t.Fatalf("move the run's deployment's end to before the drain start: %v", err)
	}
	return end.Time
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

// drainWindow is the deployment's drain window from its spec and the profile's (DrainWindow).
func (r *liveRun) drainWindow() time.Duration {
	get := func(d *v1alpha1.Duration) time.Duration {
		if d == nil {
			return 0
		}
		x, _ := d.AsDuration()
		return x
	}
	margin := get(r.w.profile.DrainMargin)
	if margin == 0 {
		margin = 10 * time.Minute // the schema default (HBEP-4, Platform profile schema)
	}
	return DrainWindow(r.tm.MaxRequestDuration, margin, get(r.w.profile.DrainWindow))
}

// firstJobStarted is the run's first clock (Plan.PendingMax): the first
// Allocation's job must start within it of the deployment's creation. Past it
// the run ends, and Close cancels the jobs by their tags; on a shared one-job
// slot that is the owner's GPU waiting.
func (r *liveRun) firstJobStarted(p Plan) {
	stack.WaitFor(r.t, p.PendingMax, "the run's first job to start within the queue wait the run allows (HPCBURST_ALPHA_PENDING_MAX_SECONDS); the run ends and its jobs are cancelled", func() bool {
		for _, o := range r.watch.Allocations() {
			if o.Allocation.Status.AtProvider.StartTime != nil {
				return true
			}
		}
		return false
	})
}

// readyWait is how long the run waits, once its first job runs, for the
// Instance to become ready: health.startTimeout, and a margin.
func (r *liveRun) readyWait() time.Duration {
	return r.tm.StartTimeout + 5*time.Minute
}

// firstReady waits for the first job to start, then for the deployment's
// first Instance (no predecessor) to be ready and registered at the front.
func (r *liveRun) firstReady(p Plan) v1alpha1.Instance {
	r.firstJobStarted(p)
	var in v1alpha1.Instance
	stack.WaitFor(r.t, r.readyWait(), "the first Instance to be ready and registered at the front", func() bool {
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
	return in
}

// successorOf waits, within d, for the replacement of pred to be ready and
// registered at the front; ok is false when it was not.
func (r *liveRun) successorOf(pred string, d time.Duration) (v1alpha1.Instance, bool) {
	deadline, _ := stack.BoundedDeadline(r.t, d)
	for time.Now().Before(deadline) {
		for _, x := range r.instances() {
			if x.Annotations[v1alpha1.AnnotationPredecessor] == pred && x.Status.Phase == v1alpha1.InstanceReady {
				if reg, known := stack.Registered(r.h.Ports, &x); known && reg {
					return x, true
				}
			}
		}
		time.Sleep(time.Second)
	}
	return v1alpha1.Instance{}, false
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
