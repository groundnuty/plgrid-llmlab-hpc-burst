package providerctl

// The Provider's start estimates (beta; HBEP-4, Core operations, Estimate;
// HBEP-10, The release cost; HBEP-11, Controllers, Field managers; #661).
//
// A controller on Operations, registered in Setup beside the Allocation
// controller, so only a batch Provider's manager has one. For an Operation in
// Planned or AwaitingApproval whose plan has a target with a cost on this
// Provider, it builds the request of each such target's Allocation as Create
// does (without the hold), groups the targets by shape, runs Estimate once per
// plan ID and shape, and writes the answers into status.estimates by
// server-side apply under FieldManagerProviderEstimate(provider). It reads the
// plan and never writes it: an estimate is outside the plan, so it never
// changes the plan's ID, and the lifecycle controller carries the list over
// untouched (the science agent's S4 ruling on #661).

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// estimateWorkers is the estimate controller's workers. Each Estimate is one
// command on the Provider's connection, whose own bound is four at once.
const estimateWorkers = 2

// The reasons of an unknown estimate that only this controller gives. The
// plugin's own are in pkg/provider (EstimateUndecided and the others). None
// holds an error's text: an error may name the account, the host or a path.
const (
	// estimateRefused: the Allocation's submission is refused before any call, so no estimate was asked.
	estimateRefused = "the Allocation's submission is refused before any call, so no estimate was asked"
)

// estimator is one Provider's estimate controller.
type estimator struct {
	s        *shared
	provider string
	// cache reads Operations, Allocations and Instances from the manager's cache; api reads an Operation
	// from the API server, right before a decision to write, so a stale cache never makes the apply drop
	// an entry this controller wrote a moment ago.
	cache, api client.Reader
	// apply writes the Provider's entries of the Operation by server-side apply, under
	// FieldManagerProviderEstimate. It is a field so that the unit tests can see what is written.
	apply func(ctx context.Context, op *v1alpha1.Operation, entries []v1alpha1.OperationEstimate) error
	// bound is the time one Estimate may take, and window the age of a plan past which none is started: both
	// provider.EstimateTimeout in production, where they are EQUAL (HBEP-10, The release cost: an estimate not filled in
	// within 60 s reads as unknown). A test that scales one down scales the other with it.
	bound, window time.Duration

	mu       sync.Mutex
	inflight map[string]struct{}
}

func (e *estimator) now() time.Time { return e.s.o.Clock.Now() }

// begin marks an estimate (Operation, plan ID, shape) as running and reports
// whether it was not already. Reconciles of one key never overlap, so this is a
// second line of defence against a duplicate call, and the one that holds if the
// controller is ever given more than one key's worth of concurrency per plan.
func (e *estimator) begin(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, running := e.inflight[key]; running {
		return false
	}
	e.inflight[key] = struct{}{}
	return true
}

func (e *estimator) end(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.inflight, key)
}

// estimateTrigger is the Operations the controller looks at: a new one, and one
// whose phase or plan ID changed. Its own writes change neither, so they raise no
// new pass, and a pass that finds every target covered does nothing.
func estimateTrigger() predicate.Predicate {
	planID := func(o *v1alpha1.Operation) string {
		if o.Status.Plan == nil {
			return ""
		}
		return o.Status.Plan.ID
	}
	return predicate.Funcs{
		UpdateFunc: func(ev event.UpdateEvent) bool {
			o, ok1 := ev.ObjectOld.(*v1alpha1.Operation)
			n, ok2 := ev.ObjectNew.(*v1alpha1.Operation)
			return !ok1 || !ok2 || o.Status.Phase != n.Status.Phase || planID(o) != planID(n)
		},
	}
}

// setupEstimates registers the Provider's estimate controller with mgr. Its
// writes go through mgr's own client, not the Allocation controller's, which
// wraps every write in FieldManagerProvider: the apply names its own manager.
func setupEstimates(mgr manager.Manager, c *Controller) error {
	name := c.shared.o.Provider
	e := &estimator{
		s: c.shared, provider: name, cache: mgr.GetClient(), api: mgr.GetAPIReader(),
		bound: provider.EstimateTimeout, window: provider.EstimateTimeout, inflight: map[string]struct{}{},
	}
	kube := mgr.GetClient()
	e.apply = func(ctx context.Context, op *v1alpha1.Operation, entries []v1alpha1.OperationEstimate) error {
		u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, name, entries)
		if err != nil {
			return err
		}
		return kube.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u),
			client.FieldOwner(v1alpha1.FieldManagerProviderEstimate(name)), client.ForceOwnership)
	}
	return builder.ControllerManagedBy(mgr).
		Named("estimate-"+name).
		For(&v1alpha1.Operation{}, builder.WithPredicates(estimateTrigger())).
		WithOptions(controllerOptions(estimateWorkers)).
		Complete(e)
}

// writable is the plan of an Operation an estimate may still be written onto, or nil: an Operation in Planned or AwaitingApproval
// that is not being deleted and has a plan. A terminal Operation is never written to. It asks nothing about the plan's age:
// that is decided when an estimate STARTS (startable), and never again when it ends.
func writable(op *v1alpha1.Operation) *v1alpha1.OperationPlan {
	if op.DeletionTimestamp != nil || op.Status.Plan == nil {
		return nil
	}
	if ph := op.Status.Phase; ph != v1alpha1.OperationPhasePlanned && ph != v1alpha1.OperationPhaseAwaitingApproval {
		return nil
	}
	return op.Status.Plan
}

// tooOldToStart reports whether a plan is older, at now, than an estimate may take: none is asked for it, since one not filled
// in within that bound reads as unknown (HBEP-10, The release cost; `@hpc-burst-science-agent`, 17 Nov 2026, #661, Q3: none is
// asked for a plan older than 60 s). The caller reads now once, for the decision; an estimate that started in time and ends
// after the bound is written all the same, as a timeout is an entry with an unknown start (HBEP-4, Estimate).
func (e *estimator) tooOldToStart(plan *v1alpha1.OperationPlan, now time.Time) bool {
	return now.Sub(plan.ObservedAt.Time) > e.window
}

// ownTargets are the plan's targets this Provider estimates: a target with a
// cost on this Provider, whether it is the Provider's Allocation or an Instance
// on one (PlanTarget.Provider). A target without a cost lowers no capacity, and
// there is nothing to show for it.
func (e *estimator) ownTargets(plan *v1alpha1.OperationPlan) []v1alpha1.PlanTarget {
	var out []v1alpha1.PlanTarget
	for _, t := range plan.Targets {
		if t.Cost == nil || t.Provider != e.provider {
			continue
		}
		if t.Kind != v1alpha1.TargetKindAllocation && t.Kind != v1alpha1.TargetKindInstance {
			continue
		}
		out = append(out, t)
	}
	return out
}

// mine are this Provider's entries of the Operation for the plan.
func (e *estimator) mine(op *v1alpha1.Operation, planID string) []v1alpha1.OperationEstimate {
	var out []v1alpha1.OperationEstimate
	for _, est := range op.Status.Estimates {
		if est.Provider == e.provider && est.PlanID == planID {
			out = append(out, est)
		}
	}
	return out
}

// covers reports whether the entries of the plan name every target.
func covers(entries []v1alpha1.OperationEstimate, targets []v1alpha1.PlanTarget) bool {
	have := map[string]bool{}
	for _, est := range entries {
		for _, uid := range est.Targets {
			have[uid] = true
		}
	}
	for _, t := range targets {
		if !have[string(t.UID)] {
			return false
		}
	}
	return true
}

// group is the targets of one shape and the request that stands for them.
type group struct {
	shape   string
	req     provider.SubmitRequest
	targets []string
}

// Reconcile asks and records the estimates one Operation still needs.
func (e *estimator) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var op v1alpha1.Operation
	if err := e.cache.Get(ctx, req.NamespacedName, &op); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	plan := writable(&op)
	if plan == nil || e.tooOldToStart(plan, e.now()) {
		return reconcile.Result{}, nil
	}
	targets := e.ownTargets(plan)
	if len(targets) == 0 || covers(e.mine(&op, plan.ID), targets) {
		return reconcile.Result{}, nil
	}
	v, platform, err := e.s.h.get(ctx)
	if err != nil {
		// The Provider cannot be read now. Nothing is written: an entry would end the asking for this
		// plan, and the next pass may succeed within the plan's bound. The text is fixed: the reader's
		// error may name a credentials path or an account, and controller-runtime logs a pass's error. The
		// Allocation controller reports the reader's own error, under its own rules.
		return reconcile.Result{}, errors.New("estimate: the Provider is not ready: its profile or credentials could not be read")
	}
	groups, err := e.groups(ctx, &op, targets, v)
	if err != nil {
		return reconcile.Result{}, err
	}
	for _, g := range groups {
		if err := e.estimateGroup(ctx, req, op.UID, plan.ID, g, platform); err != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// groups builds each own target's request as Create does, minus the hold, and
// groups the targets by shape, in shape order. A target whose Allocation cannot
// be found, or is not this Provider's, or has been replaced since the plan, is
// left out: it gets no entry and reads as pending, then unknown.
func (e *estimator) groups(ctx context.Context, op *v1alpha1.Operation, targets []v1alpha1.PlanTarget, v slurm.Validated) ([]group, error) {
	byShape := map[string]*group{}
	for _, t := range targets {
		a, err := e.allocationOf(ctx, op.Namespace, t)
		if err != nil {
			return nil, err
		}
		if a == nil || a.Spec.ForProvider.Batch == nil {
			continue
		}
		req := provider.SubmitRequest{
			Tag: v1alpha1.JobTag(a.Namespace, a.Name, a.UID), Account: v.Settings.Account, QOS: v.Settings.QOS,
			Params: *a.Spec.ForProvider.Batch,
		}
		shape := estimateShape(req, v.Profile.Partition.TmpDirFeature)
		g := byShape[shape]
		if g == nil {
			g = &group{shape: shape, req: req}
			byShape[shape] = g
		}
		g.targets = append(g.targets, string(t.UID))
	}
	out := make([]group, 0, len(byShape))
	for _, g := range byShape {
		slices.Sort(g.targets)
		out = append(out, *g)
	}
	slices.SortFunc(out, func(a, b group) int { return strings.Compare(a.shape, b.shape) })
	return out, nil
}

// allocationOf returns the Allocation of a plan target: the target itself, or
// the Allocation an Instance runs on. It is nil when there is none to ask for:
// not found, another UID than the plan's, or not this Provider's.
func (e *estimator) allocationOf(ctx context.Context, ns string, t v1alpha1.PlanTarget) (*v1alpha1.Allocation, error) {
	name := t.Name
	if t.Kind == v1alpha1.TargetKindInstance {
		var in v1alpha1.Instance
		if err := e.cache.Get(ctx, client.ObjectKey{Namespace: ns, Name: t.Name}, &in); err != nil {
			return nil, client.IgnoreNotFound(err)
		}
		if in.UID != t.UID {
			return nil, nil
		}
		name = in.Spec.AllocationRef.Name
	}
	var a v1alpha1.Allocation
	if err := e.cache.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &a); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if t.Kind == v1alpha1.TargetKindAllocation && a.UID != t.UID {
		return nil, nil
	}
	if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != e.provider {
		return nil, nil
	}
	return &a, nil
}

// estimateGroup asks the estimate of one shape, unless the plan already has it
// or it is running, and applies this Provider's entries with the answer. The API
// server checks no UID on an apply, so an Operation deleted and created again
// under the name meanwhile is told by its UID, which both reads must show as the
// one this pass started with.
func (e *estimator) estimateGroup(ctx context.Context, req reconcile.Request, uid types.UID, planID string, g group, platform Platform) error {
	fresh, err := e.freshPlan(ctx, req, uid, planID)
	if err != nil || fresh == nil {
		return err
	}
	if e.tooOldToStart(fresh.Status.Plan, e.now()) {
		return nil // this estimate's turn comes after the bound: it is not started, and the target reads as unknown
	}
	if slices.ContainsFunc(e.mine(fresh, planID), func(est v1alpha1.OperationEstimate) bool { return est.Shape == g.shape }) {
		return nil
	}
	key := string(fresh.UID) + "/" + planID + "/" + g.shape
	if !e.begin(key) {
		return nil
	}
	defer e.end(key)

	ectx, cancel := context.WithTimeout(ctx, e.bound)
	res, estErr := platform.Estimate(ectx, g.req)
	// A time-out is decided from THIS controller's own deadline, never from the runner's error: the deadline set here fires
	// before the runner's own command time-out, and the runner then reports a transport error whose text names the context's
	// error but does not wrap it (internal/slurm/sshrun, runClient). Read before cancel, which would set Canceled.
	timedOut := estErr != nil && errors.Is(ectx.Err(), context.DeadlineExceeded)
	cancel()
	if estErr != nil {
		// A fixed class, never the error's text: Slurm's own message keeps the account, a node's name and a job ID.
		e.s.o.Log.Info("the estimate failed", "operation", req.String(), "class", estimateFailureClass(estErr, timedOut))
	}
	entry := e.entry(planID, g, res, estErr, timedOut)

	// The Operation again: the answer is written only onto the plan it was asked for, and with every
	// entry of this Provider's for that plan, since an apply removes the entries it leaves out.
	fresh, err = e.freshPlan(ctx, req, uid, planID)
	if err != nil || fresh == nil {
		return err
	}
	entries := slices.DeleteFunc(e.mine(fresh, planID), func(est v1alpha1.OperationEstimate) bool { return est.Shape == g.shape })
	// What remains is a race this controller cannot close, since the API server checks no UID on an apply: the
	// Operation is deleted and created again between the read above and the apply below, and the entry lands on
	// the new Operation under the old plan ID. Readers show only the current plan ID's entries, so under another
	// plan ID it is never shown. Under the same plan ID, which takes identical inputs (the action, the
	// ModelDeployment's UID and generation, and each target's kind, name, UID, generation, effect and policies;
	// opid.go), it is an estimate for the same plan, targets and shape: advisory and correct, with its own
	// observedAt showing its age.
	return e.apply(ctx, fresh, append(entries, entry))
}

// freshPlan reads the Operation from the API server and returns it when it is
// still the Operation with this UID, on the plan with this ID and in a phase an estimate may be written in, else nil.
// It does not look at the plan's age.
func (e *estimator) freshPlan(ctx context.Context, req reconcile.Request, uid types.UID, planID string) (*v1alpha1.Operation, error) {
	var op v1alpha1.Operation
	if err := e.api.Get(ctx, req.NamespacedName, &op); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if plan := writable(&op); op.UID != uid || plan == nil || plan.ID != planID {
		return nil, nil
	}
	return &op, nil
}

// entry is one estimate's entry: the time, or why it is unknown. A failure is a
// final unknown with a fixed reason, never an absent entry (the science agent,
// 17 Nov 2026), and never an error's text.
func (e *estimator) entry(planID string, g group, res provider.EstimateResult, err error, timedOut bool) v1alpha1.OperationEstimate {
	readAt := res.ReadAt
	if readAt.IsZero() {
		readAt = e.now()
	}
	observed := metav1.NewTime(readAt)
	fig := v1alpha1.TimeFigure{ObservedAt: &observed, Advisory: true}
	switch {
	case err != nil:
		fig.Unknown = estimateFailure(err, timedOut)
	case res.Start != nil:
		t := metav1.NewTime(res.Start.UTC())
		fig.Time = &t
	case res.Unknown != "":
		fig.Unknown = res.Unknown
	default:
		fig.Unknown = provider.EstimateNoAnswer
	}
	return v1alpha1.OperationEstimate{Provider: e.provider, PlanID: planID, Shape: g.shape, Targets: g.targets, Start: fig}
}

// estimateFailureClass is the fixed class of an estimate that failed, for the log: timeout, refused (before any call),
// transport (no connection, or one that broke), or command (the command ran and failed, or the answer could not be had).
func estimateFailureClass(err error, timedOut bool) string {
	switch {
	case timedOut, errors.Is(err, context.DeadlineExceeded), errors.Is(err, slurm.ErrTimeout):
		return "timeout"
	case errors.Is(err, provider.ErrInvalidRequest):
		return "refused"
	case errors.Is(err, slurm.ErrTransport), errors.Is(err, slurm.ErrNotStarted):
		return "transport"
	}
	return "command"
}

// estimateFailure is the fixed reason of an estimate that failed.
func estimateFailure(err error, timedOut bool) string {
	switch {
	case timedOut, errors.Is(err, context.DeadlineExceeded), errors.Is(err, slurm.ErrTimeout):
		return provider.EstimateTimedOut
	case errors.Is(err, provider.ErrInvalidRequest):
		return estimateRefused
	}
	return provider.EstimateNoAnswer
}
