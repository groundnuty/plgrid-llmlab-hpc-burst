package lifecycle

// The world the execution tests of Operations run in (#589, PR 5): the scenario rig's real passes over two
// Providers, each with its own fake Slurm and call log, a deployment of two Instances on Provider A whose
// first, R, is ready and whose second, P, pends behind a queue, and the global ApprovalPolicy naming the person.
// Every Operation is created, planned, confirmed and approved through the API objects, as a person and an agent
// would; the passes are Reconcile's own. A fault or a kill is a wrapper on the lifecycle controller's own client
// (faultClient), which leaves the Provider's fake and the agents' writes alone.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// faultClient wraps the lifecycle controller's client. before runs ahead of every write, with the method's name
// ("Create", "Update", "Patch", "Delete", "DeleteAllOf", "Status.Update", "Status.Patch"): an error it returns is
// the write's result and the write is not made. after runs after every write that succeeded.
type faultClient struct {
	client.Client
	mu     sync.Mutex
	before func(method string, obj client.Object) error
	after  func(method string, obj client.Object)
	// failAfter, when set, runs after a write that was made: an error it returns is the write's result, though the write
	// is stored (a reply that never came back).
	failAfter func(method string, obj client.Object) error
	// bumpGeneration makes every update of a ModelDeployment move its metadata.generation by one, as the API server does for a
	// change of the spec: the fake client leaves it as it is.
	bumpGeneration bool
}

// set replaces both hooks.
func (f *faultClient) set(before func(string, client.Object) error, after func(string, client.Object)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.before, f.after = before, after
}

func (f *faultClient) hooks() (func(string, client.Object) error, func(string, client.Object)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.before, f.after
}

// write runs one write through the hooks.
func (f *faultClient) write(method string, obj client.Object, do func() error) error {
	if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" && f.bumpGeneration {
		obj.SetGeneration(obj.GetGeneration() + 1)
	}
	before, after := f.hooks()
	if before != nil {
		if err := before(method, obj); err != nil {
			return err
		}
	}
	if err := do(); err != nil {
		return err
	}
	if after != nil {
		after(method, obj)
	}
	f.mu.Lock()
	failAfter := f.failAfter
	f.mu.Unlock()
	if failAfter != nil {
		return failAfter(method, obj)
	}
	return nil
}

// setFailAfter sets the hook of a write that is stored and still returns an error.
func (f *faultClient) setFailAfter(h func(method string, obj client.Object) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter = h
}

func (f *faultClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return f.write("Create", obj, func() error { return f.Client.Create(ctx, obj, opts...) })
}

func (f *faultClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return f.write("Update", obj, func() error { return f.Client.Update(ctx, obj, opts...) })
}

func (f *faultClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	return f.write("Patch", obj, func() error { return f.Client.Patch(ctx, obj, p, opts...) })
}

func (f *faultClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return f.write("Delete", obj, func() error { return f.Client.Delete(ctx, obj, opts...) })
}

func (f *faultClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return f.write("DeleteAllOf", obj, func() error { return f.Client.DeleteAllOf(ctx, obj, opts...) })
}

func (f *faultClient) Status() client.SubResourceWriter {
	return faultStatus{f: f, SubResourceWriter: f.Client.Status()}
}

type faultStatus struct {
	client.SubResourceWriter
	f *faultClient
}

func (s faultStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.f.write("Status.Update", obj, func() error { return s.SubResourceWriter.Update(ctx, obj, opts...) })
}

func (s faultStatus) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.f.write("Status.Patch", obj, func() error { return s.SubResourceWriter.Patch(ctx, obj, p, opts...) })
}

// execWorld is the hedge rig with the deployment hoMD of two Instances on Provider A: R, ready, and P, pending
// behind a queue. A's Slurm logs every call; the causeLog holds the release cause the store held at each delete of an
// Allocation.
type execWorld struct {
	*hedgeRig
	log *causeLog
	fc  *faultClient

	ready, pending           string // the Instances' names
	readyAlloc, pendingAlloc string // their Allocations'
}

// instanceOrNil returns the named Instance, or nil once it is gone: an ended Instance is removed with its Allocation.
func (w *execWorld) instanceOrNil(name string) *v1alpha1.Instance {
	w.t.Helper()
	var in v1alpha1.Instance
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &in); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		w.t.Fatal(err)
	}
	return &in
}

// endedOrGone reports whether the Instance has ended or is gone.
func (w *execWorld) endedOrGone(name string) bool {
	in := w.instanceOrNil(name)
	return in == nil || ended(in)
}

// newThreeWorld is the deployment with three ready Instances on A, their jobs started five minutes apart, so that their
// lease ends, and with them the order a scale-down takes them in, are R1's first, then R2's, then R3's.
func newThreeWorld(t *testing.T) (w *execWorld, insts [3]string, allocs [3]string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{}, pfake.Script{}, pfake.Script{})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	for n := int32(1); n <= 3; n++ {
		if n > 1 {
			h.run(5*time.Minute, 30*time.Second)
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = n })
		}
		h.until(10*time.Minute, 10*time.Second, "ready", func() bool { return h.readyCount(hoMD) == int(n) })
	}
	for i, in := range h.liveInstances(hoMD) {
		insts[i], allocs[i] = in.Name, in.Spec.AllocationRef.Name
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, insts, allocs
}

// newExecWorld builds the world; P starts after pQueue. The lifecycle controller's client is a faultClient with no hook.
func newExecWorld(t *testing.T, pQueue time.Duration, opts ...rigOption) *execWorld {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, append([]rigOption{withInterceptors(log.funcs())}, opts...)...)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: pQueue})
	h.Step() // the Providers' first status: Ready
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 2
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	// With no queue both start at once: "pending" is then the second Instance, ready like the first.
	wantReady := 1
	if pQueue == 0 {
		wantReady = 2
	}
	h.until(5*time.Minute, 10*time.Second, "the Instances", func() bool {
		return h.readyCount(hoMD) == wantReady && len(h.liveInstances(hoMD)) == 2
	})
	w := &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	for i, in := range h.liveInstances(hoMD) {
		isReady := (pQueue == 0 && i == 0) || (pQueue != 0 && in.Status.Phase == v1alpha1.InstanceReady)
		if isReady {
			w.ready, w.readyAlloc = in.Name, in.Spec.AllocationRef.Name
		} else {
			w.pending, w.pendingAlloc = in.Name, in.Spec.AllocationRef.Name
		}
	}
	if w.ready == "" || w.pending == "" {
		t.Fatalf("the world has ready %q and pending %q", w.ready, w.pending)
	}
	h.r.Client = w.fc
	return w
}

// tick advances the clock by 10 s and runs one round.
func (w *execWorld) tick() {
	w.t.Helper()
	w.clock.Advance(10 * time.Second)
	w.Step()
}

// ticks runs n rounds.
func (w *execWorld) ticks(n int) {
	w.t.Helper()
	for i := 0; i < n; i++ {
		w.tick()
	}
}

func scaleTo(n int32) v1alpha1.OperationSpec {
	return v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: n}}
}

func endSpec(del bool) v1alpha1.OperationSpec {
	s := v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd}
	if del {
		s.End = &v1alpha1.EndParameters{Delete: true}
	}
	return s
}

func releaseSpec(allocs ...string) v1alpha1.OperationSpec {
	return v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, Release: &v1alpha1.ReleaseParameters{Allocations: allocs}}
}

func (w *execWorld) getOp(name string) *v1alpha1.Operation {
	w.t.Helper()
	var op v1alpha1.Operation
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &op); err != nil {
		w.t.Fatalf("get Operation %s: %v", name, err)
	}
	return &op
}

// plan creates the Operation on the world's deployment as agent A's and runs rounds until its plan is written.
func (w *execWorld) plan(name string, spec v1alpha1.OperationSpec) *v1alpha1.Operation {
	w.t.Helper()
	return w.planOn(hoMD, name, spec)
}

// planOn is plan for the named deployment.
func (w *execWorld) planOn(md, name string, spec v1alpha1.OperationSpec) *v1alpha1.Operation {
	w.t.Helper()
	op := opRigOp(name, spec)
	op.Spec.ModelDeploymentRef.Name = md
	w.create(op)
	w.until(time.Minute, 10*time.Second, name+" planned", func() bool {
		return w.getOp(name).Status.Phase == v1alpha1.OperationPhasePlanned
	})
	return w.getOp(name)
}

// confirm sets the owner's confirm of the plan that is in status, as an update that carries the resourceVersion read.
func (w *execWorld) confirm(name string) {
	w.t.Helper()
	op := w.getOp(name)
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: op.Status.Plan.ID}
	if err := w.c.Update(w.ctx, op); err != nil {
		w.t.Fatalf("confirm %s: %v", name, err)
	}
}

// approve creates an Approval of the Operation's plan, named by label, by the approver.
func (w *execWorld) approve(name, label, approver string) {
	w.t.Helper()
	op := w.getOp(name)
	w.create(&v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ap-%s-%s", name, label), Namespace: rigNS},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: name, UID: op.UID}, PlanID: op.Status.Plan.ID,
			Approver: v1alpha1.ApproverIdentity{Username: approver},
		},
	})
}

// phaseIs reports whether the Operation is in the phase.
func (w *execWorld) phaseIs(name string, phase v1alpha1.OperationPhase) func() bool {
	return func() bool { return w.getOp(name).Status.Phase == phase }
}

// untilPhase runs rounds until the Operation is in the phase.
func (w *execWorld) untilPhase(name string, phase v1alpha1.OperationPhase) {
	w.t.Helper()
	w.until(30*time.Minute, 10*time.Second, fmt.Sprintf("Operation %s %s", name, phase), w.phaseIs(name, phase))
}

// slurmWrites are the fake Slurm's calls of Provider A that are not reads, by name.
func (w *execWorld) slurmWrites() map[string]int { return writesOf(w.batch.Calls()) }

// objectNames lists the deployment's Instances and Allocations by name, a snapshot of what exists.
func (w *execWorld) objectNames() map[string]bool {
	out := map[string]bool{}
	for _, in := range w.instances(hoMD) {
		out["Instance "+in.Name] = true
	}
	for _, a := range w.allocations(hoMD) {
		out["Allocation "+a.Name] = true
	}
	return out
}

// operationCauseOf is the release cause an Operation's release must have written, spelled out by hand from the
// Operation object the API server holds: "operation <namespace>/<name> <uid>", and not through the code that writes it.
func operationCauseOf(op *v1alpha1.Operation) string {
	return fmt.Sprintf("operation %s/%s %s", op.Namespace, op.Name, op.UID)
}

// newStartingWorld is a deployment of two Instances on A: R, ready, and S, whose job has started and whose engine takes
// three minutes to be ready, so that S is still starting. A Scale to one takes S, as EndStarting.
func newStartingWorld(t *testing.T) (w *execWorld, ready, starting string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	started := 0
	h.engine = func(string) afake.Engine {
		started++
		if started == 2 {
			return afake.Engine{StartDelay: 3 * time.Minute}
		}
		return afake.Engine{}
	}
	h.batch.Enqueue(pfake.Script{}, pfake.Script{})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 2
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	h.until(10*time.Minute, 10*time.Second, "R ready and S starting on a started job", func() bool {
		live := h.liveInstances(hoMD)
		if len(live) != 2 || h.readyCount(hoMD) != 1 {
			return false
		}
		for _, in := range live {
			if in.Status.Phase != v1alpha1.InstanceReady {
				a, ok := h.allocation(in.Spec.AllocationRef.Name)
				return ok && a.Status.AtProvider.StartTime != nil
			}
		}
		return false
	})
	for _, in := range h.liveInstances(hoMD) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			ready = in.Name
		} else {
			starting = in.Name
		}
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, ready, starting
}

// stepsUntil runs rounds with the controller's passes allowed to fail with an error that says "injected" or "the controller
// was killed", until cond holds.
func (w *execWorld) stepsUntil(limit, tick time.Duration, what string, cond func() bool) {
	w.t.Helper()
	for end := w.clock.Now().Add(limit); ; {
		if cond() {
			return
		}
		if !w.clock.Now().Before(end) {
			w.t.Fatalf("%s: not reached within %s (now %s)", what, limit, w.clock.Now().Format(time.RFC3339))
		}
		w.clock.Advance(tick)
		if err := w.stepKeepingErrors(); err != nil && !strings.Contains(err.Error(), "injected") && !errors.Is(err, errKilled) {
			w.t.Fatalf("%s: a pass failed with %v", what, err)
		}
	}
}

// passRanAfterNow returns once a pass of the deployment's key has run after this call, which it shows with a fresh Operation
// that is never confirmed: it reads Planned only when a pass has planned it. A negative check, that something did NOT
// happen, needs it: a fixed count of rounds says nothing about whether the pass that would have acted has run (IMPL-COMMON,
// timing tests wait on observable state). The sentinel stays unconfirmed, and no check of the test reads it.
func (w *execWorld) passRanAfterNow(md, sentinel string) {
	w.t.Helper()
	w.planOn(md, sentinel, endSpec(false))
	if got := w.getOp(sentinel); got.Spec.Confirm != nil || got.Status.Phase != v1alpha1.OperationPhasePlanned {
		w.t.Fatalf("the sentinel %s is %q with confirm %v, want Planned and unconfirmed", sentinel, got.Status.Phase, got.Spec.Confirm)
	}
}

// newTwoWorld is a deployment of two ready Instances on A, their jobs started five minutes apart, so that R1's lease end is
// the nearer.
func newTwoWorld(t *testing.T) (w *execWorld, insts [2]string, allocs [2]string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{}, pfake.Script{})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	for n := int32(1); n <= 2; n++ {
		if n > 1 {
			h.run(5*time.Minute, 30*time.Second)
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = n })
		}
		h.until(10*time.Minute, 10*time.Second, "ready", func() bool { return h.readyCount(hoMD) == int(n) })
	}
	for i, in := range h.liveInstances(hoMD) {
		insts[i], allocs[i] = in.Name, in.Spec.AllocationRef.Name
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, insts, allocs
}

// newTwoPendingWorld is a deployment of two Instances on A whose jobs both pend: R behind qR, then X behind qX. A Scale to
// one takes X, the newest pending one, as CancelPending.
func newTwoPendingWorld(t *testing.T, qR, qX time.Duration) (w *execWorld, r, x string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{QueueWait: qR}, pfake.Script{QueueWait: qX})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	h.until(5*time.Minute, 10*time.Second, "R pending", func() bool { return len(h.liveInstances(hoMD)) == 1 })
	r = h.liveInstances(hoMD)[0].Name
	h.run(time.Minute, 10*time.Second) // X is made later than R, so that a Scale to one takes X
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	h.until(5*time.Minute, 10*time.Second, "X pending", func() bool { return len(h.liveInstances(hoMD)) == 2 })
	for _, in := range h.liveInstances(hoMD) {
		if in.Name != r {
			x = in.Name
		}
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, r, x
}

// newTwoStartingWorld is a deployment of two Instances on A whose jobs have both started: R's engine never comes up, and X's
// takes three minutes. The Allocations are released the moment they are empty (release when idle after no time), so a Scale to
// one takes X as EndStarting and plans its Allocation as ReleaseStarted.
func newTwoStartingWorld(t *testing.T) (w *execWorld, r, x string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	started := 0
	h.engine = func(string) afake.Engine {
		started++
		if started == 2 {
			return afake.Engine{StartDelay: 3 * time.Minute}
		}
		return afake.Engine{StartDelay: 6 * time.Hour}
	}
	h.batch.Enqueue(pfake.Script{}, pfake.Script{})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	jobStarted := func(in v1alpha1.Instance) bool {
		a, ok := h.allocation(in.Spec.AllocationRef.Name)
		return ok && a.Status.AtProvider.StartTime != nil
	}
	h.until(10*time.Minute, 10*time.Second, "R on a started job", func() bool {
		live := h.liveInstances(hoMD)
		return len(live) == 1 && jobStarted(live[0])
	})
	r = h.liveInstances(hoMD)[0].Name
	h.run(time.Minute, 10*time.Second) // X is made later than R, so that a Scale to one takes X
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	h.until(10*time.Minute, 10*time.Second, "X on a started job", func() bool {
		live := h.liveInstances(hoMD)
		if len(live) != 2 || h.readyCount(hoMD) != 0 {
			return false
		}
		for _, in := range live {
			if !jobStarted(in) {
				return false
			}
		}
		return true
	})
	for _, in := range h.liveInstances(hoMD) {
		if in.Name != r {
			x = in.Name
		}
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, r, x
}

// conflictOnce makes the first update of the ModelDeployment fail with a Conflict after racing: edit runs first, as a person's
// edit that lands between the pass's read and its write, and the write is not made. It stands for the API server's refusal of an
// update that carries a resourceVersion that is no longer current.
func (w *execWorld) conflictOnce(method string, edit func()) {
	armed := true
	w.fc.set(func(m string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && m == method && armed {
			armed = false
			edit()
			return apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("modeldeployments").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
		}
		return nil
	}, nil)
}
