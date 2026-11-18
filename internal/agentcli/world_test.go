package agentcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The identities and the clock of the tests. Written out by hand, never read from the code under test.
const (
	userA  = "system:serviceaccount:hpcburst-agents:agent-a"
	userB  = "system:serviceaccount:hpcburst-agents:agent-b"
	testNS = "team"
)

var (
	groupsOfAgent = []string{"system:serviceaccounts", "system:serviceaccounts:hpcburst-agents", "system:authenticated"}
	t0            = time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
)

// recorder wraps a client and keeps every write that goes through it, by verb and kind.
type recorder struct {
	client.Client
	mu     sync.Mutex
	writes []string
	// before, when set, runs before each create of an Operation, with what has been printed so far.
	before func(client.Object)
}

func (r *recorder) note(verb string, obj client.Object) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, fmt.Sprintf("%s %T", verb, obj))
}

func (r *recorder) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	r.note("create", obj)
	if _, ok := obj.(*v1alpha1.Operation); ok && r.before != nil {
		r.before(obj)
	}
	return r.Client.Create(ctx, obj, opts...)
}

func (r *recorder) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	r.note("update", obj)
	if _, ok := obj.(*v1alpha1.Operation); ok {
		// The API server moves metadata.generation when the spec changes, which is what a confirm does; the fake client does not.
		obj.SetGeneration(obj.GetGeneration() + 1)
	}
	return r.Client.Update(ctx, obj, opts...)
}

func (r *recorder) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	r.note("patch", obj)
	return r.Client.Patch(ctx, obj, p, opts...)
}

func (r *recorder) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	r.note("delete", obj)
	return r.Client.Delete(ctx, obj, opts...)
}

func (r *recorder) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	r.note("deleteAllOf", obj)
	return r.Client.DeleteAllOf(ctx, obj, opts...)
}

func (r *recorder) Status() client.SubResourceWriter {
	return &subWriter{r: r, SubResourceWriter: r.Client.Status(), sub: "status"}
}

func (r *recorder) SubResource(name string) client.SubResourceClient {
	return &subClient{r: r, SubResourceClient: r.Client.SubResource(name), sub: name}
}

type subWriter struct {
	client.SubResourceWriter
	r   *recorder
	sub string
}

func (s *subWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.r.note(s.sub+" update", obj)
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s *subWriter) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	s.r.note(s.sub+" patch", obj)
	return s.SubResourceWriter.Patch(ctx, obj, p, opts...)
}

func (s *subWriter) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	s.r.note(s.sub+" create", obj)
	return s.SubResourceWriter.Create(ctx, obj, sub, opts...)
}

type subClient struct {
	client.SubResourceClient
	r   *recorder
	sub string
}

func (s *subClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.r.note(s.sub+" update", obj)
	return s.SubResourceClient.Update(ctx, obj, opts...)
}

func (s *subClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	s.r.note(s.sub+" patch", obj)
	return s.SubResourceClient.Patch(ctx, obj, p, opts...)
}

func (s *subClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	s.r.note(s.sub+" create", obj)
	return s.SubResourceClient.Create(ctx, obj, sub, opts...)
}

// world is a fake control plane, a caller, a clock and the buffers of one command line.
type world struct {
	t      *testing.T
	store  client.Client // the control plane, as the controllers and the person see it
	rec    *recorder     // what the CLI sees, and the writes it makes
	cli    *cli
	user   string
	groups []string
	now    time.Time
	tty    bool
	names  []string
	// onPause runs at each pause between two reads: where the test's "controller" acts.
	onPause func()
	out     bytes.Buffer
	errs    bytes.Buffer
}

// newWorld makes a control plane holding objs. interceptors, when given, act on the store itself.
func newWorld(t *testing.T, objs []client.Object, f ...interceptor.Funcs) *world {
	t.Helper()
	s, err := scheme()
	if err != nil {
		t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1alpha1.Operation{})
	if len(f) > 0 {
		b = b.WithInterceptorFuncs(f[0])
	}
	store := b.Build()
	w := &world{t: t, store: store, rec: &recorder{Client: store}, user: userA, groups: groupsOfAgent, now: t0}
	w.cli = &cli{
		env: Env{IsTerminal: func() bool { return w.tty }},
		connect: func(context.Context, string, io.Writer) (*caller, error) {
			return &caller{c: w.rec, user: w.user, groups: w.groups, namespace: testNS}, nil
		},
		now:  func() time.Time { return w.now },
		poll: time.Second,
		pause: func(ctx context.Context, d time.Duration) error {
			w.now = w.now.Add(d)
			if w.onPause != nil {
				w.onPause()
			}
			return nil
		},
		newName: func() string {
			if len(w.names) == 0 {
				t.Fatal("the CLI generated a name the test did not expect")
			}
			n := w.names[0]
			w.names = w.names[1:]
			return n
		},
	}
	return w
}

// run executes one command line and returns its exit code; the output is in w.out and w.errs.
func (w *world) run(stdin string, args ...string) int {
	w.t.Helper()
	return w.runReader(strings.NewReader(stdin), args...)
}

// runReader is run with a stdin that is a reader of the test's own, which can act when it is read: a person who answers a
// prompt does so after the controller has moved on.
func (w *world) runReader(stdin io.Reader, args ...string) int {
	w.t.Helper()
	w.out.Reset()
	w.errs.Reset()
	return w.cli.run(context.Background(), args, stdin, &w.out, &w.errs)
}

// readerFunc is an io.Reader made of a function.
type readerFunc func(p []byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// writes are the writes the CLI made so far, by verb and kind.
func (w *world) writes() []string {
	w.rec.mu.Lock()
	defer w.rec.mu.Unlock()
	return append([]string(nil), w.rec.writes...)
}

// operation reads an Operation from the control plane.
func (w *world) operation(name string) *v1alpha1.Operation {
	w.t.Helper()
	op := &v1alpha1.Operation{}
	if err := w.store.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, op); err != nil {
		w.t.Fatalf("read Operation %s: %v", name, err)
	}
	return op
}

// operations lists the names of the Operations of the control plane.
func (w *world) operations() []string {
	w.t.Helper()
	var l v1alpha1.OperationList
	if err := w.store.List(context.Background(), &l); err != nil {
		w.t.Fatal(err)
	}
	var out []string
	for _, o := range l.Items {
		out = append(out, o.Name)
	}
	return out
}

// planFor makes the controller's plan of an Operation: the status it writes once it has planned.
func (w *world) planFor(name string, plan *v1alpha1.OperationPlan) {
	w.t.Helper()
	op := w.operation(name)
	op.Status.Phase = v1alpha1.OperationPhasePlanned
	op.Status.Plan = plan
	if err := w.store.Status().Update(context.Background(), op); err != nil {
		w.t.Fatalf("write the plan of %s: %v", name, err)
	}
}

func mustNotExist(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("%T %s: want not found, got %v", obj, obj.GetName(), err)
	}
}

// opObject is an Operation as the controller leaves it, for the tests that start from a stored one.
func opObject(name, owner string, spec v1alpha1.OperationSpec, status v1alpha1.OperationStatus) *v1alpha1.Operation {
	spec.Owner = owner
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("uid-" + name)},
		Spec:       spec, Status: status,
	}
}

func scaleSpec(md string, n int32) v1alpha1.OperationSpec {
	return v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}, Scale: &v1alpha1.ScaleParameters{Instances: n}}
}

// simplePlan is a plan with one target that needs approval under the Provider's policy.
func simplePlan(id string) *v1alpha1.OperationPlan {
	return &v1alpha1.OperationPlan{
		ID: id, ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m", Generation: 4},
		Targets: []v1alpha1.PlanTarget{{
			Kind: v1alpha1.TargetKindAllocation, Name: "alloc-p", UID: "uid-alloc-p", Provider: "sano", Effect: v1alpha1.PlanEffectCancelPending, NeedsApproval: true,
			Policies: []v1alpha1.AppliedPolicy{{Name: "provider.sano", Approvers: []v1alpha1.Approver{{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}}}},
		}},
	}
}

func contains(t *testing.T, what, got string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Errorf("%s lacks %q:\n%s", what, want, got)
		}
	}
}

func lacks(t *testing.T, what, got string, nots ...string) {
	t.Helper()
	for _, not := range nots {
		if strings.Contains(got, not) {
			t.Errorf("%s holds %q:\n%s", what, not, got)
		}
	}
}

// controller makes the test's controller act at each pause between two reads of the CLI: it writes the plan
// of the named Operation, and moves a confirmed one from Planned to AwaitingApproval with a condition that
// says what it waits for.
func (w *world) controller(name string, plan *v1alpha1.OperationPlan, waiting string) {
	w.onPause = func() {
		op := w.operation(name)
		switch {
		case op.Status.Plan == nil:
			w.planFor(name, plan)
		case op.Spec.Confirm != nil && op.Status.Phase == v1alpha1.OperationPhasePlanned:
			op.Status.Phase, op.Status.ObservedGeneration = v1alpha1.OperationPhaseAwaitingApproval, op.Generation
			op.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse,
				Reason: v1alpha1.ReasonAwaitingApproval, Message: waiting, LastTransitionTime: metav1.NewTime(w.now)}}
			if err := w.store.Status().Update(context.Background(), op); err != nil {
				w.t.Fatal(err)
			}
		}
	}
}

// reasonExecutionNotBuilt is the reason the controller of the test gives, written out: where the execution is built (PR 664) no
// such constant exists, and the test only needs the controller to say something and leave the phase alone.
const reasonExecutionNotBuilt = "ExecutionNotBuilt"

// controllerNoApproval is the controller of a plan that needs no approval, as it is where the execution is not built (HBEP-10,
// beta step 1): it writes the plan, and on the confirm it records that it has seen the generation and says that nothing runs,
// and the phase stays Planned.
func (w *world) controllerNoApproval(name string, plan *v1alpha1.OperationPlan) {
	w.onPause = func() {
		op := w.operation(name)
		switch {
		case op.Status.Plan == nil:
			w.planFor(name, plan)
		case op.Spec.Confirm != nil && op.Status.ObservedGeneration < op.Generation:
			op.Status.ObservedGeneration = op.Generation
			op.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: reasonExecutionNotBuilt,
				Message: "confirmed, and no target needs approval; this release plans Operations and does not execute them", LastTransitionTime: metav1.NewTime(w.now)}}
			if err := w.store.Status().Update(context.Background(), op); err != nil {
				w.t.Fatal(err)
			}
		}
	}
}
