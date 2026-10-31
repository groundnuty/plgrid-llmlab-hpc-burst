package providerctl

// Helpers of the protocol test suite (protocol_*_test.go, races_test.go,
// delete_test.go, status_test.go). Everything here is prefixed pt so it cannot
// clash with the harness or with other test files in the package.
//
// Counts of submissions and lifts come from fake.Batch's call log, never from
// a return value of the code under test (HBEP-4, Test plan). The log records
// every call, the test's own included: a test that submits straight to the fake,
// or reads through its FindByTag, takes a mark (ptMark) afterwards and counts
// from the mark.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The profile's windows in newEnv (harness_test.go) and the reject backoff
// (DefaultRejectBackoff).
const (
	ptWindow       = 5 * time.Minute
	ptLostGrace    = 2 * time.Minute
	ptUnknownBound = 10 * time.Minute
	ptBackoff      = time.Minute
)

// ptModes are the two submission modes. A scenario that holds in both runs
// under each (HBEP-7, open question 11: Hold mode, Fallback mode).
var ptModes = []struct {
	name string
	hold bool
	mods []envOption
}{
	{"hold", true, nil},
	{"fallback", false, []envOption{fallback}},
}

// ptMark returns the length of the call log.
func ptMark(e *env) int { return len(e.batch.Calls()) }

// ptCountSince counts the calls of op logged after mark.
func ptCountSince(e *env, mark int, op string) int {
	n := 0
	for _, c := range e.batch.Calls()[mark:] {
		if c.Op == op {
			n++
		}
	}
	return n
}

// ptCallsWithArg counts the calls of op logged after mark whose first argument
// is arg.
func ptCallsWithArg(e *env, mark int, op, arg string) int {
	n := 0
	for _, c := range e.batch.Calls()[mark:] {
		if c.Op == op && len(c.Args) > 0 && c.Args[0] == arg {
			n++
		}
	}
	return n
}

// ptLiftAttempts counts every scontrol-release call, whatever its result: the
// calls an admin-held, requeued or foreign-limit job must never get.
func ptLiftAttempts(e *env) int { return e.count("scontrol-release") }

// ptSubmit submits a job with a's tag straight to the fake: a job a lost
// record would leave behind, or a late registration. It consumes a queued
// Script.
func ptSubmit(e *env, a *v1alpha1.Allocation, hold bool) string {
	e.t.Helper()
	id, outcome, err := e.batch.Submit(e.ctx, provider.SubmitRequest{
		Tag: tagOf(a), Hold: hold, Account: "example-account", Params: *a.Spec.ForProvider.Batch,
	})
	if outcome != provider.Accepted || err != nil {
		e.t.Fatalf("test setup: direct submit: outcome %s, error %v", outcome, err)
	}
	return id
}

// ptNow is the manual clock's time as a metav1.Time.
func ptNow(e *env) *metav1.Time { return &metav1.Time{Time: e.clock.Now()} }

// ptInFlight is a claim record of attempt n written now by a claimant that has
// since died.
func ptInFlight(e *env, n int32) v1alpha1.SubmissionRecord {
	return v1alpha1.SubmissionRecord{Attempt: n, Token: "dead-claimant-token", State: v1alpha1.SubmissionInFlight,
		Writer: "dead-claimant", Time: ptNow(e)}
}

// ptPlantRecord writes the submission record annotation, as a claimant that
// died before its sbatch would have left it.
func ptPlantRecord(e *env, name string, r v1alpha1.SubmissionRecord) {
	e.t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		e.t.Fatal(err)
	}
	ptSetAnnotation(e, name, v1alpha1.AnnotationSubmission, string(b))
}

// ptSetAnnotation sets one annotation with a plain Update.
func ptSetAnnotation(e *env, name, key, value string) {
	e.t.Helper()
	a := e.get(name)
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[key] = value
	if err := e.c.Update(e.ctx, a); err != nil {
		e.t.Fatalf("set annotation %s: %v", key, err)
	}
}

// ptRecordOf parses the submission annotation without the code under test.
func ptRecordOf(e *env, name string) *v1alpha1.SubmissionRecord {
	e.t.Helper()
	s := e.get(name).Annotations[v1alpha1.AnnotationSubmission]
	if s == "" {
		return nil
	}
	var r v1alpha1.SubmissionRecord
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		e.t.Fatalf("submission annotation %q: %v", s, err)
	}
	return &r
}

func ptJobID(e *env, name string) string {
	return e.get(name).Annotations[v1alpha1.AnnotationJobID]
}

func ptPhase(e *env, name string) v1alpha1.AllocationPhase {
	return e.get(name).Status.AtProvider.Phase
}

// ptStep moves the clock by d, reconciles once and returns the Allocation.
func ptStep(e *env, name string, d time.Duration) *v1alpha1.Allocation {
	e.t.Helper()
	e.clock.Advance(d)
	_, _ = e.reconcile(name)
	return e.get(name)
}

func ptCondStatus(a *v1alpha1.Allocation, t xpv2.ConditionType) corev1.ConditionStatus {
	return a.GetCondition(t).Status
}

// ptGone reports whether the Allocation no longer exists.
func ptGone(e *env, name string) bool {
	e.t.Helper()
	err := e.c.Get(e.ctx, clientKey(name), &v1alpha1.Allocation{})
	if err == nil {
		return false
	}
	if client.IgnoreNotFound(err) != nil {
		e.t.Fatal(err)
	}
	return true
}

// ptHasFinalizer reports whether the Allocation carries the cancel-all finalizer.
func ptHasFinalizer(a *v1alpha1.Allocation) bool {
	for _, f := range a.Finalizers {
		if f == v1alpha1.FinalizerCancelAll {
			return true
		}
	}
	return false
}

// ptFailJobIDWrite wraps c so that the first Update that carries the job-id
// annotation fails with an error that is not a conflict: the controller was
// killed after sbatch returned, before the job ID reached the record. It
// returns the switch, which is cleared when the failure has been injected.
func ptFailJobIDWrite(c client.Client) (client.Client, *atomic.Bool) {
	armed := &atomic.Bool{}
	armed.Store(true)
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if obj.GetAnnotations()[v1alpha1.AnnotationJobID] != "" && armed.CompareAndSwap(true, false) {
				return errors.New("scripted: the controller was killed before the job ID was recorded")
			}
			return w.Update(ctx, obj, opts...)
		},
	}), armed
}

// ptGate wraps a Platform so that one Submit blocks, before it reaches the
// fake, until the test releases it. Nothing is registered while it waits.
type ptGate struct {
	*fakePlatform
	mu      sync.Mutex
	armed   bool
	submits int
	entered chan struct{}
	release chan struct{}
}

var _ Platform = (*ptGate)(nil)

func ptNewGate(p *fakePlatform) *ptGate {
	return &ptGate{fakePlatform: p, entered: make(chan struct{}, 8), release: make(chan struct{})}
}

// arm makes the next Submit block.
func (g *ptGate) arm() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = true
}

// submitCalls is the number of Submit calls that reached the gate.
func (g *ptGate) submitCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.submits
}

func (g *ptGate) Submit(ctx context.Context, req provider.SubmitRequest) (string, provider.SubmitOutcome, error) {
	g.mu.Lock()
	g.submits++
	block := g.armed
	g.armed = false
	g.mu.Unlock()
	if block {
		g.entered <- struct{}{}
		select {
		case <-g.release:
		case <-ctx.Done():
			return "", provider.Unknown, ctx.Err()
		}
	}
	return g.fakePlatform.Submit(ctx, req)
}

// ptWaitEntered waits until a gated Submit has been reached.
func ptWaitEntered(t *testing.T, g *ptGate) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("no Submit reached the gate within 20 s")
	}
}

// ptWaitDone waits for an asynchronous reconcile.
func ptWaitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the reconcile did not return within 20 s")
	}
}

// ptViaGate rebuilds e's controller over a gated platform and returns the gate.
func ptViaGate(e *env) *ptGate {
	g := ptNewGate(e.platform)
	e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) { return g, io.NopCloser(nil), nil }
	e.build()
	return g
}

// ptControllerOver makes a second controller over e's client and options, with
// platform p. extra options are applied to its reconciler after New, as a later
// option wins in managed.NewReconciler too.
func ptControllerOver(e *env, p Platform, extra ...managed.ReconcilerOption) *Controller {
	e.t.Helper()
	o := e.opts
	o.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) { return p, io.NopCloser(nil), nil }
	ctl, err := New(e.c, e.c, e.c.Scheme(), o)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, x := range extra {
		x(ctl.Reconciler)
	}
	return ctl
}

func ptRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}}
}

// ptReconcileWith reconciles once with ctl.
func ptReconcileWith(e *env, ctl *Controller, name string) error {
	_, err := ctl.Reconciler.Reconcile(e.ctx, ptRequest(name))
	return err
}

// ptReconcileAsync reconciles once with ctl in a goroutine, which never fails
// the test: it reports the error on the channel.
func ptReconcileAsync(e *env, ctl *Controller, name string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- ptReconcileWith(e, ctl, name) }()
	return done
}

// ptEndsAfter is the script of a job that ends d after it starts,
// COMPLETED.
func ptEndsAfter(d time.Duration) fake.Script {
	return fake.Script{FailAfter: &d, FailState: "COMPLETED"}
}
