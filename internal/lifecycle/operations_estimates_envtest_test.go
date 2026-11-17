package lifecycle

// An Operation's status.estimates against a real API server (#661; HBEP-10, The release cost;
// `@hpc-burst-science-agent`, 17 Nov 2026, the S4 ruling). The Provider's controller writes the list by server-side
// apply, under its own field manager, outside the plan; the lifecycle controller carries it over untouched. The rows
// are S4-1, S4-2, S4-3 and S4-5 of the plan, run through the Operations step as operations_envtest_test.go runs it,
// over its control plane and its opsEnv. S4-4 (two Providers' entries survive each other's applies) is in
// api/v1alpha1/e2e.
//
// Each row fails if the estimate sat inside the plan's costs, where a re-plan recomputes it: the first by comparing
// the list before and after a re-plan that really wrote, the second by showing that the apply raises no write at all,
// the third by letting the Provider's apply land between the pass's read and its write. The writes this tree has
// are Planned, AwaitingApproval and Refused; the commit to Executing, Done and the rest come with #664's, and S4-5
// is extended to them when it merges.

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// estimateTime is the probe's start, 2026-11-30T05:48:10Z (#661, 16 Nov 2026).
var estimateTime = time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC)

// applyEstimate is the Provider's controller writing its entry for the Operation's current plan: one entry
// covering the plan's targets on that Provider, by server-side apply under that Provider's field manager. It is
// the writer of #661 (EstimateApply); the test makes no write of its own beyond it.
func (e *opsEnv) applyEstimate(t *testing.T, op *v1alpha1.Operation, provider string, start time.Time) {
	t.Helper()
	var uids []string
	for _, tg := range op.Status.Plan.Targets {
		if tg.Provider == provider {
			uids = append(uids, string(tg.UID))
		}
	}
	if len(uids) == 0 {
		t.Fatalf("the plan has no target on %s", provider)
	}
	st, observed := metav1.NewTime(start), metav1.NewTime(opAt)
	entry := v1alpha1.OperationEstimate{Provider: provider, PlanID: op.Status.Plan.ID, Shape: "aaaaaaaaaaaaaaaa", Targets: uids,
		Start: v1alpha1.TimeFigure{Time: &st, ObservedAt: &observed, Advisory: true}}
	u, err := v1alpha1.EstimateApply(op.Namespace, op.Name, op.UID, provider, []v1alpha1.OperationEstimate{entry})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.admin.c.Status().Apply(e.ctx, client.ApplyConfigurationFromUnstructured(u),
		client.FieldOwner(v1alpha1.FieldManagerProviderEstimate(provider)), client.ForceOwnership); err != nil {
		t.Fatalf("apply %s's estimate: %v", provider, err)
	}
}

// estimatesOf is the Operation's status.estimates as JSON, to compare byte for byte.
func estimatesOf(t *testing.T, op *v1alpha1.Operation) string {
	t.Helper()
	b, err := json.Marshal(op.Status.Estimates)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// sanoWaits is the queue-waits figure of the plan's first costed target on sano, the Provider whose waits the rows move.
func sanoWaits(t *testing.T, op *v1alpha1.Operation) *v1alpha1.QueueWaitsFigure {
	t.Helper()
	for _, tg := range op.Status.Plan.Targets {
		if tg.Provider == "sano" && tg.Cost != nil && tg.Cost.QueueWaits != nil {
			return tg.Cost.QueueWaits
		}
	}
	t.Fatal("the plan has no costed target on sano")
	return nil
}

// stepWith is step over another client, returning the pass's error: the row that races a write needs to see it.
func (e *opsEnv) stepWith(t *testing.T, f *opFixture, now time.Time, c client.Client) error {
	t.Helper()
	p := f.pass()
	p.key.Namespace = rigNS
	p.r, p.ctx, p.now = &Reconciler{Client: c, Reader: e.admin.c, Backend: &Backend{}}, e.ctx, now
	return p.operations()
}

// raceClient runs before once, just before the first status Update it is asked for: the Provider's apply lands
// between the pass's read of the Operation and its write.
type raceClient struct {
	client.Client
	before func()
}

type raceStatus struct {
	client.SubResourceWriter
	r *raceClient
}

func (r *raceClient) Status() client.SubResourceWriter {
	return &raceStatus{SubResourceWriter: r.Client.Status(), r: r}
}

func (s *raceStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if b := s.r.before; b != nil {
		s.r.before = nil
		b()
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestOperationEstimatesSurviveTheLifecycleControllersWrites(t *testing.T) {
	e := newOpsEnv(t)

	// A planned Operation with an estimate from each of the two Providers its targets are on. The first
	// subtest makes the rows below it: S4-2, then S4-1, then S4-5's AwaitingApproval.
	t.Run("S4-2, S4-1 and S4-5 (AwaitingApproval): an estimate keeps the ID, a re-plan keeps the estimate, a confirm keeps it", func(t *testing.T) {
		e.setPolicies(t, globalFor(e.person))
		f := opsFixture(t)
		e.request(t, e.a, "est-1", scaleOp(1))
		e.step(t, f, opAt)
		planned := e.get(t, "est-1")
		id := planned.Status.Plan.ID
		e.applyEstimate(t, planned, "sano", estimateTime)
		e.applyEstimate(t, planned, "helios", estimateTime.Add(2*time.Hour))
		withEstimates := e.get(t, "est-1")
		wantList := estimatesOf(t, withEstimates)
		if len(withEstimates.Status.Estimates) != 2 || !strings.Contains(wantList, `"2026-11-30T05:48:10Z"`) {
			t.Fatalf("the applied estimates are %s, want one per Provider with the probe's time", wantList)
		}

		// S4-2: the estimate changes neither the plan's ID nor the phase, and the pass finds nothing to write:
		// had the estimate sat in the plan's costs, the plan would differ from the stored one and be written.
		if got := withEstimates.Status; got.Plan.ID != id || got.Phase != v1alpha1.OperationPhasePlanned {
			t.Fatalf("after the estimates: plan %s phase %s, want %s Planned", got.Plan.ID, got.Phase, id)
		}
		e.step(t, f, opAt.Add(time.Second))
		quiet := e.get(t, "est-1")
		if quiet.ResourceVersion != withEstimates.ResourceVersion {
			t.Fatalf("a pass after the estimates wrote the Operation: resourceVersion %s -> %s; an estimate must raise no write", withEstimates.ResourceVersion, quiet.ResourceVersion)
		}
		if quiet.Status.Plan.ID != id || estimatesOf(t, quiet) != wantList {
			t.Fatalf("a pass after the estimates changed the plan ID (%s) or the list (%s)", quiet.Status.Plan.ID, estimatesOf(t, quiet))
		}

		// S4-1: a re-plan with the same ID and a moved cost input (a new queue wait). The pass writes, which the
		// resourceVersion and the plan's own figure show, and the list is the same bytes.
		f.pvs["sano"].prov.Status.QueueWaits = waitsOf(6, 9)
		e.step(t, f, opAt.Add(time.Minute))
		replanned := e.get(t, "est-1")
		if replanned.ResourceVersion == quiet.ResourceVersion {
			t.Fatal("the re-plan wrote nothing, so this row proves nothing: the new queue wait did not move the plan's figures")
		}
		if replanned.Status.Plan.ID != id {
			t.Fatalf("the re-plan changed the plan ID %s -> %s", id, replanned.Status.Plan.ID)
		}
		if before, after := sanoWaits(t, quiet), sanoWaits(t, replanned); before.Count == after.Count {
			t.Fatalf("the re-plan did not change the queue-waits figure (%+v -> %+v), so it moved no cost input", before, after)
		}
		if got := estimatesOf(t, replanned); got != wantList {
			t.Fatalf("the re-plan changed status.estimates\n before %s\n after  %s", wantList, got)
		}

		// S4-5, AwaitingApproval: the owner confirms, and the pass writes the new phase with the list as it was.
		e.confirmAs(t, e.a, "est-1", id)
		e.step(t, f, opAt.Add(2*time.Minute))
		awaiting := e.get(t, "est-1")
		if awaiting.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval {
			t.Fatalf("phase %q after the confirm, want AwaitingApproval", awaiting.Status.Phase)
		}
		if got := estimatesOf(t, awaiting); got != wantList {
			t.Fatalf("the write to AwaitingApproval changed status.estimates\n before %s\n after  %s", wantList, got)
		}
	})

	t.Run("S4-5 (Refused): the backstop's write keeps the list", func(t *testing.T) {
		e.setPolicies(t, globalFor(e.person))
		f := opsFixture(t)
		e.request(t, e.a, "est-2", scaleOp(1))
		e.step(t, f, opAt)
		planned := e.get(t, "est-2")
		e.applyEstimate(t, planned, "sano", estimateTime)
		wantList := estimatesOf(t, e.get(t, "est-2"))
		// The admin writes a plan ID the controller never wrote, the owner confirms that ID, and the pass refuses.
		forged := e.get(t, "est-2")
		forged.Status.Plan.ID = "0123456789abcdef"
		if err := e.admin.c.Status().Update(e.ctx, forged); err != nil {
			t.Fatal(err)
		}
		if got := estimatesOf(t, e.get(t, "est-2")); got != wantList {
			t.Fatalf("the forged write changed the list: %s", got)
		}
		confirm := e.get(t, "est-2")
		confirm.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: "0123456789abcdef"}
		if err := e.a.c.Update(e.ctx, confirm); err != nil {
			t.Fatalf("A's confirm of the forged plan's ID: %v", err)
		}
		e.step(t, f, opAt.Add(time.Minute))
		refused := e.get(t, "est-2")
		if refused.Status.Phase != v1alpha1.OperationPhaseRefused {
			t.Fatalf("phase %q, want Refused", refused.Status.Phase)
		}
		if got := estimatesOf(t, refused); got != wantList {
			t.Fatalf("the write to Refused changed status.estimates\n before %s\n after  %s", wantList, got)
		}
	})

	// S4-3: the lifecycle controller reads the Operation, the Provider's apply lands, and the controller's status
	// write, made with the resourceVersion it read, is refused with a conflict: it never writes over the estimate.
	// The next pass reads again and succeeds, with the estimate still there.
	t.Run("S4-3: a status write that races the Provider's apply is a conflict, retried on the next pass", func(t *testing.T) {
		e.setPolicies(t, globalFor(e.person))
		f := opsFixture(t)
		e.request(t, e.a, "est-3", scaleOp(1))
		e.step(t, f, opAt)
		planned := e.get(t, "est-3")

		// A moved cost input, so that the pass has a write to make.
		f.pvs["sano"].prov.Status.QueueWaits = waitsOf(6, 9)
		racing := &raceClient{Client: e.admin.c, before: func() { e.applyEstimate(t, planned, "sano", estimateTime) }}
		err := e.stepWith(t, f, opAt.Add(time.Minute), racing)
		if !apierrors.IsConflict(err) {
			t.Fatalf("the pass whose write raced the Provider's apply returned %v, want a Conflict", err)
		}
		if racing.before != nil {
			t.Fatal("the Provider's apply never ran between the pass's read and its write, so no race happened")
		}
		raced := e.get(t, "est-3")
		if len(raced.Status.Estimates) != 1 || raced.Status.Estimates[0].Provider != "sano" || raced.Status.Estimates[0].Start.Time == nil ||
			!raced.Status.Estimates[0].Start.Time.Equal(&metav1.Time{Time: estimateTime}) {
			t.Fatalf("after the conflict the estimates are %s, want the Provider's entry as it applied it", estimatesOf(t, raced))
		}
		if sanoWaits(t, raced).Count != sanoWaits(t, planned).Count {
			t.Fatal("the conflicted write landed: the plan's queue-waits figure moved")
		}
		wantList := estimatesOf(t, raced)

		// The next pass, with nothing racing it, reads again and writes the new figure with the list as it is.
		if err := e.stepWith(t, f, opAt.Add(2*time.Minute), e.admin.c); err != nil {
			t.Fatalf("the next pass: %v", err)
		}
		retried := e.get(t, "est-3")
		if retried.ResourceVersion == raced.ResourceVersion || sanoWaits(t, retried).Count == sanoWaits(t, planned).Count {
			t.Fatalf("the next pass did not write the figure the conflicted pass wanted")
		}
		if got := estimatesOf(t, retried); got != wantList {
			t.Fatalf("the retry changed status.estimates\n before %s\n after  %s", wantList, got)
		}
	})
}

// operationStatusLiterals are the positions of the composite literals of an OperationStatus in a file.
func operationStatusLiterals(fset *token.FileSet, f *ast.File) []token.Position {
	var out []token.Position
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		switch typ := lit.Type.(type) {
		case *ast.SelectorExpr:
			if typ.Sel.Name == "OperationStatus" {
				out = append(out, fset.Position(lit.Pos()))
			}
		case *ast.Ident:
			if typ.Name == "OperationStatus" {
				out = append(out, fset.Position(lit.Pos()))
			}
		}
		return true
	})
	return out
}

// No write path of the lifecycle controller builds a fresh OperationStatus, which would drop status.estimates: each
// copies the status it read and mutates the copy (setOperationStatus). The check reads the package's own sources.
func TestNoLifecycleWriteBuildsAFreshOperationStatus(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var seen int
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		seen++
		for _, pos := range operationStatusLiterals(fset, f) {
			t.Errorf("%s builds an OperationStatus: a write built from nothing drops status.estimates", pos)
		}
	}
	if seen < 20 {
		t.Fatalf("the check read %d source files, want the package's own", seen)
	}
	// The control: the check does find such a literal, qualified or not.
	for _, src := range []string{
		"package p\nimport v1alpha1 \"x\"\nvar s = v1alpha1.OperationStatus{}\n",
		"package p\ntype OperationStatus struct{}\nvar s = OperationStatus{}\n",
	} {
		cfset := token.NewFileSet()
		f, err := parser.ParseFile(cfset, "control.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := operationStatusLiterals(cfset, f); len(got) != 1 {
			t.Fatalf("the check found %v in %q, want one literal", got, src)
		}
	}
}
