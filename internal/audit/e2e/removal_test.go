package e2e

// (c), the Keep link after the Operation's removal (issue #660, PR B; HBEP-10, Removal and Audit). Agent B's approved End kept md2's
// started Allocation, which its plan lists as a Keep that needs approval. The controller's clock then moves 7 days: the removal takes
// both finished Operations, and retention, past its 30 minutes of idle, releases the kept Allocation with its own cause and
// deletes it. The plan is then found only in the log's FILES, since its Operation is gone from the API server: the join from the
// plan's Keep to the release is the Allocation's UID.

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// removalAfter is how long a finished Operation stays (HBEP-10, Removal), and the controller's clock is moved a little further.
const removalAfter = 7 * 24 * time.Hour

// checkKeepLink is (c). The Operations are asserted GONE (NotFound) before the log is searched, since a link found while the
// Operation still exists proves nothing about the removal. Before the clock moves each Operation is asserted Done, finished, and
// free of the operation-unfinished finalizer, so that the delete of it cannot wait on one: a removal that left it deleting would
// not be NotFound. That the finalizer was ever on is read from the log's files, not assumed (finalizerCycle): the lifecycle
// identity's successful update that added it at the commit, and its later successful update that took it off at the finish, with
// the Operation shown finished in its body, both before the removal. Without them an Operation that never carried the finalizer
// would make "free of it" true of anything, and one released while still Executing would pass.
func (s *scenario) checkKeepLink(t *testing.T) {
	t.Helper()
	w, rd, uid, name := s.world, s.rd, s.keptAlloc.UID, s.keptAlloc.Name
	ops := []string{"end-a", "end-b"}
	for _, n := range ops {
		op := w.getOperation(n)
		if op.Status.Phase != v1alpha1.OperationPhaseDone || op.Status.FinishedAt == nil {
			t.Fatalf("Operation %s is %q before the clock moves, want Done and finished", n, op.Status.Phase)
		}
		eventually(t, 30*time.Second, "Operation "+n+"'s finalizer is off from its finish", func() (bool, string) {
			cur := w.getOperation(n)
			return !controllerutil.ContainsFinalizer(cur, v1alpha1.FinalizerOperationUnfinished), fmt.Sprintf("finalizers %v", cur.Finalizers)
		})
	}

	w.clock.Advance(removalAfter + time.Hour)
	if err := w.refreshProvider(s.prov); err != nil {
		t.Fatal(err)
	}
	// The controllers read objects when they change, not when the clock does: touch each Operation and md2, as a person's edit
	// would, so that each is reconciled at the new time.
	touch := func(obj client.Object) {
		eventually(t, 30*time.Second, "touch "+obj.GetName(), func() (bool, string) {
			err := w.admin.c.Patch(w.ctx, obj, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"example.test/touched":"after the clock moved"}}}`)))
			return err == nil || apierrors.IsNotFound(err), fmt.Sprintf("%v", err)
		})
	}
	for _, n := range ops {
		touch(&v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Namespace: exampleNS, Name: n}})
	}
	touch(&v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: exampleNS, Name: "md2"}})

	for _, n := range ops {
		eventually(t, 90*time.Second, "Operation "+n+" removed by the controller (NotFound, not deleting)", func() (bool, string) {
			var op v1alpha1.Operation
			err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: n}, &op)
			return apierrors.IsNotFound(err), fmt.Sprintf("get: %v", err)
		})
	}
	eventually(t, 90*time.Second, "the kept Allocation released", func() (bool, string) {
		var a v1alpha1.Allocation
		err := w.admin.c.Get(w.ctx, client.ObjectKeyFromObject(&s.keptAlloc), &a)
		return apierrors.IsNotFound(err) || !a.DeletionTimestamp.IsZero(), fmt.Sprintf("get: %v", err)
	})

	set := w.waitLog(rd, "(c) the removals and the kept Allocation's release", func(l *logSet) bool {
		return len(l.find("delete", "operations", "", "end-a", w.lc.user)) > 0 && len(l.find("delete", "operations", "", "end-b", w.lc.user)) > 0 &&
			len(l.find("delete", "allocations", "", name, w.lc.user)) > 0
	})
	var removal *auditEvent
	for _, n := range ops {
		evs := set.find("delete", "operations", "", n, w.lc.user)
		if len(evs) != 1 || evs[0].Level != "RequestResponse" {
			t.Errorf("the removal of Operation %s is in the log as %v, want one delete by the lifecycle identity at RequestResponse", n, evs)
			continue
		}
		if n == "end-b" {
			removal = &evs[0]
		}
		released, problems := finalizerCycle(t, set, w.lc.user, n)
		for _, p := range problems {
			t.Errorf("Operation %s: %s", n, p)
		}
		if released != nil && !released.RequestReceivedTimestamp.Before(evs[0].RequestReceivedTimestamp) {
			t.Errorf("Operation %s: the finalizer came off at %s, not before its removal at %s", n, released.RequestReceivedTimestamp, evs[0].RequestReceivedTimestamp)
		}
	}
	// The plan, as the log holds it: the Allocation's UID is a Keep that needs approval, with its cost.
	var plan *auditEvent
	for _, e := range set.find("update", "operations", "status", "end-b", w.lc.user) {
		e := e
		if p := e.requestOperation(t).Status.Plan; p != nil {
			for _, tg := range p.Targets {
				if tg.UID == uid && tg.Kind == v1alpha1.TargetKindAllocation && tg.Effect == v1alpha1.PlanEffectKeep && tg.NeedsApproval && tg.Cost != nil && plan == nil {
					plan = &e
				}
			}
		}
	}
	if plan == nil {
		t.Fatalf("no logged status write of end-b lists the Allocation %s as Keep with needsApproval and its cost", uid)
	}
	// The release: the cause written, then the delete, at Request level, by the lifecycle identity, for this UID.
	var cause, del *auditEvent
	for _, e := range set.find("update", "allocations", "", name, w.lc.user) {
		e := e
		var a v1alpha1.Allocation
		if jsonUnmarshal(e.RequestObject, &a) == nil && a.UID == uid && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" && cause == nil {
			cause = &e
		}
	}
	for _, e := range set.find("delete", "allocations", "", name, w.lc.user) {
		e := e
		if deletesUID(e, uid) && del == nil {
			del = &e
		}
	}
	if cause == nil || del == nil {
		t.Fatalf("the release of the kept Allocation %s (%s): cause write %v, delete by that UID %v in the log, want both", name, uid, cause != nil, del != nil)
	}
	var a v1alpha1.Allocation
	_ = jsonUnmarshal(cause.RequestObject, &a)
	text := a.Annotations[v1alpha1.AnnotationReleaseCause]
	if _, _, _, ok := v1alpha1.ParseReleaseCauseOperation(text); ok || text != "released when idle, after 30m0s" {
		t.Errorf("the logged release cause is %q, want retention's own, released when idle after 30m0s, which names no Operation", text)
	}
	if cause.Level != "Request" || del.Level != "Request" {
		t.Errorf("the release is logged at %s (cause) and %s (delete), want Request for both", cause.Level, del.Level)
	}
	if !plan.RequestReceivedTimestamp.Before(cause.RequestReceivedTimestamp) || cause.RequestReceivedTimestamp.After(del.RequestReceivedTimestamp) {
		t.Errorf("the order in the log is wrong: plan %s, cause %s, delete %s; want the plan, then the cause, then the delete", plan.RequestReceivedTimestamp, cause.RequestReceivedTimestamp, del.RequestReceivedTimestamp)
	}
	if removal != nil && !plan.RequestReceivedTimestamp.Before(removal.RequestReceivedTimestamp) {
		t.Errorf("the plan (%s) is logged after the removal of its Operation (%s)", plan.RequestReceivedTimestamp, removal.RequestReceivedTimestamp)
	}
}

// deletesUID reports whether the logged delete's request body, the DeleteOptions, carries a precondition on the UID: the
// lifecycle controller deletes an Allocation only with its UID, so a delete that names another object is not this release.
func deletesUID(e auditEvent, uid types.UID) bool {
	var opts struct {
		Preconditions struct {
			UID string `json:"uid"`
		} `json:"preconditions"`
	}
	return len(e.RequestObject) > 0 && json.Unmarshal(e.RequestObject, &opts) == nil && opts.Preconditions.UID == string(uid)
}

// finalizerCycle reads the operation-unfinished finalizer's life for one Operation from the log's files. It returns the update
// that took the finalizer off (nil when there is none) and what is wrong, one text for each problem; none is wrong when the
// finalizer was added at the commit and taken off at the finish. Only the lifecycle identity's SUCCESSFUL requests count (a
// response code in 2xx: a 409 Conflict that carries the finalizer added nothing, and one that lacks it removed nothing), and the
// order is that of the requests' own receive times (requestReceivedTimestamp, the audit's stamp of when the API server got each
// request; the controller sends these one after the other from one goroutine, so each is received after the one before it):
//   - the COMMIT is the first successful write of the Operation's status whose logged body sets phase Executing with committed
//     targets (commitOperation records the frozen plan in one status write);
//   - the FINISH is the first successful write of its status whose body sets finishedAt;
//   - the finalizer is ADDED at the commit: the first successful update of the Operation whose body carries it, received after the
//     commit's write and before the finish's (keepOperationUntilFinished runs right after the commit, in the same pass);
//   - it is taken off AT THE FINISH: the first successful update after the add whose body does not carry it must be received
//     after the finish's write, and its own logged body must show the Operation finished (status.finishedAt set, or phase Done,
//     Refused or Stale, the phases releaseOperationFinalizer takes as finished; PartlyFailed counts only with finishedAt). A
//     removal while the Operation is still Executing fails on that body, whatever the Operation does after it.
func finalizerCycle(t testing.TB, set *logSet, lifecycleUser, name string) (released *auditEvent, problems []string) {
	t.Helper()
	succeeded := func(e auditEvent) bool { return e.ResponseStatus.Code >= 200 && e.ResponseStatus.Code < 300 }
	var commit, finish, added *auditEvent
	for _, e := range set.find("update", "operations", "status", name, lifecycleUser) {
		e := e
		if !succeeded(e) {
			continue
		}
		op := e.requestOperation(t)
		if commit == nil && op.Status.Phase == v1alpha1.OperationPhaseExecuting && len(op.Status.Committed) > 0 {
			commit = &e
		}
		if finish == nil && op.Status.FinishedAt != nil {
			finish = &e
		}
	}
	var releasedOp *v1alpha1.Operation
	for _, e := range set.find("update", "operations", "", name, lifecycleUser) {
		e := e
		var op v1alpha1.Operation
		if !succeeded(e) || len(e.RequestObject) == 0 || jsonUnmarshal(e.RequestObject, &op) != nil {
			continue
		}
		switch has := controllerutil.ContainsFinalizer(&op, v1alpha1.FinalizerOperationUnfinished); {
		case has && added == nil:
			added = &e
		case !has && added != nil && released == nil:
			released, releasedOp = &e, &op
		}
	}
	finalizer := v1alpha1.FinalizerOperationUnfinished
	if commit == nil {
		problems = append(problems, "no successful status write by the lifecycle identity sets phase Executing with committed targets, so the commit is not in the log")
	}
	if finish == nil {
		problems = append(problems, "no successful status write by the lifecycle identity sets finishedAt, so the finish is not in the log")
	}
	switch {
	case added == nil:
		problems = append(problems, "no successful update by the lifecycle identity carries the "+finalizer+" finalizer: it was never on, so its absence before the removal proves nothing")
	case commit != nil && !commit.RequestReceivedTimestamp.Before(added.RequestReceivedTimestamp):
		problems = append(problems, fmt.Sprintf("the finalizer was added at %s, not after the commit at %s", added.RequestReceivedTimestamp, commit.RequestReceivedTimestamp))
	case finish != nil && !added.RequestReceivedTimestamp.Before(finish.RequestReceivedTimestamp):
		problems = append(problems, fmt.Sprintf("the finalizer was added at %s, not before the finish at %s", added.RequestReceivedTimestamp, finish.RequestReceivedTimestamp))
	}
	if added != nil && released == nil {
		problems = append(problems, "no successful update by the lifecycle identity takes the "+finalizer+" finalizer off after the one that added it")
	}
	if released != nil {
		finished := releasedOp.Status.FinishedAt != nil || slices.Contains([]v1alpha1.OperationPhase{
			v1alpha1.OperationPhaseDone, v1alpha1.OperationPhaseRefused, v1alpha1.OperationPhaseStale}, releasedOp.Status.Phase)
		if !finished {
			problems = append(problems, fmt.Sprintf("the first update without the finalizer (%s) is made while the Operation is not finished: its logged body has phase %q and no finishedAt", released.RequestReceivedTimestamp, releasedOp.Status.Phase))
		}
		if finish != nil && !finish.RequestReceivedTimestamp.Before(released.RequestReceivedTimestamp) {
			problems = append(problems, fmt.Sprintf("the finalizer came off at %s, not after the finish at %s", released.RequestReceivedTimestamp, finish.RequestReceivedTimestamp))
		}
	}
	return released, problems
}
