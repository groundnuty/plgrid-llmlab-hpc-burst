package fake

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests for what the Provider's status and the Allocation's status carry
// across passes: a later hard end, the conditions of other writers, the field
// manager, and the queue waits across a restart. Expected times are written out
// by hand from the scripts.

func TestLaterHardEndIsCopiedIntoStatus(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute}) // starts 12:05:00; 30m limit
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 10, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "hardEnd", o.HardEnd, at(12, 35, 0))

	// An administrator raises the limit to 45m: start 12:05:00 + 45m.
	e.batch.SetTimeLimit("998", 45*time.Minute)
	e.syncAt(at(12, 20, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "hardEnd", o.HardEnd, at(12, 50, 0))
	wantMT(t, "startTime", o.StartTime, at(12, 5, 0)) // the start does not move

	e.syncAt(at(12, 36, 0)) // past the old hard end, which no longer ends the job
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "hardEnd", o.HardEnd, at(12, 50, 0))

	e.syncAt(at(12, 50, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationEnded)
	wantMT(t, "endTime", o.EndTime, at(12, 50, 0))
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1", n)
	}
}

// applyDialHome plays the lifecycle controller: it sets DialHomeUnreachable on
// the named Provider through the contract's own server-side apply object, under
// its field manager.
func applyDialHome(t *testing.T, c client.Client, providerName string, cond metav1.Condition) {
	t.Helper()
	u, err := v1alpha1.DialHomeUnreachableApply(providerName, cond)
	if err != nil {
		t.Fatal(err)
	}
	var prov v1alpha1.Provider
	if err := c.Get(context.Background(), client.ObjectKey{Name: providerName}, &prov); err != nil {
		t.Fatal(err)
	}
	// The fake client, unlike the API server, wants a resourceVersion on an
	// apply to the status subresource.
	u.SetResourceVersion(prov.ResourceVersion)
	if err := c.Status().Apply(context.Background(), client.ApplyConfigurationFromUnstructured(u),
		client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership); err != nil {
		t.Fatalf("apply DialHomeUnreachable: %v", err)
	}
}

// findDialHome returns the Provider's DialHomeUnreachable condition, and fails
// the test when it is gone.
func findDialHome(t *testing.T, prov *v1alpha1.Provider) metav1.Condition {
	t.Helper()
	c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	if c == nil {
		t.Fatalf("the Provider's DialHomeUnreachable condition is gone: %+v", prov.Status.Conditions)
	}
	return *c
}

func (e *env) applyDialHome(cond metav1.Condition) {
	e.t.Helper()
	applyDialHome(e.t, e.c, testProvider, cond)
}

func (e *env) dialHome() metav1.Condition {
	e.t.Helper()
	return findDialHome(e.t, e.provider())
}

func TestProviderStatusKeepsAConditionTheFakeDoesNotOwn(t *testing.T) {
	e := newEnv(t, withManagedFields)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00

	set := metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example: the agent did not register",
		LastTransitionTime: *mt(at(12, 0, 30)), ObservedGeneration: 1,
	}
	e.applyDialHome(set)
	want := e.dialHome()
	if want.Status != metav1.ConditionTrue || want.Reason != "NoRegistration" || !want.LastTransitionTime.Time.Equal(at(12, 0, 30)) {
		t.Fatalf("setup: the condition as stored = %+v", want)
	}

	boom := errors.New("example transport failure")
	steps := []struct {
		when time.Time
		fail error
	}{
		{at(12, 1, 0), nil},
		{at(12, 5, 0), nil}, // the job starts: a queue wait is written
		{at(12, 6, 0), nil},
		{at(12, 7, 0), boom}, // a failing read: Ready is not refreshed
	}
	for _, st := range steps {
		before := e.provider().ResourceVersion
		e.batch.FailReads(st.fail)
		e.syncAt(st.when)
		prov := e.provider()
		if prov.ResourceVersion == before {
			t.Fatalf("at %s the Provider's status was not written, so the test proves nothing about survival", st.when.Format("15:04:05"))
		}
		if got := e.dialHome(); !apiequality.Semantic.DeepEqual(got, want) {
			t.Fatalf("at %s DialHomeUnreachable = %+v, want it unchanged: %+v", st.when.Format("15:04:05"), got, want)
		}
		if apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady) == nil {
			t.Fatalf("at %s the fake's own Ready condition is missing", st.when.Format("15:04:05"))
		}
	}
	e.batch.FailReads(nil)

	// The lifecycle controller changes its condition. The fake keeps the new
	// value and does not write the old one back.
	later := set
	later.Status, later.Reason, later.Message = metav1.ConditionFalse, "Registered", "example: the agent registered"
	later.LastTransitionTime = *mt(at(12, 8, 0))
	e.applyDialHome(later)
	e.syncAt(at(12, 9, 0))
	got := e.dialHome()
	if got.Status != metav1.ConditionFalse || got.Reason != "Registered" || !got.LastTransitionTime.Time.Equal(at(12, 8, 0)) {
		t.Fatalf("after the other writer's change DialHomeUnreachable = %+v, want False, Registered, 12:08:00", got)
	}
}

// TestProviderManagedFieldsNameOnlyTheTwoWriters checks the stored object: the
// fake's writes carry the Provider controller's field manager, next to the
// lifecycle controller's apply. The fake client has no CRD schema, so it keeps
// conditions as one atomic list and cannot show the per-entry ownership a real
// API server records (listType=map); that part is not tested here.
func TestProviderManagedFieldsNameOnlyTheTwoWriters(t *testing.T) {
	e := newEnv(t, withManagedFields)
	e.newAlloc("a1", testProvider)
	e.sync()
	e.applyDialHome(metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example", LastTransitionTime: *mt(at(12, 0, 30)),
	})
	e.syncAt(at(12, 1, 0))
	e.syncAt(at(12, 2, 0))

	var sawLifecycle, sawProvider bool
	for _, mf := range e.provider().ManagedFields {
		switch mf.Manager {
		case v1alpha1.FieldManagerLifecycle:
			sawLifecycle = mf.Operation == metav1.ManagedFieldsOperationApply
		case v1alpha1.FieldManagerProvider:
			sawProvider = mf.Operation == metav1.ManagedFieldsOperationUpdate
		default:
			t.Fatalf("a write by field manager %q on the Provider; want only %q and %q",
				mf.Manager, v1alpha1.FieldManagerProvider, v1alpha1.FieldManagerLifecycle)
		}
	}
	if !sawLifecycle || !sawProvider {
		t.Fatalf("managed fields = %+v, want an apply by the lifecycle controller and an update by the Provider's", e.provider().ManagedFields)
	}
}

type write struct {
	op, kind string
	manager  string
	deleting bool
}

func TestEveryWriteNamesTheProviderFieldManager(t *testing.T) {
	e := newEnv(t)
	var writes []write
	e.ctl.Client = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			var o client.UpdateOptions
			o.ApplyOptions(opts)
			writes = append(writes, write{"Update", fmt.Sprintf("%T", obj), o.FieldManager, obj.GetDeletionTimestamp() != nil})
			return c.Update(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			var o client.SubResourceUpdateOptions
			o.ApplyOptions(opts)
			writes = append(writes, write{"Update/" + sub, fmt.Sprintf("%T", obj), o.FieldManager, obj.GetDeletionTimestamp() != nil})
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.batch.FailNextLiftHold(errors.New("example: scontrol failed"))
	e.newAlloc("a1", testProvider)
	_ = e.ctl.Sync(context.Background()) // the first lift fails: that Sync errors
	e.syncAt(at(12, 1, 0))
	e.syncAt(at(12, 6, 0))
	e.del("a1")
	e.syncAt(at(12, 7, 0))
	if !e.gone("a1") {
		t.Fatal("setup: the Allocation was not released")
	}

	seen := map[string]bool{}
	for _, w := range writes {
		if w.manager != v1alpha1.FieldManagerProvider {
			t.Fatalf("write %+v: field manager %q, want %q", w, w.manager, v1alpha1.FieldManagerProvider)
		}
		key := w.op + " " + w.kind
		if w.deleting {
			key += " (deleting)"
		}
		seen[key] = true
	}
	for _, want := range []string{
		"Update *v1alpha1.Allocation",
		"Update/status *v1alpha1.Allocation",
		"Update/status *v1alpha1.Provider",
		"Update *v1alpha1.Allocation (deleting)", // the finalizer's removal
	} {
		if !seen[want] {
			t.Fatalf("no write %q went through the client; saw %v", want, seen)
		}
	}
}

func TestQueueWaitIsRecordedByARestartedControllerFromAccountingTimes(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute}) // submitted 12:00:00, starts 12:05:00
	e.newAlloc("a1", testProvider)
	e.sync() // pending at 12:00:00; no wait yet

	// The controller is down while the job starts. A fresh Controller, on the
	// same client, makes the first observation of the started job at 12:20:00.
	e.ctl = &Controller{Client: e.c, Provider: testProvider, Batch: e.batch, Clock: e.clock}
	e.syncAt(at(12, 20, 0))
	waits := e.provider().Status.QueueWaits
	if len(waits) != 1 {
		t.Fatalf("queue waits = %+v, want exactly one", waits)
	}
	wantMT(t, "wait submit", &waits[0].SubmitTime, at(12, 0, 0))
	wantMT(t, "wait start", &waits[0].StartTime, at(12, 5, 0)) // the job's start, not the 12:20:00 of the observation
	if waits[0].JobID != "998" || waits[0].Wait() != 5*time.Minute {
		t.Fatalf("queue wait = %+v, want job 998 and 5m", waits[0])
	}
}

func TestQueueWaitsAddNoDuplicateAndKeepTheOldAcrossARestart(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute}, Script{QueueWait: 10 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()               // job 998 submitted 12:00:00
	e.syncAt(at(12, 6, 0)) // it started 12:05:00
	if got := waitIDs(e.provider()); len(got) != 1 || got[0] != "998" {
		t.Fatalf("setup: queue wait job IDs = %v, want [998]", got)
	}

	e.ctl = &Controller{Client: e.c, Provider: testProvider, Batch: e.batch, Clock: e.clock} // restart
	e.syncAt(at(12, 7, 0))                                                                   // observes 998 again
	if got := waitIDs(e.provider()); len(got) != 1 || got[0] != "998" {
		t.Fatalf("after the restart queue wait job IDs = %v, want [998] and no duplicate", got)
	}

	e.newAlloc("a2", testProvider) // job 999 submitted 12:07:00, starts 12:17:00
	e.sync()
	e.ctl = &Controller{Client: e.c, Provider: testProvider, Batch: e.batch, Clock: e.clock} // another restart
	e.syncAt(at(12, 18, 0))
	waits := e.provider().Status.QueueWaits
	if got := waitIDs(e.provider()); len(got) != 2 || got[0] != "998" || got[1] != "999" {
		t.Fatalf("queue wait job IDs = %v, want [998 999]: the old wait kept, the new one added once", got)
	}
	wantMT(t, "old wait start", &waits[0].StartTime, at(12, 5, 0))
	wantMT(t, "new wait submit", &waits[1].SubmitTime, at(12, 7, 0))
	wantMT(t, "new wait start", &waits[1].StartTime, at(12, 17, 0))
}
