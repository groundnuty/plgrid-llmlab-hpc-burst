package providerctl

// The Provider's status (HBEP-4, Provider status; docs/development.md, Single
// writer) and the field managers of the controller's writes: the queue waits,
// Ready and the time of the last successful queue read, the conditions of other
// writers, and managedFields. Expected values come from HBEP-4, api/v1alpha1
// (MaxQueueWaits, the field managers) and the doc comment of
// pkg/provider/fake/controller.go. Counts come from the fake's call log.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func ptProvider(e *env) *v1alpha1.Provider {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &p); err != nil {
		e.t.Fatal(err)
	}
	return &p
}

// ptSync writes the Provider's status once.
func ptSync(e *env) *v1alpha1.Provider {
	e.t.Helper()
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		e.t.Fatalf("SyncStatus: %v", err)
	}
	return ptProvider(e)
}

func ptWaitIDs(p *v1alpha1.Provider) []string {
	var out []string
	for _, w := range p.Status.QueueWaits {
		out = append(out, w.JobID)
	}
	return out
}

// Each started job adds exactly one wait, with times from the job's own
// submit and start (HBEP-4, Provider status: "never from when the controller
// saw the change"); a repeated observe adds none; waits survive a restart. The
// expected times are the fake's records of the job and the script's 3 minutes.
func TestQueueWaitIsAddedOncePerStartedJob(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{QueueWait: 3 * time.Minute})
	e.newAlloc("a")
	e.run("a", 4) // submitted and lifted at 12:00:00; starts at 12:03:00
	if p := ptSync(e); len(p.Status.QueueWaits) != 0 {
		t.Fatalf("waits %v before the job started, want none", ptWaitIDs(p))
	}

	// Seen well after the start: the wait is still the job's own 3 minutes.
	ptStep(e, "a", 7*time.Minute)
	if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
		t.Fatalf("precondition: phase %s, want Running", p)
	}
	p := ptSync(e)
	if len(p.Status.QueueWaits) != 1 {
		t.Fatalf("waits %v, want exactly one", ptWaitIDs(p))
	}
	w := p.Status.QueueWaits[0]
	j, _ := e.batch.Job(ptFirstJob)
	if w.JobID != ptFirstJob || !w.SubmitTime.Time.Equal(j.SubmitTime) || !w.StartTime.Time.Equal(*j.StartTime) {
		t.Errorf("wait %+v, want job %s submit %s start %s (the fake's records)", w, ptFirstJob, j.SubmitTime, j.StartTime)
	}
	if !w.SubmitTime.Time.Equal(at(12, 0, 0)) || !w.StartTime.Time.Equal(at(12, 3, 0)) || w.Wait() != 3*time.Minute {
		t.Errorf("wait %+v, want 12:00:00 to 12:03:00 (3m)", w)
	}

	// A repeated observe adds none.
	for i := 0; i < 3; i++ {
		ptStep(e, "a", time.Minute)
		ptSync(e)
	}
	if got := ptWaitIDs(ptProvider(e)); len(got) != 1 {
		t.Errorf("waits %v after repeated observes, want still one", got)
	}

	// Waits survive a restart, and the restarted controller adds nothing.
	e.build()
	e.run("a", 2)
	if got := ptWaitIDs(ptSync(e)); len(got) != 1 || got[0] != ptFirstJob {
		t.Errorf("waits %v after a restart, want [%s]", got, ptFirstJob)
	}
}

// A 21st wait drops the oldest (api/v1alpha1.MaxQueueWaits = 20; HBEP-4:
// "a new entry beyond the cap drops the oldest"). Twenty-one real Allocations
// start one minute apart.
func TestTwentyFirstQueueWaitDropsTheOldest(t *testing.T) {
	e := newEnv(t)
	var ids []string
	for i := 1; i <= v1alpha1.MaxQueueWaits+1; i++ {
		name := fmt.Sprintf("a%02d", i)
		e.newAlloc(name)
		e.run(name, 4)
		ids = append(ids, ptJobID(e, name))
		ptSync(e)
		e.clock.Advance(time.Minute)
	}
	got := ptWaitIDs(ptProvider(e))
	if len(got) != v1alpha1.MaxQueueWaits {
		t.Fatalf("%d waits, want %d", len(got), v1alpha1.MaxQueueWaits)
	}
	for i, id := range ids[1:] {
		if got[i] != id {
			t.Fatalf("waits %v, want the jobs of Allocations 2 to 21 in order (%v)", got, ids[1:])
		}
	}
	for _, g := range got {
		if g == ids[0] {
			t.Errorf("the oldest wait (job %s) is still there", ids[0])
		}
	}
}

// A failed write of the wait to the Provider's status leaves
// queueWaitRecorded false and is retried at the next observe; the wait is added
// once (docs/development.md, Queue waits; fake.Controller's doc comment).
func TestQueueWaitIsRetriedAfterAFailedProviderWrite(t *testing.T) {
	fails := 1
	e := newEnv(t, func(e *env) {
		e.c = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if p, ok := obj.(*v1alpha1.Provider); ok && sub == "status" && len(p.Status.QueueWaits) > 0 && fails > 0 {
					fails--
					return errors.New("scripted: the Provider status write failed")
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		})
	})
	e.newAlloc("a")
	e.run("a", 4)
	if fails != 0 {
		t.Fatal("precondition: the scripted failure was not reached")
	}
	if got := ptWaitIDs(ptProvider(e)); len(got) != 1 || got[0] != ptFirstJob {
		t.Fatalf("waits %v after the retry, want [%s]", got, ptFirstJob)
	}
	if !e.get("a").Status.AtProvider.QueueWaitRecorded {
		t.Error("queueWaitRecorded is false after the wait was written")
	}
	for i := 0; i < 3; i++ {
		e.clock.Advance(time.Minute)
		e.run("a", 1)
	}
	if got := ptWaitIDs(ptProvider(e)); len(got) != 1 {
		t.Errorf("waits %v after more observes, want one: no duplicate", got)
	}
}

// The wait is written within the observe that first sees the job started, so
// a restart before any status sync loses nothing, and the restarted
// controller adds nothing (queueWaitRecorded lives in the status).
func TestQueueWaitSurvivesARestartBeforeTheSync(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	e.run("a", 4)
	if a := e.get("a"); a.Status.AtProvider.StartTime == nil || !a.Status.AtProvider.QueueWaitRecorded {
		t.Fatalf("precondition: start %v, queueWaitRecorded %v", a.Status.AtProvider.StartTime, a.Status.AtProvider.QueueWaitRecorded)
	}
	if got := ptWaitIDs(ptProvider(e)); len(got) != 1 || got[0] != ptFirstJob {
		t.Fatalf("waits %v before any status sync, want [%s]", got, ptFirstJob)
	}
	e.build() // the restart
	e.run("a", 2)
	if got := ptWaitIDs(ptSync(e)); len(got) != 1 || got[0] != ptFirstJob {
		t.Errorf("waits %v after a restart, want [%s]", got, ptFirstJob)
	}
}

// DialHomeUnreachable is the lifecycle controller's (docs/development.md,
// Single writer): the provider's status writer keeps it, and the managedFields
// of the Provider show the apply by the lifecycle controller next to the
// update by the Provider's.
func TestProviderStatusKeepsAnotherWritersCondition(t *testing.T) {
	e := newEnv(t)
	ptSync(e)
	cond := metav1.Condition{Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example", LastTransitionTime: metav1.NewTime(at(12, 0, 30))}
	u, err := v1alpha1.DialHomeUnreachableApply(testProvider, cond)
	if err != nil {
		t.Fatal(err)
	}
	u.SetResourceVersion(ptProvider(e).ResourceVersion) // the fake client wants it on an apply to status
	if err := e.c.Status().Apply(e.ctx, client.ApplyConfigurationFromUnstructured(u),
		client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership); err != nil {
		t.Fatalf("apply DialHomeUnreachable: %v", err)
	}
	for i := 0; i < 3; i++ {
		e.clock.Advance(30 * time.Second)
		ptSync(e)
	}
	got := apimeta.FindStatusCondition(ptProvider(e).Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	if got == nil || got.Status != metav1.ConditionTrue || got.Reason != "NoRegistration" || !got.LastTransitionTime.Time.Equal(at(12, 0, 30)) {
		t.Fatalf("DialHomeUnreachable = %+v after syncs, want the lifecycle controller's, unchanged", got)
	}
	if c := apimeta.FindStatusCondition(ptProvider(e).Status.Conditions, v1alpha1.ConditionProviderReady); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %+v, want True alongside the other writer's condition", c)
	}
}

// Ready True with reason Polled after a successful poll, and the time of the
// last successful read (HBEP-4, Provider status). After the poll starts to fail
// the time stops advancing; Ready is left as it was until the last read is older
// than v1alpha1.ProviderReadyStaleness (5 min: still True at exactly 5 min),
// then False, ReadsFailing (docs/development.md, Provider readiness); when the
// poll recovers, True again.
func TestProviderReadyFollowsThePollAndGoesStale(t *testing.T) {
	e := newEnv(t)
	p := ptSync(e)
	c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPolled {
		t.Fatalf("Ready = %+v after a successful poll, want True/Polled", c)
	}
	if r := p.Status.LastSuccessfulStatusRead; r == nil || !r.Time.Equal(at(12, 0, 0)) {
		t.Fatalf("lastSuccessfulStatusRead %v, want 12:00:00", r)
	}

	e.platform.mu.Lock()
	e.platform.pollErr = errors.New("scripted: squeue failed")
	e.platform.mu.Unlock()
	for _, d := range []time.Duration{time.Minute, time.Minute, time.Minute, time.Minute, time.Minute} {
		e.clock.Advance(d)
		p = ptSync(e)
		if r := p.Status.LastSuccessfulStatusRead; r == nil || !r.Time.Equal(at(12, 0, 0)) {
			t.Fatalf("lastSuccessfulStatusRead %v after a failed poll, want it stuck at 12:00:00", r)
		}
		if c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady); c.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %s/%s at %s of failure, want True within the staleness bound", c.Status, c.Reason, e.clock.Now().Sub(at(12, 0, 0)))
		}
	}
	e.clock.Advance(time.Second) // 5 min 1 s since the last good read
	p = ptSync(e)
	if c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady); c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonReadsFailing {
		t.Errorf("Ready = %s/%s past the staleness bound, want False/ReadsFailing", c.Status, c.Reason)
	}
	if r := p.Status.LastSuccessfulStatusRead; r == nil || !r.Time.Equal(at(12, 0, 0)) {
		t.Errorf("lastSuccessfulStatusRead %v, want 12:00:00", r)
	}

	e.platform.mu.Lock()
	e.platform.pollErr = nil
	e.platform.mu.Unlock()
	p = ptSync(e)
	if c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPolled {
		t.Errorf("Ready = %s/%s after the poll recovered, want True/Polled", c.Status, c.Reason)
	}
	if r := p.Status.LastSuccessfulStatusRead; r == nil || !r.Time.Equal(e.clock.Now()) {
		t.Errorf("lastSuccessfulStatusRead %v, want %s", r, e.clock.Now())
	}
}

// ptOwns reports whether the field set of a managedFields entry holds the path.
func ptOwns(t *testing.T, mf metav1.ManagedFieldsEntry, path ...string) bool {
	t.Helper()
	if mf.FieldsV1 == nil {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(mf.FieldsV1.Raw, &m); err != nil {
		t.Fatal(err)
	}
	var cur any = m
	for _, p := range path {
		next, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		if cur, ok = next[p]; !ok {
			return false
		}
	}
	return true
}

// The writers (api/v1alpha1/writers.go; docs/development.md, Single writer):
// the Provider's controller writes with the field manager hpcburst-provider,
// and it is the writer of the Allocation's record annotations and status and of
// the Provider's status. The test's own creation of the Allocation is the only
// other writer, and it owns the spec alone. (The fake client has no schema, so
// it records one entry per manager and operation, not per subresource.)
func TestProviderControllerIsTheWriterOfAnnotationsAndStatus(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	e.run("a", 4)
	ptSync(e)

	a := e.get("a")
	var sawProvider bool
	for _, mf := range a.ManagedFields {
		switch mf.Manager {
		case v1alpha1.FieldManagerProvider:
			sawProvider = true
			for _, k := range []string{v1alpha1.AnnotationSubmission, v1alpha1.AnnotationJobID, v1alpha1.AnnotationHoldLifted} {
				if !ptOwns(t, mf, "f:metadata", "f:annotations", "f:"+k) {
					t.Errorf("%s does not own annotation %s: %s", mf.Manager, k, mf.FieldsV1.Raw)
				}
			}
			if !ptOwns(t, mf, "f:status") {
				t.Errorf("%s does not own the status: %s", mf.Manager, mf.FieldsV1.Raw)
			}
		default:
			if ptOwns(t, mf, "f:status") || ptOwns(t, mf, "f:metadata", "f:annotations", "f:"+v1alpha1.AnnotationJobID) {
				t.Errorf("manager %q owns the status or the record annotations: %s", mf.Manager, mf.FieldsV1.Raw)
			}
		}
	}
	if !sawProvider {
		t.Errorf("Allocation managedFields %+v have no entry for %s", a.ManagedFields, v1alpha1.FieldManagerProvider)
	}

	prov := ptProvider(e)
	sawProvider = false
	for _, mf := range prov.ManagedFields {
		if mf.Manager != v1alpha1.FieldManagerProvider {
			continue
		}
		sawProvider = true
		if !ptOwns(t, mf, "f:status", "f:lastSuccessfulStatusRead") || !ptOwns(t, mf, "f:status", "f:queueWaits") {
			t.Errorf("%s does not own the Provider's status fields: %s", mf.Manager, mf.FieldsV1.Raw)
		}
	}
	if !sawProvider {
		t.Errorf("Provider managedFields %+v have no entry for %s", prov.ManagedFields, v1alpha1.FieldManagerProvider)
	}
}

// Astra B7: an outage of accounting alone. The queue listing keeps
// succeeding, every sacct fails, and the Allocation cannot establish its job;
// the reads are failing, so lastSuccessfulStatusRead stops advancing and Ready
// goes False past the staleness bound, as fake.Controller judges a pass with a
// failed read. The control has no outage: Ready stays True and ProviderReady
// holds at every pass.
func TestProviderReadyFailsOnAnAccountingOnlyOutage(t *testing.T) {
	for _, outage := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "sacct fails"}[outage], func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			f := &lineFaults{}
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f))
			if err := e.ctl.SyncStatus(e.ctx); err != nil {
				t.Fatal(err)
			}
			if outage {
				f.fail("sacct", -1, slurm.ErrTransport)
			}
			e.newAlloc("alloc-a", 1)
			for i := 0; i < 12; i++ { // 6 min at the poll interval
				clock.Advance(30 * time.Second)
				e.run("alloc-a", 1)
				if err := e.ctl.SyncStatus(e.ctx); err != nil {
					t.Fatal(err)
				}
				var p v1alpha1.Provider
				if err := e.c.Get(e.ctx, client.ObjectKey{Name: e.provider}, &p); err != nil {
					t.Fatal(err)
				}
				if !outage && !v1alpha1.ProviderReady(&p, clock.Now()) {
					t.Fatalf("ProviderReady false at %s with no outage; lastSuccessfulStatusRead %v", clock.Now().Sub(at(12, 0, 0)), p.Status.LastSuccessfulStatusRead)
				}
			}
			st := e.provStatus()
			c := apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionProviderReady)
			if !outage {
				if c == nil || c.Status != metav1.ConditionTrue {
					t.Errorf("Ready %+v with no outage, want True", c)
				}
				return
			}
			if f.failed() == 0 || sl.Count("squeue") == 0 {
				t.Fatalf("precondition: %d sacct failed, %d squeue ran; want both", f.failed(), sl.Count("squeue"))
			}
			if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonReadsFailing {
				t.Errorf("Ready %+v after 6 min of failing accounting reads, want False/ReadsFailing", c)
			}
			if r := st.LastSuccessfulStatusRead; r == nil || !r.Time.Equal(at(12, 0, 0)) {
				t.Errorf("lastSuccessfulStatusRead %v, want it stuck at 12:00:00, the last pass with no failed read", r)
			}
		})
	}
}

// Opus N3: a pass that changes nothing writes nothing. lastSuccessfulStatusRead
// is refreshed once it is statusRefresh old, well inside the staleness bound,
// and a failing pass within the bound, which changes nothing, writes nothing.
func TestProviderStatusSkipsIdenticalWrites(t *testing.T) {
	e := newEnv(t)
	rv := func() string { return ptProvider(e).ResourceVersion }
	ptSync(e)
	first := rv()
	e.clock.Advance(30 * time.Second)
	if p := ptSync(e); p.ResourceVersion != first || !v1alpha1.ProviderReady(p, e.clock.Now()) {
		t.Errorf("resourceVersion %s -> %s on a pass that changed nothing (ProviderReady %v), want no write",
			first, p.ResourceVersion, v1alpha1.ProviderReady(p, e.clock.Now()))
	}
	e.clock.Advance(30 * time.Second)
	p := ptSync(e)
	if p.ResourceVersion == first || p.Status.LastSuccessfulStatusRead == nil || !p.Status.LastSuccessfulStatusRead.Time.Equal(at(12, 1, 0)) {
		t.Errorf("resourceVersion %s, lastSuccessfulStatusRead %v at 12:01:00, want it written once a minute old", p.ResourceVersion, p.Status.LastSuccessfulStatusRead)
	}
	e.platform.mu.Lock()
	e.platform.pollErr = errors.New("scripted: squeue failed")
	e.platform.mu.Unlock()
	before := rv()
	e.clock.Advance(30 * time.Second)
	if p := ptSync(e); p.ResourceVersion != before {
		t.Errorf("resourceVersion %s -> %s on a failing pass inside the staleness bound, want no write", before, p.ResourceVersion)
	}
}
