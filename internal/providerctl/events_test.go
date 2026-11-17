package providerctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The typed Allocation events' ids, types and facts, worked out by hand from
// HBEP-8 (Allocation events (beta); The durable log, Streams, The event's id).
// The unit tests read the typed recorder's double, so they judge the decisions
// and the ids; the rows that list the Events the API holds are in e2e.
//
// The ids are asserted on the double because eventcount's EventfID drops the id
// before the API: an events.k8s.io Event holds none. Until the durable log's
// publisher exists (PR E2, #625), the double is the only place an id can be read;
// E2 verifies the STORED ids from the log itself, which is where an id is kept.

func longAlloc(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.TimeLimit = "3h" }

// typedIDs returns the id of each event of the reason, in order.
func typedIDs(r *typedRecorder, reason string) []string {
	var out []string
	for _, ev := range r.of(reason) {
		out = append(out, ev.id)
	}
	return out
}

// Every reason has its type from the one map, and each type is HBEP-8's: the
// expected types are written out here from the HBEP's table, not read from the
// map.
func TestAllocationEventTypesAreHBEP8s(t *testing.T) {
	want := map[string]string{
		"JobSubmitted": "started", "JobAdopted": "started", "HoldLifted": "started",
		"JobStarted": "ready", "JobReappeared": "ready",
		"HardEndNear":          "ending-soon",
		"JobEnded":             "ended",
		"AllocationEndedEarly": "failing", "AllocationFailed": "failing", "Requeued": "failing",
		"SubmittedAsOther": "failing", "AllocationRefused": "failing", "JobLost": "failing",
		"AdminHold": "degraded", "JobCompleting": "degraded",
	}
	if len(allocationEventType) != len(want) {
		t.Errorf("allocationEventType has %d reasons, HBEP-8 lists %d", len(allocationEventType), len(want))
	}
	for r, a := range want {
		if allocationEventType[r] != a {
			t.Errorf("%s: type %q, HBEP-8 gives %q", r, allocationEventType[r], a)
		}
	}
	// The series created at 0 are the map's, each with the Kubernetes type its action gives.
	got := map[string]eventcount.Series{}
	for _, s := range RaisedEvents() {
		got[s.Reason] = s
	}
	for r, a := range want {
		wantType := provider.EventKindOrdinary
		if a == "failing" || a == "degraded" {
			wantType = provider.EventKindWarning
		}
		if s := got[r]; s.Action != a || s.Type != wantType {
			t.Errorf("series of %s: %+v, want action %s type %s", r, s, a, wantType)
		}
	}
}

// A held job is submitted, its hold lifted, it starts, and runs to its limit:
// the events and their ids, with no job ID in any note.
func TestAJobsLifeRaisesTheTypedEventsWithTheirIDs(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a", longAlloc)
	e.run("a", 4)
	if ptPhase(e, "a") != v1alpha1.AllocationRunning {
		t.Fatalf("phase %s, want Running", ptPhase(e, "a"))
	}
	a := e.get("a")
	uid := string(a.UID)
	for reason, want := range map[string][]string{
		"JobSubmitted": {uid + "/JobSubmitted/1"},
		"HoldLifted":   {uid + "/HoldLifted/0"},
		"JobStarted":   {uid + "/JobStarted/0"},
	} {
		if got := typedIDs(e.typed, reason); !slices.Equal(got, want) {
			t.Errorf("%s ids %v, want %v (all: %v)", reason, got, want, e.typed.reasons())
		}
	}
	// Not yet within an hour of the hard end (3 h limit): no HardEndNear.
	if e.typed.has("HardEndNear") {
		t.Fatal("HardEndNear before an hour is left")
	}
	end := a.Status.AtProvider.HardEnd.Time
	e.clock.Advance(end.Add(-time.Hour).Sub(e.clock.Now()) - time.Second)
	e.run("a", 1)
	if e.typed.has("HardEndNear") {
		t.Fatal("HardEndNear one second before hardEnd-1h")
	}
	e.clock.Advance(time.Second)
	e.run("a", 2)
	want := uid + "/HardEndNear/" + end.UTC().Format(time.RFC3339)
	if got := typedIDs(e.typed, "HardEndNear"); !slices.Equal(got, []string{want}) {
		t.Fatalf("HardEndNear ids %v, want exactly %s (once, though two passes ran)", got, want)
	}
	// The hard end moves 2 h later: a second one, an hour before the new end.
	id := ptJobID(e, "a")
	e.batch.SetTimeLimit(id, 5*time.Hour)
	e.run("a", 1)
	if got := typedIDs(e.typed, "HardEndNear"); len(got) != 1 {
		t.Fatalf("HardEndNear after the hard end moved 2 h later but before the new hardEnd-1h: %v, want still one", got)
	}
	moved := e.get("a").Status.AtProvider.HardEnd.Time
	e.clock.Advance(moved.Add(-time.Hour).Sub(e.clock.Now()))
	e.run("a", 2)
	wantMoved := uid + "/HardEndNear/" + moved.UTC().Format(time.RFC3339)
	if got := typedIDs(e.typed, "HardEndNear"); !slices.Equal(got, []string{want, wantMoved}) || want == wantMoved {
		t.Fatalf("HardEndNear ids %v, want %s then %s", got, want, wantMoved)
	}
	// It runs to its limit: JobEnded, planned, with the end time as its fact.
	e.clock.Advance(moved.Sub(e.clock.Now()))
	e.run("a", 2)
	ended := e.typed.of("JobEnded")
	if len(ended) != 1 {
		t.Fatalf("%d JobEnded events, want 1 (all: %v)", len(ended), e.typed.reasons())
	}
	endTime := e.get("a").Status.AtProvider.EndTime.Time
	if want := uid + "/JobEnded/" + endTime.UTC().Format(time.RFC3339); ended[0].id != want {
		t.Errorf("JobEnded id %q, want %q", ended[0].id, want)
	}
	if e.typed.has("AllocationEndedEarly") {
		t.Error("AllocationEndedEarly for a job that ran to its time limit")
	}
	checkTypedFields(t, e)
	checkNoJobID(t, e, id)
}

// checkTypedFields: each event carries its type in action, the reason alone, the
// Kubernetes type HBEP-8 gives the action, and a note of at most 1,024 bytes.
func checkTypedFields(t *testing.T, e *env) {
	t.Helper()
	for _, ev := range e.typed.events {
		wantType := provider.EventKindOrdinary
		if ev.action == "failing" || ev.action == "degraded" {
			wantType = provider.EventKindWarning
		}
		if ev.eventtype != wantType || ev.action != allocationEventType[ev.reason] || strings.ContainsAny(ev.reason, " /") || len(ev.note) > 1024 || ev.note == "" {
			t.Errorf("event %+v: type %s action %s (the map says %s), note %d bytes", ev, ev.eventtype, ev.action, allocationEventType[ev.reason], len(ev.note))
		}
	}
}

// checkNoJobID: no typed note holds the job ID as a token, and no id does.
func checkNoJobID(t *testing.T, e *env, id string) {
	t.Helper()
	if id == "" {
		t.Fatal("no job ID to look for")
	}
	for _, ev := range e.typed.events {
		if containsToken(ev.note, id) || containsToken(ev.id, id) {
			t.Errorf("a typed %s event names the job ID: id %q note %q", ev.reason, ev.id, ev.note)
		}
	}
}

// A job that ends before its hard end, from outside, with no Instance
// draining: AllocationEndedEarly; with its Instance Draining in the API: the
// planned JobEnded. The same fixture without the Draining Instance is the
// control, so the Instance is what decides.
func TestAnEndBeforeTheHardEndIsPlannedOnlyWhileItsInstanceDrains(t *testing.T) {
	for _, drains := range []bool{false, true} {
		t.Run(fmt.Sprintf("drains=%v", drains), func(t *testing.T) {
			e := newEnv(t)
			e.newAlloc("a", longAlloc)
			e.run("a", 4)
			if ptPhase(e, "a") != v1alpha1.AllocationRunning {
				t.Fatalf("phase %s, want Running", ptPhase(e, "a"))
			}
			if drains {
				in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: testNS},
					Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "a"}}}
				in.Status.Phase = v1alpha1.InstanceDraining
				if err := e.c.Create(e.ctx, in); err != nil {
					t.Fatal(err)
				}
			}
			e.clock.Advance(10 * time.Minute)
			e.batch.CancelExternally(ptJobID(e, "a"))
			e.run("a", 2)
			ended, early := e.typed.of("JobEnded"), e.typed.of("AllocationEndedEarly")
			if drains && (len(ended) != 1 || len(early) != 0) || !drains && (len(ended) != 0 || len(early) != 1) {
				t.Fatalf("drains=%v: %d JobEnded and %d AllocationEndedEarly, want the other way round (all: %v)", drains, len(ended), len(early), e.typed.reasons())
			}
			checkTypedFields(t, e)
		})
	}
}

// A requeue and a failure, and the facts that keep their ids.
func TestARequeueAndAFailureRaiseTheirOwnEvents(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a", longAlloc)
	e.run("a", 4)
	id := ptJobID(e, "a")
	e.batch.Requeue(id)
	e.run("a", 2)
	uid := string(e.get("a").UID)
	if got := typedIDs(e.typed, "Requeued"); !slices.Equal(got, []string{uid + "/Requeued/1"}) {
		t.Errorf("Requeued ids %v, want [%s/Requeued/1] (all: %v)", got, uid, e.typed.reasons())
	}
	if e.typed.has("AllocationFailed") {
		t.Error("AllocationFailed for a requeue")
	}
	checkNoJobID(t, e, id)

	f := newEnv(t)
	d := time.Minute
	f.batch.Enqueue(fake.Script{FailAfter: &d, FailState: "NODE_FAIL"})
	f.newAlloc("b", longAlloc)
	f.run("b", 4)
	f.clock.Advance(2 * time.Minute)
	f.run("b", 2)
	if ph := ptPhase(f, "b"); ph != v1alpha1.AllocationFailed {
		t.Fatalf("phase %s, want Failed", ph)
	}
	if n := len(f.typed.of("AllocationFailed")); n != 1 {
		t.Errorf("%d AllocationFailed events, want 1 (all: %v)", n, f.typed.reasons())
	}
	if f.typed.has("Requeued") || f.typed.has("JobEnded") {
		t.Errorf("a failed job raised %v", f.typed.reasons())
	}
}

// A note that names the job ID as a token is refused, counted, and not handed
// over (R2's defence); a note that holds the digits inside a longer number is
// not.
func TestANoteThatNamesTheJobIDIsRefused(t *testing.T) {
	e := newEnv(t)
	a := e.newAlloc("a", longAlloc)
	e.run("a", 4)
	id := ptJobID(e, "a")
	x := &external{s: e.ctl.shared}
	a = e.get("a")
	before := len(e.typed.events)
	x.typed(a, evJobStarted, "9", "job "+id+" started", nil)
	x.typedUnmarked(a, evAdminHold, "held ("+id+")", nil)
	if len(e.typed.events) != before {
		t.Errorf("%d events were handed over, want none", len(e.typed.events)-before)
	}
	if n := e.ctl.RefusedNotes(); n != 2 {
		t.Errorf("%d refused notes counted, want 2", n)
	}
	x.typed(a, evJobStarted, "9", "started at 2026-01-01T00:00:00Z, count "+id+"0 and x"+id, nil)
	if len(e.typed.events) != before+1 {
		t.Errorf("a note with the digits inside longer tokens was refused")
	}
}

// An admin hold read every minute for 3 h raises at most 4 AdminHold (at 0, 1 h,
// 2 h and 3 h): a repeating condition, with its own gate, never one per read.
func TestAnAdminHoldRaisesAtMostOneEventAnHour(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{AdminHold: true})
	e.newAlloc("a", longAlloc)
	e.run("a", 3) // submitted and first observed
	if ptPhase(e, "a") != v1alpha1.AllocationHeld {
		t.Fatalf("phase %s, want Held by the administrator", ptPhase(e, "a"))
	}
	for i := 0; i < 180; i++ {
		e.clock.Advance(time.Minute)
		e.run("a", 1)
	}
	n := len(e.typed.of("AdminHold"))
	if n < 3 || n > 4 {
		t.Fatalf("%d AdminHold events over 3 h read every minute, want 3 or 4 (at most 4: at 0, 1 h, 2 h, 3 h)", n)
	}
	for _, ev := range e.typed.of("AdminHold") {
		if ev.id != "" {
			t.Errorf("AdminHold marks no transition, so it carries no id of its own: %q", ev.id)
		}
	}
	checkTypedFields(t, e)
}

// COMPLETING held for 15 min gives one JobCompleting, at 10 min; held for 5 s,
// none (HBEP-8, Allocation events, Repeating conditions).
func TestCompletingRaisesAfterTenMinutesOnce(t *testing.T) {
	for _, c := range []struct {
		name string
		hold time.Duration
		want int
	}{{"15 minutes", 15 * time.Minute, 1}, {"5 seconds", 5 * time.Second, 0}} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			d := time.Minute
			e.batch.Enqueue(fake.Script{FailAfter: &d, FailState: "COMPLETED", Epilog: c.hold})
			e.newAlloc("a", longAlloc)
			e.run("a", 4)
			e.clock.Advance(d)
			// The job has ended; it carries COMPLETING for c.hold. Read every
			// 5 s for the shorter hold and every minute for the longer, then
			// well past it.
			step := time.Minute
			if c.hold < time.Minute {
				step = time.Second
			}
			for el := time.Duration(0); el < c.hold+2*time.Minute; el += step {
				e.run("a", 1)
				e.clock.Advance(step)
			}
			if got := len(e.typed.of("JobCompleting")); got != c.want {
				t.Fatalf("%d JobCompleting events for COMPLETING held %s, want %d (all: %v)", got, c.hold, c.want, e.typed.reasons())
			}
		})
	}
}

// SubmittedAsOther and AllocationRefused are marked by the submission attempt's
// number, not by the time of their condition (R1 of #625): a condition's start
// is set anew in each pass, so a re-raise after a failed status write would
// otherwise get a new id. Both are attempt 1 here.
func TestSubmittedAsOtherAndRefusedAreMarkedByTheAttempt(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{AsTimeLimit: 20 * time.Minute})
	e.newAlloc("a", longAlloc)
	e.run("a", 4)
	uid := string(e.get("a").UID)
	got := typedIDs(e.typed, "SubmittedAsOther")
	if want := []string{uid + "/SubmittedAsOther/1"}; !slices.Equal(got, want) {
		t.Errorf("SubmittedAsOther ids %v, want %v (all: %v)", got, want, e.typed.reasons())
	}
	if ev := e.typed.of("SubmittedAsOther"); len(ev) == 1 && !strings.Contains(ev[0].note, "timeLimit: requested 3h0m0s, got 20m0s") {
		t.Errorf("SubmittedAsOther note %q, want the differing field with both values", ev[0].note)
	}
	checkNoJobID(t, e, ptJobID(e, "a"))

	// A job stored under another name than the tag is refused (JobRenamed), and
	// Refused is announced once.
	r := newEnv(t)
	r.newAlloc("b", longAlloc)
	r.run("b", 1)
	ptSetAnnotation(r, "b", v1alpha1.AnnotationExternalName, "not-the-tag")
	r.run("b", 2)
	if ph := ptPhase(r, "b"); ph != v1alpha1.AllocationRefused {
		t.Fatalf("phase %s, want Refused for an external name that is not the job tag", ph)
	}
	if got, want := typedIDs(r.typed, "AllocationRefused"), []string{string(r.get("b").UID) + "/AllocationRefused/1"}; !slices.Equal(got, want) {
		t.Errorf("AllocationRefused ids %v, want %v: marked by the attempt's number", got, want)
	}
	if n := len(r.typed.of("AllocationRefused")); n != 1 {
		t.Errorf("%d AllocationRefused events for two passes of a Refused Allocation, want 1: Refused is kept once set", n)
	}
}

// Deleting a running Allocation: JobEnded in the deleting pass that first sees
// the job's end time, once, with the same id the live pass would have given the
// same end, and the release cause in its note.
func TestDeletingARunningAllocationRaisesJobEndedOnce(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a", longAlloc)
	e.run("a", 4)
	if ptPhase(e, "a") != v1alpha1.AllocationRunning {
		t.Fatalf("phase %s, want Running", ptPhase(e, "a"))
	}
	ptSetAnnotation(e, "a", v1alpha1.AnnotationReleaseCause, "Instance example failed: example reason")
	a := e.get("a")
	uid, id := string(a.UID), ptJobID(e, "a")
	if err := e.c.Delete(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12 && !ptGone(e, "a"); i++ {
		e.clock.Advance(time.Second)
		e.run("a", 1)
	}
	if !ptGone(e, "a") {
		t.Fatal("the Allocation is not gone")
	}
	ended := e.typed.of("JobEnded")
	if len(ended) != 1 {
		t.Fatalf("%d JobEnded events for one deletion over several deleting passes, want 1 (all: %v)", len(ended), e.typed.reasons())
	}
	if !strings.HasPrefix(ended[0].id, uid+"/JobEnded/20") || !strings.Contains(ended[0].note, "release cause: Instance example failed: example reason") {
		t.Errorf("JobEnded id %q note %q, want an end-time id and the cause", ended[0].id, ended[0].note)
	}
	if e.typed.has("AllocationEndedEarly") || e.typed.has("AllocationFailed") {
		t.Errorf("a deletion raised %v", e.typed.reasons())
	}
	checkNoJobID(t, e, id)
}

// The note of an Allocation refused for an invalid profile names the invalid
// fields and never their messages: a message can carry a value of the Provider's
// settings, such as its account, and the note goes to the durable log.
func TestTheInvalidProfileNoteNamesFieldsOnly(t *testing.T) {
	err := &provider.InvalidProfileError{Provider: "example", Fields: []provider.FieldError{
		{Field: "provider.spec.account", Message: `"example-account-a1" contains a character that is not allowed`},
		{Field: "provider.spec.credentialsRef.file", Message: "/home/example-user/creds: no such file"},
	}}
	got := invalidProfileNote(fmt.Errorf("make the profile: %w", err))
	want := "the Provider's profile or settings are invalid at provider.spec.account, provider.spec.credentialsRef.file"
	if got != want {
		t.Errorf("note %q, want %q", got, want)
	}
	for _, leak := range []string{"example-account-a1", "/home/example-user", "no such file"} {
		if strings.Contains(got, leak) {
			t.Errorf("note %q carries %q", got, leak)
		}
	}
	if got := invalidProfileNote(fmt.Errorf("some other refusal")); got != "the Provider's profile or settings are invalid" {
		t.Errorf("note %q for an error that is no InvalidProfileError", got)
	}
}

// A failed Instance List at a job's first end before its hard end fails the
// pass and keeps the stored status: the planned-or-not decision needs it, and a
// planned end must never be announced as a failure (nor a failure as planned) on
// a guess. No end event is raised in that pass, and the next pass, with the List
// working, raises the right one: JobEnded while an Instance of the Allocation
// drains, AllocationEndedEarly while none does.
func TestAFailedInstanceListAtAnEarlyEndKeepsTheStoredStatus(t *testing.T) {
	for _, drains := range []bool{true, false} {
		t.Run(fmt.Sprintf("drains=%v", drains), func(t *testing.T) {
			e := newEnv(t)
			var failing atomic.Bool
			wc, ok := e.c.(client.WithWatch)
			if !ok {
				t.Fatal("the harness's client is not a WithWatch client")
			}
			reader := interceptor.NewClient(wc, interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, isInstances := list.(*v1alpha1.InstanceList); isInstances && failing.Load() {
						return errors.New("an injected failure of the Instance list")
					}
					return cl.List(ctx, list, opts...)
				},
			})
			ctl, err := New(e.c, reader, e.c.Scheme(), e.opts)
			if err != nil {
				t.Fatal(err)
			}
			e.ctl = ctl
			e.newAlloc("a", longAlloc)
			e.run("a", 4)
			if ptPhase(e, "a") != v1alpha1.AllocationRunning {
				t.Fatalf("phase %s, want Running", ptPhase(e, "a"))
			}
			if drains {
				in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: testNS},
					Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "a"}}}
				in.Status.Phase = v1alpha1.InstanceDraining
				if err := e.c.Create(e.ctx, in); err != nil {
					t.Fatal(err)
				}
			}
			before := e.get("a").Status.AtProvider
			e.clock.Advance(10 * time.Minute)
			e.batch.CancelExternally(ptJobID(e, "a"))

			failing.Store(true)
			e.run("a", 1)
			got := e.get("a")
			synced := got.GetCondition(xpv2.TypeSynced)
			if synced.Status != corev1.ConditionFalse || !strings.Contains(synced.Message, "to tell a planned end from an early one") {
				t.Errorf("Synced is %s %q, want False with the failed list named", synced.Status, synced.Message)
			}
			if got.Status.AtProvider.Phase != before.Phase || got.Status.AtProvider.EndTime != nil || got.Status.AtProvider.Reason != before.Reason {
				t.Errorf("the stored status moved: phase %s end %v reason %q, want phase %s, no end time and reason %q kept",
					got.Status.AtProvider.Phase, got.Status.AtProvider.EndTime, got.Status.AtProvider.Reason, before.Phase, before.Reason)
			}
			if e.typed.has("JobEnded") || e.typed.has("AllocationEndedEarly") {
				t.Fatalf("an end event was raised in the pass whose list failed: %v", e.typed.reasons())
			}

			failing.Store(false)
			e.run("a", 1)
			want, other := "AllocationEndedEarly", "JobEnded"
			if drains {
				want, other = other, want
			}
			if n := len(e.typed.of(want)); n != 1 || e.typed.has(other) {
				t.Errorf("after the list works again: %d %s and %d %s, want one %s (all: %v)", n, want, len(e.typed.of(other)), other, want, e.typed.reasons())
			}
			if ph := ptPhase(e, "a"); ph != v1alpha1.AllocationEnded {
				t.Errorf("phase %s after the list works again, want Ended", ph)
			}
		})
	}
}
