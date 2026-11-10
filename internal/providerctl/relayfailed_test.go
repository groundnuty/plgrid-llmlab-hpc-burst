package providerctl

// Issue #304, the Provider controller's half: a relay that ends for good
// through the real relay owner (hostdial.Owner, its own loop, its own reading
// of the relay's end) marks its Allocation RelayEndedForGood from the failure
// path itself, before any observation, so a controller restart cannot lose it;
// the next observation writes it when that write failed; the write refuses an
// Allocation or job that is not the relay's; and an end that is superseded,
// the job's end or retried marks nothing. The relays' ssh is a stub that
// prints what srun or the relay prints and exits as they do; the owner
// classifies it, so nothing here hands the controller an end kind. The
// expected names and texts are written from the issue and HBEP-6's What ends
// a relay, not read from the code: the condition's type and reason are spelt
// out as literals.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
)

// The condition's names, as issue #304's contract change gives them.
const (
	wantRelayCondition = "RelayEndedForGood"
	wantRelayReason    = "RelayFailed"
)

// The stubs' lines: srun's step-limit refusal (Slurm's ESLURM_STEP_LIMIT),
// srun's refusal for a job that does not exist (ESLURM_INVALID_JOB_ID), and
// the relay's own superseded line, which it ends with exit code 5.
const (
	stubStepLimit  = "echo 'srun: error: Unable to create step for job 1: Step limit reached for this job' >&2\nexit 1\n"
	stubJobGone    = "echo 'srun: error: Unable to create step for job 1: Invalid job id specified' >&2\nexit 1\n"
	stubSuperseded = "echo 'hb-relay: superseded: a relay with a higher fence holds the channel' >&2\nexit 5\n"
)

// relayStub writes an ssh stub that runs body, and returns its path.
func relayStub(t *testing.T, body string) string {
	t.Helper()
	stub := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return stub
}

// relayGate is an ssh stub whose relays wait for a gate file and end with
// srun's step-limit refusal only once it opens. Its ssh -G (the login host's
// name) answers at once. A relay that has waited 60 s without the gate writes
// the abort marker and exits 97, a retried end, never the refusal, so only
// opening the gate makes a relay end for good, and the test fails on the
// marker.
type relayGate struct{ stub, file, aborted string }

func newRelayGate(t *testing.T) relayGate {
	t.Helper()
	dir := t.TempDir()
	g := relayGate{file: filepath.Join(dir, "open"), aborted: filepath.Join(dir, "aborted")}
	g.stub = relayStub(t, "case \" $* \" in *' -G '*) exit 1;; esac\nn=0\n"+
		"while [ ! -e '"+g.file+"' ]; do\n"+
		"  n=$((n+1))\n"+
		"  if [ $n -ge 3000 ]; then : > '"+g.aborted+"'; echo 'stub: the gate never opened' >&2; exit 97; fi\n"+
		"  sleep 0.02\n"+
		"done\n"+stubStepLimit)
	t.Cleanup(func() {
		if _, err := os.Stat(g.aborted); err == nil {
			t.Error("a relay stub gave up waiting for the gate: the test ran past its 60 s")
		}
	})
	return g
}

func (g relayGate) open(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(g.file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// stubbedRelays makes the env host-dialed with its relays' ssh at stub.
func stubbedRelays(stub string) envOption {
	return func(e *env) {
		hostDialedWith(e, hostdial.NewHosts(), 1000)
		e.opts.Relays.SSHBinary = stub
	}
}

// withInterceptors rebuilds the env's store with the interceptors, keeping
// its Provider.
func withInterceptors(funcs interceptor.Funcs) envOption {
	return func(e *env) {
		var prov v1alpha1.Provider
		if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
			e.t.Fatal(err)
		}
		prov.ResourceVersion = ""
		e.c = clientfake.NewClientBuilder().WithScheme(e.c.Scheme()).WithObjects(&prov).
			WithStatusSubresource(&v1alpha1.Allocation{}, &v1alpha1.Provider{}).WithReturnManagedFields().
			WithInterceptorFuncs(funcs).Build()
	}
}

// runRelaysStoppable is runRelays with a stop: the controller's relays stop
// by PID, as they do when its process ends.
func runRelaysStoppable(e *env) (stop func()) {
	e.t.Helper()
	ctl := e.ctl
	if err := ctl.shared.relays.enroll(e.ctx, e.c); err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = ctl.RunRelays(ctx); close(done) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				e.t.Error("the relay owner did not stop")
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// restartController is a Provider controller restart: the old copy's relays
// stop by PID, and a new copy with nothing in memory, its relay owner, its
// recorded failures and its restart history alike, is built over the same
// store and runs its relays.
func restartController(e *env, stop func()) (newStop func()) {
	e.t.Helper()
	stop()
	e.build()
	return runRelaysStoppable(e)
}

// relayCondition is the Allocation's RelayEndedForGood condition, by its
// literal type.
func relayCondition(a *v1alpha1.Allocation) (status corev1.ConditionStatus, reason, msg string) {
	for _, c := range a.Status.Conditions {
		if string(c.Type) == wantRelayCondition {
			return c.Status, string(c.Reason), c.Message
		}
	}
	return "", "", ""
}

// relayFailedMessages are the messages of the RelayFailed events raised.
func (r *recorder) relayFailedMessages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		if string(e.Reason) == "RelayFailed" {
			out = append(out, e.Message)
		}
	}
	return out
}

// eventOfChannel returns the RelayFailed message of the channel that msg
// names, and the channel: each relay's failure raises its own event, and the
// condition repeats the first recorded one, whichever channel that was.
func eventOfChannel(t *testing.T, events []string, msg string) (string, string) {
	t.Helper()
	for _, ch := range []string{"control", "serving"} {
		if !strings.HasPrefix(msg, "the "+ch+" relay ended for good: ") {
			continue
		}
		for _, ev := range events {
			if strings.HasPrefix(ev, "the "+ch+" relay ended for good: ") {
				return ev, ch
			}
		}
		t.Fatalf("no RelayFailed event of the %s channel among %q", ch, events)
	}
	t.Fatalf("the condition's message %q names no channel", msg)
	return "", ""
}

// startAllocation makes alloc-a and observes it until its job runs and both
// relays' ssh run.
func startAllocation(t *testing.T, e *env) (tag string) {
	t.Helper()
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the Allocation is %s, want Running", a.Status.AtProvider.Phase)
	}
	tag = tagOf(a)
	waitRelays(t, "both relays' ssh running", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].PID != 0 && rs[1].PID != 0
	})
	return tag
}

// A relay that ends for good with srun's step-limit refusal marks its
// Allocation from the failure path, with no observation after the end: the
// relays wait at the gate while the first observations pass, unmarked; once
// it opens, both channels fail; the controller is restarted, and before any
// observation the mark is in the store, True with reason RelayFailed and the
// message of one channel's RelayFailed event. The new copy's observations
// keep it, and the Provider's controller cancels nothing itself: the release
// is the lifecycle controller's (relayfailed_release_test.go).
func TestARelayEndedForGoodIsMarkedBeforeAnyObservationAndSurvivesARestart(t *testing.T) {
	g := newRelayGate(t)
	e := newEnv(t, stubbedRelays(g.stub))
	stop := runRelaysStoppable(e)
	startAllocation(t, e)
	e.run("alloc-a", 2)
	if st, _, _ := relayCondition(e.get("alloc-a")); st != "" {
		t.Fatalf("the Allocation is marked %s while its relays run", st)
	}
	g.open(t)
	waitRelays(t, "both channels' RelayFailed raised", func() bool { return len(e.rec.relayFailedMessages()) >= 2 })
	restartController(e, stop)
	st, reason, msg := relayCondition(e.get("alloc-a"))
	if st != corev1.ConditionTrue || reason != wantRelayReason {
		t.Fatalf("after a restart before any observation the condition is %q, reason %q; want True, %s", st, reason, wantRelayReason)
	}
	ev, ch := eventOfChannel(t, e.rec.relayFailedMessages(), msg)
	if msg != ev || !strings.Contains(msg, "step limit") {
		t.Errorf("the condition's message %q: want the %s channel's RelayFailed message %q, naming the step limit", msg, ch, ev)
	}
	e.run("alloc-a", 3)
	a := e.get("alloc-a")
	if st, _, msg2 := relayCondition(a); st != corev1.ConditionTrue || msg2 != msg {
		t.Errorf("the new copy's observations left the condition %q with %q; want it kept, with %q", st, msg2, msg)
	}
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || !a.DeletionTimestamp.IsZero() {
		t.Errorf("the Provider's controller changed the Allocation itself: phase %s, deleted %v", a.Status.AtProvider.Phase, !a.DeletionTimestamp.IsZero())
	}
	if n := e.count("scancel"); n != 0 {
		t.Errorf("the Provider's controller cancelled %d jobs; the release is the lifecycle controller's", n)
	}
}

// When the failure path cannot write the mark (here the API server answers
// its status patch with 503), the record stays and the next observation
// writes the mark, the same one, through the managed reconciler's status
// write.
func TestTheNextObservationMarksTheAllocationWhenTheFailurePathCannotWrite(t *testing.T) {
	var refused atomic.Int32
	g := newRelayGate(t)
	e := newEnv(t, stubbedRelays(g.stub), withInterceptors(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*v1alpha1.Allocation); ok && sub == "status" {
				refused.Add(1)
				return apierrors.NewServiceUnavailable("example: the API server is unavailable")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}))
	runRelaysStoppable(e)
	startAllocation(t, e)
	g.open(t)
	waitRelays(t, "both channels' RelayFailed raised", func() bool { return len(e.rec.relayFailedMessages()) >= 2 })
	if refused.Load() == 0 {
		t.Fatal("the failure path made no status patch")
	}
	if st, _, _ := relayCondition(e.get("alloc-a")); st != "" {
		t.Fatalf("the Allocation is marked %s though every status patch was refused", st)
	}
	e.run("alloc-a", 1)
	st, reason, msg := relayCondition(e.get("alloc-a"))
	if st != corev1.ConditionTrue || reason != wantRelayReason {
		t.Fatalf("after the next observation the condition is %q, reason %q; want True, %s", st, reason, wantRelayReason)
	}
	if ev, ch := eventOfChannel(t, e.rec.relayFailedMessages(), msg); msg != ev {
		t.Errorf("the condition's message %q: want the %s channel's RelayFailed message %q", msg, ch, ev)
	}
}

// The failure path's write (persistRelayFailed) refuses an Allocation that is
// not the relay's: another UID under the name, or another recorded job. It
// writes nothing then. A conflict is read again and retried, and a mark
// already there is left as it is, with no write.
func TestTheFailurePathsMarkChecksTheAllocationAndItsJob(t *testing.T) {
	var patches atomic.Int32
	var conflictNext atomic.Bool
	e := newEnv(t, hostDialed, withInterceptors(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches.Add(1)
			if conflictNext.CompareAndSwap(true, false) {
				return apierrors.NewConflict(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "allocations"}, obj.GetName(), errors.New("example: the object has been modified"))
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}))
	a := e.newAlloc("alloc-a")
	a.Status.AtProvider.Phase, a.Status.AtProvider.JobID = v1alpha1.AllocationRunning, "4242"
	if err := e.c.Status().Update(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	key := clientKey("alloc-a")
	s := e.ctl.shared
	mark := func(uid, job, msg string) error {
		return s.persistRelayFailed(key, relayFailure{uid: uid, jobID: job, msg: msg, at: e.clock.Now()})
	}
	unmarked := func(what string) {
		t.Helper()
		if st, _, msg := relayCondition(e.get("alloc-a")); st != "" {
			t.Errorf("%s: the Allocation is marked %s, %q", what, st, msg)
		}
	}
	t.Run("another Allocation", func(t *testing.T) {
		before := patches.Load()
		if err := mark("uid-another", "4242", "the serving relay ended for good: x"); !errors.Is(err, errMarkRefused) {
			t.Errorf("a mark for UID uid-another on %s: %v, want refused", a.UID, err)
		}
		unmarked("another UID")
		if n := patches.Load() - before; n != 0 {
			t.Errorf("%d status patches for another Allocation, want 0", n)
		}
	})
	t.Run("another job", func(t *testing.T) {
		before := patches.Load()
		if err := mark(string(a.UID), "4243", "the serving relay ended for good: x"); !errors.Is(err, errMarkRefused) {
			t.Errorf("a mark for job 4243 on an Allocation that records 4242: %v, want refused", err)
		}
		unmarked("another job")
		if n := patches.Load() - before; n != 0 {
			t.Errorf("%d status patches for another job, want 0", n)
		}
	})
	const first = "the control relay ended for good: srun refused the step: step limit reached for this job"
	t.Run("a conflict is retried", func(t *testing.T) {
		before := patches.Load()
		conflictNext.Store(true)
		if err := mark(string(a.UID), "4242", first); err != nil {
			t.Fatalf("the mark after one conflict: %v", err)
		}
		if st, reason, msg := relayCondition(e.get("alloc-a")); st != corev1.ConditionTrue || reason != wantRelayReason || msg != first {
			t.Errorf("after a conflict and a retry the condition is %q, %q, %q; want True, %s, %q", st, reason, msg, wantRelayReason, first)
		}
		if n := patches.Load() - before; n != 2 {
			t.Errorf("%d status patches, want 2: the conflict and its retry", n)
		}
	})
	t.Run("a mark already there", func(t *testing.T) {
		before := patches.Load()
		if err := mark(string(a.UID), "4242", "the serving relay ended for good: another cause"); err != nil {
			t.Fatal(err)
		}
		if _, _, msg := relayCondition(e.get("alloc-a")); msg != first {
			t.Errorf("a second mark changed the message to %q, want %q kept", msg, first)
		}
		if n := patches.Load() - before; n != 0 {
			t.Errorf("%d status patches for a mark already there, want 0", n)
		}
	})
}

// What releases nothing (issue #304): a relay superseded by a newer copy's,
// one whose job srun says is gone, and one that is retried. Each ends through
// the real owner, as the state the owner keeps shows, and the observations
// after it leave the Allocation unmarked, Running, with no RelayFailed.
func TestRelayEndsThatReleaseNothingLeaveTheAllocationUnmarked(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stub  string
		ended func(hostdial.RelayState) bool
	}{
		{"superseded", stubSuperseded, func(r hostdial.RelayState) bool {
			return r.Final && strings.HasPrefix(r.FinalCause, "superseded: ")
		}},
		{"the job's end", stubJobGone, func(r hostdial.RelayState) bool {
			return r.Final && strings.HasPrefix(r.FinalCause, "the job's end: ")
		}},
		{"retried", "exit 1\n", func(r hostdial.RelayState) bool {
			return !r.Final && r.Starts >= 3 && strings.HasPrefix(r.LastEnd, "retried")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, stubbedRelays(relayStub(t, tc.stub)))
			runRelays(e)
			e.newAlloc("alloc-a")
			e.run("alloc-a", 4)
			tag := tagOf(e.get("alloc-a"))
			waitRelays(t, "both relays ended as "+tc.name, func() bool {
				rs := relaysOf(e, tag)
				return len(rs) == 2 && tc.ended(rs[0]) && tc.ended(rs[1])
			})
			e.run("alloc-a", 3)
			a := e.get("alloc-a")
			if st, reason, msg := relayCondition(a); st != "" {
				t.Errorf("a relay ended as %s marked the Allocation %s, %s: %q", tc.name, st, reason, msg)
			}
			if got := e.rec.relayFailedMessages(); len(got) != 0 {
				t.Errorf("a relay ended as %s raised RelayFailed: %q", tc.name, got)
			}
			if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
				t.Errorf("the Allocation is %s, want Running", a.Status.AtProvider.Phase)
			}
		})
	}
}
