package lifecycle

// IdentityUnresolved (HBEP owner, 31 Oct 2026): a class has no current ID when
// the checkpoint's file list cannot be read or the ID's display form collides
// with another identity record. The message names the cause (Hub 401, Hub 404,
// or the colliding record id-<12 hex>); transient Hub failures back off and
// retry, honouring Retry-After; a collision never loops.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// scriptedSource answers Files with the next scripted error, or the rig's
// files once the script is spent; it records the time of every call.
type scriptedSource struct {
	clock interface{ Now() time.Time }
	mu    sync.Mutex
	errs  []error
	calls []time.Time
}

func (s *scriptedSource) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, s.clock.Now())
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return nil, err
	}
	return append([]deploymentid.File(nil), rigFiles...), nil
}

func (s *scriptedSource) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

func (s *scriptedSource) callTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.calls...)
}

func hubErr(status int, retryAfter time.Duration) error {
	return &hubfiles.HTTPError{Method: "GET", Path: "/api/models/example-org/example-model/tree/" + rigRevision, Status: status, RetryAfter: retryAfter}
}

func wantDegraded(t *testing.T, rg *rig, reason, fragment string) {
	t.Helper()
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != reason || !strings.Contains(c.Message, fragment) {
		t.Fatalf("Degraded = %s/%s %q; want True/%s naming %q", c.Status, c.Reason, c.Message, reason, fragment)
	}
}

// TestIdentityUnresolvedHub401NamesTheCauseAndRetriesSlowly: a 401 is
// permanent. The condition names "Hub 401", nothing is provisioned, and the
// Hub is asked again only after 10 minutes, however many passes run between.
func TestIdentityUnresolvedHub401NamesTheCauseAndRetriesSlowly(t *testing.T) {
	rg := newRig(t)
	src := &scriptedSource{clock: rg.clock, errs: []error{hubErr(401, 0), hubErr(401, 0)}}
	rg.r.Checkpoints = NewCachedSource(src, rg.clock)
	rg.create(mdSpec("md"))
	rg.Step() // 08:00:00: the first read fails
	wantDegraded(t, rg, reasonIdentityUnresolved, "Hub 401")
	rg.run(9*time.Minute+50*time.Second, 10*time.Second) // up to 08:09:50
	if n := len(src.callTimes()); n != 1 {
		t.Fatalf("Hub asked %d times before the 10 min retry, want 1: %v", n, src.callTimes())
	}
	rg.run(10*time.Second, 10*time.Second) // 08:10:00: retry, fails again
	if calls := src.callTimes(); len(calls) != 2 || !calls[1].Equal(rigStart.Add(10*time.Minute)) {
		t.Fatalf("second Hub call at %v, want 08:10:00", calls)
	}
	wantDegraded(t, rg, reasonIdentityUnresolved, "Hub 401")
	if n := len(rg.allocations("md")); n != 0 || rg.submissions() != 0 {
		t.Fatalf("with no current ID, want no Allocation; got %d and %d submissions", n, rg.submissions())
	}
	if c := rg.md("md").Status.CurrentIDs; len(c) != 0 {
		t.Fatalf("currentIDs = %v, want none", c)
	}
}

// TestIdentityUnresolvedHub404NamesTheCause: a 404 names "Hub 404".
func TestIdentityUnresolvedHub404NamesTheCause(t *testing.T) {
	rg := newRig(t)
	rg.r.Checkpoints = NewCachedSource(&scriptedSource{clock: rg.clock, errs: []error{hubErr(404, 0)}}, rg.clock)
	rg.create(mdSpec("md"))
	rg.Step()
	wantDegraded(t, rg, reasonIdentityUnresolved, "Hub 404")
}

// TestIdentityUnresolvedTransientBacksOffThenResolves: a 503 with
// Retry-After 2 min is retried at 08:02:00, not before; a 502 without one
// backs off 60 s (the second failure: 30 s doubled), so the next read is at
// 08:03:00; it succeeds, the class gets its ID and an Allocation follows.
func TestIdentityUnresolvedTransientBacksOffThenResolves(t *testing.T) {
	rg := newRig(t)
	src := &scriptedSource{clock: rg.clock, errs: []error{hubErr(503, 2*time.Minute), hubErr(502, 0)}}
	rg.r.Checkpoints = NewCachedSource(src, rg.clock)
	rg.create(mdSpec("md"))
	rg.Step() // 08:00:00
	wantDegraded(t, rg, reasonIdentityUnresolved, "Hub 503")
	rg.run(4*time.Minute, 10*time.Second)
	want := []time.Time{rigStart, rigStart.Add(2 * time.Minute), rigStart.Add(3 * time.Minute)}
	got := src.callTimes()
	if len(got) != len(want) {
		t.Fatalf("Hub calls at %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("Hub call %d at %s, want %s", i+1, got[i], want[i])
		}
	}
	if c := rg.md("md").Status.CurrentIDs; len(c) != 1 {
		t.Fatalf("after the Hub answers, want one current ID; got %v", c)
	}
	if rg.submissions() != 1 {
		t.Fatalf("submissions = %d, want 1 once the ID resolves", rg.submissions())
	}
}

// TestIdentityUnresolvedCollisionNeverLoops: an identity record id-<12 hex>
// already holds another full ID. The class gets no ID, the condition names the
// record, exactly one degraded event fires over many passes, nothing is
// provisioned; once the record goes (a record change) the ID resolves.
func TestIdentityUnresolvedCollisionNeverLoops(t *testing.T) {
	rg := newRig(t)
	md := mdSpec("md")
	// The true ID, from the library (pkg/deploymentid), with the empty template
	// the stub's checkpoint gives.
	tmpl, err := deploymentid.ChatTemplateSHA256("")
	if err != nil {
		t.Fatal(err)
	}
	id, err := deploymentid.Compute(deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: "example-org/example-model", Revision: rigRevision},
		Engine:             deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest},
		Flags:              []deploymentid.Flag{{Name: "max-model-len", Value: "32768"}},
		ChatTemplateSHA256: tmpl,
	}, deploymentid.Accelerator{GPUModel: rigGPU, CPUArch: rigArch}, rigFiles)
	if err != nil {
		t.Fatal(err)
	}
	other := id.SHA256[:12] + strings.Repeat("f", 52)
	if other == id.SHA256 {
		other = id.SHA256[:12] + strings.Repeat("e", 52)
	}
	data, _ := json.Marshal(identityData{Scheme: "v1", SHA256: other, Canonical: json.RawMessage(`{}`)})
	recName := "id-" + id.SHA256[:12]
	rg.create(&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: recName, Namespace: rigNS},
		Data:       runtime.RawExtension{Raw: data}, Revision: 1,
	})
	rg.create(md)
	rg.run(5*time.Minute, 10*time.Second)
	wantDegraded(t, rg, reasonIdentityUnresolved, recName)
	if n := len(rg.rec.Find(eventDegraded, reasonIdentityUnresolved)); n != 1 {
		t.Fatalf("degraded/IdentityUnresolved events = %d over 30 passes, want exactly 1", n)
	}
	if rg.submissions() != 0 || len(rg.allocations("md")) != 0 {
		t.Fatal("a collided class must provision nothing")
	}
	// The operator removes the stale record: the ID resolves.
	var rec appsv1.ControllerRevision
	rec.Name, rec.Namespace = recName, rigNS
	if err := rg.c.Delete(rg.ctx, &rec); err != nil {
		t.Fatal(err)
	}
	rg.run(30*time.Second, 10*time.Second)
	ids := rg.md("md").Status.CurrentIDs
	if len(ids) != 1 || string(ids[0].ID) != id.SHA256 {
		t.Fatalf("currentIDs = %v, want %s", ids, id.SHA256)
	}
	if rg.submissions() != 1 {
		t.Fatalf("submissions = %d, want 1", rg.submissions())
	}
}
