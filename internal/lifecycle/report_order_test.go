package lifecycle

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The Backend keeps a report only if it is newer than the newest it holds for
// the same agent instance (HBEP-7, Report order; issue #221). Each case sends
// its reports out of order or repeats one: a fixture that sends them in order
// proves nothing, since it passes with or without the rule. The reports are told
// apart by the serving endpoint they name.

type orderRig struct {
	rg      *rig
	in      v1alpha1.Instance
	alloc   *v1alpha1.Allocation
	caller  agentapi.Caller
	base    agentapi.Report // the agent's last report; its Seq is what the Backend holds
	enqueue atomic.Int64
}

func newOrderRig(t *testing.T) *orderRig {
	t.Helper()
	rg, in, name := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	a, ok := rg.allocation(name)
	if !ok {
		t.Fatalf("no Allocation %s", name)
	}
	ra := rg.agentOf(name)
	base, ok := ra.agent.LastReport()
	if !ok || base.Instance == nil || base.Seq == 0 {
		t.Fatalf("last report %+v, want an Instance report with a count", base)
	}
	o := &orderRig{rg: rg, in: in, alloc: a, base: base,
		caller: agentapi.Caller{Namespace: rigNS, Allocation: name, AllocationUID: string(a.UID), AgentInstanceID: ra.agent.AgentInstanceID()}}
	rg.backend.Enqueue = func(types.NamespacedName) { o.enqueue.Add(1) }
	return o
}

// report returns the agent's last report as a report counted seq that names
// endpoint, and carries key when it is not empty.
func (o *orderRig) report(seq uint64, endpoint, key string) agentapi.Report {
	r := o.base
	inst := *o.base.Instance
	inst.ServingEndpoint, inst.Key = endpoint, key
	r.Instance, r.Seq = &inst, seq
	return r
}

// kept returns the serving endpoint of the report the Backend holds, when it
// came, and from which agent instance.
func (o *orderRig) kept(t *testing.T) (string, time.Time, string) {
	t.Helper()
	r, at, from := o.rg.backend.latest(o.alloc.UID)
	if r == nil || r.Instance == nil {
		t.Fatalf("the Backend holds no Instance report: %+v", r)
	}
	return r.Instance.ServingEndpoint, at, from
}

// The last report is delivered, then an older periodic one: the Backend keeps
// the last, answers the older as accepted, and the older takes no effect at
// all: the time of the held report is the same, and no pass is enqueued. A
// repeat of the held count is dropped too.
func TestBackendKeepsTheLastReportWhenAnOlderOneArrivesAfterIt(t *testing.T) {
	o := newOrderRig(t)
	s := o.base.Seq + 10
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/last", "")); err != nil {
		t.Fatal(err)
	}
	_, atLast, from := o.kept(t)
	if from != o.caller.AgentInstanceID {
		t.Fatalf("the report is from %q, want %q", from, o.caller.AgentInstanceID)
	}
	passes := o.enqueue.Load()

	o.rg.clock.Advance(time.Second) // a kept report would carry a later time
	for _, tc := range []struct {
		what string
		seq  uint64
	}{{"an older periodic report", s - 1}, {"a repeat of the held count", s}, {"a much older report", 1}} {
		if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(tc.seq, "unix:/stale", "")); err != nil {
			t.Fatalf("%s: %v, want it accepted", tc.what, err)
		}
		if ep, at, _ := o.kept(t); ep != "unix:/last" || !at.Equal(atLast) {
			t.Fatalf("after %s the Backend holds %q from %s, want unix:/last from %s", tc.what, ep, at, atLast)
		}
	}
	if n := o.enqueue.Load(); n != passes {
		t.Errorf("a dropped report enqueued %d passes, want none", n-passes)
	}
}

// A dropped report takes no effect at all, the Instance key included: it is
// written from a report the Backend keeps, and the test shows the key path
// works here by writing it from a newer report afterwards.
func TestADroppedReportWritesNoKey(t *testing.T) {
	o := newOrderRig(t)
	uid := string(o.in.UID)
	path := agentapi.InstanceKeyPath(o.rg.creds, uid)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) { // the test's own temporary directory
		t.Fatal(err)
	}
	key := strings.Repeat("0123456789abcdef", 4)
	s := o.base.Seq + 10
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/last", "")); err != nil {
		t.Fatal(err)
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s-1, "unix:/stale", key)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("a dropped report wrote the Instance's key")
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s+1, "unix:/newer", key)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a kept report did not write the key: %v", err)
	}
}

// Two reports of one agent can run in the Backend at once: an older one is in
// the middle of writing its key, outside the Backend's lock, when a newer one
// is kept. The older must not replace it when it gets to the end.
func TestAnOlderReportThatIsStillBeingTakenDoesNotReplaceANewerOne(t *testing.T) {
	o := newOrderRig(t)
	gate := &gateReader{Reader: o.rg.backend.Reader, entered: make(chan struct{}), release: make(chan struct{})}
	o.rg.backend.Reader = gate
	s := o.base.Seq + 5
	key := strings.Repeat("fedcba9876543210", 4)

	gate.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/older", key)) }()
	select {
	case <-gate.entered: // the older report passed the first check and waits to store its key
	case <-time.After(30 * time.Second):
		t.Fatal("the older report never reached the key's write")
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s+1, "unix:/newer", "")); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the older report: %v, want it accepted", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the older report did not return")
	}
	if ep, _, _ := o.kept(t); ep != "unix:/newer" {
		t.Errorf("the Backend holds %q, want the newer report unix:/newer", ep)
	}
}

// gateReader holds the first List it sees after it is armed until released.
type gateReader struct {
	client.Reader
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *gateReader) List(ctx context.Context, l client.ObjectList, opts ...client.ListOption) error {
	if g.armed.Swap(false) {
		close(g.entered)
		<-g.release
	}
	return g.Reader.List(ctx, l, opts...)
}

// The held count rises only when a report is kept: one the Backend refused,
// and the agent sends again with its number, is kept the second time.
func TestBackendDoesNotCountAReportItRefused(t *testing.T) {
	o := newOrderRig(t)
	s := o.base.Seq + 20
	w := o.base.Instance.Workers[0]
	twice := o.report(s, "unix:/refused", "")
	twice.Instance.Workers = []agentapi.WorkerReport{w, w}
	var ae *agentapi.Error
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, twice); !errors.As(err, &ae) || ae.Code != agentapi.CodeBadRequest {
		t.Fatalf("a report listing worker-0 twice: %v, want BadRequest", err)
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/again", "")); err != nil {
		t.Fatal(err)
	}
	if ep, _, _ := o.kept(t); ep != "unix:/again" {
		t.Errorf("the Backend holds %q, want the report sent again with its number, unix:/again", ep)
	}
}

// A report with no count is refused as a bad request, not dropped without a
// word, and the report held stays.
func TestBackendRefusesAReportWithNoCount(t *testing.T) {
	o := newOrderRig(t)
	before, _, _ := o.kept(t)
	var ae *agentapi.Error
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(0, "unix:/nocount", "")); !errors.As(err, &ae) || ae.Code != agentapi.CodeBadRequest {
		t.Fatalf("a report with no count: %v, want BadRequest", err)
	}
	if ep, _, _ := o.kept(t); ep != before {
		t.Errorf("the Backend holds %q after the refusal, want %q", ep, before)
	}
}

// The count is per agent instance, and kept with the registration. A
// registration again with the same ID changes nothing, so the count stays; a
// new agent instance ID, counting from 1, is kept though the instance before
// it reached a higher count; and the first instance's count is still its own.
func TestBackendCountsPerAgentInstance(t *testing.T) {
	o := newOrderRig(t)
	s := o.base.Seq + 30
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/first", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := o.rg.backend.Register(o.rg.ctx, o.caller, agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s-1, "unix:/after-reregister", "")); err != nil {
		t.Fatal(err)
	}
	if ep, _, _ := o.kept(t); ep != "unix:/first" {
		t.Fatalf("after a registration again the Backend holds %q, want unix:/first: the count must stay", ep)
	}

	second := o.caller
	second.AgentInstanceID = strings.Repeat("ab", 16)
	if _, err := o.rg.backend.Register(o.rg.ctx, second, agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := o.rg.backend.Report(o.rg.ctx, second, o.report(1, "unix:/second-instance", "")); err != nil {
		t.Fatal(err)
	}
	ep, _, from := o.kept(t)
	if ep != "unix:/second-instance" || from != second.AgentInstanceID {
		t.Fatalf("the Backend holds %q from %q, want the new instance's first report", ep, from)
	}
	if err := o.rg.backend.Report(o.rg.ctx, second, o.report(1, "unix:/second-repeat", "")); err != nil {
		t.Fatal(err)
	}
	if ep, _, _ := o.kept(t); ep != "unix:/second-instance" {
		t.Errorf("a repeat of the new instance's count was kept: %q", ep)
	}
}

// The counts are in memory, like registrations: a controller that restarts
// holds none, and the agent registers again and goes on with its own count.
func TestAControllerRestartClearsTheHeldCounts(t *testing.T) {
	o := newOrderRig(t)
	s := o.base.Seq + 40
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s, "unix:/before", "")); err != nil {
		t.Fatal(err)
	}
	o.rg.restartController()
	var ae *agentapi.Error
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(s+1, "unix:/x", "")); !errors.As(err, &ae) || ae.Code != agentapi.CodeNotRegistered {
		t.Fatalf("a report after the restart: %v, want NotRegistered", err)
	}
	if _, err := o.rg.backend.Register(o.rg.ctx, o.caller, agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := o.rg.backend.Report(o.rg.ctx, o.caller, o.report(1, "unix:/after", "")); err != nil {
		t.Fatal(err)
	}
	if ep, _, _ := o.kept(t); ep != "unix:/after" {
		t.Errorf("the Backend holds %q, want unix:/after: a restarted controller holds no count", ep)
	}
}
