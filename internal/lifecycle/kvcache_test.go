package lifecycle

// The controller's side of the KV cache size: status.kvCacheTokens from the agent's reports (kvcache.go;
// HBEP-7, The KV cache size; `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4). Each test feeds a
// sequence of hand-built reports to writeKVCacheTokens over a fake API and reads the field back from it.
// Every expected size is a literal typed here. 400000 and 380000 are two workers' sizes, the smaller one
// worker-1's in most rows and worker-0's in the swapped one, so that neither "the first worker's" nor "the
// last worker's" gives the right answer by itself; none is the engine double's default.

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// kvWorker is one worker in a test report: its restart generation, its reported size (nil: none) and
// whether the router holds it healthy, and ejected.
type kvWorker struct {
	name    string
	gen     int64
	tokens  *int64
	healthy bool
	ejected bool
}

func kvw(name string, gen int64, tokens *int64, healthy bool) kvWorker {
	return kvWorker{name: name, gen: gen, tokens: tokens, healthy: healthy}
}

func tok(n int64) *int64 { return &n }

var kvNow = time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)

// kvView is the agentView of one report at instance generation instGen. With router (several workers) the
// report's router part runs and holds each worker as kvWorker says; without one, none.
func kvView(instGen int64, router bool, ws ...kvWorker) agentView {
	ir := &agentapi.InstanceReport{Generation: instGen}
	read := kvNow
	clusters := kvNow
	rr := &agentapi.RouterReport{Phase: agentapi.PartRunning, ClustersReadAt: &clusters}
	for _, w := range ws {
		ir.Workers = append(ir.Workers, agentapi.WorkerReport{Name: w.name, Phase: agentapi.PartRunning, Generation: w.gen, PID: 100,
			Health: &agentapi.Health{Time: kvNow, ProbeOK: true, MetricsReadAt: &read, KVCacheTokens: w.tokens}})
		rr.Workers = append(rr.Workers, agentapi.RouterWorker{Name: w.name, Healthy: w.healthy, Ejected: w.ejected})
	}
	if router {
		ir.Router = rr
	}
	return agentView{report: ir, reportAt: kvNow, reportTime: kvNow, now: kvNow, probeInterval: 10 * time.Second}
}

// kvRig is a fake API with one Instance, a Backend that holds the Allocation's state, and the pass that
// judges each report.
type kvRig struct {
	t     *testing.T
	c     client.Client
	p     *pass
	b     *Backend
	in    *v1alpha1.Instance
	alloc *v1alpha1.Allocation
}

// newKVRig makes an Instance of one worker, or of several behind a router.
func newKVRig(t *testing.T, workers int32) *kvRig {
	t.Helper()
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "inst", UID: "11111111-2222-3333-4444-555555555555"}}
	if workers > 1 {
		in.Spec.Launch.Workers = &workers
		in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	}
	alloc := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "alloc", UID: "alloc-1"}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(in).WithStatusSubresource(&v1alpha1.Instance{}).Build()
	rg := &kvRig{t: t, c: c, in: in, alloc: alloc}
	rg.restartController()
	return rg
}

// restartController is the controller's process starting again: its memory, the Backend's record included,
// is empty, and the API keeps the Instance as it is.
func (rg *kvRig) restartController() {
	rg.b = &Backend{agents: map[string]*agentState{string(rg.alloc.UID): {}}}
	rg.p = &pass{r: &Reconciler{Client: rg.c, Reader: rg.c, Backend: rg.b, Clock: pfake.NewClock(kvNow)}, ctx: context.Background(), now: kvNow}
}

// stored is the Instance as the API holds it.
func (rg *kvRig) stored() v1alpha1.Instance {
	rg.t.Helper()
	var got v1alpha1.Instance
	if err := rg.c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "inst"}, &got); err != nil {
		rg.t.Fatal(err)
	}
	return got
}

// feed judges one report, as a pass over the Instance does, and returns what the API holds then: the
// field, "unset" for none, and whether the pass wrote the Instance (its resourceVersion moved).
func (rg *kvRig) feed(av agentView) (field string, wrote bool) {
	rg.t.Helper()
	before := rg.stored().ResourceVersion
	if err := rg.p.writeKVCacheTokens(rg.in, rg.alloc, av); err != nil {
		rg.t.Fatal(err)
	}
	got := rg.stored()
	return kvString(got.Status.KVCacheTokens), got.ResourceVersion != before
}

func kvString(n *int64) string {
	if n == nil {
		return "unset"
	}
	return strconv.FormatInt(*n, 10)
}

// step is one report and what the field is after it.
type kvStep struct {
	what string
	view agentView
	want string
	// wrote: whether the pass wrote the Instance. Checked only when checkWrite.
	wrote, checkWrite bool
}

func (rg *kvRig) run(steps []kvStep) {
	rg.t.Helper()
	for _, s := range steps {
		got, wrote := rg.feed(s.view)
		if got != s.want {
			rg.t.Errorf("%s: the field is %s, want %s", s.what, got, s.want)
		}
		if s.checkWrite && wrote != s.wrote {
			rg.t.Errorf("%s: wrote the Instance %v, want %v", s.what, wrote, s.wrote)
		}
	}
}

// Two workers the router holds healthy, reporting 400000 and 380000: the smallest, 380000. With the
// 380000 worker out of the router's healthy set (unhealthy, or ejected) it is 400000; back, 380000 again.
// The sizes swap between workers in the last rows, so "the first worker's" is wrong in one order and
// "the last worker's" in the other.
func TestTheKVCacheSizeIsTheSmallestAmongTheRouterHealthyWorkers(t *testing.T) {
	rg := newKVRig(t, 2)
	both := func(a, b *int64, aHealthy, bHealthy bool) agentView {
		return kvView(0, true, kvw("worker-0", 0, a, aHealthy), kvw("worker-1", 0, b, bHealthy))
	}
	ejected := kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvWorker{name: "worker-1", tokens: tok(380000), healthy: true, ejected: true})
	rg.run([]kvStep{
		{"worker-0 400000, worker-1 380000, both healthy", both(tok(400000), tok(380000), true, true), "380000", true, true},
		{"the same report again writes nothing", both(tok(400000), tok(380000), true, true), "380000", false, true},
		{"the 380000 worker unhealthy", both(tok(400000), tok(380000), true, false), "400000", true, true},
		{"the 380000 worker healthy again", both(tok(400000), tok(380000), true, true), "380000", true, true},
		{"the 380000 worker ejected: healthy and ejected is not in the set", ejected, "400000", true, true},
		{"swapped: worker-0 380000, worker-1 400000", both(tok(380000), tok(400000), true, true), "380000", true, true},
		{"swapped, the 380000 worker unhealthy", both(tok(380000), tok(400000), false, true), "400000", true, true},
	})
}

// A healthy worker that reports no size is left out of the smallest, and the others' stand; a router with
// no healthy worker that reports one has nothing to give.
func TestAHealthyWorkerWithNoSizeIsLeftOutOfTheSmallest(t *testing.T) {
	rg := newKVRig(t, 2)
	rg.run([]kvStep{
		{"worker-1 reports none", kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, nil, true)), "400000", true, true},
		{"worker-0 reports none and worker-1 380000", kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, tok(380000), true)), "380000", true, true},
	})
	// A running worker that the router does not hold healthy is not counted for the router, whatever it reports.
	rg = newKVRig(t, 2)
	rg.run([]kvStep{
		{"no worker healthy", kvView(0, true, kvw("worker-0", 0, tok(400000), false), kvw("worker-1", 0, tok(380000), false)), "unset", false, true},
	})
	// An Instance of several workers with no router part in the report says nothing about who is healthy.
	rg.run([]kvStep{
		{"no router part", kvView(0, false, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true)), "unset", false, true},
	})
}

// One worker: its reported size, with no router or one that holds nothing healthy (the router's set is for
// several workers).
func TestOneWorkersKVCacheSizeIsItsReportedSize(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		{"worker-0 reports 400000", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", true, true},
		{"the same again", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", false, true},
	})
	rg = newKVRig(t, 1)
	rg.run([]kvStep{
		{"a router that holds the worker unhealthy does not matter with one worker", kvView(0, true, kvw("worker-0", 0, tok(380000), false)), "380000", true, true},
	})
	// A worker that is not running gives nothing: its report carries no Health, and a starting one is not "the running worker".
	rg = newKVRig(t, 1)
	av := kvView(0, false, kvw("worker-0", 0, tok(400000), false))
	av.report.Workers[0].Phase = agentapi.PartStarting
	rg.run([]kvStep{{"worker-0 starting", av, "unset", false, true}})
}

// No reading ever: the field stays unset and the pass writes nothing.
func TestNoKVCacheReadingLeavesTheFieldUnset(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		{"a report with no size", kvView(0, false, kvw("worker-0", 0, nil, false)), "unset", false, true},
		{"and another", kvView(0, false, kvw("worker-0", 0, nil, false)), "unset", false, true},
	})
	rg = newKVRig(t, 1)
	noReport := agentView{now: kvNow}
	rg.run([]kvStep{{"no report at all", noReport, "unset", false, true}})
}

// A report with no size after a written one leaves the field, while the worker's generations are the
// same; a new worker generation unsets it; so does a new whole-Instance generation; the size the new
// process then reports is written. The first of each pair shows the field was there to lose.
func TestAReportWithNoSizeLeavesTheFieldUnlessTheProcessChanged(t *testing.T) {
	one := func(inst, gen int64, tokens *int64) agentView {
		return kvView(inst, false, kvw("worker-0", gen, tokens, false))
	}
	for _, tc := range []struct {
		name  string
		steps []kvStep
	}{
		{"the same generations", []kvStep{
			{"a size", one(0, 0, tok(400000)), "400000", true, true},
			{"no size, the same generations", one(0, 0, nil), "400000", false, true},
			{"no size again", one(0, 0, nil), "400000", false, true},
			{"the size again", one(0, 0, tok(400000)), "400000", false, true},
		}},
		{"a new worker generation", []kvStep{
			{"a size", one(0, 0, tok(400000)), "400000", true, true},
			{"no size, worker generation 1", one(0, 1, nil), "unset", true, true},
			{"no size, worker generation 1, still", one(0, 1, nil), "unset", false, true},
			{"the new process reads its size", one(0, 1, tok(420000)), "420000", true, true},
			{"no size after it, the same generation", one(0, 1, nil), "420000", false, true},
		}},
		{"a new whole-Instance generation", []kvStep{
			{"a size", one(0, 0, tok(400000)), "400000", true, true},
			{"no size, Instance generation 1", one(1, 0, nil), "unset", true, true},
			{"the new process reads its size", one(1, 0, tok(400000)), "400000", true, true},
		}},
		{"a report of the old process after the raise still carries the size", []kvStep{
			{"a size", one(0, 0, tok(400000)), "400000", true, true},
			{"the old process again", one(0, 0, tok(400000)), "400000", false, true},
			{"the new generation with no size", one(0, 1, nil), "unset", true, true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newKVRig(t, 1).run(tc.steps)
		})
	}
}

// A size below 1 is no size (the field's minimum, 1): ignored, so a report that carries one writes nothing
// and does not replace a written size.
func TestASizeBelowOneIsIgnored(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		{"zero", kvView(0, false, kvw("worker-0", 0, tok(0), false)), "unset", false, true},
		{"negative", kvView(0, false, kvw("worker-0", 0, tok(-5), false)), "unset", false, true},
		{"a size", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", true, true},
		{"zero after it", kvView(0, false, kvw("worker-0", 0, tok(0), false)), "400000", false, true},
		{"one is the smallest size there is", kvView(0, false, kvw("worker-0", 0, tok(1), false)), "1", true, true},
	})
}

// The order of a worker's restart (w769, 18 Nov 2026): its new-generation reports carry no size, and it is
// outside the counted set; then its first good sample carries the new size, and the field is that size at
// the same pass that counts the worker again, never the old one.
func TestARestartedWorkersSizeIsUnsetThenTheNewOne(t *testing.T) {
	// One worker: its restart's reports have no size, then its own.
	rg := newKVRig(t, 1)
	one := func(gen int64, tokens *int64) agentView { return kvView(0, false, kvw("worker-0", gen, tokens, false)) }
	rg.run([]kvStep{
		{"serving at generation 0", one(0, tok(400000)), "400000", true, true},
		{"generation 1 started, no size yet", one(1, nil), "unset", true, true},
		{"generation 1 reads its size", one(1, tok(410000)), "410000", true, true},
	})

	// Two workers, worker-1 restarts alone: while it is out of the router's set, and has no size, the field
	// is worker-0's alone; when its new process is counted again with its new size, the field is the smallest
	// of the two at that very pass.
	rg = newKVRig(t, 2)
	two := func(gen1 int64, size1 *int64, healthy1 bool) agentView {
		return kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", gen1, size1, healthy1))
	}
	rg.run([]kvStep{
		{"both serving", two(0, tok(380000), true), "380000", true, true},
		{"worker-1 killed, restarted at generation 1: no size, not healthy", two(1, nil, false), "400000", true, true},
		{"worker-1's new process reads 370000 before it is verified: not healthy yet", two(1, tok(370000), false), "400000", false, true},
		{"worker-1 verified: healthy again, at the same pass its 370000 counts", two(1, tok(370000), true), "370000", true, true},
	})

	// Two workers, the whole Instance restarts: nothing is counted and no worker has a size, which unsets
	// the field; the new sizes then give the smallest.
	rg = newKVRig(t, 2)
	both := func(inst int64, s0, s1 *int64, healthy bool) agentView {
		return kvView(inst, true, kvw("worker-0", 0, s0, healthy), kvw("worker-1", 0, s1, healthy))
	}
	rg.run([]kvStep{
		{"both serving", both(0, tok(400000), tok(380000), true), "380000", true, true},
		{"the Instance's generation 1: no size, not healthy", both(1, nil, nil, false), "unset", true, true},
		{"the new processes read their sizes, not yet healthy", both(1, tok(400000), tok(390000), false), "unset", false, true},
		{"healthy", both(1, tok(400000), tok(390000), true), "390000", true, true},
	})
}

// After a controller restart its memory is empty: a report with no size leaves the field, since it cannot
// tell a changed process from a restart's; the next report with a size writes it. The stated limit.
func TestAControllerRestartLosesTheRecordAndASizelessReportLeavesTheField(t *testing.T) {
	rg := newKVRig(t, 1)
	one := func(gen int64, tokens *int64) agentView { return kvView(0, false, kvw("worker-0", gen, tokens, false)) }
	rg.run([]kvStep{{"a size", one(0, tok(400000)), "400000", true, true}})
	rg.restartController()
	rg.run([]kvStep{
		{"generation 1 with no size, after a controller restart", one(1, nil), "400000", false, true},
		{"its size", one(1, tok(410000)), "410000", true, true},
		{"and now a change is seen again", one(2, nil), "unset", true, true},
	})
}

// The set the size is taken over is the set healthyCount counts.
func TestTheKVCacheSizeIsTakenOverHealthyCountsSet(t *testing.T) {
	in := &v1alpha1.Instance{}
	two := int32(2)
	in.Spec.Launch.Workers = &two
	in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	for _, tc := range []struct {
		name string
		ws   []kvWorker
		want []string
	}{
		{"both", []kvWorker{kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)}, []string{"worker-0", "worker-1"}},
		{"one unhealthy", []kvWorker{kvw("worker-0", 0, nil, false), kvw("worker-1", 0, nil, true)}, []string{"worker-1"}},
		{"one ejected", []kvWorker{{name: "worker-0", healthy: true, ejected: true}, kvw("worker-1", 0, nil, true)}, []string{"worker-1"}},
		{"none", []kvWorker{kvw("worker-0", 0, nil, false), kvw("worker-1", 0, nil, false)}, nil},
	} {
		av := kvView(0, true, tc.ws...)
		names, ok := routerHealthy(in, av)
		n, nok := healthyCount(in, av)
		if !ok || !nok || int(n) != len(tc.want) || len(names) != len(tc.want) {
			t.Errorf("%s: routerHealthy %v (%v), healthyCount %d (%v), want %v", tc.name, names, ok, n, nok, tc.want)
			continue
		}
		for i := range names {
			if names[i] != tc.want[i] {
				t.Errorf("%s: routerHealthy %v, want %v", tc.name, names, tc.want)
			}
		}
	}
}
