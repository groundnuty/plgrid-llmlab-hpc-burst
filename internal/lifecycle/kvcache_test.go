package lifecycle

// The controller's side of the KV cache size: status.kvCacheTokens from the agent's reports (kvcache.go;
// HBEP-7, The KV cache size; `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4 as revised, comment
// 6988932328). Each test feeds a sequence of hand-built reports to writeWorkerCounts over a fake API and
// reads the fields back from it. Every expected number is a literal typed here. 400000 and 380000 are two
// workers' sizes, the smaller one worker-1's in most rows and worker-0's in the swapped one, so that
// neither "the first worker's" nor "the last worker's" gives the right answer by itself; none is the
// engine double's default.

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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// kvWorker is one worker in a test report: its restart generation, its reported size (nil: none), whether
// the router holds it healthy, ejected, and when its process started (zero: an hour before the report).
type kvWorker struct {
	name    string
	gen     int64
	tokens  *int64
	healthy bool
	ejected bool
	started time.Time
}

func kvw(name string, gen int64, tokens *int64, healthy bool) kvWorker {
	return kvWorker{name: name, gen: gen, tokens: tokens, healthy: healthy}
}

func tok(n int64) *int64 { return &n }

var kvNow = time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)

// kvProbe is the probe interval of these reports: a router read is fresh for twice it, 20 s.
const kvProbe = 10 * time.Second

// kvView is the agentView of one report at kvNow, instance generation instGen, whose router read was at kvNow.
func kvView(instGen int64, router bool, ws ...kvWorker) agentView {
	return kvViewRead(kvNow, instGen, router, ws...)
}

// kvViewRead is kvView with the router's /clusters read taken at read. With router (several workers) the
// report's router part runs and holds each worker as kvWorker says; without one, none.
func kvViewRead(read time.Time, instGen int64, router bool, ws ...kvWorker) agentView {
	ir := &agentapi.InstanceReport{Generation: instGen}
	metricsRead := kvNow
	clusters := read
	rr := &agentapi.RouterReport{Phase: agentapi.PartRunning, ClustersReadAt: &clusters}
	for _, w := range ws {
		start := w.started
		if start.IsZero() {
			start = kvNow.Add(-time.Hour)
		}
		ir.Workers = append(ir.Workers, agentapi.WorkerReport{Name: w.name, Phase: agentapi.PartRunning, Generation: w.gen, PID: 100, StartTime: &start,
			Health: &agentapi.Health{Time: kvNow, ProbeOK: true, MetricsReadAt: &metricsRead, KVCacheTokens: w.tokens}})
		rr.Workers = append(rr.Workers, agentapi.RouterWorker{Name: w.name, Healthy: w.healthy, Ejected: w.ejected})
	}
	if router {
		ir.Router = rr
	}
	return agentView{report: ir, reportAt: kvNow, reportTime: kvNow, now: kvNow, probeInterval: kvProbe}
}

// kvRig is a fake API with one Instance and the pass that judges each report.
type kvRig struct {
	t  *testing.T
	c  client.Client
	p  *pass
	in *v1alpha1.Instance
	// updates are the status updates the API saw: the pair (healthyWorkers, kvCacheTokens) each carried.
	updates *[]string
}

// newKVRig makes an Instance of one worker, or of several behind a router.
func newKVRig(t *testing.T, workers int32) *kvRig {
	t.Helper()
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "inst", UID: "11111111-2222-3333-4444-555555555555"}}
	if workers > 1 {
		in.Spec.Launch.Workers = &workers
		in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	updates := &[]string{}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(in).WithStatusSubresource(&v1alpha1.Instance{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if i, ok := obj.(*v1alpha1.Instance); ok {
				*updates = append(*updates, countString(i.Status.HealthyWorkers)+"/"+kvString(i.Status.KVCacheTokens))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		}}).Build()
	rg := &kvRig{t: t, c: c, in: in, updates: updates}
	rg.restartController()
	return rg
}

// restartController is the controller's process starting again: the pass over the API is new, and the
// API keeps the Instance as it is. The controller holds nothing between passes for the field.
func (rg *kvRig) restartController() {
	rg.p = &pass{r: &Reconciler{Client: rg.c, Reader: rg.c, Backend: &Backend{}, Clock: pfake.NewClock(kvNow)}, ctx: context.Background(), now: kvNow}
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

func countString(n *int32) string {
	if n == nil {
		return "-"
	}
	return strconv.Itoa(int(*n))
}

func kvString(n *int64) string {
	if n == nil {
		return "unset"
	}
	return strconv.FormatInt(*n, 10)
}

// feed judges one report, as a pass over the Instance does, and returns what the API holds then: the KV
// cache size ("unset" for none), healthyWorkers ("-" for none), and how many status updates the pass made.
func (rg *kvRig) feed(av agentView) (field, healthy string, updates int) {
	rg.t.Helper()
	before := len(*rg.updates)
	if err := rg.p.writeWorkerCounts(rg.in, av); err != nil {
		rg.t.Fatal(err)
	}
	got := rg.stored()
	return kvString(got.Status.KVCacheTokens), countString(got.Status.HealthyWorkers), len(*rg.updates) - before
}

// kvStep is one report and what the fields are after it. healthy is checked when not empty; updates is the
// number of status updates the pass made, checked when updatesSet.
type kvStep struct {
	what       string
	view       agentView
	want       string
	healthy    string
	updates    int
	updatesSet bool
}

func (rg *kvRig) run(steps []kvStep) {
	rg.t.Helper()
	for _, s := range steps {
		got, healthy, updates := rg.feed(s.view)
		if got != s.want {
			rg.t.Errorf("%s: the KV cache size is %s, want %s", s.what, got, s.want)
		}
		if s.healthy != "" && healthy != s.healthy {
			rg.t.Errorf("%s: healthyWorkers is %s, want %s", s.what, healthy, s.healthy)
		}
		if s.updatesSet && updates != s.updates {
			rg.t.Errorf("%s: %d status updates, want %d", s.what, updates, s.updates)
		}
	}
}

// kvstep is a kvStep with the update count checked.
func kvstep(what string, view agentView, want, healthy string, updates int) kvStep {
	return kvStep{what: what, view: view, want: want, healthy: healthy, updates: updates, updatesSet: true}
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
		kvstep("worker-0 400000, worker-1 380000, both healthy", both(tok(400000), tok(380000), true, true), "380000", "2", 1),
		kvstep("the same report again writes nothing", both(tok(400000), tok(380000), true, true), "380000", "2", 0),
		kvstep("the 380000 worker unhealthy", both(tok(400000), tok(380000), true, false), "400000", "1", 1),
		kvstep("the 380000 worker healthy again", both(tok(400000), tok(380000), true, true), "380000", "2", 1),
		kvstep("the 380000 worker ejected: healthy and ejected is not in the set", ejected, "400000", "1", 1),
		kvstep("swapped: worker-0 380000, worker-1 400000", both(tok(380000), tok(400000), true, true), "380000", "2", 1),
		kvstep("swapped, the 380000 worker unhealthy", both(tok(380000), tok(400000), false, true), "400000", "1", 1),
	})
}

// A healthy worker that reports no size is left out of the smallest, and the others' stand; a router with
// no healthy worker has no size to give, and healthyWorkers says 0 as it does today.
func TestAHealthyWorkerWithNoSizeIsLeftOutOfTheSmallest(t *testing.T) {
	rg := newKVRig(t, 2)
	rg.run([]kvStep{
		kvstep("worker-1 reports none", kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, nil, true)), "400000", "2", 1),
		kvstep("worker-0 reports none and worker-1 380000", kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, tok(380000), true)), "380000", "2", 1),
		kvstep("no worker healthy: nothing to take the size from, healthyWorkers 0", kvView(0, true, kvw("worker-0", 0, tok(400000), false), kvw("worker-1", 0, tok(380000), false)), "unset", "0", 1),
		kvstep("no router part in the report: no size, and healthyWorkers stays", kvView(0, false, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true)), "unset", "0", 0),
	})
}

// One worker: its reported size, with no router or one that holds nothing healthy (the router's set is for
// several workers); a worker that is not running gives nothing.
func TestOneWorkersKVCacheSizeIsItsReportedSize(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		kvstep("worker-0 reports 400000", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", "-", 1),
		kvstep("the same again", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", "-", 0),
	})
	rg = newKVRig(t, 1)
	rg.run([]kvStep{
		kvstep("a router that holds the worker unhealthy does not matter with one worker", kvView(0, true, kvw("worker-0", 0, tok(380000), false)), "380000", "-", 1),
	})
	rg = newKVRig(t, 1)
	av := kvView(0, false, kvw("worker-0", 0, tok(400000), false))
	av.report.Workers[0].Phase = agentapi.PartStarting
	rg.run([]kvStep{kvstep("worker-0 starting", av, "unset", "-", 0)})
}

// No reading ever: the field stays unset and the pass writes nothing; nor does a pass with no report.
func TestNoKVCacheReadingLeavesTheFieldUnset(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		kvstep("a report with no size", kvView(0, false, kvw("worker-0", 0, nil, false)), "unset", "-", 0),
		kvstep("and another", kvView(0, false, kvw("worker-0", 0, nil, false)), "unset", "-", 0),
		kvstep("no report at all", agentView{now: kvNow}, "unset", "-", 0),
	})
}

// The revised Q4: a report with no size UNSETS the field, whatever the worker's generations; the field is
// the size of the counted workers of this report, nothing the controller remembers. The sizes the workers
// then report are written. (This replaces the rows that asserted a sizeless report leaves the field.)
func TestAReportWithNoSizeUnsetsTheField(t *testing.T) {
	one := func(inst, gen int64, tokens *int64) agentView {
		return kvView(inst, false, kvw("worker-0", gen, tokens, false))
	}
	newKVRig(t, 1).run([]kvStep{
		kvstep("a size", one(0, 0, tok(400000)), "400000", "-", 1),
		kvstep("no size, the same generations", one(0, 0, nil), "unset", "-", 1),
		kvstep("no size again writes nothing more", one(0, 0, nil), "unset", "-", 0),
		kvstep("the size again", one(0, 0, tok(400000)), "400000", "-", 1),
		kvstep("no size, worker generation 1", one(0, 1, nil), "unset", "-", 1),
		kvstep("the new process reads its size", one(0, 1, tok(420000)), "420000", "-", 1),
		kvstep("no size, Instance generation 1", one(1, 1, nil), "unset", "-", 1),
		kvstep("and its size", one(1, 1, tok(400000)), "400000", "-", 1),
	})
}

// A size below 1 is no size (the field's minimum, 1): ignored, so a report that carries only one leaves
// nothing to write, and a written size is unset by it like by any report with none.
func TestASizeBelowOneIsIgnored(t *testing.T) {
	rg := newKVRig(t, 1)
	rg.run([]kvStep{
		kvstep("zero", kvView(0, false, kvw("worker-0", 0, tok(0), false)), "unset", "-", 0),
		kvstep("negative", kvView(0, false, kvw("worker-0", 0, tok(-5), false)), "unset", "-", 0),
		kvstep("a size", kvView(0, false, kvw("worker-0", 0, tok(400000), false)), "400000", "-", 1),
		kvstep("zero after it", kvView(0, false, kvw("worker-0", 0, tok(0), false)), "unset", "-", 1),
		kvstep("one is the smallest size there is", kvView(0, false, kvw("worker-0", 0, tok(1), false)), "1", "-", 1),
	})
}

// The order of a worker's restart: its new-generation reports carry no size, and it is outside the
// counted set; then its first good sample carries the new size, and the field is that size at the pass
// that counts the worker again, never the old one.
func TestARestartedWorkersSizeIsUnsetThenTheNewOne(t *testing.T) {
	// One worker: its restart's reports have no size, then its own.
	rg := newKVRig(t, 1)
	one := func(gen int64, tokens *int64) agentView { return kvView(0, false, kvw("worker-0", gen, tokens, false)) }
	rg.run([]kvStep{
		kvstep("serving at generation 0", one(0, tok(400000)), "400000", "-", 1),
		kvstep("generation 1 started, no size yet", one(1, nil), "unset", "-", 1),
		kvstep("generation 1 reads its size", one(1, tok(410000)), "410000", "-", 1),
	})

	// Two workers, worker-1 restarts alone: while it is out of the router's set the field is worker-0's
	// alone; when its new process is counted again with its new size, the field is the smallest of the two
	// at that very pass.
	rg = newKVRig(t, 2)
	two := func(gen1 int64, size1 *int64, healthy1 bool) agentView {
		return kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", gen1, size1, healthy1))
	}
	rg.run([]kvStep{
		kvstep("both serving", two(0, tok(380000), true), "380000", "2", 1),
		kvstep("worker-1 restarted at generation 1: no size, not healthy", two(1, nil, false), "400000", "1", 1),
		kvstep("worker-1's new process reads 370000 before it is verified: not healthy yet", two(1, tok(370000), false), "400000", "1", 0),
		kvstep("worker-1 verified: healthy again, at the same pass its 370000 counts", two(1, tok(370000), true), "370000", "2", 1),
	})

	// Two workers, the whole Instance restarts: nothing is counted, which unsets the field; the new sizes
	// then give the smallest once the router holds the workers healthy.
	rg = newKVRig(t, 2)
	both := func(inst int64, s0, s1 *int64, healthy bool) agentView {
		return kvView(inst, true, kvw("worker-0", 0, s0, healthy), kvw("worker-1", 0, s1, healthy))
	}
	rg.run([]kvStep{
		kvstep("both serving", both(0, tok(400000), tok(380000), true), "380000", "2", 1),
		kvstep("the Instance's generation 1: no size, not healthy", both(1, nil, nil, false), "unset", "0", 1),
		kvstep("the new processes read their sizes, not yet healthy", both(1, tok(400000), tok(390000), false), "unset", "0", 0),
		kvstep("healthy", both(1, tok(400000), tok(390000), true), "390000", "2", 1),
	})
}

// Row (1) of the revised Q4: after a controller restart, which leaves the controller no memory, a
// one-worker Instance whose replacement process reports NO size is UNSET, not left at its predecessor's.
func TestAControllerRestartNeverLeavesAPredecessorsSize(t *testing.T) {
	rg := newKVRig(t, 1)
	one := func(gen int64, tokens *int64) agentView { return kvView(0, false, kvw("worker-0", gen, tokens, false)) }
	rg.run([]kvStep{kvstep("the predecessor's size", one(0, tok(400000)), "400000", "-", 1)})
	rg.restartController()
	rg.run([]kvStep{
		kvstep("the replacement at generation 1, no size, after a controller restart", one(1, nil), "unset", "-", 1),
		kvstep("its size", one(1, tok(410000)), "410000", "-", 1),
	})
	// The same with the replacement reporting at the predecessor's own generations, and a pass with no
	// report at all, which is what a restarted controller sees before the agent's first report.
	rg = newKVRig(t, 1)
	rg.run([]kvStep{kvstep("a size", one(0, tok(400000)), "400000", "-", 1)})
	rg.restartController()
	rg.run([]kvStep{
		kvstep("a controller restart, before the agent's first report", agentView{now: kvNow}, "unset", "-", 1),
		kvstep("the first report", one(0, tok(400000)), "400000", "-", 1),
	})
}

// Row (3): two workers, only worker-0 reports a size; ejecting it leaves no counted worker with one, and
// the field is UNSET, not the ejected worker's size.
func TestEjectingTheOnlyWorkerThatReportsASizeUnsetsTheField(t *testing.T) {
	rg := newKVRig(t, 2)
	ejected := kvView(0, true, kvWorker{name: "worker-0", tokens: tok(400000), healthy: true, ejected: true}, kvw("worker-1", 0, nil, true))
	unhealthy := kvView(0, true, kvw("worker-0", 0, tok(400000), false), kvw("worker-1", 0, nil, true))
	rg.run([]kvStep{
		kvstep("worker-0 400000 healthy, worker-1 reports none", kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, nil, true)), "400000", "2", 1),
		kvstep("worker-0 ejected", ejected, "unset", "1", 1),
		kvstep("worker-0 back", kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, nil, true)), "400000", "2", 1),
		kvstep("worker-0 unhealthy", unhealthy, "unset", "1", 1),
	})
}

// Row (2): healthyWorkers and the KV cache size change in the same pass, 1 worker at 400000 to 2 at 100000
// when a 100000 worker rejoins, and the API sees ONE status update carrying both, never a mixed pair
// between two. The pair each update carried is recorded, so the check is the updates' content, not timing.
func TestHealthyWorkersAndTheKVCacheSizeAreWrittenInOneUpdate(t *testing.T) {
	rg := newKVRig(t, 2)
	alone := kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(100000), false))
	rejoined := kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(100000), true))
	rg.run([]kvStep{
		kvstep("worker-1 not healthy yet", alone, "400000", "1", 1),
		kvstep("worker-1 rejoins: both fields change in one pass", rejoined, "100000", "2", 1),
	})
	if want := []string{"1/400000", "2/100000"}; len(*rg.updates) != 2 || (*rg.updates)[0] != want[0] || (*rg.updates)[1] != want[1] {
		t.Errorf("the status updates carried (healthyWorkers/kvCacheTokens) %v, want %v: a mixed pair 2/400000 was published", *rg.updates, want)
	}
	// And leaving it again: 2/100000 to 1/400000 in one update.
	rg.run([]kvStep{kvstep("worker-1 leaves again", alone, "400000", "1", 1)})
	if got := (*rg.updates)[len(*rg.updates)-1]; got != "1/400000" || len(*rg.updates) != 3 {
		t.Errorf("the updates %v, want the third to be 1/400000", *rg.updates)
	}
}

// Row (4a): the router's report names a worker and no process. A replacement whose process started after
// the router's last read is not counted for the size under its predecessor's healthy flag: the field stays
// the other worker's until a read taken after the replacement's start marks it healthy. healthyWorkers
// follows the same identity rule since the ruling on #764 of 18 Nov 2026 (identityHealthy): 1 meanwhile, not 2.
func TestAReplacementCountsForTheKVCacheSizeOnlyOnceTheRouterHasReadItsProcess(t *testing.T) {
	rg := newKVRig(t, 2)
	read := kvNow.Add(-10 * time.Second) // the router's last read, fresh
	replaced := kvWorker{name: "worker-1", gen: 1, tokens: tok(100000), healthy: true, started: kvNow.Add(-5 * time.Second)}
	rg.run([]kvStep{
		kvstep("both serving: the old worker-1 is 380000", kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true)), "380000", "2", 1),
		kvstep("worker-1 replaced after the router's read, still flagged healthy: its 100000 does not count", kvViewRead(read, 0, true, kvw("worker-0", 0, tok(400000), true), replaced), "400000", "1", 1),
		kvstep("the router reads again, after the replacement started: it is healthy and counts", kvView(0, true, kvw("worker-0", 0, tok(400000), true), replaced), "100000", "2", 1),
	})
	// A worker with no start time is a process the controller cannot place: not counted.
	rg = newKVRig(t, 2)
	av := kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true))
	av.report.Workers[1].StartTime = nil
	rg.run([]kvStep{kvstep("worker-1 with no start time", av, "400000", "1", 1)})
}

// Row (4b): a failed or stale /clusters read is unknown, so no worker counts for the size and it is UNSET.
// The agent keeps the last read when a read fails and its time says how old it is (routerRead): a read
// older than two probe intervals (20 s here) at the report's time is unknown, exactly two is still
// fresh. healthyWorkers follows the same read (identityHealthy): past the window it is 0, the Instance is
// skipped, and a fresh read writes the count again.
func TestAStaleRouterReadMakesEveryWorkerUnknownForTheKVCacheSize(t *testing.T) {
	rg := newKVRig(t, 2)
	both := func(read time.Time) agentView {
		return kvViewRead(read, 0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true))
	}
	rg.run([]kvStep{
		kvstep("a fresh read", both(kvNow), "380000", "2", 1),
		kvstep("a read exactly two probe intervals old is still fresh", both(kvNow.Add(-2*kvProbe)), "380000", "2", 0),
		kvstep("a read a second older than that is unknown: no size, healthyWorkers 0", both(kvNow.Add(-2*kvProbe-time.Second)), "unset", "0", 1),
		kvstep("a read a minute old, the same", both(kvNow.Add(-time.Minute)), "unset", "0", 0),
		kvstep("a read again, fresh", both(kvNow), "380000", "2", 1),
	})
	// A probe interval that is not known judges nothing fresh.
	rg = newKVRig(t, 2)
	av := both(kvNow)
	av.probeInterval = 0
	rg.run([]kvStep{kvstep("no probe interval", av, "unset", "0", 1)})
	// A router report with no read at all says nothing about the workers: healthyCount takes none either.
	rg = newKVRig(t, 2)
	av = both(kvNow)
	av.report.Router.ClustersReadAt = nil
	rg.run([]kvStep{kvstep("no read yet", av, "unset", "-", 0)})
}

// The set the size is taken over is a subset of healthyCount's, and since the ruling of 18 Nov 2026 on
// healthyWorkers they are one join (identityHealthy): the counts below are typed by hand, and equal the
// KV set's length in every row but the past-the-window one, where the count is 0 and the set empty.
func TestTheKVCacheSetIsASubsetOfHealthyCountsSet(t *testing.T) {
	in := &v1alpha1.Instance{}
	two := int32(2)
	in.Spec.Launch.Workers = &two
	in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	for _, tc := range []struct {
		name  string
		av    agentView
		count int32
		kv    []string
	}{
		{"both", kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)), 2, []string{"worker-0", "worker-1"}},
		{"one unhealthy", kvView(0, true, kvw("worker-0", 0, nil, false), kvw("worker-1", 0, nil, true)), 1, []string{"worker-1"}},
		{"one ejected", kvView(0, true, kvWorker{name: "worker-0", healthy: true, ejected: true}, kvw("worker-1", 0, nil, true)), 1, []string{"worker-1"}},
		{"none", kvView(0, true, kvw("worker-0", 0, nil, false), kvw("worker-1", 0, nil, false)), 0, nil},
		{"one replaced after the read", kvViewRead(kvNow.Add(-time.Second), 0, true, kvw("worker-0", 0, nil, true), kvWorker{name: "worker-1", healthy: true, started: kvNow}), 1, []string{"worker-0"}},
		{"a stale read", kvViewRead(kvNow.Add(-time.Minute), 0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)), 0, nil},
	} {
		n, ok := healthyCount(in, tc.av)
		got := kvRouterWorkers(in, tc.av)
		if !ok || n != tc.count || len(got) != len(tc.kv) {
			t.Errorf("%s: healthyCount %d (%v), KV set %v, want %d and %v", tc.name, n, ok, got, tc.count, tc.kv)
			continue
		}
		for i := range got {
			if got[i] != tc.kv[i] {
				t.Errorf("%s: KV set %v, want %v", tc.name, got, tc.kv)
			}
		}
	}
}
