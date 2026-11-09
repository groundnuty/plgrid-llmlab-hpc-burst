package stack

// The catch-up barrier (issue #281), checked without a stack: the Recorder is
// fed by hand, and the pause between two reads of it is the test's, so that the
// order of the API and the watch is the test's, not the machine's. A late
// delivery waits on that count, not on a fixed window, and ends at once.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func inst(uid, name, rv string) *v1alpha1.Instance {
	return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: name, ResourceVersion: rv}}
}

func alloc(uid, name, rv string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: name, ResourceVersion: rv}}
}

func deployment(uid, name, rv string) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: name, ResourceVersion: rv}}
}

func listOf(collection map[Kinds]string, objs ...listedObject) func() (listing, error) {
	return func() (listing, error) { return listing{objs: objs, collection: collection}, nil }
}

func listed(kind Kinds, uid, name, rv string) listedObject {
	return listedObject{kind: kind, uid: types.UID(uid), name: name, rv: rv}
}

// pausing returns a pause that counts its calls and runs step on each.
func pausing(step func(n int)) (pause func(), calls *int) {
	n := 0
	return func() { n++; step(n) }, &n
}

// reach moves the kind's stream to the version, as Follow does on an event or a
// bookmark.
func reach(rec *Recorder, kind Kinds, rv string) { rec.stream(kind, nil).Reached(rv) }

// bg is a catch-up with no deadline of its own.
var bg = context.Background()

// noDeadlineT is a test with no deadline, so that a small -test.timeout cannot
// cut a wait these tests order themselves.
type noDeadlineT struct{ *testing.T }

func (noDeadlineT) Deadline() (time.Time, bool) { return time.Time{}, false }

// An update the API lists that the watch delivers only after the first read:
// the catch-up waits for it, and says by how long the watch trailed.
func TestCatchUpWaitsForALateVersion(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "100"), false)
	reach(rec, KindInstances, "100")
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.add(inst("i1", "inst-a", "105"), false)
			reach(rec, KindInstances, "105")
		}
	})
	l := listOf(map[Kinds]string{KindInstances: "105"}, listed(KindInstances, "i1", "inst-a", "105"))
	res := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances, "a late update", WaitDeadline, pause)
	if !res.OK() || res.Waited <= 0 || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK, a wait, and one pause", res, *calls)
	}
}

// An object the API lists that the watch has not delivered at all.
func TestCatchUpWaitsForAnObjectNotYetSeen(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindAllocations, "5")
	pause, calls := pausing(func(n int) {
		if n == 2 {
			rec.addAlloc(alloc("a1", "alloc-a", "7"), false)
			reach(rec, KindAllocations, "7")
		}
	})
	l := listOf(map[Kinds]string{KindAllocations: "7"}, listed(KindAllocations, "a1", "alloc-a", "7"))
	res := catchUp(bg, noDeadlineT{t}, rec, l, KindAllocations, "a late creation", WaitDeadline, pause)
	if !res.OK() || *calls != 2 {
		t.Errorf("result %+v after %d pauses; want OK after two", res, *calls)
	}
}

// An object the API no longer lists, whose deletion the watch delivers only
// after the first read (issue #281): the catch-up waits for the deletion.
func TestCatchUpWaitsForALateDeletion(t *testing.T) {
	rec := &Recorder{}
	rec.addAlloc(alloc("a1", "alloc-a", "150"), false)
	reach(rec, KindAllocations, "150")
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.addAlloc(alloc("a1", "alloc-a", "180"), true)
			reach(rec, KindAllocations, "200")
		}
	})
	res := catchUp(bg, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "a late deletion", WaitDeadline, pause)
	if !res.OK() || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK after one", res, *calls)
	}
}

// PR #292's B1: an object created and deleted while the watch was behind is in
// neither the LIST nor the Recorder, so that no comparison of objects can tell.
// The catch-up waits for the stream to reach the LIST's version, and the read
// after it holds the object's versions, a Failed one among them.
func TestCatchUpWaitsForAnObjectThatCameAndWent(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindAllocations, "100")
	failed := alloc("a1", "alloc-brief", "150")
	failed.Status.AtProvider.Phase = v1alpha1.AllocationFailed
	pause, calls := pausing(func(n int) {
		switch n {
		case 1:
			rec.addAlloc(failed, false)
			reach(rec, KindAllocations, "150")
		case 2:
			rec.addAlloc(alloc("a1", "alloc-brief", "180"), true)
			reach(rec, KindAllocations, "180")
		case 3:
			reach(rec, KindAllocations, "200") // a bookmark
		}
	})
	res := catchUp(bg, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "the Failed scan", WaitDeadline, pause)
	if !res.OK() || *calls != 3 {
		t.Fatalf("result %+v after %d pauses; want OK after three, when the stream reached the LIST's version", res, *calls)
	}
	if got := rec.Allocations(); len(got) != 2 || got[0].Phase != v1alpha1.AllocationFailed || !got[1].Deleted {
		t.Errorf("the record holds %+v; want the Failed version and the deletion", got)
	}
}

// A stream that does not reach the LIST's version in the wait cannot tell: it is
// not known, not caught up, and nothing is called lost.
func TestCatchUpWhenTheStreamDoesNotReachTheListIsNotKnown(t *testing.T) {
	for name, at := range map[string]string{"short of it": "100", "no version yet": ""} {
		t.Run(name, func(t *testing.T) {
			rec := &Recorder{}
			if at != "" {
				reach(rec, KindAllocations, at)
			}
			pause := func() { time.Sleep(time.Millisecond) }
			res := catchUp(bg, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "a quiet stream", 20*time.Millisecond, pause)
			if res.OK() || len(res.Missing) != 0 || !strings.Contains(res.Unknown, "stream") || !strings.Contains(res.Unknown, "200") {
				t.Errorf("result %+v; want not known, naming the stream and the LIST's version", res)
			}
		})
	}
}

// A stream at the LIST's version with a listed object still missing has lost that
// object's event; the catch-up names the object rather than taking the stream's
// word for it.
func TestCatchUpWaitsForAListedObjectBehindAStreamAtTheList(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindInstances, "500")
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.add(inst("i1", "inst-old", "300"), false)
		}
	})
	l := listOf(map[Kinds]string{KindInstances: "500"}, listed(KindInstances, "i1", "inst-old", "300"))
	res := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances, "an object behind the stream", WaitDeadline, pause)
	if !res.OK() || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK after one", res, *calls)
	}
}

// A stream does not go back, and one version that does not parse leaves it where
// it is.
func TestStreamReachedNeverGoesBack(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindInstances, "500")
	reach(rec, KindInstances, "300")
	reach(rec, KindInstances, "not-a-number")
	if got := rec.streams[KindInstances].reached; got != "500" {
		t.Errorf("the stream is at %q, want 500", got)
	}
	reach(rec, KindInstances, "501")
	if got := rec.streams[KindInstances].reached; got != "501" {
		t.Errorf("the stream is at %q, want 501", got)
	}
}

// An object the watch holds that the LIST lacks because it was created or
// changed after the LIST is not a deletion the watch is behind on.
func TestCatchUpDoesNotTakeANewerObjectForADeletion(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-new", "210"), false)
	reach(rec, KindInstances, "210")
	pause, calls := pausing(func(int) { t.Error("the catch-up waited for an object created after its LIST") })
	res := catchUp(bg, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindInstances: "200"}), KindInstances, "a newer object", WaitDeadline, pause)
	if !res.OK() || *calls != 0 {
		t.Errorf("result %+v after %d pauses; want OK with none", res, *calls)
	}
}

// A newer version than the one listed, an object already deleted, and a kind
// the catch-up does not cover are all caught up.
func TestCatchUpIgnoresWhatIsAheadDeletedOrNotCovered(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "300"), false)
	rec.add(inst("i2", "inst-gone", "120"), true)
	reach(rec, KindInstances, "300")
	rec.addAlloc(alloc("a1", "alloc-behind", "50"), false)
	reach(rec, KindAllocations, "50")
	l := listOf(map[Kinds]string{KindInstances: "200", KindAllocations: "200"},
		listed(KindInstances, "i1", "inst-a", "250"), listed(KindAllocations, "a1", "alloc-behind", "60"))
	pause, calls := pausing(func(int) {})
	if res := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances, "covered", time.Minute, pause); !res.OK() || *calls != 0 {
		t.Errorf("the Instances: result %+v after %d pauses; want OK with none", res, *calls)
	}
}

// ModelDeployments: a listed one the watch lacks is behind; the Recorder keeps
// no deletion of one, so an absent one is not.
func TestCatchUpDeployments(t *testing.T) {
	rec := &Recorder{}
	rec.addDeployment(deployment("d-old", "md-old", "20"))
	reach(rec, KindDeployments, "20")
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.addDeployment(deployment("d1", "md-a", "40"))
			reach(rec, KindDeployments, "100")
		}
	})
	l := listOf(map[Kinds]string{KindDeployments: "100"}, listed(KindDeployments, "d1", "md-a", "40"))
	if res := catchUp(bg, noDeadlineT{t}, rec, l, KindDeployments, "deployments", WaitDeadline, pause); !res.OK() || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK after one", res, *calls)
	}
}

// An event that never comes is a lost event, reported with the objects and
// versions. It is reported once, and a later catch-up does not wait for it
// again; but the later result still says it is lost: the record does not hold
// it, whichever catch-up found that (PR #292's review).
func TestCatchUpReportsALostEventOnceAndKeepsItLost(t *testing.T) {
	rec := &Recorder{}
	rec.addAlloc(alloc("a1", "alloc-a", "150"), false)
	rec.add(inst("i1", "inst-a", "100"), false)
	reach(rec, KindAllocations, "200")
	reach(rec, KindInstances, "200")
	l := listOf(map[Kinds]string{KindAllocations: "200", KindInstances: "200"}, listed(KindInstances, "i1", "inst-a", "105"))
	pause := func() { time.Sleep(2 * time.Millisecond) }
	res := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances|KindAllocations, "a lost event", 150*time.Millisecond, pause)
	if res.OK() || res.Unknown != "" || len(res.Missing) != 2 {
		t.Fatalf("result %+v; want two lost events and no unknown", res)
	}
	p := res.Problem()
	for _, want := range []string{"the event was lost", "Allocation alloc-a: gone from the API, and the watch delivered no deletion (its newest version is 150)",
		"Instance inst-a: the API lists version 105, and the watch delivered up to 100"} {
		if !strings.Contains(p, want) {
			t.Errorf("the problem %q does not say %q", p, want)
		}
	}
	if !res.Lost("a1") || !res.Lost("i1") || res.Lost("other") {
		t.Errorf("Lost() of the reported objects: %v %v %v", res.Lost("a1"), res.Lost("i1"), res.Lost("other"))
	}
	pause2, calls := pausing(func(int) {})
	again := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances|KindAllocations, "the next site", time.Minute, pause2)
	if *calls != 0 {
		t.Errorf("the next catch-up waited: %d pauses, want none", *calls)
	}
	if again.Problem() != "" {
		t.Errorf("the next catch-up reported %q again, want nothing", again.Problem())
	}
	if again.OK() || !again.Lost("a1") || !again.Lost("i1") {
		t.Errorf("the next catch-up: %+v; want the objects still lost, and not OK", again)
	}
	onlyAllocs := catchUp(bg, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "one kind", time.Minute, func() {})
	if !onlyAllocs.Lost("a1") || onlyAllocs.Lost("i1") {
		t.Errorf("a catch-up of the Allocations: %+v; want a1 lost and the Instance not named", onlyAllocs)
	}
}

// A wait the test's deadline cuts short says nothing of the watch: not known,
// not lost, and the object is not counted lost for later catch-ups.
type deadlineT struct {
	*testing.T
	d time.Time
}

func (d deadlineT) Deadline() (time.Time, bool) { return d.d, true }

func TestCatchUpCutByTheTestDeadlineIsNotKnown(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "100"), false)
	reach(rec, KindInstances, "105")
	l := listOf(map[Kinds]string{KindInstances: "105"}, listed(KindInstances, "i1", "inst-a", "105"))
	// WaitCap is CleanupMargin before the deadline: this deadline puts it in the past.
	cut := deadlineT{t, time.Now().Add(CleanupMargin / 2)}
	pause, calls := pausing(func(int) {})
	res := catchUp(bg, cut, rec, l, KindInstances, "a cut wait", WaitDeadline, pause)
	if len(res.Missing) != 0 || !strings.Contains(res.Unknown, "cut short by a deadline") || *calls != 0 {
		t.Errorf("result %+v after %d pauses; want unknown, nothing lost", res, *calls)
	}
	if p := res.Problem(); !strings.Contains(p, "is not known") || strings.Contains(p, "the event was lost") {
		t.Errorf("the problem %q; want not known", p)
	}
	rec.add(inst("i1", "inst-a", "105"), false)
	if later := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances, "later", time.Minute, func() {}); !later.OK() {
		t.Errorf("a later catch-up: %+v; want OK once the watch has it", later)
	}
}

// A row's context stops the wait, and gives the rest of the row its time: the
// catch-up ends when the context does, not at CatchUpWait, and says not known.
func TestCatchUpStopsAtItsContext(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindAllocations, "100")
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	pause, calls := pausing(func(n int) {
		if n == 2 {
			cancel()
		}
	})
	res := catchUp(ctx, noDeadlineT{t}, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "a row's end", WaitDeadline, pause)
	if *calls != 2 || res.OK() || len(res.Missing) != 0 || !strings.Contains(res.Unknown, "cut short by a deadline") {
		t.Errorf("result %+v after %d pauses; want it cut short at the second, nothing lost", res, *calls)
	}
}

// A row's deadline shortens the wait, however long the wait asked for.
func TestCatchUpWaitEndsAtTheContextsDeadline(t *testing.T) {
	soon := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(bg, soon)
	defer cancel()
	if d, cut := waitDeadline(ctx, noDeadlineT{t}, 2*time.Hour); !d.Equal(soon) || !cut {
		t.Errorf("a deadline an hour off: %s, cut %v; want the deadline, cut", d, cut)
	}
	if d, cut := waitDeadline(ctx, noDeadlineT{t}, time.Minute); !d.Before(soon) || cut {
		t.Errorf("a wait of a minute: %s, cut %v; want it not cut", d, cut)
	}
}

// The LIST runs under the row's context too.
func TestCatchUpListsUnderItsContext(t *testing.T) {
	var seen error
	c := interceptor.NewClient(fake.NewClientBuilder().WithScheme(APIScheme(t)).Build(), interceptor.Funcs{
		List: func(ctx context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			seen = ctx.Err()
			return ctx.Err()
		},
	})
	ctx, cancel := context.WithCancel(bg)
	cancel()
	rec := &Recorder{}
	res := rec.CatchUp(ctx, noDeadlineT{t}, c, Namespace, KindAllocations, "a row that ended")
	if !errors.Is(seen, context.Canceled) || !strings.Contains(res.Unknown, "could not be listed") {
		t.Errorf("the LIST saw %v, result %+v; want the row's cancelled context, and not known", seen, res)
	}
}

// An API that cannot be listed, or a version that does not parse, leaves it
// not known, never "lost" and never "caught up": whether the version is the
// Recorder's or the API's, of an object the Recorder holds or not, or the
// collection's with nothing listed.
func TestCatchUpWhenItCannotTell(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "not-a-number"), false)
	reach(rec, KindInstances, "100")
	failing := func() (listing, error) { return listing{}, errors.New("connection refused") }
	res := catchUp(bg, noDeadlineT{t}, rec, failing, KindInstances, "a failing LIST", time.Minute, func() { t.Error("waited on a LIST that failed") })
	if res.OK() || !strings.Contains(res.Unknown, "could not be listed") || !strings.Contains(res.Unknown, "connection refused") || len(res.Missing) != 0 {
		t.Errorf("a failing LIST: %+v", res)
	}
	for name, c := range map[string]struct {
		rec  *Recorder
		l    func() (listing, error)
		says string
	}{
		"the Recorder's version":                                  {rec, listOf(map[Kinds]string{KindInstances: "100"}, listed(KindInstances, "i1", "inst-a", "105")), "not-a-number"},
		"a listed version of an object the Recorder has not seen": {&Recorder{}, listOf(map[Kinds]string{KindInstances: "100"}, listed(KindInstances, "i9", "inst-new", "abc")), "abc"},
		"the collection's version, with nothing listed":           {&Recorder{}, listOf(map[Kinds]string{KindInstances: "abc"}), "abc"},
		"no collection version":                                   {&Recorder{}, listOf(nil), "collection"},
	} {
		res := catchUp(bg, noDeadlineT{t}, c.rec, c.l, KindInstances, name, time.Minute, func() { t.Errorf("%s: waited on a version that does not parse", name) })
		if res.OK() || !strings.Contains(res.Unknown, c.says) || len(res.Missing) != 0 {
			t.Errorf("%s: %+v; want not known, saying %q, nothing lost", name, res, c.says)
		}
	}
}

// listKinds reads the kinds the catch-up covers, with each object's version and
// the collection's, and only those.
func TestListKindsReadsTheCoveredKinds(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(APIScheme(t)).WithObjects(
		&v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "inst-a", Namespace: Namespace, UID: "i1"}},
		&v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "inst-other-ns", Namespace: "elsewhere", UID: "i2"}},
		&v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "alloc-a", Namespace: Namespace, UID: "a1"}},
	).Build()
	l, err := listKinds(bg, noDeadlineT{t}, c, Namespace, KindInstances)
	if err != nil || len(l.objs) != 1 || l.objs[0].name != "inst-a" || l.objs[0].uid != "i1" || l.objs[0].kind != KindInstances || l.objs[0].rv == "" {
		t.Errorf("Instances: %+v, %v", l, err)
	}
	if _, ok := l.collection[KindAllocations]; ok {
		t.Errorf("Allocations listed although not covered: %+v", l)
	}
	l, err = listKinds(bg, noDeadlineT{t}, c, Namespace, KindInstances|KindAllocations|KindDeployments)
	if err != nil || len(l.objs) != 2 {
		t.Errorf("all kinds: %+v, %v", l, err)
	}
}
