package stack

// The catch-up barrier (issue #281), checked without a stack: the Recorder is
// fed by hand, and the pause between two reads of it is the test's, so that the
// order of the API and the watch is the test's, not the machine's. A late
// delivery waits on that count, not on a fixed window, and ends at once.

import (
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

// An update the API lists that the watch delivers only after the first read:
// the catch-up waits for it, and says by how long the watch trailed.
func TestCatchUpWaitsForALateVersion(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "100"), false)
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.add(inst("i1", "inst-a", "105"), false)
		}
	})
	res := catchUp(t, rec, listOf(nil, listed(KindInstances, "i1", "inst-a", "105")), KindInstances, "a late update", WaitDeadline, pause)
	if !res.OK() || res.Waited <= 0 || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK, a wait, and one pause", res, *calls)
	}
}

// An object the API lists that the watch has not delivered at all.
func TestCatchUpWaitsForAnObjectNotYetSeen(t *testing.T) {
	rec := &Recorder{}
	pause, calls := pausing(func(n int) {
		if n == 2 {
			rec.addAlloc(alloc("a1", "alloc-a", "7"), false)
		}
	})
	res := catchUp(t, rec, listOf(nil, listed(KindAllocations, "a1", "alloc-a", "7")), KindAllocations, "a late creation", WaitDeadline, pause)
	if !res.OK() || *calls != 2 {
		t.Errorf("result %+v after %d pauses; want OK after two", res, *calls)
	}
}

// An object the API no longer lists, whose deletion the watch delivers only
// after the first read (issue #281): the catch-up waits for the deletion.
func TestCatchUpWaitsForALateDeletion(t *testing.T) {
	rec := &Recorder{}
	rec.addAlloc(alloc("a1", "alloc-a", "150"), false)
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.addAlloc(alloc("a1", "alloc-a", "180"), true)
		}
	})
	res := catchUp(t, rec, listOf(map[Kinds]string{KindAllocations: "200"}), KindAllocations, "a late deletion", WaitDeadline, pause)
	if !res.OK() || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK after one", res, *calls)
	}
}

// An object the watch holds that the LIST lacks because it was created or
// changed after the LIST is not a deletion the watch is behind on.
func TestCatchUpDoesNotTakeANewerObjectForADeletion(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-new", "210"), false)
	pause, calls := pausing(func(int) { t.Error("the catch-up waited for an object created after its LIST") })
	res := catchUp(t, rec, listOf(map[Kinds]string{KindInstances: "200"}), KindInstances, "a newer object", WaitDeadline, pause)
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
	rec.addAlloc(alloc("a1", "alloc-behind", "50"), false)
	l := listOf(map[Kinds]string{KindInstances: "200", KindAllocations: "200"},
		listed(KindInstances, "i1", "inst-a", "250"), listed(KindAllocations, "a1", "alloc-behind", "60"))
	pause, calls := pausing(func(int) {})
	if res := catchUp(t, rec, l, KindInstances, "covered", time.Minute, pause); !res.OK() || *calls != 0 {
		t.Errorf("the Instances: result %+v after %d pauses; want OK with none", res, *calls)
	}
}

// ModelDeployments: a listed one the watch lacks is behind; the Recorder keeps
// no deletion of one, so an absent one is not.
func TestCatchUpDeployments(t *testing.T) {
	rec := &Recorder{}
	rec.addDeployment(deployment("d-old", "md-old", "20"))
	pause, calls := pausing(func(n int) {
		if n == 1 {
			rec.addDeployment(deployment("d1", "md-a", "40"))
		}
	})
	l := listOf(map[Kinds]string{KindDeployments: "100"}, listed(KindDeployments, "d1", "md-a", "40"))
	if res := catchUp(t, rec, l, KindDeployments, "deployments", WaitDeadline, pause); !res.OK() || *calls != 1 {
		t.Errorf("result %+v after %d pauses; want OK after one", res, *calls)
	}
}

// An event that never comes is a lost event, reported with the objects and
// versions; it is reported once, and later catch-ups leave the object out.
func TestCatchUpReportsALostEventOnce(t *testing.T) {
	rec := &Recorder{}
	rec.addAlloc(alloc("a1", "alloc-a", "150"), false)
	rec.add(inst("i1", "inst-a", "100"), false)
	l := listOf(map[Kinds]string{KindAllocations: "200", KindInstances: "200"}, listed(KindInstances, "i1", "inst-a", "105"))
	pause := func() { time.Sleep(2 * time.Millisecond) }
	res := catchUp(t, rec, l, KindInstances|KindAllocations, "a lost event", 150*time.Millisecond, pause)
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
	again := catchUp(t, rec, l, KindInstances|KindAllocations, "the next site", time.Minute, pause2)
	if !again.OK() || *calls != 0 {
		t.Errorf("the next catch-up: %+v after %d pauses; want OK at once, the lost objects left out", again, *calls)
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
	l := listOf(nil, listed(KindInstances, "i1", "inst-a", "105"))
	// WaitCap is CleanupMargin before the deadline: this deadline puts it in the past.
	cut := deadlineT{t, time.Now().Add(CleanupMargin / 2)}
	pause, calls := pausing(func(int) {})
	res := catchUp(cut, rec, l, KindInstances, "a cut wait", WaitDeadline, pause)
	if len(res.Missing) != 0 || !strings.Contains(res.Unknown, "cut short by the test's deadline") || *calls != 0 {
		t.Errorf("result %+v after %d pauses; want unknown, nothing lost", res, *calls)
	}
	if p := res.Problem(); !strings.Contains(p, "is not known") || strings.Contains(p, "the event was lost") {
		t.Errorf("the problem %q; want not known", p)
	}
	rec.add(inst("i1", "inst-a", "105"), false)
	if later := catchUp(t, rec, l, KindInstances, "later", time.Minute, func() {}); !later.OK() {
		t.Errorf("a later catch-up: %+v; want OK once the watch has it", later)
	}
}

// An API that cannot be listed, or a version that does not parse, leaves it
// not known, never "lost" and never "caught up".
func TestCatchUpWhenItCannotTell(t *testing.T) {
	rec := &Recorder{}
	rec.add(inst("i1", "inst-a", "not-a-number"), false)
	failing := func() (listing, error) { return listing{}, errors.New("connection refused") }
	res := catchUp(t, rec, failing, KindInstances, "a failing LIST", time.Minute, func() { t.Error("waited on a LIST that failed") })
	if res.OK() || !strings.Contains(res.Unknown, "could not be listed") || !strings.Contains(res.Unknown, "connection refused") || len(res.Missing) != 0 {
		t.Errorf("a failing LIST: %+v", res)
	}
	res = catchUp(t, rec, listOf(nil, listed(KindInstances, "i1", "inst-a", "105")), KindInstances, "a bad version", time.Minute,
		func() { t.Error("waited on a version that does not parse") })
	if res.OK() || !strings.Contains(res.Unknown, "not-a-number") || len(res.Missing) != 0 {
		t.Errorf("a version that does not parse: %+v", res)
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
	l, err := listKinds(t, c, Namespace, KindInstances)
	if err != nil || len(l.objs) != 1 || l.objs[0].name != "inst-a" || l.objs[0].uid != "i1" || l.objs[0].kind != KindInstances || l.objs[0].rv == "" {
		t.Errorf("Instances: %+v, %v", l, err)
	}
	if _, ok := l.collection[KindAllocations]; ok {
		t.Errorf("Allocations listed although not covered: %+v", l)
	}
	l, err = listKinds(t, c, Namespace, KindInstances|KindAllocations|KindDeployments)
	if err != nil || len(l.objs) != 2 {
		t.Errorf("all kinds: %+v, %v", l, err)
	}
}
