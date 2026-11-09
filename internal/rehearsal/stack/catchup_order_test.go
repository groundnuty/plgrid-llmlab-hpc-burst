package stack

// Two orders a normal run does not hit (PR #292's delta review), forced: a reset
// between a catch-up's reads of the Recorder (D1), and a stream that stays behind
// a LIST for the whole wait (D2). The order is the test's, through the Recorder's
// hook and scripted watchers; nothing here waits on a window.

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/watch"
)

// noPause is a pause that does not spin.
func noPause() func() {
	pause, _ := pausing(func(int) {})
	return pause
}

// D1. The stream is at 100. An Allocation appears at 150 and goes at 180, behind
// the watch, and the catch-up's LIST is empty at 200. Then, while the catch-up is
// looking at the Recorder, the stream's version expires, and the LIST that starts
// it again is at 250: past the catch-up's LIST, with neither event delivered. A
// look that takes "no reset" from before that and the progress from after it
// returns OK without them.
func TestCatchUpDoesNotTakeAResetsListForProgress(t *testing.T) {
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	defer ct.run()
	c := s.next(t, allocList)
	c.w.Action(watch.Bookmark, bookmark("100"))
	waitReached(t, rec, KindAllocations, "100")

	s.set("200")
	list := func() (listing, error) { return listKinds(bg, noDeadlineT{t}, s, Namespace, KindAllocations) }
	hooks := 0
	rec.afterLook = func() {
		if hooks++; hooks > 1 {
			return
		}
		s.set("250")
		c.w.Action(watch.Error, expiredStatus())
		c.w.Stop()
		waitReached(t, rec, KindAllocations, "250")
	}
	res := catchUp(bg, noDeadlineT{t}, rec, list, KindAllocations, "the Failed scan", WaitDeadline, noPause())
	if hooks == 0 {
		t.Fatal("the hook never ran: this test forced nothing")
	}
	if res.OK() || !strings.Contains(res.Unknown, "was reset") {
		t.Errorf("result %+v; want not known, naming the reset: the record holds neither event of the Allocation, and the stream only reached the LIST through the reset", res)
	}
}

// delayed is one way a stream stays behind a LIST for a whole wait, with what the
// watch later delivers.
type delayed struct {
	held    func(rec *Recorder) // what the Recorder holds before the LIST
	objs    []listedObject      // what the LIST shows, at version 105
	deliver func(c watchCall)   // the event, after the wait
}

var delayedEvents = map[string]delayed{
	"an object not seen yet": {
		objs:    []listedObject{listed(KindAllocations, "a1", "alloc-a", "105")},
		deliver: func(c watchCall) { c.w.Action(watch.Added, alloc("a1", "alloc-a", "105")) },
	},
	"a version not seen yet": {
		held:    func(rec *Recorder) { rec.addAlloc(alloc("a1", "alloc-a", "90"), false) },
		objs:    []listedObject{listed(KindAllocations, "a1", "alloc-a", "105")},
		deliver: func(c watchCall) { c.w.Action(watch.Modified, alloc("a1", "alloc-a", "105")) },
	},
	"a deletion": {
		held:    func(rec *Recorder) { rec.addAlloc(alloc("a1", "alloc-a", "90"), false) },
		deliver: func(c watchCall) { c.w.Action(watch.Deleted, alloc("a1", "alloc-a", "105")) },
	},
}

// D2. The LIST shows an Allocation's change at 105 while the watch stays at 100 for
// the whole wait, so that an object and its stream are both behind. A stream short
// of the LIST's version can still deliver it: not known, nothing recorded as lost,
// and a delivery after the wait gives a clean catch-up.
func TestCatchUpOnATrailingStreamCallsNothingLost(t *testing.T) {
	for name, d := range delayedEvents {
		t.Run(name, func(t *testing.T) {
			s := newScripted(t)
			rec := startWatch(t, s, Namespace, nil)
			c := s.next(t, allocList)
			if d.held != nil {
				d.held(rec)
			}
			c.w.Action(watch.Bookmark, bookmark("100"))
			waitReached(t, rec, KindAllocations, "100")

			l := listOf(map[Kinds]string{KindAllocations: "105"}, d.objs...)
			pause, _, wait := noWait()
			res := catchUp(bg, noDeadlineT{t}, rec, l, KindAllocations, "a read", wait, pause)
			if res.OK() || len(res.Missing) != 0 || res.Lost("a1") {
				t.Fatalf("after the wait: %+v; want not known, and nothing lost", res)
			}
			for _, want := range []string{"the Allocation stream is at version 100, and the LIST was at 105", "alloc-a"} {
				if !strings.Contains(res.Unknown, want) {
					t.Errorf("not known because %q, which does not say %q", res.Unknown, want)
				}
			}
			if p := res.Problem(); strings.Contains(p, "the event was lost") || !strings.Contains(p, "is not known") {
				t.Errorf("the problem %q; want not known, and no loss", p)
			}

			d.deliver(c)
			c.w.Action(watch.Bookmark, bookmark("105"))
			waitReached(t, rec, KindAllocations, "105")
			pause, calls, wait := noWait()
			if later := catchUp(bg, noDeadlineT{t}, rec, l, KindAllocations, "the next read", wait, pause); !later.OK() || *calls != 0 {
				t.Errorf("once the event came: %+v after %d pauses; want OK with none: a delayed event is no permanent loss", later, *calls)
			}
		})
	}
}

// The other side of D2: a stream that reached the LIST's version without the event
// has passed it, since a kind's stream is in order, so the event is lost, and stays
// lost.
func TestCatchUpOnAStreamAtTheListStillReportsALostEvent(t *testing.T) {
	for name, d := range delayedEvents {
		t.Run(name, func(t *testing.T) {
			s := newScripted(t)
			rec := startWatch(t, s, Namespace, nil)
			c := s.next(t, allocList)
			if d.held != nil {
				d.held(rec)
			}
			c.w.Action(watch.Bookmark, bookmark("105"))
			waitReached(t, rec, KindAllocations, "105")

			l := listOf(map[Kinds]string{KindAllocations: "105"}, d.objs...)
			pause, _, wait := noWait()
			res := catchUp(bg, noDeadlineT{t}, rec, l, KindAllocations, "a read", wait, pause)
			if res.OK() || res.Unknown != "" || len(res.Missing) != 1 || !res.Lost("a1") || !strings.Contains(res.Problem(), "the event was lost") {
				t.Fatalf("result %+v, problem %q; want the event lost, and nothing not known", res, res.Problem())
			}
			pause, _, wait = noWait()
			if again := catchUp(bg, noDeadlineT{t}, rec, l, KindAllocations, "the next read", wait, pause); !again.Lost("a1") || again.OK() {
				t.Errorf("the next catch-up: %+v; want the event still lost", again)
			}
		})
	}
}

// A catch-up of two kinds judges each stream alone: an object behind a stream at
// the LIST's version is lost, while one behind a stream short of it is not known.
func TestCatchUpJudgesEachStreamAlone(t *testing.T) {
	rec := &Recorder{}
	reach(rec, KindInstances, "200")
	reach(rec, KindAllocations, "100")
	l := listOf(map[Kinds]string{KindInstances: "200", KindAllocations: "200"},
		listed(KindInstances, "i1", "inst-a", "150"), listed(KindAllocations, "a1", "alloc-a", "150"))
	pause, _, wait := noWait()
	res := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances|KindAllocations, "a read", wait, pause)
	if !res.Lost("i1") || res.Lost("a1") || !strings.Contains(res.Unknown, "the Allocation stream") {
		t.Fatalf("result %+v; want the Instance's event lost, and the Allocation not known", res)
	}
	p := res.Problem()
	if !strings.Contains(p, "is not known") || !strings.Contains(p, "Instance inst-a") || !strings.Contains(p, "the event was lost") {
		t.Errorf("the problem %q; want both said", p)
	}
	rec.addAlloc(alloc("a1", "alloc-a", "150"), false)
	reach(rec, KindAllocations, "200")
	pause, _, wait = noWait()
	later := catchUp(bg, noDeadlineT{t}, rec, l, KindInstances|KindAllocations, "the next read", wait, pause)
	if later.Lost("a1") || !later.Lost("i1") || later.Unknown != "" {
		t.Errorf("once the Allocation came: %+v; want only the Instance lost", later)
	}
}
