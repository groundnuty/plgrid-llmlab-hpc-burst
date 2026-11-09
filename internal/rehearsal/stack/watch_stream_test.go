package stack

// Follow and the Recorder's streams (PR #292), through real calls of Follow on a
// client whose watches the test scripts: each Watch call is handed to the test
// with its options and a fake watcher, so that the test orders what the server
// sends and waits on the calls, never on a fixed window.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// watchCall is one Watch call the client received.
type watchCall struct {
	rv        string
	bookmarks bool
	timeout   *int64
	w         *watch.FakeWatcher // nil when the call failed
}

// scripted is a client whose Watch calls go to the test, one channel per list
// type, and whose LIST answers what the test set.
type scripted struct {
	client.WithWatch
	mu       sync.Mutex
	calls    map[string]chan watchCall
	fail     map[string]error // the next Watch call of the list type fails with this
	listErr  error            // the next LIST fails with this
	listRV   string
	listObjs map[string][]runtime.Object
}

func listTypeName(l client.ObjectList) string { return fmt.Sprintf("%T", l) }

const allocList = "*v1alpha1.AllocationList"

// newScripted is a client on an empty namespace, whose LIST is at version 1.
func newScripted(t *testing.T) *scripted {
	t.Helper()
	s := &scripted{calls: map[string]chan watchCall{}, fail: map[string]error{}, listObjs: map[string][]runtime.Object{}, listRV: "1"}
	base := fake.NewClientBuilder().WithScheme(APIScheme(t)).Build()
	s.WithWatch = interceptor.NewClient(base, interceptor.Funcs{
		Watch: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
			name := listTypeName(list)
			s.mu.Lock()
			err := s.fail[name]
			delete(s.fail, name)
			ch := s.channel(name)
			s.mu.Unlock()
			var lo client.ListOptions
			lo.ApplyOptions(opts)
			c := watchCall{}
			if lo.Raw != nil {
				c.rv, c.bookmarks, c.timeout = lo.Raw.ResourceVersion, lo.Raw.AllowWatchBookmarks, lo.Raw.TimeoutSeconds
			}
			if err == nil {
				c.w = watch.NewFakeWithChanSize(16, false)
				// As a real watch does: the connection ends with its context.
				go func(w *watch.FakeWatcher) {
					<-ctx.Done()
					w.Stop()
				}(c.w)
			}
			select {
			case ch <- c:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if err != nil {
				return nil, err
			}
			return c.w, nil
		},
		List: func(_ context.Context, _ client.WithWatch, list client.ObjectList, _ ...client.ListOption) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			if err := s.listErr; err != nil {
				s.listErr = nil
				return err
			}
			list.SetResourceVersion(s.listRV)
			return meta.SetList(list, s.listObjs[listTypeName(list)])
		},
	})
	return s
}

func (s *scripted) channel(name string) chan watchCall {
	if s.calls[name] == nil {
		s.calls[name] = make(chan watchCall, 16)
	}
	return s.calls[name]
}

// next waits for the list type's next Watch call.
func (s *scripted) next(t *testing.T, name string) watchCall {
	t.Helper()
	s.mu.Lock()
	ch := s.channel(name)
	s.mu.Unlock()
	select {
	case c := <-ch:
		return c
	case <-time.After(WaitDeadline):
		t.Fatalf("no Watch call of %s came", name)
	}
	return watchCall{}
}

// set is what the next LIST shows.
func (s *scripted) set(rv string, objs ...runtime.Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listRV = rv
	s.listObjs[allocList] = objs
}

// failNext makes the next Watch call of the Allocations fail.
func (s *scripted) failNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[allocList] = err
}

func expiredStatus() *metav1.Status {
	st := apierrors.NewResourceExpired("too old resource version: 5 (90)").Status()
	return &st
}

func bookmark(rv string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{ResourceVersion: rv}}
}

// followed runs Follow on the Allocations and keeps what it passes on.
type followed struct {
	mu                      sync.Mutex
	events, reached, resets []string
}

func (f *followed) stream() Stream {
	return Stream{
		Event: func(et watch.EventType, o client.Object) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.events = append(f.events, fmt.Sprintf("%s %s", et, o.GetResourceVersion()))
		},
		Reached: func(rv string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.reached = append(f.reached, rv)
		},
		Reset: func(why string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.resets = append(f.resets, why)
		},
	}
}

func (f *followed) snapshot() (events, reached, resets []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events), slices.Clone(f.reached), slices.Clone(f.resets)
}

func runFollow(t *testing.T, s *scripted) *followed {
	t.Helper()
	f := &followed{}
	ctx, cancel := context.WithCancel(bg)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		Follow(ctx, s, Namespace, &v1alpha1.AllocationList{}, f.stream())
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
	return f
}

// Follow passes on what exists, reaches the LIST's version only after that, and
// watches from it with bookmarks asked for, on a short connection: bookmarks are
// what moves a quiet stream to a LIST's version.
func TestFollowStartsFromAListWithBookmarksOnShortConnections(t *testing.T) {
	s := newScripted(t)
	s.set("40", alloc("a1", "alloc-a", "30"), alloc("a2", "alloc-b", "20"))
	f := runFollow(t, s)
	first := s.next(t, allocList)
	if first.rv != "40" || !first.bookmarks {
		t.Errorf("the first connection asked for version %q, bookmarks %v; want the LIST's 40, and bookmarks", first.rv, first.bookmarks)
	}
	if first.timeout == nil || *first.timeout != connectionSeconds {
		t.Errorf("the first connection asked for a timeout of %v; want %d s", first.timeout, connectionSeconds)
	}
	events, reached, resets := f.snapshot()
	if !slices.Equal(events, []string{"ADDED 30", "ADDED 20"}) || !slices.Equal(reached, []string{"40"}) || len(resets) != 0 {
		t.Errorf("events %v, reached %v, resets %v; want both objects added, then version 40, and no reset", events, reached, resets)
	}
}

// A bookmark moves the stream without being passed on as an event.
func TestFollowBookmarkMovesTheStream(t *testing.T) {
	s := newScripted(t)
	f := runFollow(t, s)
	c := s.next(t, allocList)
	c.w.Action(watch.Bookmark, bookmark("70"))
	c.w.Stop()
	c = s.next(t, allocList)
	if c.rv != "70" || !c.bookmarks || c.timeout == nil {
		t.Errorf("the next connection asked for version %q, bookmarks %v, timeout %v; want the bookmark's 70, bookmarks and a timeout", c.rv, c.bookmarks, c.timeout)
	}
	events, reached, resets := f.snapshot()
	if len(events) != 0 || !slices.Equal(reached, []string{"1", "70"}) || len(resets) != 0 {
		t.Errorf("events %v, reached %v, resets %v; want no event, reached 1 then 70, no reset", events, reached, resets)
	}
}

// An ordinary reconnect resumes from the last version, with no gap: the server
// closing a connection, an error event that is not an expired version, and a
// Watch call that fails.
func TestFollowReconnectResumesWithNoGap(t *testing.T) {
	s := newScripted(t)
	f := runFollow(t, s)
	c := s.next(t, allocList)
	c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
	c.w.Stop() // the server closes the connection

	c = s.next(t, allocList)
	if c.rv != "5" {
		t.Fatalf("after a close, the next connection asked for %q, want 5", c.rv)
	}
	st := apierrors.NewInternalError(fmt.Errorf("the storage is restarting")).Status()
	c.w.Action(watch.Error, &st)
	c.w.Stop()

	s.failNext(apierrors.NewServiceUnavailable("the server is starting"))
	c = s.next(t, allocList)
	if c.rv != "5" || c.w != nil {
		t.Fatalf("after a transient error event, the next connection asked for %q (failing: %v), want 5, failing", c.rv, c.w == nil)
	}
	c = s.next(t, allocList)
	if c.rv != "5" {
		t.Fatalf("after a failed Watch call, the next connection asked for %q, want 5", c.rv)
	}
	if _, _, resets := f.snapshot(); len(resets) != 0 {
		t.Errorf("an ordinary reconnect reported a reset: %v", resets)
	}
}

// An expired version, as an error event or as the Watch call's own error, starts
// again from a LIST, and says so.
func TestFollowReportsAnExpiredVersionAsAReset(t *testing.T) {
	for name, expire := range map[string]func(s *scripted, c watchCall){
		"an error event": func(_ *scripted, c watchCall) { c.w.Action(watch.Error, expiredStatus()) },
		"the Watch call": func(s *scripted, _ watchCall) {
			s.failNext(apierrors.NewResourceExpired("too old resource version: 5 (90)"))
		},
		"HTTP 410 Gone": func(s *scripted, _ watchCall) { s.failNext(apierrors.NewGone("too old")) },
	} {
		t.Run(name, func(t *testing.T) {
			s := newScripted(t)
			f := runFollow(t, s)
			c := s.next(t, allocList)
			c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
			s.set("100", alloc("a1", "alloc-a", "90"))
			expire(s, c)
			c.w.Stop()
			for c = s.next(t, allocList); c.w == nil || c.rv == "5"; c = s.next(t, allocList) {
				// The Watch call that fails asks for version 5.
			}
			if c.rv != "100" {
				t.Errorf("after the reset the connection asked for %q, want the LIST's 100", c.rv)
			}
			events, _, resets := f.snapshot()
			if len(resets) != 1 || !strings.Contains(resets[0], "version 5") {
				t.Errorf("resets %q; want one that names version 5", resets)
			}
			if !slices.Contains(events, "ADDED 90") {
				t.Errorf("events %v; want the current version from the new LIST", events)
			}
		})
	}
}

// A LIST that fails is tried again, and is no reset: nothing was delivered yet.
func TestFollowRetriesAFailedListWithoutAReset(t *testing.T) {
	s := newScripted(t)
	s.mu.Lock()
	s.listErr = apierrors.NewServiceUnavailable("the server is starting")
	s.mu.Unlock()
	f := runFollow(t, s)
	if c := s.next(t, allocList); c.rv != "1" {
		t.Errorf("the connection asked for %q, want the LIST's 1", c.rv)
	}
	if _, _, resets := f.snapshot(); len(resets) != 0 {
		t.Errorf("resets %q; want none", resets)
	}
}

// A test that ends is not a reset.
func TestFollowEndingWithItsContextIsNotAReset(t *testing.T) {
	s := newScripted(t)
	f := &followed{}
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Follow(ctx, s, Namespace, &v1alpha1.AllocationList{}, f.stream())
	}()
	c := s.next(t, allocList)
	cancel()
	c.w.Stop()
	<-done
	if _, _, resets := f.snapshot(); len(resets) != 0 {
		t.Errorf("resets %q; want none", resets)
	}
}

// cleanupT keeps what the Recorder's clean-up reports.
type cleanupT struct {
	*testing.T
	mu   sync.Mutex
	fns  []func()
	errs []string
}

func (c *cleanupT) Cleanup(f func()) { c.fns = append(c.fns, f) }
func (c *cleanupT) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}
func (c *cleanupT) run() {
	for _, f := range slices.Backward(c.fns) {
		f()
	}
}

// waitReached waits until the kind's stream has reached the version.
func waitReached(t *testing.T, rec *Recorder, kind Kinds, rv string) {
	t.Helper()
	deadline := time.Now().Add(WaitDeadline)
	for {
		rec.mu.Lock()
		got := ""
		if s := rec.streams[kind]; s != nil {
			got = s.reached
		}
		rec.mu.Unlock()
		if got == rv {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream did not reach %s; it is at %q", rv, got)
		}
		time.Sleep(time.Millisecond)
	}
}

// PR #292's B2, the whole path: a reset leaves a current version that says
// nothing of the versions before it. An Allocation's versions between 5 and 90
// are gone for good; the Recorder holds the current one after the reset, which
// satisfies every comparison of versions, and the catch-up still says not known,
// and so does the end of the test.
func TestCatchUpAfterAResetIsNotKnown(t *testing.T) {
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	c := s.next(t, allocList)
	c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
	current := alloc("a1", "alloc-a", "90")
	s.set("100", current)
	c.w.Action(watch.Error, expiredStatus())
	c.w.Stop()
	if c = s.next(t, allocList); c.rv != "100" {
		t.Fatalf("after the 410 the connection asked for %q, want the new LIST's 100", c.rv)
	}
	waitReached(t, rec, KindAllocations, "100")

	res := rec.CatchUp(bg, noDeadlineT{t}, s, Namespace, KindAllocations, "a read across the reset")
	if res.OK() || !strings.Contains(res.Unknown, "was reset") || !strings.Contains(res.Problem(), "is not known") {
		t.Errorf("the catch-up after a reset: %+v, problem %q; want not known, naming the reset", res, res.Problem())
	}
	ct.run()
	if len(ct.errs) != 1 || !strings.Contains(ct.errs[0], "not known to be complete") {
		t.Errorf("the end of the test reported %q; want one failure, for the reads a catch-up does not cover", ct.errs)
	}
}

// The other half of B2: an ordinary reconnect reports no gap, and the catch-up is
// satisfied by the resumed stream.
func TestCatchUpAfterAnOrdinaryReconnectIsOK(t *testing.T) {
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	c := s.next(t, allocList)
	c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
	c.w.Stop()
	if c = s.next(t, allocList); c.rv != "5" {
		t.Fatalf("after a close the connection asked for %q, want 5", c.rv)
	}
	current := alloc("a1", "alloc-a", "90")
	c.w.Action(watch.Modified, current)
	c.w.Action(watch.Bookmark, bookmark("100"))
	waitReached(t, rec, KindAllocations, "100")

	s.set("100", current)
	res := rec.CatchUp(bg, noDeadlineT{t}, s, Namespace, KindAllocations, "a read across the reconnect")
	if !res.OK() {
		t.Errorf("the catch-up after an ordinary reconnect: %+v, problem %q; want OK", res, res.Problem())
	}
	ct.run()
	if len(ct.errs) != 0 {
		t.Errorf("the end of the test reported %q; want nothing", ct.errs)
	}
}

// PR #292's B1, the whole path: an Allocation created, Failed and deleted while
// the watch is behind is in no LIST, and in nothing the Recorder holds: only the
// stream's progress shows it has not been delivered yet. The events come during
// the catch-up's wait, and the read after it holds them.
func TestCatchUpHoldsAnObjectThatCameAndWentBehindTheWatch(t *testing.T) {
	s := newScripted(t)
	rec := startWatch(t, s, Namespace, nil)
	c := s.next(t, allocList)
	c.w.Action(watch.Bookmark, bookmark("100"))
	waitReached(t, rec, KindAllocations, "100")

	s.set("200") // the LIST shows no Allocation at all
	failed := alloc("a1", "alloc-brief", "150")
	failed.Status.AtProvider.Phase = v1alpha1.AllocationFailed
	pause, calls := pausing(func(n int) {
		if n == 1 {
			c.w.Action(watch.Added, failed)
			c.w.Action(watch.Deleted, alloc("a1", "alloc-brief", "180"))
			c.w.Action(watch.Bookmark, bookmark("200"))
		}
		time.Sleep(time.Millisecond)
	})
	list := func() (listing, error) { return listKinds(bg, noDeadlineT{t}, s, Namespace, KindAllocations) }
	res := catchUp(bg, noDeadlineT{t}, rec, list, KindAllocations, "the Failed scan", WaitDeadline, pause)
	if !res.OK() || *calls == 0 {
		t.Fatalf("result %+v after %d pauses; want OK after a wait: the stream was behind the LIST", res, *calls)
	}
	var phases []v1alpha1.AllocationPhase
	deleted := false
	for _, a := range rec.Allocations() {
		phases = append(phases, a.Phase)
		deleted = deleted || a.Deleted
	}
	if !slices.Contains(phases, v1alpha1.AllocationFailed) || !deleted {
		t.Errorf("the record after the catch-up holds phases %v, deleted %v; want the Failed version and the deletion", phases, deleted)
	}
}
