package main

// The tunnel's two components (tunnel.go) with no manager, no API server and no frps: the manager's
// cache is a stub whose first sync the test opens, the clients are controller-runtime's fake, and
// the proxy list is a stub Reader. What these tests pin is the components' own logic: what the
// writer writes and when it does not, how a conflict is handled, that nothing is written from a
// read the stop cut, that the plugin refuses until the feeder has loaded the index, and that both
// components return after their context ends, whatever they were doing. The real frps and frpc,
// and the whole binary, are in tunnel_run_test.go.
//
// Every expected value is written out by hand from HBEP-6 (Path state; Admission); the stub Reader
// returns conditions the test spells out, never ones the writer or BuildReport computed.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Test credentials: keys' form, and nothing else.
const (
	credA = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	credB = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	credC = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
)

// waitUntil polls cond every few milliseconds until it holds, and fails the test after d.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("after %s: %s", d, what)
		}
	}
}

// await receives from ch, and fails the test when nothing comes in a minute.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Minute):
		t.Fatal("nothing arrived within a minute")
		panic("unreachable")
	}
}

// setVars sets a package-level timing variable for one test and puts it back when the test has
// ended. Every run a test starts must have returned by then: tests register their stops after
// the call, so that they run first.
func setVars[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// stubCache is the manager's cache as the components use it: its first sync is the test's to open.
type stubCache struct {
	gate     chan struct{}
	once     sync.Once
	entered  chan struct{} // a value for each entry into WaitForCacheSync, dropped when full
	mu       sync.Mutex
	informer map[string]*stubInformer
	getErr   error
}

func newStubCache() *stubCache {
	return &stubCache{gate: make(chan struct{}), entered: make(chan struct{}, 64), informer: map[string]*stubInformer{}}
}

func (c *stubCache) open() { c.once.Do(func() { close(c.gate) }) }

func (c *stubCache) WaitForCacheSync(ctx context.Context) bool {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.gate:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *stubCache) GetInformer(ctx context.Context, obj client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.getErr != nil {
		return nil, c.getErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%T", obj)
	if c.informer[key] == nil {
		c.informer[key] = &stubInformer{handlers: map[int]toolscache.ResourceEventHandler{}}
	}
	return c.informer[key], nil
}

// fire tells every handler on every informer that an object changed.
func (c *stubCache) fire() {
	c.mu.Lock()
	var infs []*stubInformer
	for _, i := range c.informer {
		infs = append(infs, i)
	}
	c.mu.Unlock()
	for _, i := range infs {
		i.fire()
	}
}

// handlers is how many event handlers are registered now, and how many were ever added.
func (c *stubCache) handlers() (registered, added int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, i := range c.informer {
		i.mu.Lock()
		registered += len(i.handlers)
		added += i.added
		i.mu.Unlock()
	}
	return registered, added
}

type stubInformer struct {
	cache.Informer // the methods the components do not call stay nil
	mu             sync.Mutex
	next, added    int
	handlers       map[int]toolscache.ResourceEventHandler
}

type stubRegistration struct{ id int }

func (stubRegistration) HasSynced() bool { return true }

func (i *stubInformer) AddEventHandler(h toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.next++
	i.added++
	i.handlers[i.next] = h
	return stubRegistration{i.next}, nil
}

func (i *stubInformer) RemoveEventHandler(reg toolscache.ResourceEventHandlerRegistration) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.handlers, reg.(stubRegistration).id)
	return nil
}

func (i *stubInformer) fire() {
	i.mu.Lock()
	hs := make([]toolscache.ResourceEventHandler, 0, len(i.handlers))
	for _, h := range i.handlers {
		hs = append(hs, h)
	}
	i.mu.Unlock()
	for _, h := range hs {
		h.OnAdd(nil, false)
	}
}

// staleReader lists the Instances as they were when it was made: a cache that is behind the API server.
type staleReader struct{ items []v1alpha1.Instance }

func (r staleReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("staleReader.Get is not used")
}

func (r staleReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	l, ok := list.(*v1alpha1.InstanceList)
	if !ok {
		return fmt.Errorf("staleReader lists Instances only, not %T", list)
	}
	l.Items = nil
	for _, in := range r.items {
		l.Items = append(l.Items, *in.DeepCopy())
	}
	return nil
}

// readerFunc is a pathReader that is a function.
type readerFunc func(ctx context.Context, uids []string) frpsplugin.Report

func (f readerFunc) Read(ctx context.Context, uids []string) frpsplugin.Report { return f(ctx, uids) }

// cond spells a TunnelConnected condition out.
func cond(s metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: "TunnelConnected", Status: s, Reason: reason, Message: msg}
}

// reportOf answers every asked UID with the same condition.
func reportOf(c metav1.Condition) readerFunc {
	return func(_ context.Context, uids []string) frpsplugin.Report {
		rep := frpsplugin.Report{Conditions: map[string]metav1.Condition{}}
		for _, u := range uids {
			rep.Conditions[u] = c
		}
		return rep
	}
}

func tunnelScheme(t *testing.T) *fake.ClientBuilder {
	t.Helper()
	s, err := providerScheme()
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1alpha1.Instance{}, &v1alpha1.Allocation{})
}

func unitInstance(name, uid string, gen int64) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name, UID: types.UID(uid), Generation: gen}}
	in.Spec.AllocationRef.Name = "al-a"
	return in
}

func getInstance(t *testing.T, c client.Client, name string) *v1alpha1.Instance {
	t.Helper()
	in := &v1alpha1.Instance{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "team-a", Name: name}, in); err != nil {
		t.Fatal(err)
	}
	return in
}

// stateFor builds a pathState over the clients, with a cache that has synced.
func stateFor(c client.Client, cached client.Reader, rd pathReader, trigger <-chan struct{}) *pathState {
	sc := newStubCache()
	sc.open()
	return &pathState{cache: sc, cached: cached, fresh: c, writer: c, reader: rd, trigger: trigger, log: logr.Discard()}
}

// counted counts the status updates, and what each one answered.
type counted struct {
	updates atomic.Int64
	// fail, when set, answers an update instead of the client.
	fail func(n int64, obj client.Object) error
}

func (k *counted) funcs() interceptor.Funcs {
	return interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		n := k.updates.Add(1)
		if k.fail != nil {
			if err := k.fail(n, obj); err != nil {
				return err
			}
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}}
}

var instanceGR = schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "instances"}

// The condition is written once, with the Instance's generation, keeping what else the status holds.
func TestPathStateWritesTheConditionAndKeepsTheRest(t *testing.T) {
	seed := unitInstance("in-1", "uid-1", 3)
	seed.Status.Phase = v1alpha1.InstanceReady
	seed.Status.Conditions = []metav1.Condition{{Type: "Reconciled", Status: metav1.ConditionTrue, Reason: "Because", Message: "the reconciler's own"}}
	seed.Status.Node = "node-7"
	k := &counted{}
	c := tunnelScheme(t).WithObjects(seed).WithInterceptorFuncs(k.funcs()).Build()
	cached := staleReader{items: []v1alpha1.Instance{*getInstance(t, c, "in-1")}}

	s := stateFor(c, cached, reportOf(cond(metav1.ConditionTrue, "ProxyOnline", "frps lists the Instance's proxy as online")), nil)
	s.readOnce(t.Context())

	got := getInstance(t, c, "in-1")
	tc := meta.FindStatusCondition(got.Status.Conditions, "TunnelConnected")
	if tc == nil || tc.Status != metav1.ConditionTrue || tc.Reason != "ProxyOnline" || tc.ObservedGeneration != 3 || tc.LastTransitionTime.IsZero() {
		t.Fatalf("stored TunnelConnected = %+v, want True/ProxyOnline at generation 3 with a transition time", tc)
	}
	if r := meta.FindStatusCondition(got.Status.Conditions, "Reconciled"); r == nil || r.Reason != "Because" || r.Status != metav1.ConditionTrue {
		t.Errorf("the reconciler's condition is %+v after the write, want it kept", r)
	}
	if got.Status.Phase != v1alpha1.InstanceReady || got.Status.Node != "node-7" {
		t.Errorf("status fields changed: phase %q, node %q", got.Status.Phase, got.Status.Node)
	}
	if n := k.updates.Load(); n != 1 {
		t.Errorf("%d status updates, want 1", n)
	}
}

// A condition whose status and reason are unchanged is not written again, whatever its message
// says: the Unknown message holds the error's text, which varies per read.
func TestPathStateDoesNotRewriteForAChangedMessage(t *testing.T) {
	seed := unitInstance("in-1", "uid-1", 1)
	k := &counted{}
	c := tunnelScheme(t).WithObjects(seed).WithInterceptorFuncs(k.funcs()).Build()
	msg := "frps's proxy list could not be read: dial tcp 127.0.0.1:1: connect: connection refused"
	var reads atomic.Int64
	rd := readerFunc(func(_ context.Context, uids []string) frpsplugin.Report {
		n := reads.Add(1)
		return reportOf(cond(metav1.ConditionUnknown, "ProxyListUnreadable", fmt.Sprintf("%s (attempt %d)", msg, n)))(context.Background(), uids)
	})
	s := stateFor(c, c, rd, nil)
	s.readOnce(t.Context())
	first := getInstance(t, c, "in-1")
	for range 3 {
		s.readOnce(t.Context())
	}
	if reads.Load() != 4 {
		t.Fatalf("%d reads, want 4", reads.Load())
	}
	after := getInstance(t, c, "in-1")
	if n := k.updates.Load(); n != 1 {
		t.Errorf("%d status updates over four reads that differ only in the message, want 1", n)
	}
	if after.ResourceVersion != first.ResourceVersion {
		t.Errorf("resourceVersion moved from %s to %s: the status was written again", first.ResourceVersion, after.ResourceVersion)
	}
}

// lastTransitionTime moves when the status changes and stays when only the reason does
// (meta.SetStatusCondition's rule, which the lifecycle controller's writes follow too).
func TestPathStateTransitionTime(t *testing.T) {
	old := metav1.NewTime(time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC))
	seed := unitInstance("in-1", "uid-1", 1)
	seed.Status.Conditions = []metav1.Condition{{Type: "TunnelConnected", Status: metav1.ConditionUnknown, Reason: "SomeOldReason", LastTransitionTime: old}}
	seed2 := unitInstance("in-2", "uid-2", 1)
	seed2.Status.Conditions = []metav1.Condition{{Type: "TunnelConnected", Status: metav1.ConditionTrue, Reason: "ProxyOnline", LastTransitionTime: old}}
	c := tunnelScheme(t).WithObjects(seed, seed2).Build()
	rd := readerFunc(func(_ context.Context, uids []string) frpsplugin.Report {
		return frpsplugin.Report{Conditions: map[string]metav1.Condition{
			"uid-1": cond(metav1.ConditionUnknown, "ProxyListUnreadable", "x"), // same status, new reason
			"uid-2": cond(metav1.ConditionFalse, "ProxyOffline", "y"),          // new status
		}}
	})
	stateFor(c, c, rd, nil).readOnce(t.Context())

	one := meta.FindStatusCondition(getInstance(t, c, "in-1").Status.Conditions, "TunnelConnected")
	if one.Reason != "ProxyListUnreadable" || !one.LastTransitionTime.Equal(&old) {
		t.Errorf("in-1: %s at %s, want ProxyListUnreadable with the old transition time %s", one.Reason, one.LastTransitionTime, old)
	}
	two := meta.FindStatusCondition(getInstance(t, c, "in-2").Status.Conditions, "TunnelConnected")
	if two.Status != metav1.ConditionFalse || two.LastTransitionTime.Equal(&old) || two.LastTransitionTime.Time.Before(old.Time) {
		t.Errorf("in-2: %s at %s, want False with a transition time after %s", two.Status, two.LastTransitionTime, old)
	}
}

// An Instance that has ended is not asked about and not written; one with no UID cannot be.
func TestPathStateSkipsEndedInstances(t *testing.T) {
	ended := unitInstance("in-end", "uid-end", 1)
	ended.Status.Phase = v1alpha1.InstanceEnded
	live := unitInstance("in-live", "uid-live", 1)
	live.Status.Phase = v1alpha1.InstanceReady
	k := &counted{}
	c := tunnelScheme(t).WithObjects(ended, live).WithInterceptorFuncs(k.funcs()).Build()
	var asked []string
	rd := readerFunc(func(ctx context.Context, uids []string) frpsplugin.Report {
		asked = append(asked, uids...)
		return reportOf(cond(metav1.ConditionFalse, "ProxyOffline", "z"))(ctx, uids)
	})
	stateFor(c, c, rd, nil).readOnce(t.Context())
	if len(asked) != 1 || asked[0] != "uid-live" {
		t.Errorf("asked about %v, want only uid-live", asked)
	}
	if meta.FindStatusCondition(getInstance(t, c, "in-end").Status.Conditions, "TunnelConnected") != nil {
		t.Error("an ended Instance got a TunnelConnected condition")
	}
	if meta.FindStatusCondition(getInstance(t, c, "in-live").Status.Conditions, "TunnelConnected") == nil {
		t.Error("the live Instance got no condition")
	}
}

// The write carries the resourceVersion that was read. When the cache is behind (the reconciler
// wrote meanwhile), the update conflicts, the Instance is read again, and the condition is written
// on what is there now: the reconciler's change is not lost.
func TestPathStateConflictIsRereadAndRetried(t *testing.T) {
	seed := unitInstance("in-1", "uid-1", 1)
	k := &counted{}
	c := tunnelScheme(t).WithObjects(seed).WithInterceptorFuncs(k.funcs()).Build()
	stale := staleReader{items: []v1alpha1.Instance{*getInstance(t, c, "in-1")}}
	// The reconciler writes after the cache's copy was taken.
	cur := getInstance(t, c, "in-1")
	cur.Status.Conditions = []metav1.Condition{{Type: "Reconciled", Status: metav1.ConditionTrue, Reason: "Later", Message: "written after the cache's copy"}}
	cur.Status.Phase = v1alpha1.InstanceStarting
	if err := c.Status().Update(t.Context(), cur); err != nil {
		t.Fatal(err)
	}
	k.updates.Store(0) // count the writer's calls only

	stateFor(c, stale, reportOf(cond(metav1.ConditionTrue, "ProxyOnline", "up")), nil).readOnce(t.Context())

	got := getInstance(t, c, "in-1")
	if r := meta.FindStatusCondition(got.Status.Conditions, "Reconciled"); r == nil || r.Reason != "Later" {
		t.Errorf("the reconciler's later condition is %+v, want it kept: a write over a stale copy dropped it", r)
	}
	if got.Status.Phase != v1alpha1.InstanceStarting {
		t.Errorf("phase %q, want the reconciler's later %q kept", got.Status.Phase, v1alpha1.InstanceStarting)
	}
	if tc := meta.FindStatusCondition(got.Status.Conditions, "TunnelConnected"); tc == nil || tc.Status != metav1.ConditionTrue {
		t.Errorf("TunnelConnected = %+v, want True after the retry", tc)
	}
	if n := k.updates.Load(); n != 2 {
		t.Errorf("%d update calls, want 2: the conflicting one and the retry", n)
	}
}

// The conflict's re-read decides afresh: an Instance that already holds the condition, has ended,
// is gone, or is another object under the same name is not written.
func TestPathStateAfterAConflictDecidesOnTheFreshObject(t *testing.T) {
	want := cond(metav1.ConditionTrue, "ProxyOnline", "up")
	for _, tc := range []struct {
		name string
		// change is what happened to the object after the cache's copy was taken.
		change func(t *testing.T, c client.Client)
		// updates is the number of update calls that must have been made: the stale one alone.
		updates int64
	}{
		{"it already holds the condition", func(t *testing.T, c client.Client) {
			in := getInstance(t, c, "in-1")
			in.Status.Conditions = []metav1.Condition{{Type: "TunnelConnected", Status: metav1.ConditionTrue, Reason: "ProxyOnline", LastTransitionTime: metav1.Now()}}
			if err := c.Status().Update(t.Context(), in); err != nil {
				t.Fatal(err)
			}
		}, 1},
		{"it has ended", func(t *testing.T, c client.Client) {
			in := getInstance(t, c, "in-1")
			in.Status.Phase = v1alpha1.InstanceEnded
			if err := c.Status().Update(t.Context(), in); err != nil {
				t.Fatal(err)
			}
		}, 1},
		{"it is gone", func(t *testing.T, c client.Client) {
			if err := c.Delete(t.Context(), getInstance(t, c, "in-1")); err != nil {
				t.Fatal(err)
			}
		}, 1},
		{"another Instance has its name", func(t *testing.T, c client.Client) {
			if err := c.Delete(t.Context(), getInstance(t, c, "in-1")); err != nil {
				t.Fatal(err)
			}
			if err := c.Create(t.Context(), unitInstance("in-1", "uid-other", 1)); err != nil {
				t.Fatal(err)
			}
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &counted{}
			c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).WithInterceptorFuncs(k.funcs()).Build()
			stale := staleReader{items: []v1alpha1.Instance{*getInstance(t, c, "in-1")}}
			tc.change(t, c)
			k.updates.Store(0) // count the writer's calls only
			s := stateFor(c, stale, reportOf(want), nil)
			s.readOnce(t.Context())
			if n := k.updates.Load(); n != tc.updates {
				t.Errorf("%d update calls, want %d", n, tc.updates)
			}
		})
	}
}

// A conflict that never clears gives up after tunnelWriteAttempts, and says so.
func TestPathStateGivesUpAgainstAConstantConflict(t *testing.T) {
	k := &counted{fail: func(int64, client.Object) error {
		return apierrors.NewConflict(instanceGR, "in-1", errors.New("the object has been modified"))
	}}
	c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).WithInterceptorFuncs(k.funcs()).Build()
	in := getInstance(t, c, "in-1")
	err := stateFor(c, c, nil, nil).apply(t.Context(), in, cond(metav1.ConditionTrue, "ProxyOnline", "up"))
	if err == nil {
		t.Fatal("apply returned nil against a conflict that never clears")
	}
	if n := k.updates.Load(); n != tunnelWriteAttempts {
		t.Errorf("%d update calls, want %d", n, tunnelWriteAttempts)
	}
}

// An Instance that is gone when the write comes is done, not an error.
func TestPathStateNotFoundIsDone(t *testing.T) {
	k := &counted{fail: func(int64, client.Object) error { return apierrors.NewNotFound(instanceGR, "in-1") }}
	c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).WithInterceptorFuncs(k.funcs()).Build()
	in := getInstance(t, c, "in-1")
	if err := stateFor(c, c, nil, nil).apply(t.Context(), in, cond(metav1.ConditionTrue, "ProxyOnline", "up")); err != nil {
		t.Errorf("apply returned %v for an Instance that is gone", err)
	}
}

// loopRig is a pathState running in a goroutine, over a stub Reader that records each read.
type loopRig struct {
	cache  *stubCache
	client client.Client
	k      *counted
	state  *pathState
	reads  chan time.Time // the time of each read, buffered
	nreads atomic.Int64
	// readBeforeSync is set when a read happens while the cache has not synced.
	readBeforeSync atomic.Bool
	cancel         context.CancelFunc
	done           chan error
}

// newLoopRig starts the loop. block, when set, runs inside each read, and returns the report.
func newLoopRig(t *testing.T, trigger <-chan struct{}, block func(ctx context.Context, uids []string) frpsplugin.Report) *loopRig {
	t.Helper()
	r := &loopRig{cache: newStubCache(), k: &counted{}, reads: make(chan time.Time, 1024), done: make(chan error, 1)}
	r.client = tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).WithInterceptorFuncs(r.k.funcs()).Build()
	rd := readerFunc(func(ctx context.Context, uids []string) frpsplugin.Report {
		select {
		case <-r.cache.gate:
		default:
			r.readBeforeSync.Store(true)
		}
		r.nreads.Add(1)
		select {
		case r.reads <- time.Now():
		default:
		}
		if block != nil {
			return block(ctx, uids)
		}
		return reportOf(cond(metav1.ConditionFalse, "ProxyOffline", "none"))(ctx, uids)
	})
	r.state = &pathState{cache: r.cache, cached: r.client, fresh: r.client, writer: r.client, reader: rd, trigger: trigger, log: logr.Discard()}
	var ctx context.Context
	ctx, r.cancel = context.WithCancel(context.Background())
	go func() { r.done <- r.state.run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

func (r *loopRig) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("run returned %v after its context ended, want nil", err)
		}
		r.done <- nil // a second stop finds it done
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return within 10 s of its context's end")
	}
}

// Nothing is read until the cache has synced, then one read at the start and one every period.
func TestPathStateReadsAfterTheSyncAtStartAndEveryPeriod(t *testing.T) {
	setVars(t, &tunnelReadPeriod, 20*time.Millisecond)
	r := newLoopRig(t, nil, nil)
	await(t, r.cache.entered) // the loop is waiting for the sync: it has read nothing before it
	r.cache.open()
	waitUntil(t, 30*time.Second, "four reads", func() bool { return r.nreads.Load() >= 4 })
	if r.readBeforeSync.Load() {
		t.Error("a read of frps's list happened before the cache had synced")
	}
	stored := meta.FindStatusCondition(getInstance(t, r.client, "in-1").Status.Conditions, "TunnelConnected")
	if stored == nil || stored.Status != metav1.ConditionFalse {
		t.Errorf("stored %+v, want the read's False", stored)
	}
}

// A signal of the trigger causes a read, no sooner than the settle time after it, and with the
// period far away it is the only thing that can.
func TestPathStateReadsAfterATriggerSignal(t *testing.T) {
	const settle = 300 * time.Millisecond
	setVars(t, &tunnelReadPeriod, time.Hour)
	setVars(t, &tunnelReadSettle, settle)
	tr := frpsplugin.NewTrigger()
	r := newLoopRig(t, tr.C(), nil)
	r.cache.open()
	await(t, r.reads) // the read at the start
	for range 5 {
		signalled := time.Now()
		tr.ProxyChanged("NewProxy", "uid-1")
		select {
		case at := <-r.reads:
			if d := at.Sub(signalled); d < settle {
				t.Fatalf("the read came %s after the signal, before the settle time %s", d, settle)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no read within 30 s of a signal, with the period an hour")
		}
	}
}

// A read the stop cut says nothing about frps: Unknown must not be written from it.
func TestPathStateWritesNothingFromAReadTheStopCut(t *testing.T) {
	inRead := make(chan struct{}, 1)
	r := newLoopRig(t, nil, func(ctx context.Context, uids []string) frpsplugin.Report {
		inRead <- struct{}{}
		<-ctx.Done()
		return frpsplugin.Report{Err: ctx.Err(), Conditions: map[string]metav1.Condition{
			"uid-1": cond(metav1.ConditionUnknown, "ProxyListUnreadable", "frps's proxy list could not be read: context canceled"),
		}}
	})
	r.cache.open()
	await(t, inRead)
	r.stop(t)
	if n := r.k.updates.Load(); n != 0 {
		t.Errorf("%d status updates after a stop cut the read, want none", n)
	}
	if c := meta.FindStatusCondition(getInstance(t, r.client, "in-1").Status.Conditions, "TunnelConnected"); c != nil {
		t.Errorf("TunnelConnected %+v was written from a read the stop cut", c)
	}
}

// ---------------------------------------------------------------------------------------------

// pluginRig is a pluginServer running in a goroutine on a port the kernel chose.
type pluginRig struct {
	t       *testing.T
	cache   *stubCache
	index   *frpsplugin.MemIndex
	client  client.Client
	creds   string
	addr    string
	ln      net.Listener
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

// newPluginRig starts the server over a fake client holding objs, with credentials under creds.
func newPluginRig(t *testing.T, creds string, cl client.Client) *pluginRig {
	t.Helper()
	r := &pluginRig{t: t, cache: newStubCache(), index: &frpsplugin.MemIndex{}, client: cl, creds: creds, done: make(chan error, 1)}
	h, err := frpsplugin.NewHandler(r.index, frpsplugin.Options{})
	if err != nil {
		t.Fatal(err)
	}
	bound := make(chan struct{})
	srv := &pluginServer{
		addr: "127.0.0.1:0", handler: h, log: logr.Discard(),
		feed: &pluginFeed{cache: r.cache, reader: cl, index: r.index, credDir: creds, log: logr.Discard()},
		listen: func(ctx context.Context) (net.Listener, error) {
			var lc net.ListenConfig
			ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
			if err == nil {
				r.ln, r.addr = ln, ln.Addr().String()
			}
			close(bound)
			return ln, err
		},
	}
	var ctx context.Context
	ctx, r.cancel = context.WithCancel(context.Background())
	go func() { r.done <- srv.start(ctx) }()
	await(t, bound)
	t.Cleanup(func() { r.stop() })
	return r
}

func (r *pluginRig) stop() {
	r.t.Helper()
	if r.stopped {
		return
	}
	r.stopped = true
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			r.t.Errorf("the plugin component returned %v after its context ended, want nil", err)
		}
	case <-time.After(10 * time.Second):
		r.t.Fatal("the plugin component did not return within 10 s of its context's end")
	}
}

// call posts a plugin call as frps does and returns its verdict.
func (r *pluginRig) call(op string, content any) (reject bool, reason string) {
	r.t.Helper()
	body, err := json.Marshal(map[string]any{"version": "0.1.0", "op": op, "content": content})
	if err != nil {
		r.t.Fatal(err)
	}
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := hc.Post("http://"+r.addr+agentapi.FrpsPluginPath+"?op="+op, "application/json", bytes.NewReader(body))
	if err != nil {
		r.t.Fatalf("plugin call %s: %v", op, err)
	}
	defer resp.Body.Close()
	var a struct {
		Reject       bool   `json:"reject"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		r.t.Fatalf("plugin call %s: %v", op, err)
	}
	return a.Reject, a.RejectReason
}

func (r *pluginRig) login(token string) (bool, string) {
	return r.call("Login", map[string]any{"metas": map[string]string{"token": token}})
}

func (r *pluginRig) newProxy(token, name string) (bool, string) {
	return r.call("NewProxy", map[string]any{
		"user": map[string]any{"metas": map[string]string{"token": token}}, "proxy_name": name, "proxy_type": "tcpmux",
		"custom_domains": []string{name}, "multiplexer": "httpconnect",
	})
}

// the plugin's refusals, as handler.go words them (checked as substrings).
const (
	textNotLoaded = "has not loaded the Allocations yet"
	textCredental = "is not the credential of a live Allocation"
	textNotPlaced = "is not the UID of an Instance placed on this Allocation"
)

func unitAllocation(name, uid string, phase v1alpha1.AllocationPhase) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name, UID: types.UID(uid)}}
	a.Status.AtProvider.Phase = phase
	return a
}

func writeUnitCredential(t *testing.T, dir, uid, key string) {
	t.Helper()
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(dir, uid), key); err != nil {
		t.Fatal(err)
	}
}

// The plugin refuses every login until the feeder has loaded the index; then it answers by what
// the cache and the credential files hold, and follows them: a credential file that appears with
// no object change, and an Allocation that ends.
func TestPluginAnswersFromTheFeed(t *testing.T) {
	setVars(t, &feedPeriod, 20*time.Millisecond)
	setVars(t, &feedMinGap, time.Millisecond)
	creds := t.TempDir()
	writeUnitCredential(t, creds, "uid-live", credA)
	writeUnitCredential(t, creds, "uid-ended", credC)
	live := unitAllocation("al-live", "uid-live", v1alpha1.AllocationRunning)
	pending := unitAllocation("al-pending", "uid-pending", v1alpha1.AllocationPending) // no credential file yet
	ended := unitAllocation("al-ended", "uid-ended", v1alpha1.AllocationEnded)
	inLive, inPending := unitInstance("in-live", "inst-live", 1), unitInstance("in-pending", "inst-pending", 1)
	inLive.Spec.AllocationRef.Name, inPending.Spec.AllocationRef.Name = "al-live", "al-pending"
	cl := tunnelScheme(t).WithObjects(live, pending, ended, inLive, inPending).Build()
	r := newPluginRig(t, creds, cl)

	// The cache has not synced: the feeder has not read anything, so every login is refused, as one
	// whose token is right.
	await(t, r.cache.entered)
	if reject, reason := r.login(credA); !reject || !contains(reason, textNotLoaded) {
		t.Fatalf("before the sync: reject=%v %q, want a refusal saying the Allocations are not loaded", reject, reason)
	}
	r.cache.open()
	waitUntil(t, 30*time.Second, "the plugin to admit the live Allocation's credential", func() bool {
		reject, _ := r.login(credA)
		return !reject
	})
	for _, c := range []struct {
		what   string
		call   func() (bool, string)
		reject bool
		reason string // the part of the plugin's reason that must show, "" for an acceptance
	}{
		{"the ended Allocation's credential", func() (bool, string) { return r.login(credC) }, true, textCredental},
		{"a Pending Allocation with no credential file", func() (bool, string) { return r.login(credB) }, true, textCredental},
		{"a proxy of an Instance placed on the live Allocation", func() (bool, string) { return r.newProxy(credA, "inst-live") }, false, ""},
		{"a proxy of another Allocation's Instance", func() (bool, string) { return r.newProxy(credA, "inst-pending") }, true, textNotPlaced},
		{"a proxy nobody has", func() (bool, string) { return r.newProxy(credA, "inst-nobody") }, true, textNotPlaced},
	} {
		if reject, reason := c.call(); reject != c.reject || !contains(reason, c.reason) {
			t.Errorf("%s: reject=%v %q, want reject=%v %q", c.what, reject, reason, c.reject, c.reason)
		}
	}

	// The credential file appears with no change to any object: the periodic refresh finds it.
	writeUnitCredential(t, creds, "uid-pending", credB)
	waitUntil(t, 30*time.Second, "the plugin to admit a credential whose file appeared after the first load", func() bool {
		reject, _ := r.login(credB)
		return !reject
	})
	if reject, reason := r.newProxy(credB, "inst-pending"); reject {
		t.Errorf("a proxy of the Instance placed on the newly admitted Allocation is refused: %q", reason)
	}

	// The Allocation ends: the next refresh stops admitting it.
	got := &v1alpha1.Allocation{}
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(live), got); err != nil {
		t.Fatal(err)
	}
	got.Status.AtProvider.Phase = v1alpha1.AllocationEnded
	if err := cl.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	r.cache.fire()
	waitUntil(t, 30*time.Second, "the plugin to refuse the credential of the Allocation that ended", func() bool {
		reject, _ := r.login(credA)
		return reject
	})
}

// With the periodic refresh an hour away, an Allocation or Instance event alone makes the feeder
// refresh: an Allocation that ends stops being admitted, and a new Instance can register.
func TestPluginRefreshesOnInformerEvents(t *testing.T) {
	setVars(t, &feedPeriod, time.Hour)
	setVars(t, &feedMinGap, time.Millisecond)
	creds := t.TempDir()
	writeUnitCredential(t, creds, "uid-live", credA)
	live := unitAllocation("al-live", "uid-live", v1alpha1.AllocationRunning)
	cl := tunnelScheme(t).WithObjects(live).Build()
	r := newPluginRig(t, creds, cl)
	r.cache.open()
	waitUntil(t, 30*time.Second, "the plugin to admit the live Allocation", func() bool {
		reject, _ := r.login(credA)
		return !reject
	})

	// A new Instance placed on the Allocation: refused until the event, accepted after it.
	if reject, _ := r.newProxy(credA, "inst-new"); !reject {
		t.Fatal("a proxy of an Instance that does not exist yet was accepted")
	}
	in := unitInstance("in-new", "inst-new", 1)
	in.Spec.AllocationRef.Name = "al-live"
	if err := cl.Create(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	r.cache.fire()
	waitUntil(t, 30*time.Second, "the plugin to accept the proxy of the Instance that was created", func() bool {
		reject, _ := r.newProxy(credA, "inst-new")
		return !reject
	})

	// The Allocation ends.
	got := &v1alpha1.Allocation{}
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(live), got); err != nil {
		t.Fatal(err)
	}
	got.Status.AtProvider.Phase = v1alpha1.AllocationFailed
	if err := cl.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	r.cache.fire()
	waitUntil(t, 30*time.Second, "the plugin to refuse the credential of the Allocation that failed", func() bool {
		reject, _ := r.login(credA)
		return reject
	})
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }

// The index is marked synced only after a refresh it accepted: while the lists fail, the plugin
// refuses every login with the "not loaded" reason, though the cache itself has synced.
func TestPluginIsNotSyncedWhileTheFirstRefreshFails(t *testing.T) {
	setVars(t, &feedPeriod, 10*time.Millisecond)
	setVars(t, &feedMinGap, time.Millisecond)
	creds := t.TempDir()
	writeUnitCredential(t, creds, "uid-live", credA)
	var failing atomic.Bool
	failing.Store(true)
	var attempts atomic.Int64
	cl := tunnelScheme(t).WithObjects(unitAllocation("al-live", "uid-live", v1alpha1.AllocationRunning)).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			attempts.Add(1)
			if failing.Load() {
				return errors.New("example: the cache cannot list")
			}
			return c.List(ctx, list, opts...)
		},
	}).Build()
	r := newPluginRig(t, creds, cl)
	r.cache.open()
	waitUntil(t, 30*time.Second, "three refreshes that failed", func() bool { return attempts.Load() >= 3 })
	if reject, reason := r.login(credA); !reject || !contains(reason, textNotLoaded) {
		t.Fatalf("with every refresh failing: reject=%v %q, want the not-loaded refusal", reject, reason)
	}
	failing.Store(false)
	waitUntil(t, 30*time.Second, "the plugin to admit once the lists work", func() bool {
		reject, _ := r.login(credA)
		return !reject
	})
}

// A port that is taken is an error, not a plugin that never listens.
func TestPluginBindFailureIsAnError(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	h, err := frpsplugin.NewHandler(&frpsplugin.MemIndex{}, frpsplugin.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := &pluginServer{addr: held.Addr().String(), handler: h, log: logr.Discard(), feed: &pluginFeed{cache: newStubCache(), log: logr.Discard()}}
	done := make(chan error, 1)
	go func() { done <- srv.start(t.Context()) }()
	select {
	case err := <-done:
		if err == nil || !contains(err.Error(), "bind the plugin endpoint") {
			t.Errorf("start returned %v, want a bind error", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("start did not return for a port that is taken")
	}
}

// ---------------------------------------------------------------------------------------------

// stopRounds is IMPL-COMMON's "200-round stop test" for a builder path: in each round the component
// starts, is brought to the state the variant names, its context ends, and it must return nil
// within the deadline.
const stopRounds = 200

func TestPathStateStopsFromEveryState(t *testing.T) {
	variants := []struct {
		name string
		// run starts one round's component and returns its done channel, once it is in the state the
		// variant names.
		round func(t *testing.T, ctx context.Context) (done <-chan error, after func())
	}{
		{"waiting for the cache to sync", func(t *testing.T, ctx context.Context) (<-chan error, func()) {
			sc := newStubCache()
			c := tunnelScheme(t).Build()
			s := stateFor(c, c, reportOf(cond(metav1.ConditionFalse, "ProxyOffline", "")), nil)
			s.cache = sc
			done := make(chan error, 1)
			go func() { done <- s.run(ctx) }()
			await(t, sc.entered)
			return done, func() {}
		}},
		{"blocked in a read of the list", func(t *testing.T, ctx context.Context) (<-chan error, func()) {
			inRead := make(chan struct{}, 1)
			c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).Build()
			s := stateFor(c, c, readerFunc(func(ctx context.Context, _ []string) frpsplugin.Report {
				inRead <- struct{}{}
				<-ctx.Done()
				return frpsplugin.Report{Err: ctx.Err()}
			}), nil)
			done := make(chan error, 1)
			go func() { done <- s.run(ctx) }()
			await(t, inRead)
			return done, func() {}
		}},
		{"idle between reads", func(t *testing.T, ctx context.Context) (<-chan error, func()) {
			read := make(chan struct{}, 8)
			c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).Build()
			s := stateFor(c, c, readerFunc(func(ctx context.Context, uids []string) frpsplugin.Report {
				read <- struct{}{}
				return reportOf(cond(metav1.ConditionFalse, "ProxyOffline", ""))(ctx, uids)
			}), nil)
			done := make(chan error, 1)
			go func() { done <- s.run(ctx) }()
			await(t, read)
			return done, func() {}
		}},
		{"waiting out the settle time after a signal", func(t *testing.T, ctx context.Context) (<-chan error, func()) {
			read := make(chan struct{}, 8)
			tr := frpsplugin.NewTrigger()
			c := tunnelScheme(t).WithObjects(unitInstance("in-1", "uid-1", 1)).Build()
			s := stateFor(c, c, readerFunc(func(ctx context.Context, uids []string) frpsplugin.Report {
				read <- struct{}{}
				return reportOf(cond(metav1.ConditionFalse, "ProxyOffline", ""))(ctx, uids)
			}), tr.C())
			done := make(chan error, 1)
			go func() { done <- s.run(ctx) }()
			await(t, read)
			tr.ProxyChanged("NewProxy", "uid-1")
			for len(tr.C()) > 0 { // the loop took the signal: it is in the settle wait
				runtime.Gosched()
			}
			return done, func() {}
		}},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			setVars(t, &tunnelReadPeriod, time.Hour)
			setVars(t, &tunnelReadSettle, time.Hour)
			for round := range stopRounds {
				ctx, cancel := context.WithCancel(context.Background())
				done, after := v.round(t, ctx)
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("round %d: run returned %v after its context ended, want nil", round, err)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("round %d: run did not return within 10 s of its context's end", round)
				}
				after()
			}
		})
	}
}

func TestPluginComponentStopsFromEveryState(t *testing.T) {
	setVars(t, &feedPeriod, time.Millisecond)
	setVars(t, &feedMinGap, time.Millisecond)
	creds := t.TempDir()
	writeUnitCredential(t, creds, "uid-live", credA)
	live := unitAllocation("al-live", "uid-live", v1alpha1.AllocationRunning)
	variants := []struct {
		name string
		// ready waits until the component is in the state the variant names.
		ready func(r *pluginRig)
		// open, when set, lets the cache sync before the wait.
		open bool
		// atOnce stops the component without waiting for it to bind.
		atOnce bool
	}{
		{"waiting for the cache to sync, bound", func(r *pluginRig) { await(r.t, r.cache.entered) }, false, false},
		{"synced and refreshing", func(r *pluginRig) {
			waitUntil(r.t, 30*time.Second, "the index to be synced", r.index.Synced)
		}, true, false},
		{"stopped as it starts", func(*pluginRig) {}, false, true},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			for round := range stopRounds {
				cl := tunnelScheme(t).WithObjects(live).Build()
				r := newPluginRig(t, creds, cl)
				if v.open {
					r.cache.open()
				}
				if !v.atOnce {
					v.ready(r)
				}
				r.stop()
				if registered, added := r.cache.handlers(); registered != 0 {
					t.Fatalf("round %d: %d of %d event handlers are still registered after the stop", round, registered, added)
				}
				if r.addr != "" {
					if c, err := net.DialTimeout("tcp", r.addr, time.Second); err == nil {
						c.Close()
						t.Fatalf("round %d: the plugin endpoint %s still listens after the stop", round, r.addr)
					}
				}
			}
			// Nothing outlives the component: the goroutines come back to where they were.
			waitUntil(t, 30*time.Second, "the goroutines to return to their count before the rounds", func() bool {
				return runtime.NumGoroutine() <= before+2
			})
		})
	}
}

// A feeder that fails with its context alive is the component's error, and the server it started
// is shut down: the manager is told, and nothing is left listening.
func TestPluginComponentReturnsTheFeedersError(t *testing.T) {
	r := newPluginRigWithCacheError(t, errors.New("example: no such kind"))
	select {
	case err := <-r.done:
		r.stopped = true
		if err == nil || !contains(err.Error(), "example: no such kind") {
			t.Errorf("the component returned %v, want the feeder's error", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the component did not return the feeder's error")
	}
	if c, err := net.DialTimeout("tcp", r.addr, time.Second); err == nil {
		c.Close()
		t.Errorf("the plugin endpoint %s still listens after the component failed", r.addr)
	}
}

func newPluginRigWithCacheError(t *testing.T, getErr error) *pluginRig {
	t.Helper()
	cl := tunnelScheme(t).Build()
	r := &pluginRig{t: t, cache: newStubCache(), index: &frpsplugin.MemIndex{}, client: cl, creds: t.TempDir(), done: make(chan error, 1)}
	r.cache.getErr = getErr
	r.cache.open()
	h, err := frpsplugin.NewHandler(r.index, frpsplugin.Options{})
	if err != nil {
		t.Fatal(err)
	}
	bound := make(chan struct{})
	srv := &pluginServer{
		addr: "127.0.0.1:0", handler: h, log: logr.Discard(),
		feed: &pluginFeed{cache: r.cache, reader: cl, index: r.index, credDir: r.creds, log: logr.Discard()},
		listen: func(ctx context.Context) (net.Listener, error) {
			var lc net.ListenConfig
			ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
			if err == nil {
				r.ln, r.addr = ln, ln.Addr().String()
			}
			close(bound)
			return ln, err
		},
	}
	var ctx context.Context
	ctx, r.cancel = context.WithCancel(context.Background())
	go func() { r.done <- srv.start(ctx) }()
	await(t, bound)
	t.Cleanup(func() { r.cancel() })
	return r
}
