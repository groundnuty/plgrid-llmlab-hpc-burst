package main

// The TunnelConnected writer and the lifecycle reconciler write the same Instance's status (a
// science-agent ruling of 1 Nov 2026, for #90). Both write a read-modify-write Status().Update that
// carries the resourceVersion, and both retry on Conflict, so neither loses the other's conditions.
// A JSON merge patch would break that on a CRD, where it replaces the whole conditions list with
// the writer's possibly stale copy.
//
// This test runs the two against a real API server (envtest), concurrently, on one Instance. The
// reconciler's side is a goroutine doing what internal/lifecycle/writes.go setStatus does: copy the
// status of a possibly stale read, change it, Update with that read's resourceVersion, and, on a
// Conflict, read again and repeat, as the reconciler's requeue does. The tunnel's side is the
// writer's own write function, pathState.apply, flipping TunnelConnected between True/ProxyOnline
// and False/ProxyOffline over a possibly stale copy too. Each side's reads lag its writes by a few
// rounds, so both meet conflicts; the test counts them, and fails if either side met none (then
// nothing interleaved). At the end the stored Instance must hold both sides' last values, and a
// watch shows that no condition either side had written was ever missing from a later state.

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const interleaveRounds = 300

// conflictCounter wraps a client so that the number of status updates, and of those that
// were Conflicts, can be read.
type conflictCounter struct {
	updates, conflicts atomic.Int64
}

func (c *conflictCounter) wrap(t *testing.T, api *apiServer) client.Client {
	t.Helper()
	wc, err := client.NewWithWatch(api.env.Config, client.Options{Scheme: api.client.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	return interceptor.NewClient(wc, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		c.updates.Add(1)
		err := cl.SubResource(sub).Update(ctx, obj, opts...)
		if apierrors.IsConflict(err) {
			c.conflicts.Add(1)
		}
		return err
	}})
}

// laggingReads gives a writer the copy of an Instance that its cache would hold: the last one it
// took, refreshed every few reads. The copy is stale whenever the other writer has written since.
type laggingReads struct {
	c     client.Client
	key   client.ObjectKey
	rnd   *rand.Rand
	cache *v1alpha1.Instance
}

func (l *laggingReads) get(ctx context.Context) (*v1alpha1.Instance, error) {
	if l.cache == nil || l.rnd.Intn(3) == 0 {
		fresh := &v1alpha1.Instance{}
		if err := l.c.Get(ctx, l.key, fresh); err != nil {
			return nil, err
		}
		l.cache = fresh
	}
	return l.cache.DeepCopy(), nil
}

// lifecycleStyleUpdate does what the reconciler's setStatus does: change a copy of the status of
// the read given and Update with that read's resourceVersion, and on a Conflict read again and
// redo the change, as the requeue does, until it is written.
func lifecycleStyleUpdate(ctx context.Context, c client.Client, read *v1alpha1.Instance, mutate func(*v1alpha1.InstanceStatus)) error {
	cur := read
	for {
		next := cur.DeepCopy()
		mutate(&next.Status)
		err := c.Status().Update(ctx, next, client.FieldOwner(v1alpha1.FieldManagerLifecycle))
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		fresh := &v1alpha1.Instance{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(read), fresh); err != nil {
			return err
		}
		cur = fresh
	}
}

func setLifecycleCondition(s *v1alpha1.InstanceStatus, typ, reason, msg string) {
	meta.SetStatusCondition(&s.Conditions, metav1.Condition{Type: typ, Status: metav1.ConditionTrue, Reason: reason, Message: msg, LastTransitionTime: metav1.Now()})
}

// healthyMessage is the changing message the reconciler's side writes on its Healthy condition.
func healthyMessage(round int) string { return fmt.Sprintf("round %05d", round) }

// interleaveWatch checks every state the Instance passes through: a lifecycle condition type
// that has appeared must be present in every later state, the Healthy message must never go back
// to an earlier round, and TunnelConnected, once written, must stay.
type interleaveWatch struct {
	mu         sync.Mutex
	seen       map[string]bool
	healthy    string
	rv         string
	violations []string
}

func startInterleaveWatch(t *testing.T, api *apiServer, key client.ObjectKey) *interleaveWatch {
	t.Helper()
	wc, err := client.NewWithWatch(api.env.Config, client.Options{Scheme: api.client.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w, err := wc.Watch(ctx, &v1alpha1.InstanceList{}, client.InNamespace(key.Namespace))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	iw := &interleaveWatch{seen: map[string]bool{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range w.ResultChan() {
			in, ok := ev.Object.(*v1alpha1.Instance)
			if !ok || in.Name != key.Name || (ev.Type != watch.Added && ev.Type != watch.Modified) {
				continue
			}
			iw.check(in)
		}
	}()
	t.Cleanup(func() {
		cancel()
		w.Stop()
		<-done
	})
	return iw
}

func (iw *interleaveWatch) check(in *v1alpha1.Instance) {
	iw.mu.Lock()
	defer iw.mu.Unlock()
	iw.rv = in.ResourceVersion
	have := map[string]bool{}
	for _, c := range in.Status.Conditions {
		have[c.Type] = true
	}
	for typ := range iw.seen {
		if !have[typ] {
			iw.violations = append(iw.violations, fmt.Sprintf("at resourceVersion %s the condition %s that was stored before is gone", in.ResourceVersion, typ))
		}
	}
	for typ := range have {
		iw.seen[typ] = true
	}
	if h := meta.FindStatusCondition(in.Status.Conditions, "Healthy"); h != nil {
		if h.Message < iw.healthy {
			iw.violations = append(iw.violations, fmt.Sprintf("at resourceVersion %s Healthy went back from %q to %q", in.ResourceVersion, iw.healthy, h.Message))
		}
		iw.healthy = h.Message
	}
}

func (iw *interleaveWatch) problems() []string {
	iw.mu.Lock()
	defer iw.mu.Unlock()
	return append([]string(nil), iw.violations...)
}

func TestTunnelWriterAndLifecycleStatusWritesLoseNothing(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	if err := api.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tunnelNS}}); err != nil {
		t.Fatal(err)
	}
	r := &tunnelRig{t: t, api: api}
	in := r.instance("in-interleave", "al-x", "")
	key := client.ObjectKeyFromObject(in)
	iw := startInterleaveWatch(t, api, key)

	var tunnelC, lifeC conflictCounter
	tunnelClient, lifeClient := tunnelC.wrap(t, api), lifeC.wrap(t, api)
	state := &pathState{writer: tunnelClient, fresh: tunnelClient}

	online := metav1.Condition{Type: "TunnelConnected", Status: metav1.ConditionTrue, Reason: "ProxyOnline", Message: "frps lists the Instance's proxy as online"}
	offline := metav1.Condition{Type: "TunnelConnected", Status: metav1.ConditionFalse, Reason: "ProxyOffline", Message: "frps does not list a proxy for the Instance"}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	// The reconciler's side: its conditions come in over the rounds, and Healthy's message changes
	// every round.
	go func() {
		defer wg.Done()
		reads := &laggingReads{c: lifeClient, key: key, rnd: rand.New(rand.NewSource(1))}
		for round := 0; round < interleaveRounds; round++ {
			read, err := reads.get(ctx)
			if err != nil {
				errs <- err
				return
			}
			err = lifecycleStyleUpdate(ctx, lifeClient, read, func(s *v1alpha1.InstanceStatus) {
				switch {
				case round >= 150:
					s.Phase = v1alpha1.InstanceReady
					setLifecycleCondition(s, "WarmedUp", "WarmUpDone", "warmed up")
					fallthrough
				case round >= 100:
					setLifecycleCondition(s, "Qualified", "QualificationPassed", "qualified")
					fallthrough
				case round >= 50:
					if s.Phase == "" || s.Phase == v1alpha1.InstancePending {
						s.Phase = v1alpha1.InstanceVerifying
					}
					setLifecycleCondition(s, "Verified", "VerificationPassed", "verified")
				}
				if s.Phase == "" {
					s.Phase = v1alpha1.InstanceStarting
				}
				s.Node = "node-7"
				setLifecycleCondition(s, "Healthy", "Probing", healthyMessage(round))
			})
			if err != nil {
				errs <- fmt.Errorf("the reconciler's write in round %d: %w", round, err)
				return
			}
		}
	}()
	// The tunnel's side: the real write function, over a lagging copy, flipping the condition.
	var last metav1.Condition
	var gaveUp atomic.Int64
	go func() {
		defer wg.Done()
		reads := &laggingReads{c: tunnelClient, key: key, rnd: rand.New(rand.NewSource(2))}
		for round := 0; round < interleaveRounds; round++ {
			read, err := reads.get(ctx)
			if err != nil {
				errs <- err
				return
			}
			want := online
			if round%2 == 1 {
				want = offline
			}
			last = want
			if err := state.apply(ctx, read, want); err != nil {
				// Giving up after tunnelWriteAttempts is the writer's own rule: the next read of
				// the list retries, as the next round does here.
				gaveUp.Add(1)
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// The tunnel's cache has caught up: its next read of the list writes the last value, as the
	// periodic read does, and the reconciler's last write is already there.
	fresh := &v1alpha1.Instance{}
	if err := api.client.Get(ctx, key, fresh); err != nil {
		t.Fatal(err)
	}
	if err := state.apply(ctx, fresh, last); err != nil {
		t.Fatalf("the last write of TunnelConnected: %v", err)
	}
	got := &v1alpha1.Instance{}
	if err := api.client.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if tc := meta.FindStatusCondition(got.Status.Conditions, "TunnelConnected"); tc == nil || tc.Status != last.Status || tc.Reason != last.Reason {
		t.Errorf("stored TunnelConnected = %+v, want the tunnel writer's last value %s/%s", tc, last.Status, last.Reason)
	}
	for _, typ := range []string{"Verified", "Qualified", "WarmedUp"} {
		if c := meta.FindStatusCondition(got.Status.Conditions, typ); c == nil || c.Status != metav1.ConditionTrue {
			t.Errorf("stored condition %s = %+v, want the reconciler's True: its write was lost", typ, c)
		}
	}
	if h := meta.FindStatusCondition(got.Status.Conditions, "Healthy"); h == nil || h.Message != healthyMessage(interleaveRounds-1) {
		t.Errorf("stored Healthy = %+v, want the reconciler's last message %q", h, healthyMessage(interleaveRounds-1))
	}
	if got.Status.Phase != v1alpha1.InstanceReady || got.Status.Node != "node-7" {
		t.Errorf("stored phase %q, node %q, want the reconciler's last ready and node-7", got.Status.Phase, got.Status.Node)
	}
	// Both sides really met each other: each had Conflicts to retry, and wrote many times.
	if tunnelC.conflicts.Load() == 0 || lifeC.conflicts.Load() == 0 {
		t.Errorf("conflicts: tunnel writer %d of %d updates, reconciler %d of %d; both sides must have met some, or nothing interleaved",
			tunnelC.conflicts.Load(), tunnelC.updates.Load(), lifeC.conflicts.Load(), lifeC.updates.Load())
	}
	// The watch has seen the stored state itself, which is the last event the API server sent.
	waitUntil(t, 60*time.Second, "the watch to deliver the stored state", func() bool {
		iw.mu.Lock()
		defer iw.mu.Unlock()
		return iw.rv == got.ResourceVersion
	})
	if p := iw.problems(); len(p) > 0 {
		t.Errorf("%d states lost a write; the first: %s", len(p), p[0])
	}
	t.Logf("tunnel writer: %d updates, %d conflicts, gave up %d times; reconciler: %d updates, %d conflicts",
		tunnelC.updates.Load(), tunnelC.conflicts.Load(), gaveUp.Load(), lifeC.updates.Load(), lifeC.conflicts.Load())
}
