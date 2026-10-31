package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

const sha1 = "0123456789abcdef0123456789abcdef01234567"
const sha2 = "fedcba9876543210fedcba9876543210fedcba98"

// newNS creates a uniquely named namespace. Cleanup is best effort and never waits:
// a bare kube-apiserver has no namespace controller, so a namespace may stay Terminating.
func newNS(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("s3-%s-%06x", "t", rand.Uint32N(1<<24))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.Create(context.Background(), ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), ns) })
	return name
}

func eventually(t *testing.T, timeout time.Duration, what string, fn func() (bool, error)) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		ok, e := fn()
		last = e
		return ok, nil
	})
	if err != nil {
		t.Fatalf("timed out after %s waiting for %s (last error: %v)", timeout, what, last)
	}
}

func consistently(t *testing.T, d time.Duration, what string, fn func() error) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if err := fn(); err != nil {
			t.Fatalf("%s did not hold: %v", what, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func fleet(ns, name string, replicas int64, labels map[string]string) *unstructured.Unstructured {
	f := kit.New(kit.FleetGVK)
	f.SetNamespace(ns)
	f.SetName(name)
	f.SetLabels(labels)
	_ = unstructured.SetNestedField(f.Object, "model-a", "spec", "model")
	_ = unstructured.SetNestedField(f.Object, sha1, "spec", "revision")
	_ = unstructured.SetNestedField(f.Object, replicas, "spec", "replicas")
	return f
}

// owned adds an ownerReference to owner (with the owner's real UID) to obj.
func owned(obj *unstructured.Unstructured, owner *unstructured.Unstructured) *unstructured.Unstructured {
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion:         owner.GetAPIVersion(),
		Kind:               owner.GetKind(),
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}})
	return obj
}

func worker(ns, name string) *unstructured.Unstructured {
	w := kit.New(kit.WorkerGVK)
	w.SetNamespace(ns)
	w.SetName(name)
	return w
}

func slot(ns, name string) *unstructured.Unstructured {
	s := kit.New(kit.SlotGVK)
	s.SetNamespace(ns)
	s.SetName(name)
	_ = unstructured.SetNestedField(s.Object, "provider-a", "spec", "provider")
	return s
}

func must(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// gone reports whether the object with this UID no longer exists.
func gone(obj *unstructured.Unstructured, uid types.UID) (bool, error) {
	got := kit.New(obj.GroupVersionKind())
	err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), got)
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}
	if err != nil {
		return true, nil
	}
	return got.GetUID() != uid, nil
}

func listWorkers(ns string, sel client.MatchingLabels) ([]unstructured.Unstructured, error) {
	l := kit.NewList(kit.WorkerGVK)
	if err := c.List(context.Background(), l, client.InNamespace(ns), sel); err != nil {
		return nil, err
	}
	return l.Items, nil
}

// canceller is a provider plugin double: Cancel fails until the gate opens, and
// records every attempt and every success per slot UID.
type canceller struct {
	mu       sync.Mutex
	open     bool
	attempts map[types.UID]int
	done     map[types.UID]int
}

func newCanceller() *canceller {
	return &canceller{attempts: map[types.UID]int{}, done: map[types.UID]int{}}
}

func (g *canceller) Cancel(_ context.Context, s *unstructured.Unstructured) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.attempts[s.GetUID()]++
	if !g.open {
		return errors.New("provider unreachable")
	}
	g.done[s.GetUID()]++
	return nil
}

func (g *canceller) setOpen(v bool) { g.mu.Lock(); g.open = v; g.mu.Unlock() }
func (g *canceller) counts(uid types.UID) (attempts, done int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.attempts[uid], g.done[uid]
}

// startManager runs the controller in this process and returns a stop func that
// waits for the manager to exit, so "stopped" really means stopped.
func startManager(t *testing.T, cn kit.Canceller) (stop func()) {
	t.Helper()
	mgr, err := kit.Manager(cfg, cn)
	must(t, err, "build manager")
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		t.Fatalf("manager cache did not sync")
	}
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-exited:
			case <-time.After(30 * time.Second):
				t.Errorf("manager did not stop within 30s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func nsObj(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}
