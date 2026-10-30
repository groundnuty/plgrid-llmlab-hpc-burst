package e2e

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

var off = map[string]string{kit.LabelController: "off"}

// makeTree creates Fleet -> 4 Workers -> (1 Slot under worker 0), all through the
// API with real owner UIDs. The controller is NOT running in the callers, so the
// host's own garbage collector is the only thing that can remove the children.
func makeTree(t *testing.T, ns, name string) (f *unstructured.Unstructured, ws []*unstructured.Unstructured, s *unstructured.Unstructured) {
	t.Helper()
	f = fleet(ns, name, 4, off)
	must(t, c.Create(bg, f), "create fleet "+name)
	for i := 0; i < 4; i++ {
		w := owned(worker(ns, fmt.Sprintf("%s-w%d", name, i)), f)
		must(t, c.Create(bg, w), "create worker")
		ws = append(ws, w)
	}
	s = owned(slot(ns, name+"-s0"), ws[0])
	must(t, c.Create(bg, s), "create slot")
	return f, ws, s
}

// TestCascadeBackground: deleting an owner must delete what it owns, two levels
// deep, and nothing else. A bare kube-apiserver (no controller manager) must FAIL
// this test; that is the negative control that shows the test can fail.
func TestCascadeBackground(t *testing.T) {
	ns := newNS(t)
	f, ws, s := makeTree(t, ns, "gc")
	_, otherWs, _ := makeTree(t, ns, "keep") // owned by another Fleet: must survive
	lone := worker(ns, "lone")               // no owner: must survive
	must(t, c.Create(bg, lone), "create unowned worker")

	victims := append([]*unstructured.Unstructured{s}, ws...)
	uids := map[*unstructured.Unstructured]types.UID{}
	for _, o := range victims {
		uids[o] = o.GetUID()
	}
	start := time.Now()
	must(t, c.Delete(bg, f, client.PropagationPolicy(metav1.DeletePropagationBackground)), "delete fleet")
	for _, o := range victims {
		o := o
		eventually(t, 90*time.Second, fmt.Sprintf("garbage collection of %s %s", o.GetKind(), o.GetName()), func() (bool, error) {
			return gone(o, uids[o])
		})
	}
	t.Logf("garbage collection of 5 dependents (2 levels) took %s", time.Since(start).Round(10*time.Millisecond))
	for _, keep := range append([]*unstructured.Unstructured{lone}, otherWs...) {
		if g, err := gone(keep, keep.GetUID()); err != nil || g {
			t.Errorf("%s %s should have survived (gone=%v err=%v)", keep.GetKind(), keep.GetName(), g, err)
		}
	}
}

// TestCascadeForeground: with foreground deletion the owner goes last. When the
// owner is gone, every dependent that blocks its deletion is already gone.
func TestCascadeForeground(t *testing.T) {
	ns := newNS(t)
	f, ws, s := makeTree(t, ns, "fg")
	must(t, c.Delete(bg, f, client.PropagationPolicy(metav1.DeletePropagationForeground)), "delete fleet (foreground)")
	eventually(t, 90*time.Second, "owner to disappear", func() (bool, error) { return gone(f, f.GetUID()) })
	for _, o := range append([]*unstructured.Unstructured{s}, ws...) {
		if g, err := gone(o, o.GetUID()); err != nil || !g {
			t.Errorf("%s %s outlived its foreground-deleted owner (gone=%v err=%v)", o.GetKind(), o.GetName(), g, err)
		}
	}
}

// TestFinalizerHoldsDeletion: a Slot with the cancel finalizer stays until the
// provider job is cancelled, and is cancelled exactly once. Direct delete.
func TestFinalizerHoldsDeletion(t *testing.T) {
	ns := newNS(t)
	cn := newCanceller()
	startManager(t, cn)
	s := slot(ns, "direct")
	must(t, c.Create(bg, s), "create slot")
	waitFinalizer(t, s)
	must(t, c.Delete(bg, s), "delete slot")
	holdThenRelease(t, s, cn)
}

// TestFinalizerThroughCascade: the HBEP-11 story. Deleting a Fleet makes the
// garbage collector delete its Workers and then their Slot; the Slot's provider
// job is cancelled before the object disappears.
func TestFinalizerThroughCascade(t *testing.T) {
	ns := newNS(t)
	cn := newCanceller()
	startManager(t, cn)
	f := fleet(ns, "story", 2, nil)
	must(t, c.Create(bg, f), "create fleet")
	var w0 *unstructured.Unstructured
	eventually(t, 30*time.Second, "worker 0", func() (bool, error) {
		w := worker(ns, "story-0")
		if err := c.Get(bg, client.ObjectKeyFromObject(w), w); err != nil {
			return false, err
		}
		w0 = w
		return true, nil
	})
	s := owned(slot(ns, "story-slot"), w0)
	must(t, c.Create(bg, s), "create slot")
	waitFinalizer(t, s)
	must(t, c.Delete(bg, f, client.PropagationPolicy(metav1.DeletePropagationBackground)), "delete fleet")
	eventually(t, 90*time.Second, "worker 0 to be garbage collected", func() (bool, error) { return gone(w0, w0.GetUID()) })
	holdThenRelease(t, s, cn)
}

func waitFinalizer(t *testing.T, s *unstructured.Unstructured) {
	t.Helper()
	eventually(t, 30*time.Second, "controller to add the cancel finalizer", func() (bool, error) {
		if err := c.Get(bg, client.ObjectKeyFromObject(s), s); err != nil {
			return false, err
		}
		for _, f := range s.GetFinalizers() {
			if f == kit.FinalizerCancel {
				return true, nil
			}
		}
		return false, nil
	})
}

// holdThenRelease expects slot s to be terminating but present while the provider
// is unreachable, then released exactly once after the provider answers.
func holdThenRelease(t *testing.T, s *unstructured.Unstructured, cn *canceller) {
	t.Helper()
	uid := s.GetUID()
	eventually(t, 90*time.Second, "slot to be marked for deletion", func() (bool, error) {
		cur := kit.New(kit.SlotGVK)
		if err := c.Get(bg, client.ObjectKeyFromObject(s), cur); err != nil {
			return false, err
		}
		return cur.GetDeletionTimestamp() != nil, nil
	})
	eventually(t, 30*time.Second, "controller to try to cancel", func() (bool, error) {
		a, _ := cn.counts(uid)
		return a >= 1, nil
	})
	consistently(t, 3*time.Second, "slot stays while the provider is unreachable", func() error {
		cur := kit.New(kit.SlotGVK)
		if err := c.Get(bg, client.ObjectKeyFromObject(s), cur); err != nil {
			return fmt.Errorf("slot vanished before cancel succeeded: %w", err)
		}
		if _, d := cn.counts(uid); d != 0 {
			return fmt.Errorf("cancel succeeded while the gate was closed")
		}
		return nil
	})
	cn.setOpen(true)
	eventually(t, 30*time.Second, "slot to be released after cancel", func() (bool, error) { return gone(s, uid) })
	if _, d := cn.counts(uid); d != 1 {
		t.Errorf("provider job cancelled %d times, want exactly 1", d)
	}
}
