package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

var bg = context.Background()

// TestDiscovery: the CRDs are served and have a status subresource.
func TestDiscovery(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	must(t, err, "discovery client")
	rl, err := dc.ServerResourcesForGroupVersion(kit.Group + "/" + kit.Version)
	must(t, err, "discover group version")
	have := map[string]bool{}
	for _, r := range rl.APIResources {
		have[r.Name] = true
	}
	for _, want := range []string{"fleets", "fleets/status", "workers", "workers/status", "slots", "slots/status"} {
		if !have[want] {
			t.Errorf("resource %q not served; discovered %v", want, have)
		}
	}
}

// TestValidation: invalid specs are refused before they are stored (HBEP-9, HBEP-11).
func TestValidation(t *testing.T) {
	ns := newNS(t)
	bad := fleet(ns, "unpinned", 1, nil)
	_ = unstructured.SetNestedField(bad.Object, "main", "spec", "revision")
	if err := c.Create(bg, bad); !apierrors.IsInvalid(err) {
		t.Errorf("unpinned revision: want Invalid, got %v", err)
	}
	big := fleet(ns, "toobig", 51, nil)
	if err := c.Create(bg, big); !apierrors.IsInvalid(err) {
		t.Errorf("replicas over maximum: want Invalid, got %v", err)
	}
	nomodel := fleet(ns, "nomodel", 1, nil)
	unstructured.RemoveNestedField(nomodel.Object, "spec", "model")
	if err := c.Create(bg, nomodel); !apierrors.IsInvalid(err) {
		t.Errorf("missing model: want Invalid, got %v", err)
	}
	// A refused object must not exist.
	if err := c.Get(bg, client.ObjectKey{Namespace: ns, Name: "unpinned"}, kit.New(kit.FleetGVK)); !apierrors.IsNotFound(err) {
		t.Errorf("refused object is stored: %v", err)
	}
	good := fleet(ns, "good", 1, map[string]string{kit.LabelController: "off"})
	must(t, c.Create(bg, good), "create valid fleet")
	if r, _, _ := unstructured.NestedInt64(good.Object, "spec", "replicas"); r != 1 {
		t.Errorf("replicas = %d, want 1", r)
	}
	// Immutability rule (CEL): changing the pinned revision is refused.
	_ = unstructured.SetNestedField(good.Object, sha2, "spec", "revision")
	err := c.Update(bg, good)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "immutable") {
		t.Errorf("revision change: want Invalid mentioning immutable, got %v", err)
	}
	// Defaulting: replicas omitted becomes 1.
	def := fleet(ns, "defaulted", 1, map[string]string{kit.LabelController: "off"})
	unstructured.RemoveNestedField(def.Object, "spec", "replicas")
	must(t, c.Create(bg, def), "create fleet without replicas")
	got := kit.New(kit.FleetGVK)
	must(t, c.Get(bg, client.ObjectKeyFromObject(def), got), "get defaulted fleet")
	if r, found, _ := unstructured.NestedInt64(got.Object, "spec", "replicas"); !found || r != 1 {
		t.Errorf("defaulted replicas = %d (found %v), want 1", r, found)
	}
}

// TestReconcileAndScale: the controller creates owned Workers, reports status,
// scales both ways without recreating survivors, and heals a deleted Worker.
func TestReconcileAndScale(t *testing.T) {
	ns := newNS(t)
	startManager(t, newCanceller())
	f := fleet(ns, "web", 3, nil)
	must(t, c.Create(bg, f), "create fleet")
	sel := client.MatchingLabels{kit.LabelFleet: "web"}

	settle := func(n int) []unstructured.Unstructured {
		var ws []unstructured.Unstructured
		eventually(t, 30*time.Second, "workers and status to reach the desired count", func() (bool, error) {
			var err error
			if ws, err = listWorkers(ns, sel); err != nil || len(ws) != n {
				return false, err
			}
			cur := kit.New(kit.FleetGVK)
			if err := c.Get(bg, client.ObjectKeyFromObject(f), cur); err != nil {
				return false, err
			}
			rw, _, _ := unstructured.NestedInt64(cur.Object, "status", "readyWorkers")
			og, _, _ := unstructured.NestedInt64(cur.Object, "status", "observedGeneration")
			return rw == int64(n) && og == cur.GetGeneration(), nil
		})
		return ws
	}
	ws := settle(3)
	must(t, c.Get(bg, client.ObjectKeyFromObject(f), f), "get fleet")
	for _, w := range ws {
		refs := w.GetOwnerReferences()
		if len(refs) != 1 || refs[0].UID != f.GetUID() || refs[0].Controller == nil || !*refs[0].Controller || refs[0].Kind != "Fleet" {
			t.Errorf("worker %s: ownerReferences %+v do not name the fleet %s as controller", w.GetName(), refs, f.GetUID())
		}
	}
	uid := map[string]types.UID{}
	for _, w := range ws {
		uid[w.GetName()] = w.GetUID()
	}

	patch := func(n string) {
		must(t, c.Patch(bg, f, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":`+n+`}}`))), "patch replicas")
	}
	patch("5")
	settle(5)
	patch("2")
	ws = settle(2)
	for _, w := range ws {
		if uid[w.GetName()] != w.GetUID() {
			t.Errorf("survivor %s was recreated (uid changed)", w.GetName())
		}
	}
	// Self-heal: delete one, it comes back with a new UID.
	victim := ws[0]
	must(t, c.Delete(bg, &victim), "delete worker")
	eventually(t, 30*time.Second, "deleted worker to be recreated", func() (bool, error) {
		ws, err := listWorkers(ns, sel)
		if err != nil || len(ws) != 2 {
			return false, err
		}
		for _, w := range ws {
			if w.GetName() == victim.GetName() && w.GetUID() == victim.GetUID() {
				return false, nil
			}
		}
		return true, nil
	})
}

// TestRestartConvergence: level-triggered. While no controller runs the world
// changes; a new controller reads everything and converges.
func TestRestartConvergence(t *testing.T) {
	ns := newNS(t)
	stop := startManager(t, newCanceller())
	f := fleet(ns, "restart", 2, nil)
	must(t, c.Create(bg, f), "create fleet")
	sel := client.MatchingLabels{kit.LabelFleet: "restart"}
	eventually(t, 30*time.Second, "2 workers", func() (bool, error) {
		ws, err := listWorkers(ns, sel)
		return len(ws) == 2, err
	})
	stop()
	ws, _ := listWorkers(ns, sel)
	victim := ws[0]
	must(t, c.Delete(bg, &victim), "delete worker while controller is down")
	must(t, c.Patch(bg, f, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":4}}`))), "patch replicas")
	consistently(t, 2*time.Second, "nothing changes while the controller is stopped", func() error {
		ws, err := listWorkers(ns, sel)
		if err == nil && len(ws) != 1 {
			return fmt.Errorf("worker count changed to %d", len(ws))
		}
		return err
	})
	startManager(t, newCanceller())
	eventually(t, 30*time.Second, "4 workers after the restart", func() (bool, error) {
		ws, err := listWorkers(ns, sel)
		return len(ws) == 4, err
	})
}

// TestSelectors: the inventory by label selector and field selector.
func TestSelectors(t *testing.T) {
	ns := newNS(t)
	off := func(team string) map[string]string {
		return map[string]string{"team": team, kit.LabelController: "off"}
	}
	for name, team := range map[string]string{"a1": "a", "a2": "a", "b1": "b"} {
		must(t, c.Create(bg, fleet(ns, name, 0, off(team))), "create "+name)
	}
	names := func(opts ...client.ListOption) []string {
		l := kit.NewList(kit.FleetGVK)
		must(t, c.List(bg, l, append(opts, client.InNamespace(ns))...), "list")
		var n []string
		for _, i := range l.Items {
			n = append(n, i.GetName())
		}
		return n
	}
	eq := func(got []string, want ...string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v want %v", got, want)
		}
		m := map[string]bool{}
		for _, g := range got {
			m[g] = true
		}
		for _, w := range want {
			if !m[w] {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	}
	eq(names(client.MatchingLabels{"team": "a"}), "a1", "a2")
	eq(names(client.MatchingLabelsSelector{Selector: mustSel(t, "team in (a,b)")}), "a1", "a2", "b1")
	eq(names(client.MatchingLabelsSelector{Selector: mustSel(t, "team!=a")}), "b1")
	eq(names(client.MatchingFields{"metadata.name": "a2"}), "a2")
}

// TestConflict: optimistic concurrency on resourceVersion.
func TestConflict(t *testing.T) {
	ns := newNS(t)
	f := fleet(ns, "occ", 1, map[string]string{kit.LabelController: "off"})
	must(t, c.Create(bg, f), "create")
	stale := f.DeepCopy()
	f.SetLabels(map[string]string{"x": "1", kit.LabelController: "off"})
	must(t, c.Update(bg, f), "first update")
	stale.SetLabels(map[string]string{"x": "2", kit.LabelController: "off"})
	if err := c.Update(bg, stale); !apierrors.IsConflict(err) {
		t.Errorf("update with a stale resourceVersion: want Conflict, got %v", err)
	}
}

// TestWatch: a watch from a list's resourceVersion delivers add, modify and delete in order.
func TestWatch(t *testing.T) {
	ns := newNS(t)
	l := kit.NewList(kit.FleetGVK)
	must(t, c.List(bg, l, client.InNamespace(ns)), "list")
	w, err := c.Watch(bg, l, client.InNamespace(ns), &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: l.GetResourceVersion()}})
	must(t, err, "watch")
	defer w.Stop()
	f := fleet(ns, "watched", 1, map[string]string{kit.LabelController: "off"})
	must(t, c.Create(bg, f), "create")
	f.SetLabels(map[string]string{"m": "1", kit.LabelController: "off"})
	must(t, c.Update(bg, f), "update")
	must(t, c.Delete(bg, f), "delete")
	var got []watch.EventType
	deadline := time.After(20 * time.Second)
	for len(got) < 3 {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed early after %v", got)
			}
			got = append(got, ev.Type)
		case <-deadline:
			t.Fatalf("timed out; events so far %v", got)
		}
	}
	want := []watch.EventType{watch.Added, watch.Modified, watch.Deleted}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
}

func mustSel(t *testing.T, expr string) labels.Selector {
	t.Helper()
	sel, err := labels.Parse(expr)
	must(t, err, "parse selector "+expr)
	return sel
}
