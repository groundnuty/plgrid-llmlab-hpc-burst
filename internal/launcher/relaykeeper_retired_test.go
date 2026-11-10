package launcher

// The keeper retired marker (relaykeeper.go; HBEP-6, Compatibility and upgrades, as amended 10 Nov
// 2026; the science agent's ruling on PR 361's review): written only on a read that saw no
// Allocation running, removed by up --relay-keeper before the keeper starts, and while it exists
// the guard passes.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// failingServer answers every request with a 500, an API read that fails.
func failingServer(t *testing.T) *rest.Config {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "the store is unavailable", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

// pagedServer answers the Allocations' list in pages, following the continue token as the API
// server does: each page's phases, the items built from the API type, every page but the last
// with a continue token. It counts the pages served.
func pagedServer(t *testing.T, pages ...[]string) (*rest.Config, *atomic.Int64) {
	t.Helper()
	var served atomic.Int64
	path := "/apis/" + v1alpha1.GroupVersion.String() + "/allocations"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		n := 0
		if c := r.URL.Query().Get("continue"); c != "" {
			if _, err := fmt.Sscanf(c, "page-%d", &n); err != nil {
				http.Error(w, "bad continue", http.StatusBadRequest)
				return
			}
		}
		served.Add(1)
		items := []map[string]any{}
		for i, p := range pages[n] {
			a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("alloc-%d-%d", n, i), Namespace: "example"}}
			a.APIVersion, a.Kind = v1alpha1.GroupVersion.String(), "Allocation"
			a.Status.AtProvider.Phase = v1alpha1.AllocationPhase(p)
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&a)
			if err != nil {
				t.Error(err)
				return
			}
			items = append(items, obj)
		}
		meta := map[string]any{}
		if n+1 < len(pages) {
			meta["continue"] = fmt.Sprintf("page-%d", n+1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": v1alpha1.GroupVersion.String(), "kind": "AllocationList", "metadata": meta, "items": items})
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}, &served
}

func markerExists(t *testing.T, l layout) bool {
	t.Helper()
	_, err := os.Stat(retiredMarker(l))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestTheKeeperRetiredMarker(t *testing.T) {
	t.Run("a drained switch back writes it", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		cfg, _ := allocationsServer(t, []string{"Ended", "Pending"}, "AllocationList")
		var log strings.Builder
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, &log); err != nil {
			t.Fatalf("a drained switch back refused: %v", err)
		}
		b, err := os.ReadFile(retiredMarker(l))
		if err != nil {
			t.Fatalf("no marker after a drained switch back: %v", err)
		}
		var m struct {
			RetiredAt time.Time `json:"retired_at"`
		}
		if err := json.Unmarshal(b, &m); err != nil || m.RetiredAt.IsZero() {
			t.Errorf("the marker %q: %v; want it whole, with its time", b, err)
		}
		if fi, _ := os.Stat(retiredMarker(l)); fi.Mode().Perm() != 0o600 {
			t.Errorf("the marker's mode %v, want 600", fi.Mode().Perm())
		}
		if !strings.Contains(log.String(), "the relay keeper is retired") {
			t.Errorf("up's log %q does not say the keeper was retired", log.String())
		}
	})
	t.Run("a failed read refuses and writes nothing", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		if err := checkKeeperSwitchBack(context.Background(), failingServer(t), l, false, io.Discard); err == nil {
			t.Fatal("a switch back whose read failed passed")
		}
		if markerExists(t, l) {
			t.Error("a failed read wrote the marker")
		}
	})
	t.Run("a server that does not serve Allocations refuses and writes nothing", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		cfg, _ := allocationsServer(t, nil, "AllocationList") // a 404
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard); err == nil || !strings.Contains(err.Error(), "does not serve Allocations") {
			t.Errorf("a switch back on a 404: %v; want refused", err)
		}
		if markerExists(t, l) {
			t.Error("a 404 wrote the marker")
		}
	})
	t.Run("a paginated list with Running on page 2 refuses", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		cfg, served := pagedServer(t, []string{"Ended", "Pending"}, []string{"Ended", "Running"})
		err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "example/alloc-1-1") {
			t.Errorf("a switch back with Running on page 2: %v; want refused, naming it", err)
		}
		if served.Load() != 2 || markerExists(t, l) {
			t.Errorf("pages read %d (want 2), marker written %v (want not)", served.Load(), markerExists(t, l))
		}
	})
	t.Run("a complete paginated list with none Running writes it", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		cfg, served := pagedServer(t, []string{"Ended"}, []string{"Pending"}, []string{"Failed"})
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard); err != nil {
			t.Fatalf("a drained switch back over three pages: %v", err)
		}
		if served.Load() != 3 || !markerExists(t, l) {
			t.Errorf("pages read %d (want 3), marker written %v (want it)", served.Load(), markerExists(t, l))
		}
	})
	t.Run("Allocations running refuse and write nothing", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		cfg, _ := allocationsServer(t, []string{"Running"}, "AllocationList")
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard); err == nil {
			t.Fatal("a switch back with an Allocation running passed")
		}
		if markerExists(t, l) {
			t.Error("a refused switch back wrote the marker")
		}
	})
	t.Run("with the marker, up passes whatever runs", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		if err := writeRetiredMarker(l, time.Now()); err != nil {
			t.Fatal(err)
		}
		cfg, lists := allocationsServer(t, []string{"Running", "Running"}, "AllocationList")
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard); err != nil {
			t.Errorf("an alpha up with the marker and alpha Allocations running refused: %v", err)
		}
		if lists.Load() != 0 {
			t.Error("the guard read the Allocations though the keeper was retired")
		}
	})
	t.Run("up --relay-keeper removes it", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		if err := writeRetiredMarker(l, time.Now()); err != nil {
			t.Fatal(err)
		}
		cfg, _ := allocationsServer(t, []string{"Running"}, "AllocationList")
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, true, io.Discard); err != nil {
			t.Fatal(err)
		}
		if markerExists(t, l) {
			t.Error("the marker is still there after up --relay-keeper's guard")
		}
		// Without the marker, a switch back with Allocations running is refused again.
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, false, io.Discard); err == nil {
			t.Error("a switch back after the keeper ran again passed with an Allocation running")
		}
	})
	t.Run("a marker that cannot be removed keeps the keeper from starting", func(t *testing.T) {
		l := guardLayout(t)
		keeperRan(t, l)
		// A directory with a file in it: os.Remove cannot remove it.
		if err := os.MkdirAll(retiredMarker(l)+"/held", 0o700); err != nil {
			t.Fatal(err)
		}
		cfg, _ := allocationsServer(t, []string{}, "AllocationList")
		if err := checkKeeperSwitchBack(context.Background(), cfg, l, true, io.Discard); err == nil || !strings.Contains(err.Error(), "could not be removed") {
			t.Errorf("up --relay-keeper with a marker it cannot remove: %v; want refused", err)
		}
	})
}
