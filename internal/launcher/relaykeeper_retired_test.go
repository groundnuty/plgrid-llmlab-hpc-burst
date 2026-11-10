package launcher

// The keeper retired marker (relaykeeper.go; HBEP-6, Compatibility and upgrades, as amended 10 Nov
// 2026; the science agent's ruling on PR 361's review): written only on a read that saw no
// Allocation running, removed by up --relay-keeper before the keeper starts, and while it exists
// the guard passes.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
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
