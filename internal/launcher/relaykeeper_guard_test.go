package launcher

// up's switch-back guard (relaykeeper.go, checkKeeperSwitchBack; HBEP-6, Compatibility and
// upgrades; Test plan: "after down, up without --relay-keeper refuses while host-dialed
// Allocations run"), against a stub API server that answers the Allocations' list as the real one
// does, and a keeper state file the keeper itself wrote (relaykeeper.OpenState).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
)

// allocationsServer answers the Allocations' list with items of the given phases, named alloc-0,
// alloc-1 and so on in namespace example; a nil phases answers 404, a server without the CRD. It
// counts the lists.
func allocationsServer(t *testing.T, phases []string, kind string) (*rest.Config, *atomic.Int64) {
	t.Helper()
	var lists atomic.Int64
	path := "/apis/" + v1alpha1.GroupVersion.String() + "/allocations"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		lists.Add(1)
		if phases == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
			return
		}
		// The items come from the API type, as the server encodes it: the phase is the observed
		// one, status.atProvider.phase (a hand-written status.phase made this test pass while the
		// guard read a field no Allocation has; the real k3s test caught it).
		items := []map[string]any{}
		for i, p := range phases {
			a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "alloc-" + string(rune('0'+i)), Namespace: "example"}}
			a.APIVersion, a.Kind = v1alpha1.GroupVersion.String(), "Allocation"
			a.Status.AtProvider.Phase = v1alpha1.AllocationPhase(p)
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&a)
			if err != nil {
				t.Error(err)
				return
			}
			items = append(items, obj)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": v1alpha1.GroupVersion.String(), "kind": kind, "metadata": map[string]any{}, "items": items})
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}, &lists
}

// keeperRan writes the keeper's state file as a keeper's first start leaves it.
func keeperRan(t *testing.T, l layout) {
	t.Helper()
	if _, err := relaykeeper.OpenState(relaykeeper.StatePath(l.dir)); err != nil {
		t.Fatal(err)
	}
}

func guardLayout(t *testing.T) layout {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return l
}

// The guard refuses only when the keeper ran here, up is without --relay-keeper, and an Allocation
// is Running, naming it; every other case passes. A keeper state file the keeper would refuse to
// load refuses too: it cannot show that the keeper never ran.
func TestTheSwitchBackGuard(t *testing.T) {
	for _, c := range []struct {
		name        string
		keeperRan   bool
		corrupt     bool
		relayKeeper bool
		phases      []string
		refuse      string // "" passes
		lists       bool   // the guard lists the Allocations
	}{
		{"no keeper ever ran", false, false, false, []string{"Running"}, "", false},
		{"the keeper is asked for", true, false, true, []string{"Running"}, "", false},
		{"the keeper ran, nothing runs", true, false, false, []string{"Pending", "Ended", "Failed"}, "", true},
		{"the keeper ran, no Allocation CRD", true, false, false, nil, "", true},
		{"the keeper ran, Allocations run", true, false, false, []string{"Ended", "Running", "Running"}, "example/alloc-1 example/alloc-2", true},
		{"a keeper state file that does not load", true, true, false, []string{}, "cannot be read", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := guardLayout(t)
			if c.keeperRan {
				keeperRan(t, l)
			}
			if c.corrupt {
				if err := os.WriteFile(relaykeeper.StatePath(l.dir), []byte("{torn"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, lists := allocationsServer(t, c.phases, "AllocationList")
			err := checkKeeperSwitchBack(context.Background(), cfg, l, c.relayKeeper)
			switch {
			case c.refuse == "" && err != nil:
				t.Errorf("refused: %v; want it to pass", err)
			case c.refuse != "" && (err == nil || !strings.Contains(err.Error(), c.refuse)):
				t.Errorf("got %v; want a refusal naming %q", err, c.refuse)
			}
			if got := lists.Load() > 0; got != c.lists {
				t.Errorf("listed the Allocations: %v, want %v", got, c.lists)
			}
		})
	}
}

// A list that is not an AllocationList refuses, never passes as "nothing runs".
func TestTheSwitchBackGuardRefusesAListThatIsNotAllocations(t *testing.T) {
	l := guardLayout(t)
	keeperRan(t, l)
	cfg, _ := allocationsServer(t, []string{}, "PlatformProfileList")
	if err := checkKeeperSwitchBack(context.Background(), cfg, l, false); err == nil || !strings.Contains(err.Error(), "not an AllocationList") {
		t.Errorf("got %v; want refused", err)
	}
}
