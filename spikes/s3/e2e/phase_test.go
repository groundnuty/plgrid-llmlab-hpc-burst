package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

// The two tests below are driven by the benchmark harness (S3_PHASE) and are
// skipped in a plain run: one creates the ~100-object population, the other
// checks that the population still works after a host upgrade.

const (
	loadNS     = "s3-load"
	loadFleets = 20 // 20 Fleets x 4 Workers = 100 custom objects
	loadReps   = 4
)

func phase(t *testing.T, want string) {
	if os.Getenv("S3_PHASE") != want {
		t.Skipf("only runs with S3_PHASE=%s", want)
	}
}

// TestPhaseLoad creates the population with the controller running, waits until it
// is reconciled and counted by listing, then signals the harness through marker
// files and keeps the controller alive until the harness has taken its samples.
func TestPhaseLoad(t *testing.T) {
	phase(t, "load")
	ready, done := os.Getenv("S3_MARKER_READY"), os.Getenv("S3_MARKER_DONE")
	if ready == "" || done == "" {
		t.Fatal("S3_MARKER_READY and S3_MARKER_DONE must be set")
	}
	startManager(t, newCanceller())
	_ = c.Create(bg, nsObj(loadNS))
	for i := 0; i < loadFleets; i++ {
		must(t, c.Create(bg, fleet(loadNS, fmt.Sprintf("load-%02d", i), loadReps, map[string]string{"s3-load": "1"})), "create load fleet")
	}
	eventually(t, 120*time.Second, "100 objects reconciled", func() (bool, error) {
		fl := kit.NewList(kit.FleetGVK)
		wl := kit.NewList(kit.WorkerGVK)
		if err := c.List(bg, fl, client.InNamespace(loadNS)); err != nil {
			return false, err
		}
		if err := c.List(bg, wl, client.InNamespace(loadNS)); err != nil {
			return false, err
		}
		if len(fl.Items) != loadFleets || len(wl.Items) != loadFleets*loadReps {
			return false, nil
		}
		for _, f := range fl.Items {
			if r, _, _ := unstructured.NestedInt64(f.Object, "status", "readyWorkers"); r != loadReps {
				return false, nil
			}
		}
		return true, nil
	})
	must(t, os.WriteFile(ready, []byte("ready\n"), 0o600), "write ready marker")
	eventually(t, 10*time.Minute, "harness to finish sampling", func() (bool, error) {
		_, err := os.Stat(done)
		return err == nil, nil
	})
}

// TestPhasePostUpgrade checks the population created before the upgrade: the same
// objects are still reconciled by a controller started now, and a cascade still works.
func TestPhasePostUpgrade(t *testing.T) {
	phase(t, "postupgrade")
	startManager(t, newCanceller())
	f0 := kit.New(kit.FleetGVK)
	must(t, c.Get(bg, client.ObjectKey{Namespace: loadNS, Name: "load-00"}, f0), "get load-00")
	must(t, c.Patch(bg, f0, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":5}}`))), "scale load-00")
	eventually(t, 60*time.Second, "load-00 to have 5 workers", func() (bool, error) {
		ws, err := listWorkers(loadNS, client.MatchingLabels{kit.LabelFleet: "load-00"})
		return len(ws) == 5, err
	})
	// The controller has no deletion logic for a Fleet's Workers: only the host's GC can do this.
	f1 := kit.New(kit.FleetGVK)
	must(t, c.Get(bg, client.ObjectKey{Namespace: loadNS, Name: "load-01"}, f1), "get load-01")
	ws, err := listWorkers(loadNS, client.MatchingLabels{kit.LabelFleet: "load-01"})
	must(t, err, "list load-01 workers")
	if len(ws) != loadReps {
		t.Fatalf("load-01 has %d workers before the cascade, want %d", len(ws), loadReps)
	}
	must(t, c.Delete(bg, f1), "delete load-01")
	for i := range ws {
		w := &ws[i]
		eventually(t, 90*time.Second, "cascade of "+w.GetName(), func() (bool, error) { return gone(w, w.GetUID()) })
	}
}
