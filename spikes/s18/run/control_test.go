package run

// The controls that make S18's measurements valid (#528, Valid):
//   - with no fault, every request is answered whole, every one is begun exactly once by one engine,
//     no engine dies, and the client's and the engines' books balance both ways;
//   - so under arm (a), the front as it is, and under the retry layer at budget 0.

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func checkNoFault(t *testing.T, r *rig.Rig, c *rig.Client, results []rig.Result) {
	t.Helper()
	for _, res := range results {
		if res.Status != 200 || !res.Complete || res.Err != "" {
			t.Errorf("%s (stream %v): %d %s %s %q, want a whole 200", res.ID, res.Stream, res.Status, res.Code, res.Detail, res.Err)
		}
	}
	begins, err := r.Begins()
	if err != nil {
		t.Fatal(err)
	}
	for _, res := range results {
		if n := rig.Admissions(begins[res.ID]); n != 1 {
			t.Errorf("%s begun %d times, want once: %+v", res.ID, n, begins[res.ID])
		}
	}
	for _, p := range rig.Reconcile(results, begins) {
		t.Error(p)
	}
	for _, e := range r.Doubles.Exits() {
		t.Errorf("an engine ended with no fault injected: %+v", e)
	}
}

func TestNoFaultEveryRequestIsBegunOnceUnderAlpha(t *testing.T) {
	r := stand(t, "", nil)
	c := rig.NewClient(r.Front.URL, r.Front.ClientKey, "nofaultA")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	checkNoFault(t, r, c, sendMany(ctx, c, 60, 5))
}

func TestNoFaultEveryRequestIsBegunOnceUnderTheLayerAtBudget0(t *testing.T) {
	p := rig.Budget0
	r := stand(t, "", &p)
	c := rig.NewClient(r.Front.URL, r.Front.ClientKey, "nofaultL")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	checkNoFault(t, r, c, sendMany(ctx, c, 60, 5))
}
