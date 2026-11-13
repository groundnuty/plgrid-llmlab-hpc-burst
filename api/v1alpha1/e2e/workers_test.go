package e2e

// Several workers per Instance (beta; HBEP-7, Several workers; HBEP-11's rows): the ModelDeployment's
// workersPerInstance, the Instance's launch.workers, kept by the spec's immutability rule, and its
// status.workers[], keyed by the worker's name. The bounds the science agent ruled (#439, Q3) are
// checked in TestWorkersBounds.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestWorkersPerInstanceRoundTrips(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "workers")
	must(t, c.Create(bg(), md), "create without workersPerInstance")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if got.Spec.WorkersPerInstance != nil || got.Spec.WorkersPerInstanceCount() != 1 {
		t.Errorf("a deployment without the field: %v, count %d; want absent and 1", got.Spec.WorkersPerInstance, got.Spec.WorkersPerInstanceCount())
	}
	two := int32(2)
	got.Spec.WorkersPerInstance = &two
	must(t, c.Update(bg(), got), "set workersPerInstance 2")
	again := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), again), "get")
	if again.Spec.WorkersPerInstance == nil || *again.Spec.WorkersPerInstance != 2 {
		t.Errorf("workersPerInstance read back as %v, want 2", again.Spec.WorkersPerInstance)
	}
}

func TestLaunchWorkersRoundTripsAndIsImmutable(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "launch-workers")
	two := int32(2)
	in.Spec.Launch.Workers = &two
	must(t, c.Create(bg(), in), "create with launch.workers 2")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Spec.Launch.Workers == nil || *got.Spec.Launch.Workers != 2 || v1alpha1.LaunchWorkers(got.Spec.Launch) != 2 {
		t.Fatalf("launch.workers read back as %v, want 2", got.Spec.Launch.Workers)
	}
	three := int32(3)
	got.Spec.Launch.Workers = &three
	requireRefused(t, c.Update(bg(), got), "immutable")

	alpha := newInstance(ns, "alpha-launch")
	must(t, c.Create(bg(), alpha), "create without launch.workers")
	must(t, c.Get(bg(), objKey(alpha), got), "get")
	if got.Spec.Launch.Workers != nil || v1alpha1.LaunchWorkers(got.Spec.Launch) != 1 {
		t.Errorf("an Instance without launch.workers: %v, count %d; want absent and 1", got.Spec.Launch.Workers, v1alpha1.LaunchWorkers(got.Spec.Launch))
	}
}

func TestInstanceWorkersStatusRoundTripsKeyedByName(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "status-workers")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if len(got.Status.Workers) != 0 {
		t.Errorf("a new Instance has status.workers %+v, want none", got.Status.Workers)
	}
	gen := int64(1)
	got.Status.Workers = []v1alpha1.InstanceWorker{
		{Name: "worker-0", VerifiedGeneration: &gen, Record: "ver-uid-worker-0-1", Verdict: "Pass", WarmedUp: true},
		{Name: "worker-1"},
	}
	must(t, c.Status().Update(bg(), got), "write two workers")
	again := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), again), "get")
	if len(again.Status.Workers) != 2 || again.Status.Workers[0].VerifiedGeneration == nil || *again.Status.Workers[0].VerifiedGeneration != 1 ||
		again.Status.Workers[0].Record != "ver-uid-worker-0-1" || !again.Status.Workers[0].WarmedUp || again.Status.Workers[1].VerifiedGeneration != nil {
		t.Errorf("status.workers read back as %+v", again.Status.Workers)
	}
	// Keyed by name: two entries of one name, and a name that is not a worker's, are refused.
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Workers = []v1alpha1.InstanceWorker{{Name: "worker-0"}, {Name: "worker-0"}}
	requireRefused(t, c.Status().Update(bg(), got), "Duplicate")
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Workers = []v1alpha1.InstanceWorker{{Name: "router"}}
	requireRefused(t, c.Status().Update(bg(), got), "name")
}

// The ruled bounds (#439, Q3): workersPerInstance and launch.workers are 1 to 8, and status.workers[]
// holds at most 8 entries. 0 and 9 are refused, 8 is accepted; 9 workers in status are refused.
func TestWorkersBounds(t *testing.T) {
	ns := newNamespace(t)
	for _, tc := range []struct {
		n  int32
		ok bool
	}{{0, false}, {1, true}, {8, true}, {9, false}} {
		md := newModelDeployment(ns, "bounds-"+string(rune('a'+tc.n)))
		v := tc.n
		md.Spec.WorkersPerInstance = &v
		err := c.Create(bg(), md)
		if tc.ok {
			must(t, err, "create with workersPerInstance in bounds")
		} else {
			requireRefused(t, err, "workersPerInstance")
		}
		in := newInstance(ns, "bounds-in-"+string(rune('a'+tc.n)))
		in.Spec.Launch.Workers = &v
		err = c.Create(bg(), in)
		if tc.ok {
			must(t, err, "create with launch.workers in bounds")
		} else {
			requireRefused(t, err, "workers")
		}
	}
	in := newInstance(ns, "nine-in-status")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	for i := 0; i < 9; i++ {
		got.Status.Workers = append(got.Status.Workers, v1alpha1.InstanceWorker{Name: v1alpha1.WorkerName(i)})
	}
	requireRefused(t, c.Status().Update(bg(), got), "workers")
	must(t, c.Get(bg(), objKey(in), got), "get")
	for i := 0; i < 8; i++ {
		got.Status.Workers = append(got.Status.Workers, v1alpha1.InstanceWorker{Name: v1alpha1.WorkerName(i)})
	}
	must(t, c.Status().Update(bg(), got), "write 8 workers")
}
