package inventory

// An Instance of several workers (HBEP-7, Several workers; HBEP-5, Load bound and cap): a set
// instanceCap is scaled by healthy workers over the Instance's own launch.workers, rounded down, and
// the default cap is launch.maxNumSeqs (48 in batchSnap) times its healthy workers. Expected values are
// worked out by hand.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// workersSnap is routedSnap's Instance with n workers and h healthy.
func workersSnap(n, h int32, instanceCap *int32) Snapshot {
	s := routedSnap(&h)
	s.Instances[0].Spec.Launch.Workers = &n
	if instanceCap != nil {
		s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: instanceCap}
	}
	return s
}

func TestTheCapScalesByTheInstancesOwnWorkers(t *testing.T) {
	ten := int32(10)
	for _, c := range []struct {
		name       string
		n, healthy int32
		cap        *int32
		want       int
	}{
		{"instanceCap 10, 2 workers, both healthy: 10 x 2 / 2", 2, 2, &ten, 10},
		{"instanceCap 10, 2 workers, one healthy: 10 x 1 / 2", 2, 1, &ten, 5},
		{"instanceCap 10, 3 workers, one healthy: 10 x 1 / 3, rounded down", 3, 1, &ten, 3},
		{"no instanceCap, 2 workers, one healthy: 48 x 1", 2, 1, nil, 48},
		{"no instanceCap, 2 workers, both healthy: 48 x 2", 2, 2, nil, 96},
	} {
		t.Run(c.name, func(t *testing.T) {
			if m := member(t, workersSnap(c.n, c.healthy, c.cap)); m.Cap != c.want || !m.Eligible {
				t.Errorf("cap %d eligible %v, want %d and eligible", m.Cap, m.Eligible, c.want)
			}
		})
	}
}

// A scaled cap that rounds down to 0 while a worker is healthy is 1, never 0, which the balancer reads as
// no cap (HBEP-5, Load bound and cap, as amended for #439): instanceCap 1, 2 workers, 1 healthy is
// 1 x 1 / 2, rounded down to 0, so 1; instanceCap 1, 8 workers, 3 healthy is 0, so 1. With no healthy
// worker the Instance is not eligible, its cap 0, as before.
func TestAScaledCapIsNeverBelowOneWhileAWorkerIsHealthy(t *testing.T) {
	one := int32(1)
	for _, c := range []struct {
		name       string
		n, healthy int32
	}{
		{"instanceCap 1, 2 workers, 1 healthy", 2, 1},
		{"instanceCap 1, 8 workers, 3 healthy", 8, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			if m := member(t, workersSnap(c.n, c.healthy, &one)); m.Cap != 1 || !m.Eligible {
				t.Errorf("cap %d eligible %v, want 1 and eligible: a cap of 0 would mean no cap", m.Cap, m.Eligible)
			}
		})
	}
	if m := member(t, workersSnap(2, 0, &one)); m.Cap != 0 || m.Eligible {
		t.Errorf("no healthy worker: cap %d eligible %v, want 0 and not eligible", m.Cap, m.Eligible)
	}
}
