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
