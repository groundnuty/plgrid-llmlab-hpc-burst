package inventory

// The front reads an Instance's status.healthyWorkers, which the lifecycle controller writes for an
// Instance behind a router (HBEP-5, Load bound and cap and Inside an Instance, Worker health; #429):
// the cap is the engine's maximum concurrent sequences times the healthy workers, a set instanceCap is
// scaled by healthy over desired workers, and an Instance with none is not eligible. An Instance with
// no healthyWorkers, alpha's, is capped and judged as before. Expected values are worked by hand from
// the fixture's maxNumSeqs of 48 and HBEP-5's rule.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// routedSnap is batchSnap's one ready Instance, with healthyWorkers set when h is not nil.
func routedSnap(h *int32) Snapshot {
	le := now.Add(48 * time.Hour)
	return batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
		in := instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
		in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
		in.Status.HealthyWorkers = h
		return in
	})
}

func member(t *testing.T, s Snapshot) *Instance {
	t.Helper()
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	d := v.Models["m"].Served[0]
	if len(d.Members) != 1 {
		t.Fatalf("members %+v, want the one Instance", d.Members)
	}
	return d.Members[0]
}

func TestCapIsSeqsTimesHealthyWorkers(t *testing.T) {
	one := int32(1)
	if m := member(t, routedSnap(&one)); m.Cap != 48 || !m.Eligible {
		t.Errorf("one healthy worker: cap %d eligible %v, want 48 x 1 and eligible", m.Cap, m.Eligible)
	}
	// A set instanceCap scales by healthy over desired workers: 10 x 1 / 1.
	s := routedSnap(&one)
	ten := int32(10)
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: &ten}
	if m := member(t, s); m.Cap != 10 {
		t.Errorf("instanceCap 10 with one healthy worker: cap %d, want 10", m.Cap)
	}
}

func TestNoHealthyWorkerIsNotEligible(t *testing.T) {
	zero := int32(0)
	m := member(t, routedSnap(&zero))
	if m.Eligible || m.Cap != 0 {
		t.Errorf("no healthy worker: eligible %v cap %d, want not eligible and cap 0", m.Eligible, m.Cap)
	}
	s := routedSnap(&zero)
	ten := int32(10)
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: &ten}
	if m := member(t, s); m.Eligible || m.Cap != 0 {
		t.Errorf("no healthy worker with instanceCap 10: eligible %v cap %d, want not eligible and cap 0", m.Eligible, m.Cap)
	}
}

// Alpha's Instance, with no healthyWorkers, is capped and judged as on main (TestCapAndBounds's
// values, 48 and 2), router or not; with healthyWorkers set, it is not (the positive control above).
func TestAlphaCapAndEligibilityWithoutHealthyWorkers(t *testing.T) {
	m := member(t, routedSnap(nil))
	if m.Cap != 48 || !m.Eligible {
		t.Errorf("no healthyWorkers: cap %d eligible %v, want main's 48 and eligible", m.Cap, m.Eligible)
	}
	s := routedSnap(nil)
	two := int32(2)
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: &two}
	if m := member(t, s); m.Cap != 2 || !m.Eligible {
		t.Errorf("no healthyWorkers, instanceCap 2: cap %d eligible %v, want main's 2 and eligible", m.Cap, m.Eligible)
	}
}
