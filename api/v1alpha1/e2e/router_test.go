package e2e

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The router's fields (#429, beta, opt-in; the science agent's rulings of 12 Nov 2026): an
// Instance's launch.router is "envoy" or absent, immutable with the rest of the spec, and the
// router rung's restarts are recorded under the part "router".

// The API server accepts a restart entry for the part router, and still refuses near misses. The
// parts instance and worker-0, which it accepted before, are the control.
func TestInstanceRestartAcceptsTheRouterPart(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "router-restarts")
	must(t, c.Create(bg(), in), "create")
	t0 := metav1.NewTime(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	entry := func(part string) []v1alpha1.InstanceRestart {
		return []v1alpha1.InstanceRestart{{Part: part, Generation: 1, Time: t0, Signal: v1alpha1.ReasonPartExited}}
	}
	for _, part := range []string{v1alpha1.PartRouter, v1alpha1.PartInstance, v1alpha1.WorkerName(0)} {
		t.Run("accept "+part, func(t *testing.T) {
			got := &v1alpha1.Instance{}
			must(t, c.Get(bg(), objKey(in), got), "get")
			got.Status.Restarts = entry(part)
			must(t, c.Status().Update(bg(), got), "write a restart of "+part)
			again := &v1alpha1.Instance{}
			must(t, c.Get(bg(), objKey(in), again), "get")
			if len(again.Status.Restarts) != 1 || again.Status.Restarts[0].Part != part {
				t.Errorf("restarts = %+v, want one of %s", again.Status.Restarts, part)
			}
		})
	}
	if v1alpha1.PartRouter != "router" {
		t.Errorf("PartRouter = %q, want router (HBEP-7)", v1alpha1.PartRouter)
	}
	for _, part := range []string{"routers", "Router", "router-0", "worker-x"} {
		t.Run("refuse "+part, func(t *testing.T) {
			got := &v1alpha1.Instance{}
			must(t, c.Get(bg(), objKey(in), got), "get")
			got.Status.Restarts = entry(part)
			requireRefused(t, c.Status().Update(bg(), got), "restarts[0].part")
		})
	}
}

// launch.router is "envoy" or absent; any other router is refused, and it cannot be added to, or
// removed from, an Instance that exists.
func TestInstanceLaunchRouterIsEnvoyOrAbsentAndImmutable(t *testing.T) {
	ns := newNamespace(t)
	withEnvoy := newInstance(ns, "with-envoy")
	withEnvoy.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	must(t, c.Create(bg(), withEnvoy), "create an Instance behind Envoy")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(withEnvoy), got), "get")
	if got.Spec.Launch.Router != "envoy" {
		t.Errorf("launch.router = %q, want envoy", got.Spec.Launch.Router)
	}

	other := newInstance(ns, "with-vllm-router")
	other.Spec.Launch.Router = "vllm-router"
	requireRefused(t, c.Create(bg(), other), "spec.launch.router", "envoy")

	alpha := newInstance(ns, "alpha")
	must(t, c.Create(bg(), alpha), "create an Instance with no router")
	must(t, c.Get(bg(), objKey(alpha), got), "get")
	if got.Spec.Launch.Router != "" {
		t.Errorf("an Instance created without a router has launch.router %q", got.Spec.Launch.Router)
	}
	got.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	requireRefused(t, c.Update(bg(), got), "immutable")

	must(t, c.Get(bg(), objKey(withEnvoy), got), "get")
	got.Spec.Launch.Router = ""
	requireRefused(t, c.Update(bg(), got), "immutable")
}

// status.healthyWorkers is the router's healthy count (HBEP-5, Worker health): absent in alpha, 0 and up
// accepted, and below 0 refused.
func TestInstanceHealthyWorkersRoundTripsAndIsNotNegative(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "healthy-workers")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Status.HealthyWorkers != nil {
		t.Errorf("a new Instance has healthyWorkers %d, want none", *got.Status.HealthyWorkers)
	}
	for _, n := range []int32{0, 1} {
		must(t, c.Get(bg(), objKey(in), got), "get")
		v := n
		got.Status.HealthyWorkers = &v
		must(t, c.Status().Update(bg(), got), "write healthyWorkers")
		again := &v1alpha1.Instance{}
		must(t, c.Get(bg(), objKey(in), again), "get")
		if again.Status.HealthyWorkers == nil || *again.Status.HealthyWorkers != n {
			t.Errorf("healthyWorkers read back as %v, want %d", again.Status.HealthyWorkers, n)
		}
	}
	must(t, c.Get(bg(), objKey(in), got), "get")
	minus := int32(-1)
	got.Status.HealthyWorkers = &minus
	requireRefused(t, c.Status().Update(bg(), got), "healthyWorkers")
}
