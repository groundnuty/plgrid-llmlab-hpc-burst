package lifecycle

import (
	"context"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// tunnelRace is the state after one pass in which TunnelConnected turned True
// after the pass began and before it read the Instances (issue #263): the path
// -state writer's write landed in the window between the pass's start and its
// read. HBEP-7 (Instance start and ready) makes TunnelConnected True a
// condition of the first entry to ready, so every time the pass stamps for the
// Instance's readiness must not be earlier than that transition.
type tunnelRace struct {
	rg       *rig
	instance string
	// passStart is the rig clock when the pass began; connected is the
	// LastTransitionTime stored on TunnelConnected, two whole seconds later.
	passStart, connected time.Time
}

// raceToReady brings one Instance to the point where every check has passed and
// only the tunnel is missing, as TestReadyWaitsForTunnelConnected does, then
// runs one round in which the TunnelConnected write lands between the
// ModelDeployment's read at the pass's start and the Instances' list. The write
// carries the rig clock two whole seconds after the pass began: the pass's
// times and the stored condition both truncate to the second, so a smaller gap
// could not show.
func raceToReady(t *testing.T) tunnelRace {
	t.Helper()
	var (
		rg    *rig
		armed bool
		fired int
		race  tunnelRace
	)
	hook := func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		lo := &client.ListOptions{}
		lo.ApplyOptions(opts)
		// The pass's observe lists by label, and is the first such list once
		// the hook is armed; the rig's own lists (reconcileAll, syncTunnels)
		// carry no label.
		if _, ok := list.(*v1alpha1.InstanceList); ok && armed && lo.LabelSelector != nil {
			armed = false
			fired++
			race.passStart = rg.clock.Now()
			rg.clock.Advance(2 * time.Second)
			var in v1alpha1.Instance
			if err := c.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: race.instance}, &in); err != nil {
				t.Fatalf("the path-state writer reads Instance %s: %v", race.instance, err)
			}
			apimeta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{
				Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOnline,
				Message: "frps lists the Instance's proxy as online", LastTransitionTime: metav1.NewTime(rg.clock.Now()),
			})
			if err := c.Status().Update(ctx, &in); err != nil {
				t.Fatalf("the path-state writer writes TunnelConnected: %v", err)
			}
		}
		return c.List(ctx, list, opts...)
	}
	rg = newRig(t, withInterceptors(interceptor.Funcs{List: hook}))
	rg.noTunnels = true // the hook is the path-state writer
	rg.create(mdSpec("md"))
	startUntil(rg, "md", 20*time.Minute, "an Instance verified, qualified and warmed up", func() bool {
		insts := rg.instances("md")
		return len(insts) == 1 && isTrue(insts[0].Status.Conditions, v1alpha1.ConditionWarmedUp)
	})
	race.instance = rg.instances("md")[0].Name
	for range 5 { // the agent reports the warm-up done
		startStep(rg, "md", startTick)
	}
	if got := rg.instance(race.instance).Status; got.Phase != v1alpha1.InstanceVerifying || got.ReadyTime != nil {
		t.Fatalf("with every check passed and no TunnelConnected: phase %q, readyTime %v; want verifying, never ready", got.Phase, got.ReadyTime)
	}
	// One round, as rig.Step runs it, armed between the agents' step and the
	// pass: the agents' own paths (Backend.Desired) list Instances by label too.
	rg.clock.Advance(startTick)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	armed = true
	rg.reconcileAll()
	if fired != 1 {
		t.Fatalf("the TunnelConnected write landed %d times in the round, want once: the test would prove nothing", fired)
	}
	tc := apimeta.FindStatusCondition(rg.instance(race.instance).Status.Conditions, v1alpha1.ConditionTunnelConnected)
	if tc == nil || tc.Status != metav1.ConditionTrue || !tc.LastTransitionTime.After(race.passStart) {
		t.Fatalf("TunnelConnected %+v after the round; want True, stored after the pass began (%s)", tc, race.passStart.Format(time.RFC3339))
	}
	race.rg, race.connected = rg, tc.LastTransitionTime.Time
	if got := rg.instance(race.instance).Status; got.Phase != v1alpha1.InstanceReady || got.ReadyTime == nil {
		t.Fatalf("the pass that read TunnelConnected True: phase %q, readyTime %v; want ready in that pass", got.Phase, got.ReadyTime)
	}
	return race
}

// notBefore fails when a stamp is missing, and reports (without stopping the
// case) one earlier than the transition it depends on.
func (r tunnelRace) notBefore(t *testing.T, what string, stamp *metav1.Time) {
	t.Helper()
	if stamp == nil {
		t.Fatalf("%s is not set", what)
	}
	if stamp.Time.Before(r.connected) {
		t.Errorf("%s is %s, before TunnelConnected's transition at %s (the pass began at %s): an Instance cannot be ready before its tunnel connected (HBEP-7, Instance start and ready)",
			what, stamp.Time.Format(time.RFC3339), r.connected.Format(time.RFC3339), r.passStart.Format(time.RFC3339))
	}
}

// TestPassStampsAreNotBeforeTheTunnelItRead is issue #263. Each case reads one
// stamp the pass wrote in the race's round; each is a requirement in its own
// right, so each fails on its own when the pass's clock is read before the
// pass reads the API.
func TestPassStampsAreNotBeforeTheTunnelItRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		check func(*testing.T, tunnelRace)
	}{
		{"the Instance's ready time", func(t *testing.T, r tunnelRace) {
			r.notBefore(t, "the ready time", r.rg.instance(r.instance).Status.ReadyTime)
		}},
		{"Healthy's transition to True", func(t *testing.T, r tunnelRace) {
			c := apimeta.FindStatusCondition(r.rg.instance(r.instance).Status.Conditions, v1alpha1.ConditionHealthy)
			if c == nil || c.Status != metav1.ConditionTrue {
				t.Fatalf("Healthy %+v; want True on a ready Instance", c)
			}
			r.notBefore(t, "Healthy's lastTransitionTime", &c.LastTransitionTime)
		}},
		{"the class's start-to-ready time", func(t *testing.T, r tunnelRace) {
			// HBEP-7, Handover: the class's start-to-ready time runs from its
			// Allocation's start to the Instance's ready time, so it covers the
			// tunnel's connection: at least from the Allocation's start to
			// the transition. Both ends of the bound come from the objects the
			// test wrote and the fake Slurm, not from the controller's own
			// arithmetic.
			md := r.rg.md("md")
			if len(md.Status.StartToReady) != 1 {
				t.Fatalf("status.startToReady %+v; want one entry, the class's first measurement", md.Status.StartToReady)
			}
			m := md.Status.StartToReady[0]
			r.notBefore(t, "the measurement's time", &m.MeasuredTime)
			start := r.rg.allocations("md")[0].Status.AtProvider.StartTime
			if start == nil {
				t.Fatal("the Allocation has no start time")
			}
			got, err := time.ParseDuration(string(m.Duration))
			if err != nil {
				t.Fatalf("start-to-ready %q: %v", m.Duration, err)
			}
			if min := r.connected.Sub(start.Time); got < min {
				t.Fatalf("start-to-ready %s is shorter than the Allocation's start (%s) to TunnelConnected's transition (%s), %s",
					got, start.Time.Format(time.RFC3339), r.connected.Format(time.RFC3339), min)
			}
		}},
		{"the ModelDeployment's Ready transition", func(t *testing.T, r tunnelRace) {
			c := r.rg.condition("md", v1alpha1.ConditionReady)
			if c.Status != metav1.ConditionTrue {
				t.Fatalf("the ModelDeployment's Ready %+v; want True with its one Instance ready", c)
			}
			r.notBefore(t, "the ModelDeployment's Ready lastTransitionTime", &c.LastTransitionTime)
		}},
		{"the Instance's last reconcile time", func(t *testing.T, r tunnelRace) {
			// A pass that read TunnelConnected True has not run before it.
			r.notBefore(t, "the Instance's lastReconcileTime", r.rg.instance(r.instance).Status.LastReconcileTime)
		}},
		{"the ModelDeployment's last reconcile time", func(t *testing.T, r tunnelRace) {
			r.notBefore(t, "the ModelDeployment's lastReconcileTime", r.rg.md("md").Status.LastReconcileTime)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.check(t, raceToReady(t)) })
	}
}
