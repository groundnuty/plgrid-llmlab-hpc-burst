package lifecycle

// HBEP-7 as merged with #74: the idle anchor survives the deletion of the
// ended Instance, and a controller restart while agents serve changes nothing.

import (
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestRetentionMaxIdleCountsFromTheEndOfADeletedInstance covers "An Allocation
// whose ended Instance is deleted, while the Allocation is kept, is still
// released at maxIdle counted from that Instance's end": the Instance ends at
// scale-down (its status.endTime), the controller writes that time into the
// Allocation's idle-since annotation, an operator deletes the ended Instance,
// and the Allocation is released 30 min (maxIdle) after the end, not 30 min
// after the deletion.
func TestRetentionMaxIdleCountsFromTheEndOfADeletedInstance(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
	a, emptyAt := retentionEmpty(t, rg, nil)
	var ended *v1alpha1.Instance
	for _, in := range rg.instances("md") {
		if in.Status.Phase == v1alpha1.InstanceEnded {
			ended = &in
		}
	}
	if ended == nil || ended.Status.EndTime == nil {
		t.Fatal("no ended Instance with an end time")
	}
	end := ended.Status.EndTime.Time
	got, _ := rg.allocation(a.Name)
	if since, ok, err := v1alpha1.IdleSince(got); err != nil || !ok || !since.Equal(end) {
		t.Fatalf("idle-since %s (set %v, %v), want the Instance's end %s", since, ok, err, end)
	}
	rg.run(10*time.Minute, time.Minute)
	if err := rg.c.Delete(rg.ctx, ended); err != nil {
		t.Fatal(err)
	}
	rg.run(time.Minute, 10*time.Second)
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(ended), &v1alpha1.Instance{}); err == nil {
		t.Fatal("the deleted ended Instance is still there")
	}
	if !emptyAt.Equal(end) && emptyAt.Sub(end) > 10*time.Second {
		t.Fatalf("the Instance ended at %s, the rig saw the Allocation empty at %s", end, emptyAt)
	}
	retentionReleaseAt(t, rg, a.Name, end.Add(30*time.Minute), 10*time.Second)
}

// TestControllerRestartWhileServingChangesNothing covers "The controller
// restarts while agents serve: each agent registers again on its next poll
// with the same agentInstanceID. No Instance fails, no restart generation
// rises, and no registration timeout or AgentUnreachable is judged before a
// long poll and a report interval have passed."
func TestControllerRestartWhileServingChangesNothing(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	before := rg.instances("md")[0]
	agentID := before.Status.Agent.AgentInstanceID
	rg.restartController()
	rg.run(5*time.Minute, 10*time.Second)
	after := rg.instance(before.Name)
	if after.Status.Phase != v1alpha1.InstanceReady || after.Status.Agent == nil || after.Status.Agent.AgentInstanceID != agentID {
		t.Fatalf("after the restart the Instance is %s with agent %+v, want ready under agent %s", after.Status.Phase, after.Status.Agent, agentID)
	}
	if g := restartGeneration(after, "worker-0") + restartGeneration(after, v1alpha1.PartInstance); g != 0 || len(after.Status.Restarts) != 0 {
		t.Fatalf("restart generations rose to %d (restarts %v) across a controller restart", g, after.Status.Restarts)
	}
	if c := startStatus(after.Status.Conditions, v1alpha1.ConditionAgentUnreachable); c != "False" {
		t.Fatalf("AgentUnreachable = %q after the restart, want False", c)
	}
	if n := len(rg.rec.Find(eventFailing, "")); n != 0 || len(rg.instances("md")) != 1 || rg.submissions() != 1 {
		t.Fatalf("%d failing events, %d Instances, %d submissions; want 0, 1, 1", n, len(rg.instances("md")), rg.submissions())
	}
	for _, e := range rg.errs {
		t.Logf("agent error (expected only CodeNotRegistered retries): %v", e)
	}
}
