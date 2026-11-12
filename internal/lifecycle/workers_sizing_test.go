package lifecycle

// Several workers per Instance, the sizing (HBEP-7, Several workers, Sizing and The count): the
// Allocation's GPUs are workersPerInstance times tensor-parallel-size times pipeline-parallel-size, on
// one node, refused, never capped, above gpusPerNode or maxGpusPerUser; each worker's launch keeps its
// own TP x PP GPUs; launch.workers copies the count at creation, set only above 1; and the count is no
// deployment-ID input. The rig's partition has 4 GPUs per node, 16 CPUs and 120Gi per GPU (planProvider
// and the rig's profile); every expected value is worked out by hand.

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// withWorkers sets a deployment's workersPerInstance.
func withWorkers(md *v1alpha1.ModelDeployment, n int32) *v1alpha1.ModelDeployment {
	md.Spec.WorkersPerInstance = &n
	return md
}

// Two workers of TP 2: the Allocation gets 2 x 2 = 4 GPUs, 64 CPUs and 480Gi, the node's whole; the
// Instance's launch keeps each worker's 2 GPUs and records 2 workers.
func TestWorkersSizeTheAllocation(t *testing.T) {
	rg := newRig(t)
	rg.create(withWorkers(provisionFlags(mdSpec("md"), "tensor-parallel-size", "2"), 2))
	rg.until(2*time.Minute, 10*time.Second, "one Allocation and its Instance", func() bool {
		return len(rg.allocations("md")) == 1 && len(rg.instances("md")) == 1
	})
	b := rg.allocations("md")[0].Spec.ForProvider.Batch
	if b == nil || b.GPUs != 4 || b.CPUsPerTask != 64 || b.Memory.Cmp(resource.MustParse("480Gi")) != 0 || b.Nodes != 1 {
		t.Fatalf("the Allocation's batch parameters %+v, want 4 GPUs, 64 CPUs, 480Gi on one node", b)
	}
	l := rg.instances("md")[0].Spec.Launch
	if l.GPUs != 2 {
		t.Errorf("launch.gpus = %d, want each worker's 2", l.GPUs)
	}
	if l.Workers == nil || *l.Workers != 2 {
		t.Errorf("launch.workers = %v, want 2", l.Workers)
	}
}

// One worker, absent or written as 1, is alpha's: launch.workers stays absent, the launch is the same
// byte for byte, and the deployment ID is the same, so the count is no ID input. Two workers of the same
// serving spec have the same ID too.
func TestOneWorkerKeepsAlphasLaunchAndTheCountIsNoIDInput(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("absent"))
	rg.create(withWorkers(mdSpec("one"), 1))
	rg.create(withWorkers(mdSpec("two"), 2))
	rg.until(2*time.Minute, 10*time.Second, "an Instance of each deployment", func() bool {
		return len(rg.instances("absent")) == 1 && len(rg.instances("one")) == 1 && len(rg.instances("two")) == 1
	})
	absent, one, two := rg.instances("absent")[0], rg.instances("one")[0], rg.instances("two")[0]
	if absent.Spec.Launch.Workers != nil || one.Spec.Launch.Workers != nil {
		t.Errorf("launch.workers = %v and %v for one worker, want absent", absent.Spec.Launch.Workers, one.Spec.Launch.Workers)
	}
	if got, want := one.Spec.Launch, absent.Spec.Launch; !equalLaunch(got, want) {
		t.Errorf("workersPerInstance 1 changed the launch:\n got %+v\nwant %+v", got, want)
	}
	if absent.Spec.DeploymentID != one.Spec.DeploymentID || absent.Spec.DeploymentID != two.Spec.DeploymentID {
		t.Errorf("deployment IDs %s, %s and %s differ; the worker count is no ID input", absent.Spec.DeploymentID, one.Spec.DeploymentID, two.Spec.DeploymentID)
	}
	if b := rg.allocations("absent")[0].Spec.ForProvider.Batch; b == nil || b.GPUs != 1 {
		t.Errorf("one worker's Allocation: %+v, want 1 GPU", b)
	}
}

// equalLaunch compares two launches through their JSON, which is what the API stores.
func equalLaunch(a, b v1alpha1.Launch) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(ja) == string(jb)
}

// A count whose GPUs exceed the node is refused, not capped: 3 workers of TP 2 need 6 GPUs on a
// 4-GPU node, so the deployment is Degraded with SpecUnsupported, names the count, and gets no
// Allocation. 2 workers of TP 2, exactly the node, is the control.
func TestWorkersAboveTheNodeAreRefusedNotCapped(t *testing.T) {
	rg := newRig(t)
	rg.create(withWorkers(provisionFlags(mdSpec("md"), "tensor-parallel-size", "2"), 3))
	provisionSettle(rg)
	c := provisionDegraded(rg, "md")
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported || !strings.Contains(c.Message, "workersPerInstance 3 times 2") || !strings.Contains(c.Message, "6 GPUs") {
		t.Fatalf("Degraded = %s/%s (%s), want True/SpecUnsupported naming 6 GPUs, workersPerInstance 3 times 2", c.Status, c.Reason, c.Message)
	}
	if n := len(rg.allocations("md")) + len(rg.instances("md")) + rg.submissions(); n != 0 {
		t.Errorf("%d Allocations, Instances and submissions, want none", n)
	}

	ctl := newRig(t)
	ctl.create(withWorkers(provisionFlags(mdSpec("md"), "tensor-parallel-size", "2"), 2))
	provisionSettle(ctl)
	if n := ctl.submissions(); n != 1 {
		t.Fatalf("the control, 4 GPUs on a 4-GPU node: %d submissions, want 1", n)
	}
}

// partitionSizingFor's refusals: above the node and above a QOS maxGpusPerUser, each naming the
// workers; with one worker it is partitionSizing, its refusal word for word.
func TestPartitionSizingForWorkers(t *testing.T) {
	var u *unsupported
	if _, err := partitionSizingFor(planProvider(), 2, 3, nil); !errors.As(err, &u) || !strings.Contains(err.Error(), "the deployment needs 6 GPUs (workersPerInstance 3 times 2") || !strings.Contains(err.Error(), "above the 4 per node") {
		t.Errorf("3 workers of 2 GPUs on a 4-GPU node: %v, want a refusal naming the workers and the node", err)
	}
	q := planProvider()
	q.limits.MaxGPUsPerUser = ptrTo(int32(2))
	q.limits.From = map[string]string{v1alpha1.LimitMaxGPUsPerUser: "normal"}
	if _, err := partitionSizingFor(q, 1, 3, nil); !errors.As(err, &u) || !strings.Contains(err.Error(), "the deployment needs 3 GPUs (workersPerInstance 3 times 1") || !strings.Contains(err.Error(), "maxGpusPerUser 2") {
		t.Errorf("3 workers of 1 GPU under a 2-GPU QOS: %v, want a refusal naming the workers and the QOS", err)
	}
	if got, err := partitionSizingFor(q, 1, 2, nil); err != nil || got.GPUs != 2 {
		t.Errorf("2 workers of 1 GPU under a 2-GPU QOS: %+v, %v; want 2 GPUs", got, err)
	}
	_, one := partitionSizingFor(planProvider(), 5, 1, nil)
	_, alpha := partitionSizing(planProvider(), 5, nil)
	if one == nil || alpha == nil || one.Error() != alpha.Error() {
		t.Errorf("one worker's refusal %q differs from partitionSizing's %q", one, alpha)
	}
}

// A count change applies only to Instances created after it (HBEP-7, Several workers, The count): an
// Instance created at 2 workers keeps 2 when the deployment moves to 3, and the next Instance, from a
// scale-up, gets 3.
func TestACountChangeAppliesToNewInstancesOnly(t *testing.T) {
	rg := newRig(t)
	rg.create(withWorkers(mdSpec("md"), 2))
	rg.until(2*time.Minute, 10*time.Second, "the first Instance", func() bool { return len(rg.instances("md")) == 1 })
	first := rg.instances("md")[0]
	rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) {
		md.Spec.WorkersPerInstance = ptrTo(int32(3))
		md.Spec.Instances = 2
	})
	rg.until(2*time.Minute, 10*time.Second, "the second Instance", func() bool { return len(rg.instances("md")) == 2 })
	for _, in := range rg.instances("md") {
		want := int32(3)
		if in.UID == first.UID {
			want = 2
		}
		if in.Spec.Launch.Workers == nil || *in.Spec.Launch.Workers != want {
			t.Errorf("Instance %s has launch.workers %v, want %d", in.Name, in.Spec.Launch.Workers, want)
		}
	}
}

// A Provider whose profile makes (workers + 2) x attemptsPerRung exceed the 32 restarts status.restarts
// keeps is refused for the deployment (HBEP-7, Several workers): 3 workers at 8 attempts is 40, refused;
// 2 workers at 8 is 32, which fits; and with one worker it never refuses, at the 8 attempts maximum
// ((1 + 2) x 8 = 24).
func TestWorkersTimesTheRungsMustFitTheRestartsKept(t *testing.T) {
	pv := planProvider()
	pv.settings.attemptsPerRung = 8
	var u *unsupported
	if _, err := partitionSizingFor(pv, 1, 3, nil); !errors.As(err, &u) || !strings.Contains(err.Error(), "is 40 restarts, above the 32") {
		t.Errorf("3 workers at 8 attempts: %v, want a refusal naming 40 above 32", err)
	}
	if got, err := partitionSizingFor(pv, 1, 2, nil); err != nil || got.GPUs != 2 {
		t.Errorf("2 workers at 8 attempts, exactly 32: %+v, %v; want sized", got, err)
	}
	if got, err := partitionSizingFor(pv, 1, 1, nil); err != nil || got.GPUs != 1 {
		t.Errorf("one worker at 8 attempts: %+v, %v; want sized", got, err)
	}
	pv.settings.attemptsPerRung = 6
	if got, err := partitionSizingFor(pv, 1, 3, nil); err != nil || got.GPUs != 3 {
		t.Errorf("3 workers at 6 attempts, 30: %+v, %v; want sized", got, err)
	}
}
