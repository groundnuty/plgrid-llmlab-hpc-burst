package lifecycle

// The shape-misfit stop (science-agent ruling, 31 Oct 2026, on the PR 88
// review): keyed on the deployment ID plus the workload's shapes, so a
// workload edit that makes the shapes fit lifts it; an API read error is not
// IdentityUnresolved (Opus nit).

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// shapeRig is a deployment that declares no max-model-len, on an engine that
// reports 4096 with a 12-token template overhead: the default workload
// (typical context 4096 + typical output 512) does not fit, so verification
// fails with a shape misfit.
func shapeRig(t *testing.T) *rig {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{Verify: afake.VerifyMaxModelLen("4096")} }
	md := mdSpec(hoMD)
	md.Spec.Source.Served.Flags = nil
	rg.create(md)
	rg.until(5*time.Minute, 10*time.Second, "the shape misfit", func() bool {
		c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
		return c.Reason == v1alpha1.ReasonVerificationFailed
	})
	rg.run(time.Minute, 10*time.Second)
	return rg
}

func (rg *rig) shapeEvents(code string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range rg.rec.Events() {
		if e.Code == code {
			out = append(out, e)
		}
	}
	return out
}

func TestShapeStopStopsTheClass(t *testing.T) {
	rg := shapeRig(t)
	before := rg.submissions()
	rg.run(10*time.Minute, time.Minute)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("Degraded %s, want VerificationFailed while the workload does not fit", c.Reason)
	}
	if rg.submissions() != before || len(rg.liveInstances(hoMD)) != 0 {
		t.Fatalf("submissions %d -> %d, %d live Instances: the stopped class provisioned", before, rg.submissions(), len(rg.liveInstances(hoMD)))
	}
}

func TestShapeStopLiftsWhenTheWorkloadFits(t *testing.T) {
	rg := shapeRig(t)
	// 1024 + 256, 512 + 256, 1024 + 512 and the default longest (4096 - 256 -
	// 12 = 3828) + 256 all fit 4096 with the overhead: the spec (and the ID)
	// is unchanged, only the workload.
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
		m.Spec.Workload.ContextTokens.Typical = 1024
		m.Spec.Workload.OutputTokens.Typical = 256
		m.Spec.Workload.OutputTokens.Longest = ptrTo(int32(512))
	})
	idBefore := rg.md(hoMD).Status.CurrentIDs[0].ID
	rg.run(10*time.Second, 10*time.Second)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonVerificationFailed {
		t.Fatalf("Degraded still VerificationFailed after the workload fits: %s", c.Message)
	}
	if n := len(rg.shapeEvents("ShapesFit")); n != 1 {
		t.Fatalf("%d ShapesFit events, want one", n)
	}
	rg.until(5*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount(hoMD) == 1 })
	if id := rg.md(hoMD).Status.CurrentIDs[0].ID; id != idBefore {
		t.Fatalf("the ID changed (%s -> %s); the lift must come from the workload alone", idBefore, id)
	}
	rg.run(5*time.Minute, time.Minute)
	if n := len(rg.shapeEvents("ShapesFit")); n != 1 {
		t.Fatalf("%d ShapesFit events after more passes, want still one", n)
	}
}

func TestShapeStopStaysForAnotherMisfit(t *testing.T) {
	rg := shapeRig(t)
	misfitsBefore := len(rg.shapeEvents(v1alpha1.ReasonVerificationFailed))
	before := rg.submissions()
	// Another misfit: a typical context of 3900 with the typical output 512.
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.ContextTokens.Typical = 3900 })
	rg.run(time.Minute, 10*time.Second)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonVerificationFailed {
		t.Fatalf("Degraded %s, want VerificationFailed for the new misfit", c.Reason)
	}
	evs := rg.shapeEvents(v1alpha1.ReasonVerificationFailed)
	if len(evs) != misfitsBefore+1 || !strings.Contains(evs[len(evs)-1].Message, "3900") {
		t.Fatalf("misfit events %d -> %d (last %q), want one more naming the new shape (3900)", misfitsBefore, len(evs), evs[len(evs)-1].Message)
	}
	if rg.submissions() != before {
		t.Fatalf("submissions %d -> %d: the class must stay stopped", before, rg.submissions())
	}
}

// TestIdentityAPIErrorIsNotIdentityUnresolved (Opus nit): a failed read of an
// identity record is returned and retried, and never shown as
// IdentityUnresolved.
func TestIdentityAPIErrorIsNotIdentityUnresolved(t *testing.T) {
	failGets := 1
	rg := newRig(t, withInterceptors(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*appsv1.ControllerRevision); ok && strings.HasPrefix(key.Name, v1alpha1.IdentityRecordPrefix) && failGets > 0 {
			failGets--
			return errInjected
		}
		return c.Get(ctx, key, obj, opts...)
	}}))
	rg.create(mdSpec(hoMD))
	errs := fixStep(rg, 10*time.Second)
	if failGets != 0 || len(errs) == 0 {
		t.Fatalf("the injected read failure was not hit (left %d) or not returned (%v)", failGets, errs)
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonIdentityUnresolved {
		t.Fatalf("an API read error set Degraded IdentityUnresolved: %s", c.Message)
	}
	rg.until(5*time.Minute, 10*time.Second, "ready after the retry", func() bool { return rg.readyCount(hoMD) == 1 })
}
