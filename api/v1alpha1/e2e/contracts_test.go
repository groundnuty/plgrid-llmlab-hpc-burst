package e2e

import (
	"strconv"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The fields the contracts PR adds for HBEP-4 and HBEP-7 (issues 59, 60, 61).

// A served deployment needs a workload: its shapes drive warm-up and
// qualification (HBEP-7, What the controller reads). A catalog entry has
// neither, so it needs none. This is the rule modeldeployments:$.spec#0.
func TestServedDeploymentNeedsAWorkload(t *testing.T) {
	const msg = "a served deployment needs a workload"
	managed := &v1alpha1.ManagedCatalogEntry{Platform: "example-managed", CatalogName: "example-model-27b"}
	runMDCases(t, []mdCase{
		{"a served deployment without a workload", func(md *v1alpha1.ModelDeployment) { md.Spec.Workload = nil }, msg},
		{"a workload without a typical context", func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.ContextTokens.Typical = 0 }, "contextTokens.typical"},
		{"a workload without a typical output", func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.OutputTokens.Typical = 0 }, "outputTokens.typical"},
		{"a workload without target sessions", func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = 0 }, "targetSessions"},
		{"a lease limit that is not a duration", func(md *v1alpha1.ModelDeployment) { md.Spec.LeaseLimit = ptr.To(v1alpha1.Duration("4 hours")) }, "leaseLimit"},
		{"an engine without an image", func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Engine.Image = "" }, "engine.image"},
		{"an engine image with a digest", func(md *v1alpha1.ModelDeployment) {
			md.Spec.Source.Served.Engine.Image = "docker.io/vllm/vllm-openai@" + digest
		}, "engine.image"},
	}, []mdCase{
		{"a served deployment with a workload", func(*v1alpha1.ModelDeployment) {}, ""},
		{"a catalog entry without a workload", func(md *v1alpha1.ModelDeployment) {
			md.Spec.Source = v1alpha1.ModelSource{Managed: managed}
			md.Spec.Workload = nil
		}, ""},
		{"a workload with every field and a lease limit", func(md *v1alpha1.ModelDeployment) {
			md.Spec.LeaseLimit = ptr.To(v1alpha1.Duration("4h"))
			md.Spec.Workload = &v1alpha1.Workload{
				ContextTokens:  v1alpha1.ContextTokens{Short: ptr.To(int32(256)), Typical: 8192, Longest: ptr.To(int32(30000))},
				OutputTokens:   v1alpha1.OutputTokens{Typical: 1024, Longest: ptr.To(int32(2048))},
				TargetSessions: 16, ToolCalls: ptr.To(true),
			}
		}, ""},
	})
}

// The workload's constant defaults are HBEP-7's: a short context of 512 tokens
// and a longest output of 4,096. The longest context and tool calls have no
// schema default: the controller resolves them.
func TestWorkloadDefaults(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "workload-defaults")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	w := got.Spec.Workload
	if w == nil || w.ContextTokens.Short == nil || *w.ContextTokens.Short != 512 ||
		w.OutputTokens.Longest == nil || *w.OutputTokens.Longest != 4096 {
		t.Fatalf("workload = %+v", w)
	}
	if w.ContextTokens.Longest != nil || w.ToolCalls != nil {
		t.Errorf("the longest context and tool calls were defaulted: %+v", w)
	}
}

// The profile's lifecycle fields take HBEP-4's defaults when absent (HBEP-4,
// Platform profile schema), and stagingDir is an absolute path or one that
// starts with a variable.
func TestPlatformProfileLifecycleFields(t *testing.T) {
	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"a relative staging dir", func(p *v1alpha1.PlatformProfile) { p.Spec.StagingDir = "scratch/hb" }},
		{"a staging dir under ~", func(p *v1alpha1.PlatformProfile) { p.Spec.StagingDir = "~/hb" }},
		{"a staging dir with a space", func(p *v1alpha1.PlatformProfile) { p.Spec.StagingDir = "/scratch/h b" }},
		{"a staging dir with a substitution", func(p *v1alpha1.PlatformProfile) { p.Spec.StagingDir = "/scratch/$(id)" }},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			requireRefused(t, c.Create(bg(), p), "stagingDir")
		})
	}
	for _, dir := range []string{"$SCRATCH", "$SCRATCH/hpc-burst", "/net/scratch/example-user"} {
		p := newProfile(uniq("prof"))
		p.Spec.StagingDir = dir
		must(t, c.Create(bg(), p), "create with stagingDir "+dir)
	}

	p := newProfile(uniq("prof"))
	p.Spec.Partitions[0].MaxTime = ptr.To(v1alpha1.Duration("48h"))
	p.Spec.Partitions[0].GPUsPerNode = ptr.To(int32(4))
	p.Spec.Partitions[0].CPUsPerGPU = ptr.To(int32(72))
	p.Spec.Partitions[0].MemPerGPU = ptr.To(resource.MustParse("120Gi"))
	p.Spec.MaxIdle = ptr.To(v1alpha1.Duration("30m"))
	must(t, c.Create(bg(), p), "create")
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	part := got.Spec.Partitions[0]
	if *part.MaxTime != "48h" || *part.GPUsPerNode != 4 || *part.CPUsPerGPU != 72 || part.MemPerGPU.String() != "120Gi" {
		t.Errorf("partition = %+v", part)
	}
	// HBEP-4's defaults, written out by hand.
	defaults := map[string]*v1alpha1.Duration{
		"drainWindow": got.Spec.DrainWindow, "handoverMargin": got.Spec.HandoverMargin,
		"health.probeInterval": got.Spec.Health.ProbeInterval, "health.noProgressTimeout": got.Spec.Health.NoProgressTimeout,
		"health.errorOnlyWindow": got.Spec.Health.ErrorOnlyWindow, "health.kvSkewTimeout": got.Spec.Health.KVSkewTimeout,
		"health.agentTimeout": got.Spec.Health.AgentTimeout, "health.registrationTimeout": got.Spec.Health.RegistrationTimeout,
		"health.startTimeout": got.Spec.Health.StartTimeout, "restart.stopGrace": got.Spec.Restart.StopGrace,
		"restart.window": got.Spec.Restart.Window, "restart.backoff": got.Spec.Restart.Backoff,
	}
	want := map[string]v1alpha1.Duration{
		"drainWindow": "30m", "handoverMargin": "30m", "health.probeInterval": "10s", "health.noProgressTimeout": "2m",
		"health.errorOnlyWindow": "1m", "health.kvSkewTimeout": "2m", "health.agentTimeout": "2m",
		"health.registrationTimeout": "15m", "health.startTimeout": "1h", "restart.stopGrace": "10s",
		"restart.window": "30m", "restart.backoff": "30s",
	}
	for k, w := range want {
		if defaults[k] == nil || *defaults[k] != w {
			t.Errorf("%s = %v, want %s", k, defaults[k], w)
		}
	}
	if got.Spec.Health.FailureThreshold == nil || *got.Spec.Health.FailureThreshold != 3 ||
		got.Spec.Restart.AttemptsPerRung == nil || *got.Spec.Restart.AttemptsPerRung != 2 {
		t.Errorf("health.failureThreshold = %v, restart.attemptsPerRung = %v; want 3 and 2",
			got.Spec.Health.FailureThreshold, got.Spec.Restart.AttemptsPerRung)
	}
	if got.Spec.MaxIdle == nil || *got.Spec.MaxIdle != "30m" {
		t.Errorf("maxIdle = %v", got.Spec.MaxIdle)
	}
	// maxIdle has no default: unset means no cap.
	plain := newProfile(uniq("prof"))
	must(t, c.Create(bg(), plain), "create")
	must(t, c.Get(bg(), objKey(plain), got), "get")
	if got.Spec.MaxIdle != nil {
		t.Errorf("maxIdle defaulted to %v", *got.Spec.MaxIdle)
	}
}

// A Provider keeps at most 20 queue waits, keyed by job ID (HBEP-4, Provider
// status).
func TestProviderQueueWaits(t *testing.T) {
	p := newProvider(uniq("prov"))
	must(t, c.Create(bg(), p), "create")
	got := &v1alpha1.Provider{}
	base := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	var waits []v1alpha1.QueueWait
	for i := range v1alpha1.MaxQueueWaits {
		waits = append(waits, v1alpha1.QueueWait{JobID: strconv.Itoa(1000 + i),
			SubmitTime: metav1.NewTime(base), StartTime: metav1.NewTime(base.Add(time.Duration(i) * time.Minute))})
	}
	must(t, c.Get(bg(), objKey(p), got), "get")
	got.Status.QueueWaits = waits
	must(t, c.Status().Update(bg(), got), "write 20 queue waits")
	must(t, c.Get(bg(), objKey(p), got), "get")
	if len(got.Status.QueueWaits) != 20 || got.Status.QueueWaits[19].Wait() != 19*time.Minute {
		t.Fatalf("queue waits = %+v", got.Status.QueueWaits)
	}

	got.Status.QueueWaits = append(got.Status.QueueWaits, v1alpha1.QueueWait{JobID: "2000",
		SubmitTime: metav1.NewTime(base), StartTime: metav1.NewTime(base)})
	requireRefused(t, c.Status().Update(bg(), got), "queueWaits")

	must(t, c.Get(bg(), objKey(p), got), "get")
	got.Status.QueueWaits[1].JobID = got.Status.QueueWaits[0].JobID
	requireRefused(t, c.Status().Update(bg(), got), "Duplicate value")
}

// An Allocation's phase is one of HBEP-4's, and a submission's state one of
// S4's. S4's own "Succeeded" is Ended here.
func TestAllocationPhasesAndSubmissionStates(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, "phases")
	must(t, c.Create(bg(), a), "create")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider.Phase = "Succeeded"
	requireRefused(t, c.Status().Update(bg(), got), "Unsupported value")

	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider.Submission = &v1alpha1.SubmissionRecord{Attempt: 1, State: "Pending"}
	requireRefused(t, c.Status().Update(bg(), got), "Unsupported value")

	for _, ph := range []v1alpha1.AllocationPhase{v1alpha1.AllocationSubmitting, v1alpha1.AllocationSubmissionUnknown,
		v1alpha1.AllocationHeld, v1alpha1.AllocationPending, v1alpha1.AllocationRunning, v1alpha1.AllocationEnded,
		v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused} {
		must(t, c.Get(bg(), objKey(a), got), "get")
		got.Status.AtProvider.Phase = ph
		got.Status.AtProvider.Submission = &v1alpha1.SubmissionRecord{Attempt: 1, State: v1alpha1.SubmissionDone}
		must(t, c.Status().Update(bg(), got), "phase "+string(ph))
	}

	// The observation fields round-trip, a requeue's reason included.
	t0 := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(5 * time.Minute))
	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider = v1alpha1.AllocationObservation{
		Tag: "hb-phases-1bda24d00504", JobID: "1001", Phase: v1alpha1.AllocationFailed, Reason: v1alpha1.ReasonRequeued,
		Requeued: true, RestartCount: 1, SubmitTime: &t0, StartTime: &t1, HardEnd: ptr.To(metav1.NewTime(t1.Add(time.Hour))),
		EndTime: &t1, AbsentSince: &t1,
	}
	got.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionSubmittedAsOther, Status: "True", Reason: "TimeLimit",
		LastTransitionTime: t1})
	must(t, c.Status().Update(bg(), got), "write the observation")
	again := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), again), "get")
	o := again.Status.AtProvider
	if o.Reason != "Requeued" || !o.Requeued || o.RestartCount != 1 || !o.SubmitTime.Equal(&t0) || !o.AbsentSince.Equal(&t1) {
		t.Errorf("observation = %+v", o)
	}
	if again.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status != "True" {
		t.Errorf("conditions = %+v", again.Status.Conditions)
	}
}

// The Instance status fields HBEP-7 and HBEP-11 add round-trip.
func TestInstanceContractStatus(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "contract-status")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	t0 := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	got.Status.Phase = v1alpha1.InstanceEnded
	got.Status.EndReason = v1alpha1.ReasonAllocationEndedEarly
	got.Status.StartInstructionTime = &t0
	got.Status.ReadyTime = ptr.To(metav1.NewTime(t0.Add(20 * time.Minute)))
	got.Status.Agent = &v1alpha1.AgentRegistration{BootID: strings.Repeat("b", 32), JobID: "1001", RegisteredTime: t0}
	got.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: "23ms", TimeToFirstToken: "410ms", TargetSessions: 8}
	got.Status.VerificationRecord = "ver-" + string(got.UID) + "-2"
	must(t, c.Status().Update(bg(), got), "write")
	again := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), again), "get")
	s := again.Status
	if s.EndReason != "AllocationEndedEarly" || !s.StartInstructionTime.Equal(&t0) || s.Agent == nil || s.Agent.JobID != "1001" ||
		s.Throughput == nil || s.Throughput.TimePerOutputToken != "23ms" || s.VerificationRecord != got.Status.VerificationRecord {
		t.Errorf("status = %+v", s)
	}
	again.Status.Throughput.TimeToFirstToken = "fast"
	requireRefused(t, c.Status().Update(bg(), again), "timeToFirstToken")
}

// A ModelDeployment keeps one start-to-ready time per accelerator class
// (HBEP-7, Handover).
func TestModelDeploymentStartToReady(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "start-to-ready")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	at := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	gh200 := v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}
	a100 := v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"}
	got.Status.StartToReady = []v1alpha1.ClassStartToReady{
		{AcceleratorClass: gh200, Duration: "23m10s", Instance: "inst-a", MeasuredTime: at},
		{AcceleratorClass: a100, Duration: "41m", MeasuredTime: at},
	}
	must(t, c.Status().Update(bg(), got), "write two classes")
	must(t, c.Get(bg(), objKey(md), got), "get")
	if len(got.Status.StartToReady) != 2 || got.Status.StartToReady[0].Duration != "23m10s" {
		t.Fatalf("startToReady = %+v", got.Status.StartToReady)
	}
	got.Status.StartToReady = append(got.Status.StartToReady, v1alpha1.ClassStartToReady{AcceleratorClass: gh200, Duration: "1m", MeasuredTime: at})
	requireRefused(t, c.Status().Update(bg(), got), "Duplicate value")
}

// The Allocation's batch fields round-trip with the names HBEP-4 gives them.
func TestAllocationBatchFieldsRoundTrip(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, "batch-fields")
	must(t, c.Create(bg(), a), "create")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	b := got.Spec.ForProvider.Batch
	if b.TimeLimit != "1h" || b.GPUs != 1 || b.CPUsPerTask != 16 || b.Memory.String() != "120Gi" || b.Nodes != 1 || b.AgentDigest != agentDigest {
		t.Errorf("batch = %+v; nodes defaults to 1", b)
	}
	// A CPU partition's job asks for no GPU; the provider refuses that only on a GPU partition.
	cpu := newAllocation(ns, "cpu-job")
	cpu.Spec.ForProvider.Batch.Partition, cpu.Spec.ForProvider.Batch.GPUs = "example-cpu", 0
	must(t, c.Create(bg(), cpu), "create a CPU job")
}
