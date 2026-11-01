package e2e

import (
	"strconv"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// The workload's typical context is at most its longest, when both are set: the
// behaviour check runs at the longest context and never below the typical one,
// so bounds that contradict each other are refused (`@hpc-burst-science-agent`,
// 31 Oct 2026). This is the rule modeldeployments:$.spec.workload.contextTokens#0.
func TestContextTokensTypicalIsAtMostLongest(t *testing.T) {
	const msg = "contextTokens.typical must not exceed contextTokens.longest"
	ctx := func(typical int32, longest *int32) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) {
			md.Spec.Workload.ContextTokens.Typical = typical
			md.Spec.Workload.ContextTokens.Longest = longest
		}
	}
	runMDCases(t, []mdCase{
		{"typical 8192 above longest 4096", ctx(8192, ptr.To(int32(4096))), msg},
		{"typical one above longest", ctx(8193, ptr.To(int32(8192))), msg},
	}, []mdCase{
		{"typical equal to longest", ctx(8192, ptr.To(int32(8192))), ""},
		{"typical one below longest", ctx(8191, ptr.To(int32(8192))), ""},
		{"typical below longest", ctx(4096, ptr.To(int32(30000))), ""},
		{"a typical context with no longest, which the controller resolves", ctx(8192, nil), ""},
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
		"drainWindow": got.Spec.DrainWindow, "drainMargin": got.Spec.DrainMargin, "handoverMargin": got.Spec.HandoverMargin,
		"health.probeInterval": got.Spec.Health.ProbeInterval, "health.noProgressTimeout": got.Spec.Health.NoProgressTimeout,
		"health.errorOnlyWindow": got.Spec.Health.ErrorOnlyWindow, "health.kvSkewTimeout": got.Spec.Health.KVSkewTimeout,
		"health.agentTimeout": got.Spec.Health.AgentTimeout, "health.registrationTimeout": got.Spec.Health.RegistrationTimeout,
		"health.startTimeout": got.Spec.Health.StartTimeout, "restart.stopGrace": got.Spec.Restart.StopGrace,
		"restart.window": got.Spec.Restart.Window, "restart.backoff": got.Spec.Restart.Backoff,
	}
	want := map[string]v1alpha1.Duration{
		"drainWindow": "2h10m", "drainMargin": "10m", "handoverMargin": "30m", "health.probeInterval": "10s", "health.noProgressTimeout": "30m",
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

	// A Rejected attempt's code is SubmitLimitReached or absent (HBEP-4, QOS
	// limits), and it round-trips in the mirror.
	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider.Submission = &v1alpha1.SubmissionRecord{Attempt: 1, State: v1alpha1.SubmissionRejected, Code: "SubmissionRejected"}
	requireRefused(t, c.Status().Update(bg(), got), "Unsupported value")
	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider.Submission = &v1alpha1.SubmissionRecord{Attempt: 1, State: v1alpha1.SubmissionRejected,
		Code: v1alpha1.ReasonSubmitLimitReached, Reason: "sbatch: error: QOSMaxSubmitJobPerUserLimit"}
	must(t, c.Status().Update(bg(), got), "a Rejected attempt with its code")
	must(t, c.Get(bg(), objKey(a), got), "get")
	if s := got.Status.AtProvider.Submission; s == nil || s.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Errorf("submission mirror read back as %+v, want code SubmitLimitReached", s)
	}

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
	got.Status.Agent = &v1alpha1.AgentRegistration{AgentInstanceID: strings.Repeat("b", 32), JobID: "1001", RegisteredTime: t0}
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

// The target sessions are at most 1024 (agentapi.MaxTargetSessions), so the
// agent's warm-up report fits the protocol's body limit. This is a schema
// bound, not a CEL rule.
func TestWorkloadTargetSessionsHaveAnUpperBound(t *testing.T) {
	sessions := func(n int32) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = n }
	}
	if v1alpha1.MaxTargetSessions != 1024 {
		t.Fatalf("MaxTargetSessions = %d, want 1024", v1alpha1.MaxTargetSessions)
	}
	runMDCases(t, []mdCase{
		{"1025 target sessions", sessions(1025), "targetSessions: Invalid value: 1025: spec.workload.targetSessions in body should be less than or equal to 1024"},
		{"100000 target sessions, the former bound", sessions(100000), "should be less than or equal to 1024"},
	}, []mdCase{
		{"1024 target sessions", sessions(1024), ""},
		{"1 target session", sessions(1), ""},
	})
}

// The Instance's end time and its recent restarts are status fields the
// lifecycle controller writes (HBEP-7, Health and restarts). They round-trip
// through the status subresource under the keys the types name.
func TestInstanceEndTimeAndRestartsRoundTrip(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "end-and-restarts")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	t0 := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	got.Status.Phase = v1alpha1.InstanceEnded
	got.Status.EndReason = v1alpha1.ReasonAllocationEndedEarly
	got.Status.EndTime = ptr.To(metav1.NewTime(t0.Add(90 * time.Minute)))
	got.Status.Restarts = []v1alpha1.InstanceRestart{
		{Part: v1alpha1.PartInstance, Generation: 1, Time: t0, Signal: v1alpha1.ReasonProbeFailing},
		{Part: v1alpha1.WorkerName(0), Generation: 2, Time: metav1.NewTime(t0.Add(time.Minute)), Signal: v1alpha1.ReasonNoProgress},
		{Part: v1alpha1.WorkerName(12), Generation: 1, Time: metav1.NewTime(t0.Add(2 * time.Minute)), Signal: v1alpha1.ReasonPartExited},
	}
	must(t, c.Status().Update(bg(), got), "write the end time and the restarts")
	again := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), again), "get")
	s := again.Status
	if s.EndTime == nil || !s.EndTime.Equal(got.Status.EndTime) {
		t.Errorf("endTime = %v, want %v", s.EndTime, got.Status.EndTime)
	}
	if len(s.Restarts) != 3 {
		t.Fatalf("restarts = %+v, want the three written, oldest first", s.Restarts)
	}
	for i, want := range got.Status.Restarts {
		r := s.Restarts[i]
		if r.Part != want.Part || r.Generation != want.Generation || !r.Time.Equal(&want.Time) || r.Signal != want.Signal {
			t.Errorf("restarts[%d] = %+v, want %+v", i, r, want)
		}
	}

	// On the wire the keys are endTime and restarts, each entry with part,
	// generation, time and signal.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Instance"))
	must(t, c.Get(bg(), objKey(in), u), "get unstructured")
	status, _, _ := unstructured.NestedMap(u.Object, "status")
	if _, ok := status["endTime"]; !ok {
		t.Errorf("status.endTime is not stored: %v", status)
	}
	restarts, _, _ := unstructured.NestedSlice(u.Object, "status", "restarts")
	if len(restarts) != 3 {
		t.Fatalf("status.restarts = %v", restarts)
	}
	first, _ := restarts[0].(map[string]any)
	for _, key := range []string{"part", "generation", "time", "signal"} {
		if _, ok := first[key]; !ok {
			t.Errorf("status.restarts[0] has no %q: %v", key, first)
		}
	}

	// Sixteen entries are the most kept.
	sixteen := make([]v1alpha1.InstanceRestart, 16)
	for i := range sixteen {
		sixteen[i] = v1alpha1.InstanceRestart{Part: v1alpha1.PartInstance, Generation: int64(i + 1), Time: t0, Signal: v1alpha1.ReasonProbeFailing}
	}
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Restarts = sixteen
	must(t, c.Status().Update(bg(), got), "write 16 restarts")
}

// Each bound of an Instance's restart entries refuses what it should, and only
// that: every case changes one thing in a valid entry.
func TestInstanceRestartEntriesAreBounded(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "restart-bounds")
	must(t, c.Create(bg(), in), "create")
	t0 := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	valid := func() v1alpha1.InstanceRestart {
		return v1alpha1.InstanceRestart{Part: v1alpha1.PartInstance, Generation: 1, Time: t0, Signal: v1alpha1.ReasonProbeFailing}
	}
	entry := func(edit func(*v1alpha1.InstanceRestart)) []v1alpha1.InstanceRestart {
		r := valid()
		edit(&r)
		return []v1alpha1.InstanceRestart{r}
	}
	seventeen := make([]v1alpha1.InstanceRestart, 17)
	for i := range seventeen {
		seventeen[i] = valid()
	}
	for _, tc := range []struct {
		name     string
		restarts []v1alpha1.InstanceRestart
		want     string
	}{
		{"17 entries", seventeen, "must have at most 16 items"},
		{"the part router", entry(func(r *v1alpha1.InstanceRestart) { r.Part = "router" }), "restarts[0].part"},
		{"the part worker-x", entry(func(r *v1alpha1.InstanceRestart) { r.Part = "worker-x" }), "restarts[0].part"},
		{"the part worker- with no number", entry(func(r *v1alpha1.InstanceRestart) { r.Part = "worker-" }), "restarts[0].part"},
		{"no part", entry(func(r *v1alpha1.InstanceRestart) { r.Part = "" }), "restarts[0].part"},
		{"generation 0", entry(func(r *v1alpha1.InstanceRestart) { r.Generation = 0 }), "should be greater than or equal to 1"},
		{"a negative generation", entry(func(r *v1alpha1.InstanceRestart) { r.Generation = -1 }), "should be greater than or equal to 1"},
		{"a signal with a space", entry(func(r *v1alpha1.InstanceRestart) { r.Signal = "probe failing" }), "restarts[0].signal"},
		{"a signal in lower case", entry(func(r *v1alpha1.InstanceRestart) { r.Signal = "probeFailing" }), "restarts[0].signal"},
		{"no signal", entry(func(r *v1alpha1.InstanceRestart) { r.Signal = "" }), "restarts[0].signal"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			got := &v1alpha1.Instance{}
			must(t, c.Get(bg(), objKey(in), got), "get")
			got.Status.Restarts = tc.restarts
			requireRefused(t, c.Status().Update(bg(), got), tc.want)
		})
	}
}

// The agent's instance ID is 32 lowercase hex characters, as the agent draws it
// (agentapi.CheckAgentInstanceID).
func TestAgentInstanceIDIsThirtyTwoLowercaseHex(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "agent-id")
	must(t, c.Create(bg(), in), "create")
	t0 := metav1.NewTime(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	write := func(id string) error {
		got := &v1alpha1.Instance{}
		must(t, c.Get(bg(), objKey(in), got), "get")
		got.Status.Agent = &v1alpha1.AgentRegistration{AgentInstanceID: id, JobID: "1001", RegisteredTime: t0}
		return c.Status().Update(bg(), got)
	}
	for _, tc := range []struct{ name, id string }{
		{"31 hex characters", strings.Repeat("a", 31)},
		{"33 hex characters", strings.Repeat("a", 33)},
		{"uppercase hex", strings.Repeat("A", 32)},
		{"mixed case hex", strings.Repeat("aB", 16)},
		{"32 characters that are not hex", strings.Repeat("g", 32)},
		{"a UUID with dashes", "0123456789abcdef-0123456789abcdef"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, write(tc.id), "agentInstanceID")
		})
	}
	t.Run("accept 32 lowercase hex characters", func(t *testing.T) {
		id := "0123456789abcdef0123456789abcdef"
		must(t, write(id), "write")
		got := &v1alpha1.Instance{}
		must(t, c.Get(bg(), objKey(in), got), "get")
		if got.Status.Agent == nil || got.Status.Agent.AgentInstanceID != id {
			t.Errorf("status.agent = %+v, want agentInstanceID %s", got.Status.Agent, id)
		}
	})
}

// The Allocation records that its queue wait is in the Provider's status, so a
// failed Provider write is retried and a wait is never added twice (HBEP-4,
// Provider status). It round-trips through the status subresource; false is
// absent.
func TestAllocationQueueWaitRecordedRoundTrips(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	must(t, c.Create(bg(), a), "create")
	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	if got.Status.AtProvider.QueueWaitRecorded {
		t.Fatalf("a new Allocation has queueWaitRecorded set")
	}
	got.Status.AtProvider.JobID = "1001"
	got.Status.AtProvider.QueueWaitRecorded = true
	must(t, c.Status().Update(bg(), got), "write queueWaitRecorded")

	again := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), again), "get")
	if !again.Status.AtProvider.QueueWaitRecorded {
		t.Errorf("queueWaitRecorded = false after a write of true: %+v", again.Status.AtProvider)
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Allocation"))
	must(t, c.Get(bg(), objKey(a), u), "get unstructured")
	obs, _, _ := unstructured.NestedMap(u.Object, "status", "atProvider")
	if obs["queueWaitRecorded"] != true {
		t.Errorf("status.atProvider.queueWaitRecorded = %v; keys %v", obs["queueWaitRecorded"], obs)
	}

	// The main resource ignores it.
	again.Status.AtProvider.QueueWaitRecorded = false
	must(t, c.Update(bg(), again), "update with a changed status")
	must(t, c.Get(bg(), objKey(a), got), "get")
	if !got.Status.AtProvider.QueueWaitRecorded {
		t.Errorf("the main resource wrote queueWaitRecorded")
	}
	// A status write that leaves it out clears it: false is absent.
	got.Status.AtProvider.QueueWaitRecorded = false
	must(t, c.Status().Update(bg(), got), "write false")
	must(t, c.Get(bg(), objKey(a), again), "get")
	if again.Status.AtProvider.QueueWaitRecorded {
		t.Errorf("queueWaitRecorded stayed true after a write of false")
	}
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
