package lifecycle

// Scenario tests for provisioning and the spec checks (HBEP-7, Test plan:
// Provisioning, Spec checks; Provisioning and What the controller reads in
// Design details; Launch; and HBEP-11's FrontNotConfigured).

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// provisionFlags adds engine flags to a deployment's serving spec.
func provisionFlags(md *v1alpha1.ModelDeployment, kv ...string) *v1alpha1.ModelDeployment {
	for i := 0; i+1 < len(kv); i += 2 {
		md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: kv[i], Value: kv[i+1]})
	}
	return md
}

// provisionSettle runs the rig long enough for the Provider to be Ready and for
// a pass that provisions (the Provider is Ready after the first Step).
func provisionSettle(rg *rig) { rg.run(2*time.Minute, 10*time.Second) }

// provisionDegraded returns the Degraded condition of a deployment.
func provisionDegraded(rg *rig, md string) metav1.Condition {
	return rg.condition(md, v1alpha1.ConditionDegraded)
}

// provisionHas reports whether the command carries a flag followed by its value.
func provisionHas(cmd []string, flag, value string) bool {
	for i := 0; i+1 < len(cmd); i++ {
		if cmd[i] == flag && cmd[i+1] == value {
			return true
		}
	}
	return false
}

// TestProvisionSubmissionFollowsSpecAndProfile covers HBEP-7's Test plan item
// "Provisioning": the Allocation's forProvider.batch follows the serving spec
// and the profile. The profile gives 16 CPUs and 120Gi per GPU, 4 GPUs per node.
// Hand calculation per case: gpus = TP x PP; cpusPerTask = gpus x 16; memory =
// gpus x 120Gi; timeLimit = min(maxTime, leaseLimit) in whole minutes.
func TestProvisionSubmissionFollowsSpecAndProfile(t *testing.T) {
	cases := []struct {
		name      string
		tp, pp    string
		maxTime   time.Duration
		lease     time.Duration // 0: the deployment sets no leaseLimit
		gpus, cpu int32
		mem       string
		limit     v1alpha1.Duration
	}{
		// 2 x 2 = 4 GPUs, the node's whole size: 64 CPUs, 480Gi; 4h30m < 48h.
		{"tp2-pp2-lease-shorter", "2", "2", 48 * time.Hour, 4*time.Hour + 30*time.Minute, 4, 64, "480Gi", "4h30m0s"},
		// 2 x 1 = 2 GPUs: 32 CPUs, 240Gi; maxTime 4h < leaseLimit 10h, so maxTime.
		{"tp2-maxtime-shorter", "2", "1", 4 * time.Hour, 10 * time.Hour, 2, 32, "240Gi", "4h0m0s"},
		// 1 x 1 = 1 GPU: 16 CPUs, 120Gi; no leaseLimit, so maxTime 48h.
		{"single-gpu-no-lease", "", "", 48 * time.Hour, 0, 1, 16, "120Gi", "48h0m0s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(tc.maxTime))
			md := mdSpec("md")
			if tc.tp != "" {
				provisionFlags(md, "tensor-parallel-size", tc.tp, "pipeline-parallel-size", tc.pp)
			}
			if tc.lease > 0 {
				md.Spec.LeaseLimit = dur(tc.lease)
			}
			rg.create(md)
			rg.until(2*time.Minute, 10*time.Second, "one Allocation", func() bool { return len(rg.allocations("md")) == 1 })
			b := rg.allocations("md")[0].Spec.ForProvider.Batch
			if b == nil {
				t.Fatal("the Allocation has no batch parameters")
			}
			if b.Partition != rigPartition {
				t.Errorf("partition = %q, want %q", b.Partition, rigPartition)
			}
			if b.GPUs != tc.gpus {
				t.Errorf("gpus = %d, want %d", b.GPUs, tc.gpus)
			}
			if b.CPUsPerTask != tc.cpu {
				t.Errorf("cpusPerTask = %d, want %d", b.CPUsPerTask, tc.cpu)
			}
			if want := resource.MustParse(tc.mem); b.Memory.Cmp(want) != 0 {
				t.Errorf("memory = %s, want %s", b.Memory.String(), tc.mem)
			}
			if b.TimeLimit != tc.limit {
				t.Errorf("timeLimit = %q, want %q", b.TimeLimit, tc.limit)
			}
			if b.Nodes != 1 {
				t.Errorf("nodes = %d, want 1", b.Nodes)
			}
			if b.AgentDigest != rigAgentDigest {
				t.Errorf("agentDigest = %q, want the rig's %q", b.AgentDigest, rigAgentDigest)
			}
			insts := rg.instances("md")
			if len(insts) != 1 || insts[0].Spec.Launch.GPUs != tc.gpus {
				t.Fatalf("want one Instance whose launch has %d GPUs, got %d Instances", tc.gpus, len(insts))
			}
		})
	}
}

// TestProvisionGPUCountAboveNodeRefused covers "a GPU count above gpusPerNode is
// refused" (Provisioning): the profile has 4 GPUs per node, so TP 8, and TP 2 x
// PP 4, need 8 GPUs and set Degraded SpecUnsupported with no Allocation and no
// submission. The count is the product, not either factor alone. TP 4 x PP 1
// (exactly one node) is accepted, which keeps the refusal from being vacuous.
func TestProvisionGPUCountAboveNodeRefused(t *testing.T) {
	for _, tc := range []struct{ name, tp, pp string }{{"tp8", "8", "1"}, {"tp2-pp4", "2", "4"}} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			rg.create(provisionFlags(mdSpec("md"), "tensor-parallel-size", tc.tp, "pipeline-parallel-size", tc.pp))
			provisionSettle(rg)
			if c := provisionDegraded(rg, "md"); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported {
				t.Fatalf("Degraded = %s/%s (%s), want True/SpecUnsupported", c.Status, c.Reason, c.Message)
			}
			if n := len(rg.allocations("md")); n != 0 {
				t.Errorf("%d Allocations, want 0", n)
			}
			if n := len(rg.instances("md")); n != 0 {
				t.Errorf("%d Instances, want 0", n)
			}
			if n := rg.submissions(); n != 0 {
				t.Errorf("submissions = %d, want 0", n)
			}
		})
	}
	t.Run("positive-control-one-whole-node", func(t *testing.T) {
		rg := newRig(t)
		rg.create(provisionFlags(mdSpec("md"), "tensor-parallel-size", "4"))
		provisionSettle(rg)
		if n := rg.submissions(); n != 1 {
			t.Fatalf("submissions = %d, want 1 for 4 GPUs on a 4-GPU node", n)
		}
		if c := provisionDegraded(rg, "md"); c.Status == metav1.ConditionTrue {
			t.Fatalf("Degraded = True/%s (%s), want not degraded", c.Reason, c.Message)
		}
	})
}

// TestProvisionLaunchCarriesDefaults covers "Launch" (Test plan) and Design
// details, What the controller reads: a serving spec without max-num-seqs gives
// a launch with --max-num-seqs 256, and the same deployment ID as the spec that
// declares 256; the launch serves the deployment's model name; a declared value
// replaces the default and is not given twice.
func TestProvisionLaunchCarriesDefaults(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("plain"))
	rg.create(provisionFlags(mdSpec("explicit"), "max-num-seqs", "256"))
	rg.create(provisionFlags(mdSpec("small"), "max-num-seqs", "128"))
	rg.until(2*time.Minute, 10*time.Second, "an Instance of each deployment", func() bool {
		return len(rg.instances("plain")) == 1 && len(rg.instances("explicit")) == 1 && len(rg.instances("small")) == 1
	})
	plain, explicit, small := rg.instances("plain")[0], rg.instances("explicit")[0], rg.instances("small")[0]
	cmd := plain.Spec.Launch.Command
	if !provisionHas(cmd, "--max-num-seqs", "256") {
		t.Errorf("default launch has no --max-num-seqs 256: %v", cmd)
	}
	if !provisionHas(cmd, "--served-model-name", rigModel) {
		t.Errorf("launch has no --served-model-name %s: %v", rigModel, cmd)
	}
	if plain.Spec.Launch.MaxNumSeqs != 256 {
		t.Errorf("launch.maxNumSeqs = %d, want 256", plain.Spec.Launch.MaxNumSeqs)
	}
	if plain.Spec.DeploymentID != explicit.Spec.DeploymentID {
		t.Errorf("default %s and declared-256 %s differ in deployment ID; max-num-seqs is capacity, not an ID input (HBEP-3)",
			plain.Spec.DeploymentID, explicit.Spec.DeploymentID)
	}
	if n := slices.Index(explicit.Spec.Launch.Command, "--max-num-seqs"); n < 0 || slices.Index(explicit.Spec.Launch.Command[n+1:], "--max-num-seqs") >= 0 {
		t.Errorf("declared max-num-seqs must appear exactly once: %v", explicit.Spec.Launch.Command)
	}
	scmd := small.Spec.Launch.Command
	if !provisionHas(scmd, "--max-num-seqs", "128") || slices.Contains(scmd, "256") || small.Spec.Launch.MaxNumSeqs != 128 {
		t.Errorf("declared 128 must replace the default: %v (maxNumSeqs %d)", scmd, small.Spec.Launch.MaxNumSeqs)
	}
}

// TestProvisionSpecChecksRefuse covers "Spec checks" (Test plan): a
// data-parallel-size above 1, and a workload shape that does not fit the
// declared max-model-len (32768 in mdSpec), set Degraded SpecUnsupported with no
// Allocation. Each refusal has an accepted neighbour, so the check is on the sum.
// Hand calculation (typical output 512, longest output 4096):
//   - longest 32300: 32300 + 512 = 32812 > 32768, refused;
//   - longest 32257: 32257 + 512 = 32769 > 32768, refused;
//   - longest 32256: 32256 + 512 = 32768 <= 32768, accepted before provisioning, since the
//     overhead is unknown then; verification measures it (12) and fails the Instance:
//     32256 + 512 + 12 = 32780 > 32768 (HBEP-7: "each rendered request, overhead included");
//   - typical context 30000 with the longest output: 30000 + 4096 = 34096 > 32768, refused.
func TestProvisionSpecChecksRefuse(t *testing.T) {
	longest := func(n int32) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.ContextTokens.Longest = &n }
	}
	cases := []struct {
		name   string
		edit   func(*v1alpha1.ModelDeployment)
		refuse bool
		// later is the Degraded reason expected once verification has run, for
		// an accepted spec; empty for none.
		later string
	}{
		{"data-parallel-size-2", func(md *v1alpha1.ModelDeployment) { provisionFlags(md, "data-parallel-size", "2") }, true, ""},
		{"data-parallel-size-1", func(md *v1alpha1.ModelDeployment) { provisionFlags(md, "data-parallel-size", "1") }, false, ""},
		{"longest-32300", longest(32300), true, ""},
		{"longest-32257", longest(32257), true, ""},
		{"longest-32256", longest(32256), false, v1alpha1.ReasonVerificationFailed},
		{"typical-context-and-longest-output", func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.ContextTokens.Typical = 30000 }, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			md := mdSpec("md")
			tc.edit(md)
			rg.create(md)
			provisionSettle(rg)
			c := provisionDegraded(rg, "md")
			if tc.refuse {
				if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported {
					t.Fatalf("Degraded = %s/%s (%s), want True/SpecUnsupported", c.Status, c.Reason, c.Message)
				}
				if n := len(rg.allocations("md")) + len(rg.instances("md")) + rg.submissions(); n != 0 {
					t.Errorf("%d Allocations, Instances and submissions in all, want none", n)
				}
				return
			}
			if c.Reason == v1alpha1.ReasonSpecUnsupported || (tc.later == "" && c.Status == metav1.ConditionTrue) {
				t.Fatalf("Degraded = %s/%s (%s), want an accepted spec", c.Status, c.Reason, c.Message)
			}
			if tc.later != "" && (c.Status != metav1.ConditionTrue || c.Reason != tc.later) {
				t.Errorf("Degraded = %s/%s (%s), want True/%s once verification has run", c.Status, c.Reason, c.Message, tc.later)
			}
			if rg.submissions() < 1 {
				t.Errorf("submissions = %d, want an Allocation submitted", rg.submissions())
			}
		})
	}
}

// TestProvisionDefaultLongestFitsEngineLimit covers the last Spec checks item:
// with the default longest context, every warm-up request the agent is sent fits
// the limit the engine double reports. Hand calculation (HBEP-7, What the
// controller reads): the default longest context is the reported limit minus the
// typical output minus the template overhead = 32768 - 512 - 12 = 32244, so the
// longest request needs 32244 + 512 + 12 = 32768 tokens. The limit and the
// overhead are the engine double's (afake.DefaultVerify), not the controller's.
func TestProvisionDefaultLongestFitsEngineLimit(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	v := afake.DefaultVerify(agentapi.DesiredInstance{}, agentapi.VerifyRequest{})
	limit, err := strconv.Atoi(v.Settings["max-model-len"])
	if err != nil || limit != 32768 || v.TemplateOverhead != 12 {
		t.Fatalf("the engine double reports limit %q overhead %d; this test's hand calculation assumes 32768 and 12", v.Settings["max-model-len"], v.TemplateOverhead)
	}
	alloc := rg.allocations("md")[0].Name
	var warm, qual int
	for _, d := range rg.agentOf(alloc).agent.Seen() {
		if d.Instance == nil || d.Instance.Checks == nil {
			continue
		}
		c := d.Instance.Checks
		for _, sh := range []*agentapi.Shapes{provisionShapes(c.WarmUp), provisionShapes(c.Qualify)} {
			if sh == nil {
				continue
			}
			if sh.ContextLongest != 32244 {
				t.Errorf("longest context = %d, want 32768 - 512 - 12 = 32244", sh.ContextLongest)
			}
			for name, need := range map[string]int32{
				"short":           sh.ContextShort + sh.OutputTypical,
				"typical":         sh.ContextTypical + sh.OutputTypical,
				"longest":         sh.ContextLongest + sh.OutputTypical,
				"typical+longOut": sh.ContextTypical + sh.OutputLongest,
			} {
				if need+v.TemplateOverhead > int32(limit) {
					t.Errorf("the %s request needs %d+%d tokens, above the engine's limit %d", name, need, v.TemplateOverhead, limit)
				}
			}
		}
		if c.WarmUp != nil {
			warm++
		}
		if c.Qualify != nil {
			qual++
		}
	}
	if warm == 0 || qual == 0 {
		t.Fatalf("the agent was sent %d warm-up and %d qualification requests; both must be seen or the fit check proved nothing", warm, qual)
	}
}

// provisionShapes returns the shapes of a warm-up or qualification request, nil when
// there is none.
func provisionShapes(req any) *agentapi.Shapes {
	switch r := req.(type) {
	case *agentapi.WarmUpRequest:
		if r != nil {
			return &r.Shapes
		}
	case *agentapi.QualifyRequest:
		if r != nil {
			return &r.Shapes
		}
	}
	return nil
}

// TestProvisionFrontNotConfigured covers "With no front configured, no
// Allocation is submitted and the ModelDeployment is Degraded with reason
// FrontNotConfigured" (HBEP-11, Single-user host). The same spec provisions once
// the front is configured, so the missing front is the cause.
func TestProvisionFrontNotConfigured(t *testing.T) {
	rg := newRig(t, withoutFront())
	rg.create(mdSpec("md"))
	provisionSettle(rg)
	if c := provisionDegraded(rg, "md"); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonFrontNotConfigured {
		t.Fatalf("Degraded = %s/%s (%s), want True/FrontNotConfigured", c.Status, c.Reason, c.Message)
	}
	if n := len(rg.allocations("md")) + len(rg.instances("md")) + rg.submissions(); n != 0 {
		t.Fatalf("%d Allocations, Instances and submissions in all, want none", n)
	}
	rg.r.Config.FrontConfigured = true
	provisionSettle(rg)
	if rg.submissions() != 1 {
		t.Fatalf("submissions = %d once the front is configured, want 1", rg.submissions())
	}
	if c := provisionDegraded(rg, "md"); c.Status == metav1.ConditionTrue {
		t.Errorf("Degraded = True/%s (%s) with the front configured", c.Reason, c.Message)
	}
}
