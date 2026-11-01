package lifecycle

// HBEP-7's Test plan, Spec checks: a serving spec that changes which generation
// config vLLM applies is refused before provisioning, with no Allocation and
// Degraded SpecUnsupported: a generation-config other than auto, an
// override-generation-config, an hf-config-path. A generation-config of auto,
// vLLM's default, is accepted (`@hpc-burst-science-agent`, 1 Nov 2026, for #169).

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestProvisionGenerationConfigFlags(t *testing.T) {
	flag := func(n, v string) v1alpha1.EngineFlag { return v1alpha1.EngineFlag{Name: n, Value: v} }
	for _, tc := range []struct {
		name  string
		flags []v1alpha1.EngineFlag
		// refuse says the spec is refused; naming is a text the message holds.
		refuse bool
		naming string
	}{
		{"generation-config auto is accepted", []v1alpha1.EngineFlag{flag("generation-config", "auto")}, false, ""},
		{"no generation flag is accepted", nil, false, ""},
		{"generation-config vllm", []v1alpha1.EngineFlag{flag("generation-config", "vllm")}, true, `generation-config "vllm"`},
		{"generation-config a folder", []v1alpha1.EngineFlag{flag("generation-config", "/data/example-config-dir")}, true, `generation-config "/data/example-config-dir"`},
		{"generation-config Auto is a path to vLLM", []v1alpha1.EngineFlag{flag("generation-config", "Auto")}, true, `generation-config "Auto"`},
		{"generation-config with an underscore name", []v1alpha1.EngineFlag{flag("generation_config", "vllm")}, true, `generation-config "vllm"`},
		{"generation-config auto then vllm: the last value wins", []v1alpha1.EngineFlag{flag("generation-config", "auto"), flag("generation-config", "vllm")}, true, `generation-config "vllm"`},
		{"override-generation-config", []v1alpha1.EngineFlag{flag("override-generation-config", `{"max_new_tokens": 16}`)}, true, "override-generation-config"},
		{"override_generation_config with an underscore name", []v1alpha1.EngineFlag{flag("override_generation_config", `{"temperature": 0.5}`)}, true, "override-generation-config"},
		{"hf-config-path", []v1alpha1.EngineFlag{flag("hf-config-path", "example-org/example-config")}, true, "hf-config-path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			md := mdSpec("md")
			md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, tc.flags...)
			rg.create(md)
			provisionSettle(rg)
			c := provisionDegraded(rg, "md")
			if !tc.refuse {
				if c.Reason == v1alpha1.ReasonSpecUnsupported {
					t.Fatalf("Degraded = %s/%s %q, want the spec accepted", c.Status, c.Reason, c.Message)
				}
				if rg.submissions() < 1 {
					t.Fatalf("%d submissions, want an Allocation submitted", rg.submissions())
				}
				return
			}
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported || !strings.Contains(c.Message, tc.naming) {
				t.Fatalf("Degraded = %s/%s %q, want True/SpecUnsupported naming %q", c.Status, c.Reason, c.Message, tc.naming)
			}
			if n := len(rg.allocations("md")) + len(rg.instances("md")) + rg.submissions(); n != 0 {
				t.Fatalf("%d Allocations, Instances and submissions in all, want none", n)
			}
		})
	}
}
