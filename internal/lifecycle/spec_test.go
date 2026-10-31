package lifecycle

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The tests in this file need no rig. Expected values are literals worked out
// by hand from HBEP-7; the arithmetic is in the comment next to each.

// specFlag builds an engine flag.
func specFlag(name, value string) v1alpha1.EngineFlag {
	return v1alpha1.EngineFlag{Name: name, Value: value}
}

// specMD returns a served deployment with the given flags and the rig's usual
// workload: typical context 4096, typical output 512, 4 target sessions.
func specMD(flags ...v1alpha1.EngineFlag) *v1alpha1.ModelDeployment {
	md := mdSpec("md")
	md.Spec.Source.Served.Flags = flags
	return md
}

// specCheck runs checkServed and fails the test on an error.
func specCheck(t *testing.T, md *v1alpha1.ModelDeployment) servedPlan {
	t.Helper()
	plan, err := checkServed(md)
	if err != nil {
		t.Fatalf("checkServed: %v", err)
	}
	return plan
}

// specRefused asserts that checkServed refuses md with an *unsupported error
// whose message contains each of the given texts.
func specRefused(t *testing.T, md *v1alpha1.ModelDeployment, contains ...string) {
	t.Helper()
	_, err := checkServed(md)
	var u *unsupported
	if !errors.As(err, &u) {
		t.Fatalf("checkServed error = %v (%T), want *unsupported", err, err)
	}
	for _, c := range contains {
		if !strings.Contains(err.Error(), c) {
			t.Errorf("error %q does not mention %q", err, c)
		}
	}
}

// TestCheckServedRefusesDataParallel covers HBEP-7, Provisioning and the Test
// plan item "Spec checks": a data-parallel-size of 2 is refused with
// SpecUnsupported, whichever way the flag is spelled. Size 1 is fine.
func TestCheckServedRefusesDataParallel(t *testing.T) {
	for _, name := range []string{"data-parallel-size", "data_parallel_size"} {
		t.Run(name, func(t *testing.T) {
			specRefused(t, specMD(specFlag(name, "2")), "data-parallel-size")
		})
	}
	if plan := specCheck(t, specMD(specFlag("data-parallel-size", "1"))); plan.gpus != 1 {
		t.Fatalf("data-parallel-size 1 gives %d GPUs, want 1", plan.gpus)
	}
}

// TestCheckServedGPUsAreTPTimesPP covers HBEP-7, Provisioning: "gpus:
// tensor-parallel-size times pipeline-parallel-size ..., each 1 when absent".
func TestCheckServedGPUsAreTPTimesPP(t *testing.T) {
	tests := []struct {
		name  string
		flags []v1alpha1.EngineFlag
		want  int32
	}{
		{"neither", nil, 1},
		// 2 * 2 = 4.
		{"tp 2, pp 2", []v1alpha1.EngineFlag{specFlag("tensor-parallel-size", "2"), specFlag("pipeline-parallel-size", "2")}, 4},
		// 2 * 1 = 2, with the underscore spelling.
		{"tp 2 only", []v1alpha1.EngineFlag{specFlag("tensor_parallel_size", "2")}, 2},
		// 1 * 3 = 3.
		{"pp 3 only", []v1alpha1.EngineFlag{specFlag("pipeline-parallel-size", "3")}, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := specCheck(t, specMD(tc.flags...)).gpus; got != tc.want {
				t.Fatalf("gpus = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCheckServedRefusesFrameworkFlags: HBEP-7, What it runs, "--served-model-name
// set to the ModelDeployment's model name" and the socket are the launch's own,
// so a spec that sets one is refused, never overridden.
func TestCheckServedRefusesFrameworkFlags(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"uds", "/tmp/x.sock"},
		{"served-model-name", "other"},
		{"served_model_name", "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			specRefused(t, specMD(specFlag(tc.name, tc.value)), tc.name)
		})
	}
}

// TestCheckServedNeedsAWorkload: HBEP-7, What the controller reads, a served
// deployment needs the shapes for warm-up and qualification.
func TestCheckServedNeedsAWorkload(t *testing.T) {
	md := specMD(specFlag("max-model-len", "32768"))
	md.Spec.Workload = nil
	specRefused(t, md, "workload")
}

// TestCheckServedShapesAgainstDeclaredMaxModelLen covers HBEP-7, What the
// controller reads ("Every shape must fit ... A workload whose longest context
// and typical output exceed a declared max-model-len is refused") and the Test
// plan item "Spec checks". Overhead is 0 before verification measures it.
func TestCheckServedShapesAgainstDeclaredMaxModelLen(t *testing.T) {
	longest := func(n int32) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.ContextTokens.Longest = &n }
	}
	tests := []struct {
		name      string
		maxLen    string
		mutate    func(*v1alpha1.ModelDeployment)
		wantFits  bool
		wantInMsg string
	}{
		// Typical context 4096 + typical output 512 = 4608 > 4096.
		{"typical shape does not fit 4096", "4096", nil, false, "typical context"},
		// Default shapes: 512+512, 4096+512 = 4608, 4096+4096 (typical context
		// with the default longest output) = 8192. All below 32768.
		{"defaults fit 32768", "32768", nil, true, ""},
		// 30000 + 512 = 30512 <= 32768.
		{"longest 30000 fits 32768", "32768", longest(30000), true, ""},
		// 32300 + 512 = 32812 > 32768.
		{"longest 32300 does not fit 32768", "32768", longest(32300), false, "longest context"},
		// 32256 + 512 = 32768, exactly the limit, fits.
		{"longest 32256 fits 32768 exactly", "32768", longest(32256), true, ""},
		// 32257 + 512 = 32769, one over.
		{"longest 32257 is one token over", "32768", longest(32257), false, "longest context"},
		// The default longest output (4096) with the typical context (4096)
		// is 8192 > 8000: the default is applied and does not fit.
		{"default longest output does not fit 8000", "8000", nil, false, "longest output"},
		// With outputTokens.longest 3000: 4096 + 3000 = 7096 <= 8000, and the
		// typical shapes 4096 + 512 = 4608 <= 8000.
		{"outputTokens.longest 3000 fits 8000", "8000", func(md *v1alpha1.ModelDeployment) {
			md.Spec.Workload.OutputTokens.Longest = ptrTo(int32(3000))
		}, true, ""},
		// contextTokens.short 8000 + typical output 512 = 8512 > 8000.
		{"short context above the limit", "8000", func(md *v1alpha1.ModelDeployment) {
			md.Spec.Workload.ContextTokens.Short = ptrTo(int32(8000))
		}, false, "short context"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := specMD(specFlag("max-model-len", tc.maxLen))
			if tc.mutate != nil {
				tc.mutate(md)
			}
			if tc.wantFits {
				plan := specCheck(t, md)
				if plan.maxModelLen == nil || *plan.maxModelLen != atoi32(t, tc.maxLen) {
					t.Fatalf("maxModelLen = %v, want %s", plan.maxModelLen, tc.maxLen)
				}
				return
			}
			specRefused(t, md, tc.wantInMsg)
		})
	}
}

func atoi32(t *testing.T, s string) int32 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	return int32(n)
}

// TestCheckServedAppliesShapeDefaults covers HBEP-7, What the controller reads:
// contextTokens.short defaults to 512 and outputTokens.longest to 4096; the
// longest context is left to verification; toolCalls follows the auto tool
// choice flag unless set.
func TestCheckServedAppliesShapeDefaults(t *testing.T) {
	plan := specCheck(t, specMD(specFlag("max-model-len", "32768")))
	sh := plan.shapes
	if sh.short != 512 || sh.outputLongest != 4096 {
		t.Errorf("short/outputLongest = %d/%d, want 512/4096", sh.short, sh.outputLongest)
	}
	if sh.typical != 4096 || sh.outputTypical != 512 || sh.targetSessions != 4 {
		t.Errorf("typical/outputTypical/targetSessions = %d/%d/%d, want 4096/512/4", sh.typical, sh.outputTypical, sh.targetSessions)
	}
	if sh.longest != nil {
		t.Errorf("longest = %d, want unset (fixed at verification)", *sh.longest)
	}
	if sh.toolCalls {
		t.Error("toolCalls is on without the flag")
	}

	md := specMD(specFlag("max-model-len", "32768"), specFlag("enable-auto-tool-choice", "true"))
	if !specCheck(t, md).shapes.toolCalls {
		t.Error("toolCalls is off although the spec enables automatic tool choice")
	}
	md.Spec.Workload.ToolCalls = ptrTo(false)
	if specCheck(t, md).shapes.toolCalls {
		t.Error("workload.toolCalls false does not override the flag")
	}

	md = specMD()
	md.Spec.Workload.ContextTokens.Short = ptrTo(int32(100))
	md.Spec.Workload.OutputTokens.Longest = ptrTo(int32(2000))
	sh = specCheck(t, md).shapes
	if sh.short != 100 || sh.outputLongest != 2000 {
		t.Errorf("explicit short/outputLongest = %d/%d, want 100/2000", sh.short, sh.outputLongest)
	}
}

// TestCheckServedRefusesNonPositiveShapes: a shape of zero tokens is an error.
func TestCheckServedRefusesNonPositiveShapes(t *testing.T) {
	md := specMD()
	md.Spec.Workload.TargetSessions = 0
	specRefused(t, md, "targetSessions")
}

// TestShapesResolved covers HBEP-7, What the controller reads: "contextTokens.longest
// defaults to the resolved context limit minus outputTokens.typical ... minus
// the template overhead", and the Test plan item "Spec checks", last sentence:
// every rendered warm-up request fits the limit the engine reports.
func TestShapesResolved(t *testing.T) {
	sh := shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4}

	// The default: limit 32768, overhead 12. longest = 32768 - 512 - 12 =
	// 32244.
	got, err := sh.resolved(32768, 12)
	if err != nil {
		t.Fatal(err)
	}
	want := agentapi.Shapes{ContextShort: 512, ContextTypical: 4096, ContextLongest: 32244, OutputTypical: 512, OutputLongest: 4096}
	if got != want {
		t.Fatalf("resolved = %+v, want %+v", got, want)
	}

	// Every request, with the overhead, fits the limit (plain arithmetic):
	// 512+512+12 = 1036; 4096+512+12 = 4620; 32244+512+12 = 32768;
	// 4096+4096+12 = 8204; all <= 32768.
	for _, r := range []struct {
		name    string
		in, out int32
	}{
		{"short", got.ContextShort, got.OutputTypical},
		{"typical", got.ContextTypical, got.OutputTypical},
		{"longest", got.ContextLongest, got.OutputTypical},
		{"long output", got.ContextTypical, got.OutputLongest},
	} {
		if total := r.in + r.out + 12; total > 32768 {
			t.Errorf("%s request needs %d tokens with the overhead, above the limit 32768", r.name, total)
		}
	}
	if total := got.ContextLongest + got.OutputTypical + 12; total != 32768 {
		t.Errorf("the default longest context should fill the limit exactly: %d", total)
	}

	// Another limit and overhead: 16384 - 512 - 40 = 15832.
	if g, err := sh.resolved(16384, 40); err != nil || g.ContextLongest != 15832 {
		t.Fatalf("resolved(16384, 40) = %+v, %v; want longest 15832", g, err)
	}
}

// TestShapesResolvedExplicitLongest: a declared longest context is used when it
// fits, and is an error, never shortened, when it does not (HBEP-7, "A shape
// that does not fit is an error, never shortened").
func TestShapesResolvedExplicitLongest(t *testing.T) {
	base := shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4}
	with := func(n int32) shapes { s := base; s.longest = &n; return s }

	// 30000 + 512 + 12 = 30524 <= 32768: used as given.
	if g, err := with(30000).resolved(32768, 12); err != nil || g.ContextLongest != 30000 {
		t.Fatalf("explicit 30000: %+v, %v", g, err)
	}
	// 32244 + 512 + 12 = 32768: fits exactly.
	if g, err := with(32244).resolved(32768, 12); err != nil || g.ContextLongest != 32244 {
		t.Fatalf("explicit 32244: %+v, %v", g, err)
	}
	// 32245 + 512 + 12 = 32769: one over.
	// 32300 + 512 + 12 = 32824: over.
	for _, n := range []int32{32245, 32300} {
		_, err := with(n).resolved(32768, 12)
		var u *unsupported
		if !errors.As(err, &u) {
			t.Errorf("explicit longest %d: error = %v, want *unsupported", n, err)
		}
	}
}

// TestShapesResolvedRefusesWhatCannotFit: a limit too small for the other
// shapes is an error too. Limit 8200 with overhead 12: the typical context
// with the longest output needs 4096 + 4096 + 12 = 8204 > 8200. A limit of 500
// leaves a longest context of 500 - 512 - 12 = -24, below one token.
func TestShapesResolvedRefusesWhatCannotFit(t *testing.T) {
	sh := shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4}
	for _, limit := range []int32{8200, 500} {
		_, err := sh.resolved(limit, 12)
		var u *unsupported
		if !errors.As(err, &u) {
			t.Errorf("limit %d: error = %v, want *unsupported", limit, err)
		}
	}
}

// specIndexes returns the indexes of s in a command.
func specIndexes(cmd []string, s string) []int {
	var out []int
	for i, c := range cmd {
		if c == s {
			out = append(out, i)
		}
	}
	return out
}

// specValueOf returns the argument after the single occurrence of flag.
func specValueOf(t *testing.T, cmd []string, flag string) string {
	t.Helper()
	idx := specIndexes(cmd, flag)
	if len(idx) != 1 || idx[0]+1 >= len(cmd) {
		t.Fatalf("flag %s occurs %d times in %v, want once with a value", flag, len(idx), cmd)
	}
	return cmd[idx[0]+1]
}

// specLaunch checks the spec as the controller does and renders its launch.
func specLaunch(t *testing.T, md *v1alpha1.ModelDeployment) v1alpha1.Launch {
	t.Helper()
	return renderLaunch(md, specCheck(t, md))
}

// TestRenderLaunchMaxNumSeqs covers HBEP-7, What the controller reads: "The
// command line always carries --max-num-seqs: the serving spec's value, else
// 256". The 256 default is the controller's: it comes out of checkServed, so
// the plan is built that way and not by hand.
func TestRenderLaunchMaxNumSeqs(t *testing.T) {
	t.Run("absent gives 256", func(t *testing.T) {
		l := specLaunch(t, specMD(specFlag("max-model-len", "32768")))
		if got := provisionSeqsArgs(l.Command); !slices.Equal(got, []string{"--max-num-seqs=256"}) {
			t.Fatalf("max-num-seqs arguments %v, want exactly --max-num-seqs=256 (one argument, the Launch's CEL rule)", got)
		}
		if l.MaxNumSeqs != 256 {
			t.Fatalf("MaxNumSeqs = %d, want 256", l.MaxNumSeqs)
		}
	})
	for _, name := range []string{"max-num-seqs", "max_num_seqs"} {
		t.Run("declared 64 as "+name, func(t *testing.T) {
			l := specLaunch(t, specMD(specFlag("max-model-len", "32768"), specFlag(name, "64")))
			if got := provisionSeqsArgs(l.Command); !slices.Equal(got, []string{"--max-num-seqs=64"}) {
				t.Fatalf("max-num-seqs arguments %v, want exactly --max-num-seqs=64", got)
			}
			if slices.Contains(l.Command, "256") || slices.Contains(l.Command, "--max_num_seqs") {
				t.Fatalf("default or raw spelling leaked into %v", l.Command)
			}
			if l.MaxNumSeqs != 64 {
				t.Fatalf("MaxNumSeqs = %d, want 64", l.MaxNumSeqs)
			}
		})
	}
}

// specLaunchArgv is a launch command written out by hand: vLLM's server on the
// bound checkpoint, on worker-0's Unix socket, serving the rig's model name,
// then rest, the spec's flags as the controller renders them.
func specLaunchArgv(rest ...string) []string {
	return append([]string{
		"vllm", "serve", "/hpcburst/model",
		"--uds", "/hpcburst/run/worker-0.sock",
		"--served-model-name", "example-model",
	}, rest...)
}

// TestRenderLaunchCommand covers HBEP-7, The allocation agent, What it runs: the
// engine listens on worker-0's Unix socket, serves the ModelDeployment's model
// name, and the spec's flags follow in order, each as one --name=value
// argument (docs/development.md, The launch).
func TestRenderLaunchCommand(t *testing.T) {
	md := specMD(specFlag("max-model-len", "32768"), specFlag("tensor-parallel-size", "2"))
	l := specLaunch(t, md)
	want := specLaunchArgv("--max-model-len=32768", "--tensor-parallel-size=2", "--max-num-seqs=256")
	if !slices.Equal(l.Command, want) {
		t.Fatalf("command\n got %q\nwant %q", l.Command, want)
	}
	if got := specValueOf(t, l.Command, "--uds"); !strings.HasSuffix(got, "/worker-0.sock") {
		t.Errorf("--uds %q is not worker-0's socket", got)
	}
	if got := specValueOf(t, l.Command, "--served-model-name"); got != md.Spec.Model || got != "example-model" {
		t.Errorf("--served-model-name %q, want the model name example-model", got)
	}
	if l.KeyEnv != "VLLM_API_KEY" {
		t.Errorf("KeyEnv = %q, want VLLM_API_KEY", l.KeyEnv)
	}
	if l.GPUs != 2 {
		t.Errorf("GPUs = %d, want 2 (tp 2 * pp 1)", l.GPUs)
	}
	if l.Image != "docker.io/vllm/vllm-openai" || l.ImageDigest != rigDigest {
		t.Errorf("image %q digest %q", l.Image, l.ImageDigest)
	}
	if l.Checkpoint.Repository != "example-org/example-model" || l.Checkpoint.Revision != rigRevision {
		t.Errorf("checkpoint %+v", l.Checkpoint)
	}
}

// TestRenderLaunchEveryFlagIsOneArgument: each spec flag is one --name=value
// argument, in spec order, spelled with "-" (an underscore name is
// normalised), so a value that starts with "-" or holds a space or a brace is
// still part of its flag; a boolean is --name or --no-name; max-num-seqs is
// last, once, as the spec gives it.
func TestRenderLaunchEveryFlagIsOneArgument(t *testing.T) {
	l := specLaunch(t, specMD(
		specFlag("max_model_len", "32768"),
		specFlag("seed", "-1"),
		specFlag("enable_prefix_caching", "false"),
		specFlag("trust-remote-code", "true"),
		specFlag("speculative-config", `{"method": "ngram", "num_speculative_tokens": 3}`),
		specFlag("max_num_seqs", "64"),
	))
	want := specLaunchArgv(
		"--max-model-len=32768",
		"--seed=-1",
		"--no-enable-prefix-caching",
		"--trust-remote-code",
		`--speculative-config={"method": "ngram", "num_speculative_tokens": 3}`,
		"--max-num-seqs=64",
	)
	if !slices.Equal(l.Command, want) {
		t.Fatalf("command\n got %q\nwant %q", l.Command, want)
	}
}

// TestRenderLaunchGPUsAreTPTimesPP: 2 * 2 = 4.
func TestRenderLaunchGPUsAreTPTimesPP(t *testing.T) {
	l := specLaunch(t, specMD(specFlag("tensor-parallel-size", "2"), specFlag("pipeline-parallel-size", "2")))
	if l.GPUs != 4 {
		t.Fatalf("GPUs = %d, want 4", l.GPUs)
	}
}

// TestRenderLaunchBooleanFlags: HBEP-3 gives boolean flags under their positive
// name with "true" or "false"; vLLM reads "false" as the --no- form and "true"
// as the bare flag.
func TestRenderLaunchBooleanFlags(t *testing.T) {
	for _, name := range []string{"enable-prefix-caching", "enable_prefix_caching"} {
		t.Run("false as "+name, func(t *testing.T) {
			l := specLaunch(t, specMD(specFlag(name, "false")))
			if want := specLaunchArgv("--no-enable-prefix-caching", "--max-num-seqs=256"); !slices.Equal(l.Command, want) {
				t.Fatalf("command\n got %q\nwant %q", l.Command, want)
			}
			if slices.Contains(l.Command, "--enable-prefix-caching") || slices.Contains(l.Command, "false") {
				t.Fatalf("positive flag or value left in %v", l.Command)
			}
		})
		t.Run("true as "+name, func(t *testing.T) {
			l := specLaunch(t, specMD(specFlag(name, "true")))
			if want := specLaunchArgv("--enable-prefix-caching", "--max-num-seqs=256"); !slices.Equal(l.Command, want) {
				t.Fatalf("command\n got %q\nwant %q", l.Command, want)
			}
			idx := specIndexes(l.Command, "--enable-prefix-caching")
			if len(idx) != 1 {
				t.Fatalf("--enable-prefix-caching occurs %d times in %v", len(idx), l.Command)
			}
			if slices.Contains(l.Command, "true") || slices.Contains(l.Command, "--no-enable-prefix-caching") {
				t.Fatalf("a value or the negated flag in %v", l.Command)
			}
			// The bare flag is followed by the next flag, not a value.
			if next := l.Command[idx[0]+1]; !strings.HasPrefix(next, "--") {
				t.Fatalf("--enable-prefix-caching is followed by %q, want another flag", next)
			}
		})
	}
}

// specServingSpec is the library's ServingSpec for a ModelDeployment's flags,
// built by hand from the rig's checkpoint and engine, with the template hash of
// no template at all.
func specServingSpec(flags ...deploymentid.Flag) deploymentid.ServingSpec {
	return deploymentid.ServingSpec{
		Checkpoint: deploymentid.Checkpoint{Repository: "example-org/example-model", Revision: rigRevision},
		Engine:     deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest},
		Flags:      flags,
		// SHA-256 of the empty text (HBEP-3: no template at all).
		ChatTemplateSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
}

// TestRenderLaunchDefaultMaxNumSeqsKeepsTheDeploymentID covers HBEP-7, Test plan,
// "Launch: a serving spec without max-num-seqs gives a launch with
// --max-num-seqs 256, and the same deployment ID as the spec that declares
// 256." max-num-seqs is capacity, not a deployment-ID input (HBEP-3's table).
// Both IDs come from the deployment-ID library, which does not share code with
// the launch, over the same files and accelerator; the launch side is the
// command line each spec renders. Negative controls: a different max-model-len
// (an input) changes the ID, so the comparison can fail.
func TestRenderLaunchDefaultMaxNumSeqsKeepsTheDeploymentID(t *testing.T) {
	accel := deploymentid.Accelerator{GPUModel: "GH200", CPUArch: "aarch64"}
	id := func(flags ...deploymentid.Flag) string {
		t.Helper()
		got, err := deploymentid.Compute(specServingSpec(flags...), accel, rigFiles)
		if err != nil {
			t.Fatal(err)
		}
		return got.SHA256
	}
	mml := deploymentid.Flag{Name: "max-model-len", Value: "32768"}
	without := id(mml)
	with256 := id(mml, deploymentid.Flag{Name: "max-num-seqs", Value: "256"})
	with64 := id(mml, deploymentid.Flag{Name: "max-num-seqs", Value: "64"})
	otherLen := id(deploymentid.Flag{Name: "max-model-len", Value: "16384"})
	if without != with256 {
		t.Errorf("ID without max-num-seqs %s differs from the ID declaring 256 %s", without, with256)
	}
	if without != with64 {
		t.Errorf("ID declaring max-num-seqs 64 %s differs from the ID without it %s (capacity is no input)", with64, without)
	}
	if without == otherLen {
		t.Fatal("the comparison cannot fail: a different max-model-len gives the same ID")
	}

	// The two specs render the same launch: --max-num-seqs 256 at the end.
	absent := specLaunch(t, specMD(specFlag("max-model-len", "32768")))
	declared := specLaunch(t, specMD(specFlag("max-model-len", "32768"), specFlag("max-num-seqs", "256")))
	if !slices.Equal(absent.Command, declared.Command) || absent.MaxNumSeqs != 256 || declared.MaxNumSeqs != 256 {
		t.Fatalf("launches differ:\n absent   %v (%d)\n declared %v (%d)", absent.Command, absent.MaxNumSeqs, declared.Command, declared.MaxNumSeqs)
	}
}

// specProviders builds Providers with labels.
func specProviders(spec ...[2]string) []v1alpha1.Provider {
	out := make([]v1alpha1.Provider, len(spec))
	for i, s := range spec {
		out[i].Name = s[0]
		if s[1] != "" {
			out[i].Labels = map[string]string{"platform": s[1]}
		}
	}
	return out
}

// TestSelectedProviderNames covers HBEP-7, What the controller reads: "the
// allowed providers are tried in the order their names are listed, then those
// that only the label selector matches, by name".
func TestSelectedProviderNames(t *testing.T) {
	// The Provider list is in no useful order, so a missing sort shows.
	all := specProviders(
		[2]string{"p-c", "hpc"}, [2]string{"p-a", "hpc"}, [2]string{"zz", "hpc"},
		[2]string{"p-b", "hpc"}, [2]string{"p-d", "cloud"}, [2]string{"p-e", ""},
	)
	hpc := &metav1.LabelSelector{MatchLabels: map[string]string{"platform": "hpc"}}

	tests := []struct {
		name string
		sel  v1alpha1.ProviderSelection
		want []string
	}{
		{"names in the order given", v1alpha1.ProviderSelection{Names: []string{"p-d", "p-a"}}, []string{"p-d", "p-a"}},
		// zz is listed first and also matches the selector: once. The other
		// matches follow by name: p-a, p-b, p-c.
		{"names, then selector matches by name, without duplicates",
			v1alpha1.ProviderSelection{Names: []string{"zz"}, Selector: hpc}, []string{"zz", "p-a", "p-b", "p-c"}},
		// p-b first, then the rest of the matches by name: p-a, p-c, zz.
		{"a listed match keeps its place", v1alpha1.ProviderSelection{Names: []string{"p-b"}, Selector: hpc}, []string{"p-b", "p-a", "p-c", "zz"}},
		{"selector only", v1alpha1.ProviderSelection{Selector: hpc}, []string{"p-a", "p-b", "p-c", "zz"}},
		{"selector matching nothing", v1alpha1.ProviderSelection{Names: []string{"p-e"},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"platform": "none"}}}, []string{"p-e"}},
		// A name that no Provider has is kept: loadProviders reports it as a
		// missing Provider.
		{"an unknown name is kept", v1alpha1.ProviderSelection{Names: []string{"gone"}}, []string{"gone"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectedProviderNames(tc.sel, all)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("names = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSelectedProviderNamesBadSelector: a selector that cannot be built is an
// error naming the field.
func TestSelectedProviderNamesBadSelector(t *testing.T) {
	bad := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "platform", Operator: "Bogus"}}}
	_, err := selectedProviderNames(v1alpha1.ProviderSelection{Selector: bad}, nil)
	if err == nil || !strings.Contains(err.Error(), "selector") {
		t.Fatalf("error = %v, want one naming the selector", err)
	}
}
