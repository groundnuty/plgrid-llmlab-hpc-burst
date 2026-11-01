package main

// The plan and the controller's judgements for the real-engine run (issue
// #94, step 1). TestRealEnginePlan writes the plan on a machine that reaches
// the Hub; TestRealEngine (realengine_test.go) runs it on a GPU node against a
// real vLLM. Both skip unless their variables are set, so go test ./... never
// runs them.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// realImage is vLLM v0.32.0's image. The digest is the tag's manifest list
// (multi-arch index) on Docker Hub, read on 31 Oct 2026; Apptainer picks the
// node's platform from it (arm64 on GH200).
const (
	realImage       = "docker.io/vllm/vllm-openai"
	realImageDigest = "sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34"
	realVersion     = "0.32.0"
)

// realPlan is what TestRealEngine runs: the Instances one after another on one
// Allocation, and the profile values the agent applies.
type realPlan struct {
	ProbeInterval agentapi.Duration `json:"probeInterval"`
	StopGrace     agentapi.Duration `json:"stopGrace"`
	Instances     []realInstance    `json:"instances"`
}

type realInstance struct {
	Label   string                  `json:"label"`
	Launch  agentapi.Launch         `json:"launch"`
	Files   []agentapi.FileDigest   `json:"files"`
	Qualify agentapi.QualifyRequest `json:"qualify"`
	WarmUp  agentapi.WarmUpRequest  `json:"warmUp"`
	// CaptureMaxTokens is the output budget of the driver's own requests to
	// the engine (the raw captures): a reasoning model needs room to finish.
	CaptureMaxTokens int `json:"captureMaxTokens"`
}

// The engine's environment: no Hub access and no usage reports. The compile
// caches (XDG_CACHE_HOME, VLLM_CACHE_ROOT, TORCHINDUCTOR_CACHE_DIR,
// TRITON_CACHE_DIR, FLASHINFER_WORKSPACE_BASE, CUDA_CACHE_PATH) are not set
// here: the agent points them into the Allocation's own cache directory
// itself, and a launch's own value would override it (HBEP-7, What it runs:
// Home and caches), so the run exercises the agent's defaults.
// VLLM_USE_FLASHINFER_SAMPLER=0 is www-2027's Helios serving recipe (spikes/s2b).
func realEnv() []agentapi.EnvVar {
	return []agentapi.EnvVar{
		{Name: "HF_HUB_OFFLINE", Value: "1"},
		{Name: "VLLM_NO_USAGE_STATS", Value: "1"},
		{Name: "DO_NOT_TRACK", Value: "1"},
		{Name: "VLLM_USE_FLASHINFER_SAMPLER", Value: "0"},
	}
}

type modelSpec struct {
	label, repo, rev, served string
	maxModelLen              int
	extra                    []string
	shapes                   agentapi.Shapes
	captureMaxTokens         int
}

// The two Instances. Qwen3-0.6B declares a reasoning parser, so its replies
// carry reasoning deltas; Qwen2.5-1.5B-Instruct is spike S2b's model and
// revision, without one. Both declare the four cache settings vLLM reports in
// cache_config_info, in forms that exercise HBEP-7's comparison rule (a float
// written 0.50, a boolean flag, an integer, and auto).
var realModels = []modelSpec{
	{
		label: "qwen3-0.6b", repo: "Qwen/Qwen3-0.6B", rev: "c1899de289a04d12100db370d81485cdf75e47ca",
		served: "hb-test/qwen3-0.6b", maxModelLen: 8192,
		extra:  []string{"--reasoning-parser=qwen3", "--enable-auto-tool-choice", "--tool-call-parser=hermes"},
		shapes: agentapi.Shapes{ContextShort: 128, ContextTypical: 1024, ContextLongest: 4096, OutputTypical: 128, OutputLongest: 512},
		// The reasoning headroom of the agent's own checks.
		captureMaxTokens: 128 + agentapi.ReasoningHeadroomTokens,
	},
	{
		label: "qwen2.5-1.5b-instruct", repo: "Qwen/Qwen2.5-1.5B-Instruct", rev: "989aa7980e4cf806f80c7fef2b1adb7bc71aa306",
		served: "hb-test/qwen2.5-1.5b-instruct", maxModelLen: 4096,
		extra:            []string{"--enable-auto-tool-choice", "--tool-call-parser=hermes"},
		shapes:           agentapi.Shapes{ContextShort: 128, ContextTypical: 1024, ContextLongest: 3072, OutputTypical: 128, OutputLongest: 512},
		captureMaxTokens: 256,
	},
}

func (m modelSpec) launch() agentapi.Launch {
	cmd := []string{"vllm", "serve", agentapi.ContainerModelDir,
		"--uds=" + agentapi.ContainerEngineSocket(agentapi.WorkerName(0)),
		"--served-model-name=" + m.served,
		"--max-model-len=" + strconv.Itoa(m.maxModelLen),
		"--max-num-seqs=8",
		"--gpu-memory-utilization=0.50",
		"--block-size=16",
		"--enable-prefix-caching",
		"--kv-cache-dtype=auto",
	}
	return agentapi.Launch{
		Image: realImage, ImageDigest: realImageDigest,
		Command:    append(cmd, m.extra...),
		Env:        realEnv(),
		KeyEnv:     "VLLM_API_KEY",
		Checkpoint: agentapi.Checkpoint{Repository: m.repo, Revision: m.rev},
		GPUs:       1,
		MaxNumSeqs: 8,
	}
}

// TestRealEnginePlan writes the plan to $HB_REAL_PLAN_OUT. Each Instance's
// file list is the identity record's, read from the Hub by package hubfiles
// (HBEP-3), not by the agent's stager.
func TestRealEnginePlan(t *testing.T) {
	out := os.Getenv("HB_REAL_PLAN_OUT")
	if out == "" {
		t.Skip("HB_REAL_PLAN_OUT is not set: the plan is written only for the real-engine run (issue #94)")
	}
	hub, err := hubfiles.New(hubfiles.Config{})
	if err != nil {
		t.Fatal(err)
	}
	plan := realPlan{
		ProbeInterval: agentapi.Duration(10 * time.Second), // profiles/helios.yaml, health.probeInterval
		StopGrace:     agentapi.Duration(10 * time.Second), // profiles/helios.yaml, restart.stopGrace
	}
	for _, m := range realModels {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		entries, err := hub.Files(ctx, m.repo, m.rev)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", m.repo, err)
		}
		files := make([]agentapi.FileDigest, 0, len(entries))
		for _, e := range entries {
			files = append(files, agentapi.FileDigest{Path: e.Path, SHA256: e.SHA256})
		}
		plan.Instances = append(plan.Instances, realInstance{
			Label: m.label, Launch: m.launch(), Files: files,
			Qualify:          agentapi.QualifyRequest{Shapes: m.shapes, ToolCalls: true},
			WarmUp:           agentapi.WarmUpRequest{Shapes: m.shapes, ToolCalls: true, TargetSessions: 8},
			CaptureMaxTokens: m.captureMaxTokens,
		})
	}
	b, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ---- the controller's judgements, as HBEP-7 states them ----

// declaredFlags reads a launch's --flag=value and --flag value arguments; a
// flag with no value is declared "true".
func declaredFlags(cmd []string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(cmd); i++ {
		a, ok := strings.CutPrefix(cmd[i], "--")
		if !ok {
			continue
		}
		if k, v, ok := strings.Cut(a, "="); ok {
			out[k] = v
			continue
		}
		if i+1 < len(cmd) && !strings.HasPrefix(cmd[i+1], "--") {
			out[a] = cmd[i+1]
			i++
			continue
		}
		out[a] = "true"
	}
	return out
}

// settingVerdict compares a declared value with the reported one under HBEP-7's
// rule (Instance start, Settings comparison): booleans if both read as
// booleans, else numbers if both read as decimal numbers, else verbatim. A
// declared auto is recorded as resolved, not compared.
func settingVerdict(declared, reported string) string {
	if declared == "auto" {
		return "resolved"
	}
	db, derr := strconv.ParseBool(strings.ToLower(declared))
	rb, rerr := strconv.ParseBool(strings.ToLower(reported))
	boolish := func(s string) bool { l := strings.ToLower(s); return l == "true" || l == "false" }
	if derr == nil && rerr == nil && boolish(declared) && boolish(reported) {
		return verdict(db == rb)
	}
	dn, derr := strconv.ParseFloat(declared, 64)
	rn, rerr := strconv.ParseFloat(reported, 64)
	if derr == nil && rerr == nil {
		return verdict(dn == rn)
	}
	return verdict(declared == reported)
}

func verdict(ok bool) string {
	if ok {
		return "equal"
	}
	return "contradiction"
}

// settled applies HBEP-7's warm-up rule to the last two rounds, as the
// controller does (agentapi.WarmUpSettled): only the fixed-length shapes
// count, and one whose mean Total rose by more than max(10% of the round
// before's, 100 ms) means not settled. A failed request in any round fails
// warm-up before the controller asks about settling (judgeWarmUp), so it is
// never settled here either.
func settled(rounds []agentapi.WarmUpRound) bool {
	if len(rounds) < 2 {
		return false
	}
	for _, r := range rounds {
		for _, q := range r.Requests {
			if q.Error != "" {
				return false
			}
		}
	}
	return agentapi.WarmUpSettled(rounds[len(rounds)-2], rounds[len(rounds)-1])
}

// throughput is the last round's concurrent requests' mean time to first
// token and mean time per output token, as agentapi.WarmUpRound documents.
type throughput struct {
	TTFTSeconds float64 `json:"ttftSeconds"`
	TTFTCount   int     `json:"ttftCount"`
	TPOTSeconds float64 `json:"tpotSeconds"`
	TPOTCount   int     `json:"tpotCount"`
}

func roundThroughput(r agentapi.WarmUpRound, shape agentapi.Shape) throughput {
	var t throughput
	for _, q := range r.Requests {
		if q.Shape != shape || q.Error != "" {
			continue
		}
		ttft := time.Duration(q.TimeToFirstToken).Seconds()
		if q.OutputTokens >= 1 {
			t.TTFTSeconds += ttft
			t.TTFTCount++
		}
		if q.OutputTokens >= 2 {
			t.TPOTSeconds += (time.Duration(q.Total).Seconds() - ttft) / float64(q.OutputTokens-1)
			t.TPOTCount++
		}
	}
	if t.TTFTCount > 0 {
		t.TTFTSeconds /= float64(t.TTFTCount)
	}
	if t.TPOTCount > 0 {
		t.TPOTSeconds /= float64(t.TPOTCount)
	}
	return t
}

// deploymentIDFor is a stand-in deployment ID: 64 hex characters, distinct per
// Instance. The run checks the engine, not the ID scheme.
func deploymentIDFor(label string) string {
	s := sha256.Sum256([]byte("issue-94/" + label))
	return hex.EncodeToString(s[:])
}

// The judgements on hand-made inputs, so the run's verdicts are not the
// code's say-so.
func TestRealEngineJudgements(t *testing.T) {
	for _, c := range []struct{ declared, reported, want string }{
		{"0.50", "0.5", "equal"},
		{"true", "True", "equal"},
		{"16", "16", "equal"},
		{"auto", "fp8_e4m3", "resolved"},
		{"fp8", "fp8_e4m3", "contradiction"},
		{"4096", "4096", "equal"},
		{"true", "False", "contradiction"},
		{"1", "True", "contradiction"}, // a number and a boolean compare verbatim
	} {
		if got := settingVerdict(c.declared, c.reported); got != c.want {
			t.Errorf("settingVerdict(%q, %q) = %s, want %s", c.declared, c.reported, got, c.want)
		}
	}
	got := declaredFlags([]string{"vllm", "serve", "/m", "--a=1", "--b", "--c", "x", "--d"})
	if len(got) != 4 || got["a"] != "1" || got["b"] != "true" || got["c"] != "x" || got["d"] != "true" {
		t.Errorf("declaredFlags = %v", got)
	}
	sec := func(f float64) agentapi.Duration { return agentapi.Duration(time.Duration(f * float64(time.Second))) }
	r1 := agentapi.WarmUpRound{Round: 1, Requests: []agentapi.RequestTiming{{Shape: "short", Total: sec(1)}, {Shape: "typical", Total: sec(2)}}}
	r2 := agentapi.WarmUpRound{Round: 2, Requests: []agentapi.RequestTiming{{Shape: "short", Total: sec(1.09)}, {Shape: "typical", Total: sec(1.5)}}}
	r3 := agentapi.WarmUpRound{Round: 3, Requests: []agentapi.RequestTiming{{Shape: "short", Total: sec(1.25)}, {Shape: "typical", Total: sec(1.5)}}}
	r4 := agentapi.WarmUpRound{Round: 4, Requests: []agentapi.RequestTiming{{Shape: "short", Total: sec(1), Error: "cut"}, {Shape: "typical", Total: sec(1.5)}}}
	if settled([]agentapi.WarmUpRound{r1}) || !settled([]agentapi.WarmUpRound{r1, r2}) || settled([]agentapi.WarmUpRound{r2, r3}) || settled([]agentapi.WarmUpRound{r3, r4}) {
		t.Error("settled: want false, true, false, false")
	}
	// The controller's rule (#138, HBEP-7), each case by hand: a tool call 10x
	// slower is not judged; a rise under the 100 ms floor settles (0.2 s to
	// 0.25 s, #94's run: 10% of 0.2 s is only 20 ms); a rise of 110 ms on 0.2 s
	// does not; the fixed-length shapes must all be there in both rounds; and a
	// failed request in an earlier round keeps warm-up from ever settling.
	at := func(n int32, short, tool float64) agentapi.WarmUpRound {
		return agentapi.WarmUpRound{Round: n, Requests: []agentapi.RequestTiming{
			{Shape: "short", Total: sec(short)}, {Shape: "toolCall", Total: sec(tool)}, {Shape: "concurrent", Total: sec(2)}, {Shape: "concurrent", Total: sec(4)}}}
	}
	for _, tc := range []struct {
		name   string
		rounds []agentapi.WarmUpRound
		want   bool
	}{
		{"the tool call 10x slower is not judged", []agentapi.WarmUpRound{at(1, 1, 1), at(2, 1, 10)}, true},
		{"a rise under the floor settles", []agentapi.WarmUpRound{at(1, 0.2, 1), at(2, 0.25, 1)}, true},
		{"a rise of 110 ms on 0.2 s does not", []agentapi.WarmUpRound{at(1, 0.2, 1), at(2, 0.31, 1)}, false},
		{"a rise of 11% on 2 s does not", []agentapi.WarmUpRound{at(1, 2, 1), at(2, 2.22, 1)}, false},
		{"a rise of 9% on 2 s does", []agentapi.WarmUpRound{at(1, 2, 1), at(2, 2.18, 1)}, true},
		{"a shape missing from the new round", []agentapi.WarmUpRound{at(1, 1, 1), {Round: 2, Requests: []agentapi.RequestTiming{{Shape: "short", Total: sec(1)}}}}, false},
		{"only the last two rounds are compared", []agentapi.WarmUpRound{at(1, 1, 1), at(2, 5, 1), at(3, 5.1, 1)}, true},
		{"an error in an earlier round", []agentapi.WarmUpRound{r4, at(5, 1, 1), at(6, 1, 1)}, false},
	} {
		if got := settled(tc.rounds); got != tc.want {
			t.Errorf("settled, %s: %v, want %v", tc.name, got, tc.want)
		}
	}
	// (Total - TTFT) / (OutputTokens - 1): (1.0 - 0.2) / 4 = 0.2, and (0.5 - 0.1) / 1 = 0.4.
	tp := roundThroughput(agentapi.WarmUpRound{Requests: []agentapi.RequestTiming{
		{Shape: "concurrent", TimeToFirstToken: sec(0.2), Total: sec(1.0), OutputTokens: 5},
		{Shape: "concurrent", TimeToFirstToken: sec(0.1), Total: sec(0.5), OutputTokens: 2},
		{Shape: "concurrent", TimeToFirstToken: sec(0.3), Total: sec(0.3), OutputTokens: 1},
		{Shape: "concurrent", Error: "x", OutputTokens: 9},
		{Shape: "short", TimeToFirstToken: sec(9), Total: sec(9), OutputTokens: 9},
	}}, "concurrent")
	if tp.TTFTCount != 3 || tp.TPOTCount != 2 || abs(tp.TTFTSeconds-0.2) > 1e-9 || abs(tp.TPOTSeconds-0.3) > 1e-9 {
		t.Errorf("throughput %+v, want TTFT 0.2 s over 3, TPOT 0.3 s over 2", tp)
	}
	// The length probe's finish reasons, from streams written by hand: a cut
	// stream whose finish chunk carries the last token, as Qwen3's did in the
	// first run, a stopped one, and no stream at all.
	cut := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"2\"},\"finish_reason\":\"length\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"completion_tokens\":4}}\n\ndata: [DONE]\n\n"
	stopped := "data: {\"choices\":[{\"delta\":{\"content\":\"Ready.\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	if got := streamFinishes([]byte(cut)); len(got) != 1 || got[0] != "length" {
		t.Errorf("streamFinishes(cut) = %v, want [length]", got)
	}
	if got := streamFinishes([]byte(stopped)); len(got) != 1 || got[0] != "stop" {
		t.Errorf("streamFinishes(stopped) = %v, want [stop]", got)
	}
	if got := streamFinishes(nil); len(got) != 0 {
		t.Errorf("streamFinishes(nil) = %v, want none", got)
	}
}

// The launch's environment leaves the cache variables to the agent, which sets
// them itself and lets a launch's own value override its default; the names are
// HBEP-7's (What it runs: Home and caches), written out here.
func TestRealEnvLeavesTheCachesToTheAgent(t *testing.T) {
	got := map[string]string{}
	for _, e := range realEnv() {
		got[e.Name] = e.Value
	}
	for _, name := range []string{"XDG_CACHE_HOME", "VLLM_CACHE_ROOT", "TORCHINDUCTOR_CACHE_DIR", "TRITON_CACHE_DIR", "FLASHINFER_WORKSPACE_BASE", "CUDA_CACHE_PATH"} {
		if v, set := got[name]; set {
			t.Errorf("realEnv sets %s=%s: the agent's own default must be what the run exercises", name, v)
		}
	}
	want := map[string]string{"HF_HUB_OFFLINE": "1", "VLLM_NO_USAGE_STATS": "1", "DO_NOT_TRACK": "1", "VLLM_USE_FLASHINFER_SAMPLER": "0"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("realEnv = %v, want %v", got, want)
	}
	for _, m := range realModels {
		for _, e := range m.launch().Env {
			if strings.HasSuffix(e.Name, "_CACHE_DIR") || strings.HasSuffix(e.Name, "_CACHE_HOME") || strings.HasSuffix(e.Name, "_CACHE_ROOT") {
				t.Errorf("%s: launch sets %s", m.label, e.Name)
			}
		}
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
