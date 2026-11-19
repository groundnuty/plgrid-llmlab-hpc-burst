package lifecycle

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// flagName normalises an engine flag's name as vLLM reads it: "_" is "-"
// (HBEP-3; the API's EngineFlag).
func flagName(n string) string { return strings.ReplaceAll(n, "_", "-") }

// flagValue returns the last value given for a flag, by normalised name, and
// whether it was given. A flag listed twice takes the last value, as argparse
// does.
func flagValue(s *v1alpha1.ServingSpec, name string) (string, bool) {
	v, ok := "", false
	for _, f := range s.Flags {
		if flagName(f.Name) == name {
			v, ok = f.Value, true
		}
	}
	return v, ok
}

// intFlag reads a positive integer flag, def when absent.
func intFlag(s *v1alpha1.ServingSpec, name string, def int32) (int32, error) {
	v, ok := flagValue(s, name)
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s %q is not a positive integer", name, v)
	}
	return int32(n), nil
}

// frameworkFlags are the flags the launch sets itself (HBEP-7, What it runs):
// the checkpoint, the socket, the served name and the template. A spec that
// sets one is refused rather than overridden.
var frameworkFlags = []string{"model", "revision", "chat-template", "uds", "host", "port", "served-model-name"}

// shapes are a workload's request shapes with the defaults applied. longest
// is nil when the spec leaves it to verification (HBEP-7: the resolved
// context limit minus the typical output minus the template overhead).
type shapes struct {
	short, typical, outputTypical, outputLongest int32
	longest                                      *int32
	targetSessions                               int32
	toolCalls                                    bool
}

// servedPlan is what a served spec gives the controller before any provider is
// chosen: the GPU count, the declared max-model-len and the shapes.
type servedPlan struct {
	gpus        int32 // each worker's: tensor-parallel-size times pipeline-parallel-size
	workers     int32 // spec.workersPerInstance, 1 when absent (HBEP-7, Several workers)
	maxNumSeqs  int32
	maxModelLen *int32
	shapes      shapes
}

// unsupported is a spec the controller refuses (Degraded, SpecUnsupported):
// nothing is provisioned for it.
type unsupported struct{ msg string }

func (u *unsupported) Error() string { return u.msg }

func refuse(format string, a ...any) error { return &unsupported{msg: fmt.Sprintf(format, a...)} }

// checkServed checks a served spec before provisioning (HBEP-7, Provisioning
// and What the controller reads). Every error it returns is *unsupported.
//
// Admission (the API's EngineFlag rules) checks seven boolean flags, those of
// HBEP-3's table, for the value "true" or "false". The controller checks every
// boolean of the engine image's own list (enginebooleans.go): admission refuses
// a bad value on the seven first, so this refusal is what is left for the
// rest, and for a repeated name and a "false" the image's list cannot place.
func checkServed(md *v1alpha1.ModelDeployment) (servedPlan, error) {
	s := md.Spec.Source.Served
	if s == nil {
		return servedPlan{}, refuse("the deployment has no served source")
	}
	for _, f := range s.Flags {
		for _, own := range frameworkFlags {
			if flagName(f.Name) == own {
				return servedPlan{}, refuse("engine flag %s is set by the framework's launch and cannot be given in the serving spec", f.Name)
			}
		}
	}
	// The boolean flags of the engine image's own parser (HBEP-3, What admission
	// checks; amended 2026-11-18, #788): a value other than "true" or "false",
	// a boolean named twice, and "false" for an image with no dump file are
	// refused here, before anything is provisioned.
	if err := refuseBooleans(s.Flags, engineBooleanDumps.forImage(s.Engine.ImageDigest)); err != nil {
		return servedPlan{}, err
	}
	if err := checkGenerationConfig(s); err != nil {
		return servedPlan{}, err
	}
	dp, err := intFlag(s, "data-parallel-size", 1)
	if err != nil {
		return servedPlan{}, refuse("%v", err)
	}
	if dp > 1 {
		return servedPlan{}, refuse("data-parallel-size %d: alpha runs one engine worker per Instance, so data-parallel-size above 1 is refused (HBEP-7, Provisioning), and an engine with it would export several vllm:cache_config_info series, where the KV cache size is read from one (HBEP-7, The KV cache size)", dp)
	}
	tp, err := intFlag(s, "tensor-parallel-size", 1)
	if err != nil {
		return servedPlan{}, refuse("%v", err)
	}
	pp, err := intFlag(s, "pipeline-parallel-size", 1)
	if err != nil {
		return servedPlan{}, refuse("%v", err)
	}
	seqs, err := intFlag(s, "max-num-seqs", v1alpha1.DefaultMaxNumSeqs)
	if err != nil {
		return servedPlan{}, refuse("%v", err)
	}
	plan := servedPlan{gpus: tp * pp, maxNumSeqs: seqs, workers: md.Spec.WorkersPerInstanceCount()}
	if plan.workers < 1 {
		return servedPlan{}, refuse("workersPerInstance %d: an Instance needs at least one worker (HBEP-7, Several workers)", plan.workers)
	}
	if _, ok := flagValue(s, "max-model-len"); ok {
		mml, err := intFlag(s, "max-model-len", 0)
		if err != nil {
			return servedPlan{}, refuse("%v", err)
		}
		plan.maxModelLen = &mml
	}
	if md.Spec.Workload == nil {
		return servedPlan{}, refuse("a served deployment needs spec.workload, the shapes for warm-up and qualification (HBEP-7)")
	}
	plan.shapes = resolveShapes(md)
	if err := plan.shapes.validate(); err != nil {
		return servedPlan{}, err
	}
	if plan.maxModelLen != nil {
		if err := plan.shapes.fit(*plan.maxModelLen, 0); err != nil {
			return servedPlan{}, err
		}
	}
	return plan, nil
}

// checkGenerationConfig refuses a serving spec that changes the generation
// config vLLM applies in a way the checks do not model: generation-config
// other than auto (vllm drops the file's values, a folder or a Hub repository
// is neither staged nor hashed), override-generation-config (an override merges
// on top, and its max_new_tokens caps every request below what the checks ask
// for) and hf-config-path (the config comes from another, separately pinned
// repository). The value is compared exactly: vLLM reads anything but auto as
// a path (HBEP-7, The allocation agent, What it runs; `@hpc-burst-science-agent`,
// 1 Nov 2026, for #169). They stay deployment-ID inputs (HBEP-3), so a later
// version can allow them.
func checkGenerationConfig(s *v1alpha1.ServingSpec) error {
	if v, ok := flagValue(s, "generation-config"); ok && v != "auto" {
		return refuse("generation-config %q: alpha serves with vLLM's default, generation-config auto, so any other value is refused (HBEP-7, What it runs)", v)
	}
	for _, name := range []string{"override-generation-config", "hf-config-path"} {
		if _, ok := flagValue(s, name); ok {
			return refuse("engine flag %s changes the generation config vLLM applies in a way the checks do not model, so alpha refuses it (HBEP-7, What it runs)", name)
		}
	}
	return nil
}

func resolveShapes(md *v1alpha1.ModelDeployment) shapes {
	w := md.Spec.Workload
	sh := shapes{
		short: defaultContextShort, typical: w.ContextTokens.Typical, longest: w.ContextTokens.Longest,
		outputTypical: w.OutputTokens.Typical, outputLongest: defaultOutputLongest, targetSessions: w.TargetSessions,
	}
	if w.ContextTokens.Short != nil {
		sh.short = *w.ContextTokens.Short
	}
	if w.OutputTokens.Longest != nil {
		sh.outputLongest = *w.OutputTokens.Longest
	}
	if w.ToolCalls != nil {
		sh.toolCalls = *w.ToolCalls
	} else if v, ok := flagValue(md.Spec.Source.Served, "enable-auto-tool-choice"); ok {
		sh.toolCalls = v == "true"
	}
	return sh
}

func (sh shapes) validate() error {
	for _, v := range []struct {
		name string
		n    int32
	}{{"contextTokens.short", sh.short}, {"contextTokens.typical", sh.typical}, {"outputTokens.typical", sh.outputTypical},
		{"outputTokens.longest", sh.outputLongest}, {"targetSessions", sh.targetSessions}} {
		if v.n < 1 {
			return refuse("workload %s is %d; it must be at least 1", v.name, v.n)
		}
	}
	if sh.targetSessions > agentapi.MaxTargetSessions {
		return refuse("workload targetSessions is %d; at most %d", sh.targetSessions, agentapi.MaxTargetSessions)
	}
	if sh.longest != nil && *sh.longest < 1 {
		return refuse("workload contextTokens.longest is %d; it must be at least 1", *sh.longest)
	}
	return nil
}

// fit checks every shape against a context limit, each with the template's
// overhead (0 before verification measures it): each context length with the
// typical output, and the typical context with the longest output (HBEP-7,
// What the controller reads: every shape must fit, and a shape that does not
// is an error, never shortened). A longest context left to verification is
// skipped.
func (sh shapes) fit(limit, overhead int32) error {
	type shape struct {
		name    string
		in, out int32
	}
	all := []shape{
		{"the short context with the typical output", sh.short, sh.outputTypical},
		{"the typical context with the typical output", sh.typical, sh.outputTypical},
		{"the typical context with the longest output", sh.typical, sh.outputLongest},
	}
	if sh.longest != nil {
		all = append(all, shape{"the longest context with the typical output", *sh.longest, sh.outputTypical})
	}
	for _, s := range all {
		if total := int64(s.in) + int64(s.out) + int64(overhead); total > int64(limit) {
			return refuse("workload shape %s needs %d+%d tokens plus a template overhead of %d, %d in all, above max-model-len %d", s.name, s.in, s.out, overhead, total, limit)
		}
	}
	return nil
}

// resolved returns the shapes the agent is sent once verification has fixed
// the context limit and the template overhead.
func (sh shapes) resolved(limit, overhead int32) (agentapi.Shapes, error) {
	longest := limit - sh.outputTypical - overhead
	if sh.longest != nil {
		longest = *sh.longest
	}
	if longest < 1 {
		return agentapi.Shapes{}, refuse("the longest context resolves to %d tokens (max-model-len %d minus the typical output %d minus the overhead %d)", longest, limit, sh.outputTypical, overhead)
	}
	withLongest := sh
	withLongest.longest = &longest
	if err := withLongest.fit(limit, overhead); err != nil {
		return agentapi.Shapes{}, err
	}
	return agentapi.Shapes{
		ContextShort: sh.short, ContextTypical: sh.typical, ContextLongest: longest,
		OutputTypical: sh.outputTypical, OutputLongest: sh.outputLongest,
	}, nil
}

// renderLaunch renders an Instance's launch from the serving spec (HBEP-7,
// What the controller reads; The allocation agent, What it runs). The command
// is vLLM's server on the checkpoint bound at ContainerModelDir, listening on
// worker-0's Unix socket, serving the deployment's model name, then the spec's
// flags in order, each as one --name=value argument, then --max-num-seqs=<n>:
// the spec's value, else the default.
//
// A boolean flag (one the engine image's parser dump lists; HBEP-3, What
// admission checks, amended 2026-11-18, #788) is never --name=value, which
// vLLM's argparse refuses: "true" is the bare --name, and "false" is
// --no-name for an optional boolean and no argument for a store_true or a
// store_false (checkServed has refused any other value, a repeated boolean,
// and "false" for an image with no dump). A name the list does not classify as
// a boolean renders as --name=value, a "false" value included.
func renderLaunch(md *v1alpha1.ModelDeployment, plan servedPlan) v1alpha1.Launch {
	s := md.Spec.Source.Served
	cmd := []string{"vllm", "serve", agentapi.ContainerModelDir,
		"--uds", agentapi.ContainerEngineSocket(agentapi.WorkerName(0)),
		"--served-model-name", md.Spec.Model}
	if s.ChatTemplate != nil {
		cmd = append(cmd, "--chat-template", agentapi.ContainerChatTemplate)
	}
	cmd = append(cmd, renderServedFlags(s.Flags, engineBooleanDumps.forImage(s.Engine.ImageDigest))...)
	cmd = append(cmd, "--max-num-seqs="+strconv.Itoa(int(plan.maxNumSeqs)))
	var env []v1alpha1.LaunchEnvVar
	for _, e := range s.Env {
		env = append(env, v1alpha1.LaunchEnvVar{Name: e.Name, Value: e.Value})
	}
	var tmpl *string
	if s.ChatTemplate != nil {
		t := *s.ChatTemplate
		tmpl = &t
	}
	return v1alpha1.Launch{
		Image: s.Engine.Image, ImageDigest: s.Engine.ImageDigest,
		Command: cmd, Env: env, KeyEnv: keyEnv,
		Checkpoint:   v1alpha1.LaunchCheckpoint{Repository: s.Checkpoint.Repository, Revision: s.Checkpoint.Revision},
		ChatTemplate: tmpl, GPUs: plan.gpus, MaxNumSeqs: plan.maxNumSeqs, Workers: launchWorkers(plan.workers),
	}
}

// launchWorkers is launch.workers for an Instance of n workers: set only above 1, so an Instance of one
// worker has the launch it had before workers were counted (HBEP-7, Several workers).
func launchWorkers(n int32) *int32 {
	if n <= 1 {
		return nil
	}
	return &n
}
