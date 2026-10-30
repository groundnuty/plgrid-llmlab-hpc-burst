package deploymentid

import "fmt"

// class says what scheme v1 does with a flag or an environment variable.
type class int

const (
	// classInput: the setting is hashed into the ID. It is also the class of
	// every name the table does not list (HBEP-3, open question 3).
	classInput class = iota
	// classNotInput: capacity, transport, placement or logging. The setting is
	// accepted and left out of the ID.
	classNotInput
	// classFrameworkPassed: the framework passes the setting itself, from the
	// checkpoint and the chat template, which are hashed elsewhere in the
	// object. A serving spec that lists it is refused.
	classFrameworkPassed
	// classNever: a key. It is refused; a key is referenced, never written in
	// the spec, so that rotating it never changes an ID.
	classNever
)

func (c class) String() string {
	switch c {
	case classInput:
		return "input"
	case classNotInput:
		return "not an input"
	case classFrameworkPassed:
		return "passed by the framework"
	case classNever:
		return "never"
	}
	return fmt.Sprintf("class(%d)", int(c))
}

// rule is one row of the classification table.
type rule struct {
	name   string
	class  class
	env    bool   // an environment variable; otherwise an engine flag
	reason string // the table's reason column, for error messages
}

// engineVLLM is the engine name that selects the vLLM table.
const engineVLLM = "vllm"

// vllmV0320 is the scheme v1 classification table for vLLM v0.32.0. It is
// HBEP-3's table (Deployment ID, "The v1 table for vLLM"), row for row; a test
// compares the two. Flag names are those of vLLM v0.32.0, without leading
// dashes. A name the table does not list is an input. Changing a row is a new
// scheme version.
var vllmV0320 = []rule{
	{"dtype", classInput, false, "sets the arithmetic precision"},
	{"quantization", classInput, false, "changes the weights the model computes with"},
	{"kv-cache-dtype", classInput, false, "sets the precision of the attention cache"},
	{"max-model-len", classInput, false, "bounds the tokens the model sees"},
	{"tokenizer", classInput, false, "decides the tokens the model sees"},
	{"tokenizer-mode", classInput, false, "decides the tokens the model sees"},
	{"tokenizer-revision", classInput, false, "decides the tokens the model sees"},
	{"trust-remote-code", classInput, false, "runs the checkpoint's own model code"},
	{"load-format", classInput, false, "decides how weights load; dummy serves random weights"},
	{"hf-overrides", classInput, false, "changes the model's configuration, RoPE scaling included"},
	{"enforce-eager", classInput, false, "changes the compiled kernels, so the arithmetic"},
	{"compilation-config", classInput, false, "changes the compiled kernels, so the arithmetic"},
	{"attention-backend", classInput, false, "picks the attention kernels"},
	{"tensor-parallel-size", classInput, false, "sets the arithmetic layout"},
	{"pipeline-parallel-size", classInput, false, "sets the arithmetic layout"},
	{"enable-expert-parallel", classInput, false, "sets the arithmetic layout"},
	{"speculative-config", classInput, false, "changes how output tokens are computed"},
	{"seed", classInput, false, "changes sampled outputs"},
	{"generation-config", classInput, false, "sets the default sampling parameters"},
	{"override-generation-config", classInput, false, "sets the default sampling parameters"},
	{"tool-call-parser", classInput, false, "turns the model's tokens into the response's tool calls"},
	{"enable-auto-tool-choice", classInput, false, "turns the model's tokens into the response's tool calls"},
	{"reasoning-parser", classInput, false, "turns the model's tokens into the response's reasoning"},
	{"chat-template-content-format", classInput, false, "changes how messages render into tokens"},
	{"default-chat-template-kwargs", classInput, false, "changes how messages render into tokens"},
	{"VLLM_USE_DEEP_GEMM", classInput, true, "picks the matrix kernels"},

	{"model", classFrameworkPassed, false, "the framework passes it from checkpoint.repository"},
	{"revision", classFrameworkPassed, false, "the framework passes it from checkpoint.revision"},
	{"chat-template", classFrameworkPassed, false, "the framework passes the chat template itself"},

	{"enable-prefix-caching", classNotInput, false, "capacity"},
	{"max-num-seqs", classNotInput, false, "capacity: batch shaping"},
	{"max-num-batched-tokens", classNotInput, false, "capacity: batch shaping"},
	{"enable-chunked-prefill", classNotInput, false, "capacity: batch shaping"},
	{"gpu-memory-utilization", classNotInput, false, "capacity: a memory budget"},
	{"kv-cache-memory-bytes", classNotInput, false, "capacity: a memory budget"},
	{"cpu-offload-gb", classNotInput, false, "capacity: a memory budget"},
	{"host", classNotInput, false, "transport: where the engine listens"},
	{"port", classNotInput, false, "transport: where the engine listens"},
	{"uds", classNotInput, false, "transport: where the engine listens"},
	{"served-model-name", classNotInput, false, "transport: the front maps model names"},
	{"download-dir", classNotInput, false, "placement: paths, devices and how workers start"},
	{"distributed-executor-backend", classNotInput, false, "placement: paths, devices and how workers start"},
	{"CUDA_VISIBLE_DEVICES", classNotInput, true, "placement: paths, devices and how workers start"},
	{"VLLM_CACHE_ROOT", classNotInput, true, "placement: paths, devices and how workers start"},
	{"HF_HOME", classNotInput, true, "placement: paths, devices and how workers start"},
	{"log-level", classNotInput, false, "logging: changes no output"},
	{"disable-log-stats", classNotInput, false, "logging: changes no output"},
	{"enable-log-requests", classNotInput, false, "logging: changes no output"},
	{"VLLM_LOGGING_LEVEL", classNotInput, true, "logging: changes no output"},

	{"api-key", classNever, false, "a key"},
	{"hf-token", classNever, false, "a key"},
	{"HF_TOKEN", classNever, true, "a key"},
}

// table is a classification table indexed for lookup. Flags and environment
// variables are separate namespaces.
type table struct {
	flags map[string]rule
	env   map[string]rule
}

// newTable indexes rules. It rejects a name listed twice in one namespace.
func newTable(rules []rule) (*table, error) {
	t := &table{flags: map[string]rule{}, env: map[string]rule{}}
	for _, r := range rules {
		m := t.flags
		if r.env {
			m = t.env
		}
		if _, dup := m[r.name]; dup {
			return nil, fmt.Errorf("classification table lists %q twice", r.name)
		}
		m[r.name] = r
	}
	return t, nil
}

// classify returns the class of a flag or, with env true, of an environment
// variable, and the matching rule's reason (empty for an unlisted name).
func (t *table) classify(name string, env bool) (class, string) {
	m := t.flags
	if env {
		m = t.env
	}
	if r, ok := m[name]; ok {
		return r.class, r.reason
	}
	return classInput, ""
}

// tables maps an engine name to its classification table. The table is per
// engine (HBEP-3, open question 3); vLLM's is the only one in alpha.
var tables = func() map[string]*table {
	t, err := newTable(vllmV0320)
	if err != nil {
		panic("deploymentid: " + err.Error()) // static data, covered by a test
	}
	return map[string]*table{engineVLLM: t}
}()
