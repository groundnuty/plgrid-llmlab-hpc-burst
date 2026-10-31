package enginedouble

import (
	"fmt"
	"time"
)

// Version is the vLLM version the double imitates (/version).
const Version = "0.32.0"

// Config configures Handler and Main. Use DefaultConfig for the documented
// defaults: a Config literal leaves PrefixCaching false and TokenDelay zero,
// because a zero value cannot say "default" for those. The other zero values
// (MaxModelLen, KVCacheTokens, BlockSize, CacheDtype, TemplateOverhead and
// NaturalLen) do mean "default".
type Config struct {
	ServedName string
	ModelPath  string
	// APIKey, when set, guards /v1 and the other prefixes vLLM guards.
	APIKey string

	// MaxModelLen is what the double enforces; default 32768.
	MaxModelLen int
	// ReportedMaxModelLen is what /v1/models and /tokenize report; 0 means
	// MaxModelLen. A different value contradicts the flag the engine was
	// started with.
	ReportedMaxModelLen int
	// MaxNumSeqs caps the requests running at once; a request beyond it
	// waits, and is counted in vllm:num_requests_waiting. 0 means no cap.
	MaxNumSeqs int

	// KVCacheTokens is the engine's KV capacity in tokens; default 1<<20.
	// kv_cache_size_tokens reports it as it is, and num_gpu_blocks reports
	// it divided by BlockSize.
	KVCacheTokens int64
	BlockSize     int    // default 16
	CacheDtype    string // default "auto"
	PrefixCaching bool

	// TemplateOverhead is the token count of one empty user message with the
	// generation prompt; default 9, at least 2.
	TemplateOverhead int
	// NaturalLen is how many tokens the model says before it stops of its
	// own accord, with or without ignore_eos; default 24. The early-stop
	// mode sets it.
	NaturalLen int
	// TokenDelay is the wait before each generated token; default 1 ms.
	TokenDelay time.Duration

	// The modes of HB_ENGINE_DOUBLE that change a request's answer.
	GreedyDiverge    bool // the k-th repeat of a greedy request differs from the first from GreedyDivergeAt on
	GreedyDivergeAt  int
	GreedyRepeat     bool          // greedy output is one token repeated
	LoopAbove        int           // a prompt above this many tokens (0: off) gets a 3-token cycle, unless ignore_eos
	ToolMalformed    bool          // tool-call arguments are invalid JSON
	ToolArgsNull     bool          // tool-call arguments are the JSON null
	ToolArgsBadType  bool          // tool-call arguments are {"city": 42}: the wrong type for the weather tool
	NoToolCall       bool          // answer with text even when tools are given
	ReportsNothing   bool          // no /version, no max_model_len, no cache_config_info, no system_fingerprint
	HealthFail       bool          // /health answers 503
	ErrorsOnly       bool          // chat answers 500
	SlowFirst        time.Duration // the first request of each shape waits this long before its first token
	NeverSettle      int           // each request of a shape waits this percent of TokenDelay*tokens longer than the last
	Stall            bool          // a streamed request stops after its first token and hangs
	ReasoningFirst   int           // the reply reasons for this many tokens (0: off) before its answer
	ReasoningForever bool          // the reply reasons until max_tokens runs out, with no answer

	// Startup modes, read by Main only.
	StartDelay   time.Duration // create the socket only after this
	ExitAfter    time.Duration // exit by itself after this, when ExitAfterSet
	ExitAfterSet bool
	ExitCode     int  // the exit code of ExitAfter
	IgnoreTerm   bool // ignore SIGTERM, with a child process that ignores it too
}

// DefaultConfig returns the documented defaults: vLLM's 32768-token context,
// 2^20 tokens of KV cache in 16-token blocks, prefix caching on, a 9-token
// template, a model that stops after 24 tokens, and 1 ms per token.
func DefaultConfig() Config {
	return Config{
		ServedName:       "model",
		ModelPath:        "/hpcburst/model",
		MaxModelLen:      32768,
		KVCacheTokens:    1 << 20,
		BlockSize:        16,
		CacheDtype:       "auto",
		PrefixCaching:    true,
		TemplateOverhead: 9,
		NaturalLen:       24,
		TokenDelay:       time.Millisecond,
	}
}

// withDefaults fills the zero values that mean "default".
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxModelLen == 0 {
		c.MaxModelLen = d.MaxModelLen
	}
	if c.KVCacheTokens == 0 {
		c.KVCacheTokens = d.KVCacheTokens
	}
	if c.BlockSize == 0 {
		c.BlockSize = d.BlockSize
	}
	if c.CacheDtype == "" {
		c.CacheDtype = d.CacheDtype
	}
	if c.TemplateOverhead == 0 {
		c.TemplateOverhead = d.TemplateOverhead
	}
	if c.NaturalLen == 0 {
		c.NaturalLen = d.NaturalLen
	}
	return c
}

// Validate reports a Config that cannot work. Handler panics on one.
func (c Config) Validate() error {
	c = c.withDefaults()
	switch {
	case c.MaxModelLen < 2:
		return fmt.Errorf("MaxModelLen %d is below 2", c.MaxModelLen)
	case c.BlockSize < 1:
		return fmt.Errorf("BlockSize %d is below 1", c.BlockSize)
	case c.TemplateOverhead < 2:
		return fmt.Errorf("TemplateOverhead %d is below 2: one empty message alone is 2 tokens", c.TemplateOverhead)
	case c.NaturalLen < 1:
		return fmt.Errorf("NaturalLen %d is below 1", c.NaturalLen)
	case c.TokenDelay < 0 || c.SlowFirst < 0 || c.StartDelay < 0:
		return fmt.Errorf("a duration is negative")
	case c.NeverSettle < 0 || c.LoopAbove < 0 || c.GreedyDivergeAt < 0 || c.ReasoningFirst < 0:
		return fmt.Errorf("a mode value is negative")
	}
	return nil
}

func (c Config) reportedMaxModelLen() int {
	if c.ReportedMaxModelLen != 0 {
		return c.ReportedMaxModelLen
	}
	return c.MaxModelLen
}
