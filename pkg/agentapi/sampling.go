package agentapi

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

// The sampling parameters of the checks (HBEP-7, The allocation agent, Checks,
// "Sampling parameters of the checks"; science-agent rulings, 1 Nov 2026). The
// rule lives here, in the package both the agent and the lifecycle controller
// import, so the agent's requests and the controller's verification record are
// computed by one function from the same inputs: the staged
// generation_config.json's sampling keys (VerifyResult.GenerationConfig), its
// do_sample (VerifyResult.DoSample) and whether the serving spec declares a
// reasoning parser. The record is written at verification time and never
// rewritten, so the controller cannot wait for a later report of what the
// checks sent.

// Seeds of the checks: every request of one check on a sampled model carries
// its check's seed, so a repeat of the check, or the same check on another
// Instance, draws the same samples (`@hpc-burst-science-agent`, 1 Nov 2026).
// The values are arbitrary and fixed; only their being fixed and distinct
// matters. A seed does not make vLLM reproducible online (HBEP-7, a known
// limit).
const (
	// SeedVerify: verification's requests (the fingerprint's chat request).
	SeedVerify int64 = 1101
	// SeedChat, SeedToolCall, SeedCorruption and SeedBehaviour:
	// qualification's checks of those names.
	SeedChat       int64 = 1102
	SeedToolCall   int64 = 1103
	SeedCorruption int64 = 1104
	SeedBehaviour  int64 = 1105
	// SeedWarmUp: every warm-up request.
	SeedWarmUp int64 = 1106
)

// Keys of Sampling.Seeds: the judged checks' names, plus verification and
// warm-up, which send requests but are not a CheckName.
const (
	SeedKeyVerify = "verify"
	SeedKeyWarmUp = "warmUp"
)

// Sampling is the sampling parameters every request of verification,
// qualification and warm-up sets explicitly, so that a default the server took
// from a generation config never changes a check by accident. The
// verification record holds it.
type Sampling struct {
	// Greedy is true when the checks decode greedily: a model without a
	// reasoning parser, or a reasoning model whose generation_config.json
	// gives none of the seven sampling keys, sets do_sample false, or is
	// absent. A greedy request sets Temperature 0, TopP 1 and the three
	// penalties' neutral values, and carries no seed.
	Greedy bool `json:"greedy"`

	// Temperature, TopP, RepetitionPenalty, PresencePenalty and
	// FrequencyPenalty are set on every check request.
	Temperature       float64 `json:"temperature"`
	TopP              float64 `json:"topP"`
	RepetitionPenalty float64 `json:"repetitionPenalty"`
	PresencePenalty   float64 `json:"presencePenalty"`
	FrequencyPenalty  float64 `json:"frequencyPenalty"`

	// TopK and MinP are set on a sampled model's requests only; a greedy
	// request does not need them, and leaves them nil.
	TopK *int64   `json:"topK,omitempty"`
	MinP *float64 `json:"minP,omitempty"`

	// Seeds is each check's seed, keyed by CheckName (chat, toolCall,
	// corruption, behaviour) and by SeedKeyVerify and SeedKeyWarmUp. Nil when
	// greedy: a greedy request carries no seed.
	Seeds map[string]int64 `json:"seeds,omitempty"`
}

// The seven generation_config.json keys vLLM takes as the server's default
// sampling parameters (vllm/config/model.py lines 1869-1877); the eighth it
// reads, max_new_tokens, each check sets itself, so it does not count here.
const (
	genTemperature       = "temperature"
	genTopP              = "top_p"
	genTopK              = "top_k"
	genMinP              = "min_p"
	genRepetitionPenalty = "repetition_penalty"
	genPresencePenalty   = "presence_penalty"
	genFrequencyPenalty  = "frequency_penalty"
)

// greedySampling is the greedy decoding every check used before 1 Nov 2026:
// temperature 0, top_p 1 and neutral penalties.
func greedySampling() Sampling {
	return Sampling{Greedy: true, Temperature: 0, TopP: 1, RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0}
}

// CheckSeeds returns each check's seed, keyed as Sampling.Seeds is. It returns
// a new map on every call.
func CheckSeeds() map[string]int64 {
	return map[string]int64{
		SeedKeyVerify:           SeedVerify,
		string(CheckChat):       SeedChat,
		string(CheckToolCall):   SeedToolCall,
		string(CheckCorruption): SeedCorruption,
		string(CheckBehaviour):  SeedBehaviour,
		SeedKeyWarmUp:           SeedWarmUp,
	}
}

// CheckSampling returns the sampling parameters of every check request
// (HBEP-7, The allocation agent, Checks):
//
//   - without a reasoning parser, greedy, whatever the file says;
//   - with one, greedy when doSample is false, or when genConfig gives none of
//     the seven sampling keys (temperature, top_p, top_k, min_p and the three
//     penalties), as when there is no generation_config.json at all;
//   - otherwise sampled: each of the seven keys that genConfig gives, and the
//     neutral value for each that it does not (temperature 1, top_p 1, top_k
//     0, min_p 0, repetition_penalty 1, presence_penalty 0,
//     frequency_penalty 0; vLLM's own defaults, vllm/sampling_params.py lines
//     305-318), with every check's fixed seed (CheckSeeds).
//
// genConfig is VerifyResult.GenerationConfig: each value a JSON number's text,
// an integer for top_k. Callers check the file with CheckGenerationConfig
// first, since a refused file fails verification; here, defensively, a value
// that does not parse counts as not given. doSample is VerifyResult.DoSample,
// nil when the file does not set it, which does not force greedy decoding.
// The result shares no memory with its inputs.
func CheckSampling(genConfig map[string]string, doSample *bool, reasoning bool) Sampling {
	if !reasoning || (doSample != nil && !*doSample) {
		return greedySampling()
	}
	num := func(key string) (float64, bool) {
		v, ok := genConfig[key]
		if !ok {
			return 0, false
		}
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	s := Sampling{Temperature: 1, TopP: 1, RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0}
	topK, minP := int64(0), 0.0
	given := false
	for _, f := range []struct {
		key string
		dst *float64
	}{
		{genTemperature, &s.Temperature}, {genTopP, &s.TopP}, {genMinP, &minP},
		{genRepetitionPenalty, &s.RepetitionPenalty}, {genPresencePenalty, &s.PresencePenalty}, {genFrequencyPenalty, &s.FrequencyPenalty},
	} {
		if v, ok := num(f.key); ok {
			*f.dst, given = v, true
		}
	}
	if v, ok := genConfig[genTopK]; ok {
		if k, err := strconv.ParseInt(v, 10, 64); err == nil {
			topK, given = k, true
		}
	}
	if !given {
		return greedySampling()
	}
	s.TopK, s.MinP, s.Seeds = &topK, &minP, CheckSeeds()
	return s
}

// GenerationConfigError is a sampling value of a staged generation_config.json
// that vLLM v0.32.0 refuses (vllm/sampling_params.py, SamplingParams.
// _verify_args, lines 631-687): a value that is not a number, a top_k that is
// not an integer, or a number outside its range. vLLM applies the file's
// values as the server's defaults, so it refuses them for every model, greedy
// or sampled, and every request the defaults reach would fail. Verification
// fails instead, with VerificationFailed, naming the key and the value, before
// any check request; there is no fallback and no clamping
// (`@hpc-burst-science-agent`, 1 Nov 2026, from the reviews of PRs #155 and
// #156).
//
// The agent reports the one it finds in VerifyResult.GenerationConfigRefused,
// since a value that is not a number never reaches VerifyResult.
// GenerationConfig.
//
// A present file the agent cannot judge at all is refused as a whole, with an
// empty Param: one larger than MaxGenerationConfigFileBytes, one it cannot
// read, one that is not valid JSON (Python's json, which vLLM uses, accepts
// NaN, Infinity and -Infinity, which Go's refuses, so such a file is among
// them), and one that is valid JSON but not an object. It is never read as
// absent, since vLLM would load the file and could refuse what the agent did
// not see (PR #169 review, Astra B1 and Opus N3). The empty Param is only for
// these; a refusal of one key always names it.
type GenerationConfigError struct {
	// Param is the file's key, which is vLLM's request field (its
	// VLLMValidationError's parameter), for example "top_p". It is empty
	// exactly when the refusal is of the file as a whole (the Value* constants
	// below); Error() then names the file in words.
	Param string `json:"param"`
	// Value is the value's JSON text as the file holds it, for example "1.5"
	// or "\"warm\"". The agent cuts it to MaxGenerationConfigValueBytes
	// (TruncateUTF8) before reporting it. For a whole-file refusal it says in
	// plain words what is wrong, never the file's content: one of
	// FileTooLarge, FileUnreadable, FileNotJSON and FileNotObject.
	Value string `json:"value"`
	// Want says what vLLM accepts: "a number", "an integer", or the range as
	// vLLM's own message writes it, for example "(0, 1]". For a whole-file
	// refusal it is FileWant.
	Want string `json:"want"`
}

// The Value and Want of a refusal of the file as a whole (empty Param).
const (
	FileTooLarge   = "larger than 1 MiB" // above MaxGenerationConfigFileBytes
	FileUnreadable = "unreadable"        // present, and it cannot be read
	FileNotJSON    = "not valid JSON"    // includes NaN, Infinity and -Infinity
	FileNotObject  = "not a JSON object" // valid JSON of another type, or null
	FileWant       = "a JSON object of at most 1 MiB"
)

func (e *GenerationConfigError) Error() string {
	if e.Param == "" {
		return fmt.Sprintf("generation_config.json is %s (want %s)", e.Value, e.Want)
	}
	if e.Want == "a number" || e.Want == "an integer" {
		return fmt.Sprintf("generation_config.json's %s is %s, not %s", e.Param, e.Value, e.Want)
	}
	return fmt.Sprintf("generation_config.json's %s is %s, outside the range vLLM accepts, %s", e.Param, e.Value, e.Want)
}

// vLLM v0.32.0's ranges for the seven keys, in the order its _verify_args
// tests them.
var generationConfigRanges = []struct {
	key     string
	integer bool
	ok      func(float64) bool // false for NaN
	want    string
}{
	{genPresencePenalty, false, func(v float64) bool { return v >= -2 && v <= 2 }, "[-2, 2]"},
	{genFrequencyPenalty, false, func(v float64) bool { return v >= -2 && v <= 2 }, "[-2, 2]"},
	{genRepetitionPenalty, false, func(v float64) bool { return !math.IsInf(v, 0) && v > 0 }, "a finite number greater than 0"},
	{genTemperature, false, func(v float64) bool { return v >= 0 && v <= 2 }, "[0, 2]"},
	{genTopP, false, func(v float64) bool { return v > 0 && v <= 1 }, "(0, 1]"},
	{genTopK, true, func(v float64) bool { return v >= -1 }, "0 (disable), -1 (disable) or at least 1"},
	{genMinP, false, func(v float64) bool { return v >= 0 && v <= 1 }, "[0, 1]"},
}

// CheckGenerationConfig returns a *GenerationConfigError for the first of the
// seven sampling keys whose value vLLM v0.32.0 refuses, in vLLM's own order,
// and nil when every one the file gives is acceptable:
//
//   - each value is a JSON number, and top_k an integer (no fraction, no
//     exponent);
//   - presence_penalty and frequency_penalty in [-2, 2];
//   - repetition_penalty finite and above 0;
//   - temperature in [0, 2];
//   - top_p in (0, 1];
//   - top_k at least -1 (0 and -1 disable it);
//   - min_p in [0, 1].
//
// values maps each key to its JSON text: the agent passes the file's own text
// of each of the seven keys it finds, and the controller passes
// VerifyResult.GenerationConfig, which carries only numbers. A JSON null is
// skipped, as vLLM skips it; other keys are not checked. It applies to every
// model, with or without a reasoning parser, because vLLM applies the file's
// values as the server's defaults. Callers run it before CheckSampling, whose
// result for a refused file is not used.
func CheckGenerationConfig(values map[string]string) error {
	for _, r := range generationConfigRanges {
		text, ok := values[r.key]
		if !ok || text == "null" {
			continue
		}
		var v float64
		if r.integer {
			n, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return &GenerationConfigError{Param: r.key, Value: text, Want: "an integer"}
			}
			v = float64(n)
		} else {
			// A JSON number too large for a float64 reads as infinity, as
			// it does in Python, and then fails its range.
			f, err := strconv.ParseFloat(text, 64)
			if (err != nil && !errors.Is(err, strconv.ErrRange)) || !jsonNumber(text) {
				return &GenerationConfigError{Param: r.key, Value: text, Want: "a number"}
			}
			v = f
		}
		if !r.ok(v) {
			return &GenerationConfigError{Param: r.key, Value: text, Want: r.want}
		}
	}
	return nil
}

// jsonNumber reports whether text has the form of a JSON number, so that Go's
// wider float syntax ("Inf", "NaN", "0x1p-2", "1_0") is not read as one.
func jsonNumber(text string) bool {
	if text == "" {
		return false
	}
	for _, c := range text {
		if !(c >= '0' && c <= '9' || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
			return false
		}
	}
	return text[0] == '-' || (text[0] >= '0' && text[0] <= '9')
}
