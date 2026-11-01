package agentapi

import "strconv"

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
// an integer for top_k. A value that does not parse as that counts as not
// given. doSample is VerifyResult.DoSample, nil when the file does not set it,
// which does not force greedy decoding. The result shares no memory with its
// inputs.
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
