package agentapi

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The expected values below are written out by hand from HBEP-7 (The
// allocation agent, Checks, "Sampling parameters of the checks") and its Test
// plan, never read back through CheckSampling.

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }
func bp(v bool) *bool        { return &v }

// greedy is HBEP-7's greedy request: temperature 0, top_p 1,
// repetition_penalty 1, presence_penalty 0, frequency_penalty 0, no seed.
var greedy = Sampling{Greedy: true, Temperature: 0, TopP: 1, RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0}

// seeds are the six fixed seeds, written out.
var seeds = map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106}

// qwen3 is Qwen3-0.6B's generation_config.json as the agent reports it
// (temperature 0.6, top_k 20, top_p 0.95), plus max_new_tokens, which is not
// one of the seven keys.
var qwen3 = map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95", "max_new_tokens": "4096"}

// qwen3Sampled is HBEP-7's Test plan for that file on a reasoning model: those
// three, min_p 0, repetition_penalty 1, presence_penalty 0 and
// frequency_penalty 0, and a seed per check.
var qwen3Sampled = Sampling{Temperature: 0.6, TopP: 0.95, TopK: i64(20), MinP: f64(0), RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0, Seeds: seeds}

// The full truth table (`@hpc-burst-science-agent`, 1 Nov 2026): reasoning or
// not; do_sample true, false or absent; sampling keys present or none.
func TestCheckSamplingTruthTable(t *testing.T) {
	noKeys := map[string]string{"max_new_tokens": "4096"} // the file sets only a key that is not one of the seven
	for _, tc := range []struct {
		name      string
		reasoning bool
		doSample  *bool
		genConfig map[string]string
		want      Sampling
	}{
		// No reasoning parser: greedy in every case.
		{"no parser, do_sample absent, keys", false, nil, qwen3, greedy},
		{"no parser, do_sample true, keys", false, bp(true), qwen3, greedy},
		{"no parser, do_sample false, keys", false, bp(false), qwen3, greedy},
		{"no parser, do_sample absent, no keys", false, nil, noKeys, greedy},
		{"no parser, do_sample true, no keys", false, bp(true), noKeys, greedy},
		{"no parser, do_sample false, no keys", false, bp(false), noKeys, greedy},
		// A reasoning parser: sampled only with keys and do_sample not false.
		{"parser, do_sample absent, keys", true, nil, qwen3, qwen3Sampled},
		{"parser, do_sample true, keys", true, bp(true), qwen3, qwen3Sampled},
		{"parser, do_sample false, keys", true, bp(false), qwen3, greedy},
		{"parser, do_sample absent, no keys", true, nil, noKeys, greedy},
		{"parser, do_sample true, no keys", true, bp(true), noKeys, greedy},
		{"parser, do_sample false, no keys", true, bp(false), noKeys, greedy},
		// No generation_config.json at all: no keys and no do_sample.
		{"parser, no file", true, nil, nil, greedy},
		{"no parser, no file", false, nil, nil, greedy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CheckSampling(tc.genConfig, tc.doSample, tc.reasoning)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("CheckSampling = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// Each of the seven keys the file gives is taken, and each it does not gets
// vLLM's neutral value; one key alone is enough to sample.
func TestCheckSamplingTakesEachKeyAndNeutralForTheRest(t *testing.T) {
	all := map[string]string{
		"temperature": "0.7", "top_p": "0.8", "top_k": "40", "min_p": "0.05",
		"repetition_penalty": "1.1", "presence_penalty": "0.3", "frequency_penalty": "0.2",
	}
	for _, tc := range []struct {
		name      string
		genConfig map[string]string
		want      Sampling
	}{
		{"all seven", all, Sampling{Temperature: 0.7, TopP: 0.8, TopK: i64(40), MinP: f64(0.05),
			RepetitionPenalty: 1.1, PresencePenalty: 0.3, FrequencyPenalty: 0.2, Seeds: seeds}},
		{"only repetition_penalty", map[string]string{"repetition_penalty": "1.1"}, Sampling{Temperature: 1, TopP: 1, TopK: i64(0), MinP: f64(0),
			RepetitionPenalty: 1.1, PresencePenalty: 0, FrequencyPenalty: 0, Seeds: seeds}},
		{"only top_k", map[string]string{"top_k": "20"}, Sampling{Temperature: 1, TopP: 1, TopK: i64(20), MinP: f64(0),
			RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0, Seeds: seeds}},
		{"only min_p", map[string]string{"min_p": "0.1"}, Sampling{Temperature: 1, TopP: 1, TopK: i64(0), MinP: f64(0.1),
			RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0, Seeds: seeds}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CheckSampling(tc.genConfig, nil, true)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("CheckSampling = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The seeds are fixed and one per check, so two Instances draw the same
// samples; each call returns its own map.
func TestCheckSeedsAreFixedAndDistinct(t *testing.T) {
	if got := CheckSeeds(); !reflect.DeepEqual(got, seeds) {
		t.Errorf("CheckSeeds = %v, want %v", got, seeds)
	}
	seen := map[int64]string{}
	for k, v := range CheckSeeds() {
		if other, dup := seen[v]; dup {
			t.Errorf("checks %s and %s share seed %d", k, other, v)
		}
		seen[v] = k
	}
	a, b := CheckSampling(qwen3, nil, true), CheckSampling(qwen3, nil, true)
	a.Seeds["chat"] = 1
	*a.TopK = 99
	if b.Seeds["chat"] != 1102 || *b.TopK != 20 {
		t.Errorf("two results share memory: %+v", b)
	}
}

// The ranges are vLLM v0.32.0's (vllm/sampling_params.py lines 631-687),
// written out by hand: each bound is tested at the bound and just past it. A
// value that is not a number, or a top_k that is not an integer, fails too; it
// is never read as absent (`@hpc-burst-science-agent`, 1 Nov 2026, from the
// reviews of PRs #155 and #156: refuse, no fallback, no clamping, for every
// model).
func TestCheckGenerationConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  map[string]string
		wantKey string // "" accepts
		want    string // the whole message, when set
	}{
		{"Qwen3-0.6B's file", qwen3, "", ""},
		{"no file", nil, "", ""},
		{"keys that are not among the seven", map[string]string{"max_new_tokens": "0", "do_sample": "true"}, "", ""},
		{"a null, which vLLM skips", map[string]string{"temperature": "null"}, "", ""},
		{"presence_penalty -2", map[string]string{"presence_penalty": "-2"}, "", ""},
		{"presence_penalty below -2", map[string]string{"presence_penalty": "-2.0001"}, "presence_penalty", ""},
		{"presence_penalty 2", map[string]string{"presence_penalty": "2"}, "", ""},
		{"presence_penalty above 2", map[string]string{"presence_penalty": "2.0001"}, "presence_penalty",
			"generation_config.json's presence_penalty is 2.0001, outside the range vLLM accepts, [-2, 2]"},
		{"frequency_penalty -2", map[string]string{"frequency_penalty": "-2"}, "", ""},
		{"frequency_penalty below -2", map[string]string{"frequency_penalty": "-2.0001"}, "frequency_penalty", ""},
		{"frequency_penalty 2", map[string]string{"frequency_penalty": "2"}, "", ""},
		{"frequency_penalty above 2", map[string]string{"frequency_penalty": "2.0001"}, "frequency_penalty", ""},
		{"repetition_penalty just above 0", map[string]string{"repetition_penalty": "0.0001"}, "", ""},
		{"repetition_penalty 0", map[string]string{"repetition_penalty": "0"}, "repetition_penalty", ""},
		{"repetition_penalty overflows to infinity", map[string]string{"repetition_penalty": "1e400"}, "repetition_penalty",
			"generation_config.json's repetition_penalty is 1e400, outside the range vLLM accepts, a finite number greater than 0"},
		{"temperature 0", map[string]string{"temperature": "0"}, "", ""},
		{"temperature below 0", map[string]string{"temperature": "-0.0001"}, "temperature", ""},
		{"temperature 2", map[string]string{"temperature": "2"}, "", ""},
		{"temperature above 2", map[string]string{"temperature": "2.5"}, "temperature",
			"generation_config.json's temperature is 2.5, outside the range vLLM accepts, [0, 2]"},
		{"top_p 1", map[string]string{"top_p": "1"}, "", ""},
		{"top_p above 1", map[string]string{"top_p": "1.0001"}, "top_p", ""},
		{"top_p just above 0", map[string]string{"top_p": "0.0001"}, "", ""},
		{"top_p 0", map[string]string{"top_p": "0"}, "top_p", ""},
		{"top_k -1", map[string]string{"top_k": "-1"}, "", ""},
		{"top_k 0", map[string]string{"top_k": "0"}, "", ""},
		{"top_k -2", map[string]string{"top_k": "-2"}, "top_k", ""},
		{"min_p 0", map[string]string{"min_p": "0"}, "", ""},
		{"min_p 1", map[string]string{"min_p": "1"}, "", ""},
		{"min_p below 0", map[string]string{"min_p": "-0.0001"}, "min_p", ""},
		{"min_p above 1", map[string]string{"min_p": "1.0001"}, "min_p", ""},
		// Not a number, or not an integer: refused, never read as absent.
		{"a temperature that is a string", map[string]string{"temperature": `"warm"`}, "temperature",
			`generation_config.json's temperature is "warm", not a number`},
		{"a top_k with a fraction", map[string]string{"top_k": "2.5"}, "top_k",
			"generation_config.json's top_k is 2.5, not an integer"},
		{"a top_k written as a float", map[string]string{"top_k": "20.0"}, "top_k", ""},
		{"a top_p that is a boolean", map[string]string{"top_p": "true"}, "top_p", ""},
		{"Go's float syntax that JSON lacks", map[string]string{"temperature": "NaN"}, "temperature", ""},
		{"the reviewers' case", map[string]string{"temperature": "warm", "top_k": "2.5"}, "temperature", ""},
		// vLLM's order: the first bad key in it is named.
		{"two bad values: presence_penalty first", map[string]string{"top_p": "0", "presence_penalty": "3"}, "presence_penalty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckGenerationConfig(tc.values)
			var ge *GenerationConfigError
			switch {
			case tc.wantKey == "" && err != nil:
				t.Errorf("CheckGenerationConfig = %v, want nil", err)
			case tc.wantKey != "" && !errors.As(err, &ge):
				t.Errorf("CheckGenerationConfig = %v, want a GenerationConfigError for %s", err, tc.wantKey)
			case tc.wantKey != "" && (ge.Key != tc.wantKey || !strings.Contains(err.Error(), tc.wantKey)):
				t.Errorf("CheckGenerationConfig = %v (key %q), want key %q named in the message", err, ge.Key, tc.wantKey)
			case tc.want != "" && err.Error() != tc.want:
				t.Errorf("CheckGenerationConfig = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}
