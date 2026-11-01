package lifecycle

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The verification record holds the sampling the checks used, or that they
// ran greedily (HBEP-7, The allocation agent, Checks, and One record per
// verification; HBEP-11; science-agent decision, 1 Nov 2026, issue #151). The
// expected values are written out by hand from the fixture files (Qwen3-0.6B's
// keys), never computed with agentapi.CheckSampling.

func fl(v float64) *float64 { return &v }
func in64(v int64) *int64   { return &v }
func bl(v bool) *bool       { return &v }

// greedyRecord is the greedy request HBEP-7 states: temperature 0, top_p 1,
// repetition_penalty 1, presence_penalty 0, frequency_penalty 0, no seed.
var greedyRecord = &agentapi.Sampling{Greedy: true, Temperature: 0, TopP: 1, RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0}

// qwen3Record is HBEP-7's Test plan for a reasoning model with Qwen3-0.6B's
// file: temperature 0.6, top_k 20, top_p 0.95, min_p 0, neutral penalties and
// a seed per check.
var qwen3Record = &agentapi.Sampling{
	Temperature: 0.6, TopP: 0.95, TopK: in64(20), MinP: fl(0), RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0,
	Seeds: map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106},
}

var qwen3File = map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}

// reasoningSpec is judgeDeclared with a reasoning parser declared, and the
// result that matches it (a context limit that fits a reasoning budget).
func reasoningSpec() (declared, *agentapi.VerifyResult, shapes) {
	d := judgeDeclared()
	d.flags["max-model-len"], d.flags["reasoning-parser"] = "8192", "qwen3"
	r := judgeResult()
	r.Settings["max-model-len"], r.TemplateOverhead = "8192", 8
	longest := int32(4096)
	return d, r, shapes{short: 128, typical: 1024, longest: &longest, outputTypical: 128, outputLongest: 512, targetSessions: 8}
}

func TestTheRecordHoldsTheSamplingTheChecksUsed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reasoning bool
		config    map[string]string
		doSample  *bool
		want      *agentapi.Sampling
	}{
		{"a reasoning model with Qwen3-0.6B's file", true, qwen3File, nil, qwen3Record},
		{"the same with do_sample true", true, qwen3File, bl(true), qwen3Record},
		{"the same checkpoint without a reasoning parser", false, qwen3File, nil, greedyRecord},
		{"a reasoning model whose file sets do_sample false", true, qwen3File, bl(false), greedyRecord},
		{"a reasoning model whose file gives none of the seven keys", true, map[string]string{"max_new_tokens": "2048"}, bl(true), greedyRecord},
		{"a reasoning model with no file", true, nil, nil, greedyRecord},
		{"no parser and no file", false, nil, nil, greedyRecord},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, r, sh := judgeDeclared(), judgeResult(), judgeShapes()
			if tc.reasoning {
				d, r, sh = reasoningSpec()
			}
			r.GenerationConfig, r.DoSample = tc.config, tc.doSample
			got := judgeVerify(d, r, true, sh)
			if got.Verdict != v1alpha1.VerdictPass {
				t.Fatalf("verdict %q: %s", got.Verdict, got.Message)
			}
			if got.Sampling == nil || !reflect.DeepEqual(*got.Sampling, *tc.want) {
				t.Errorf("Sampling %+v, want %+v", got.Sampling, tc.want)
			}
			// The record's existing field still holds the file's values.
			if !reflect.DeepEqual(got.GenerationConfig, tc.config) {
				t.Errorf("GenerationConfig %v, want %v", got.GenerationConfig, tc.config)
			}
		})
	}
}

// Without a parser, a file with repetition_penalty 1.1 is checked with 1, and
// the record holds the 1.1 from the file (HBEP-7, Test plan, Check requests).
func TestTheRecordKeepsTheFilesRepetitionPenaltyWhileTheChecksSendOne(t *testing.T) {
	r := judgeResult()
	r.GenerationConfig = map[string]string{"repetition_penalty": "1.1", "temperature": "0.7", "top_p": "0.8", "top_k": "20"}
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.GenerationConfig["repetition_penalty"] != "1.1" {
		t.Errorf("GenerationConfig %v, want the file's repetition_penalty 1.1", got.GenerationConfig)
	}
	if got.Sampling == nil || !got.Sampling.Greedy || got.Sampling.RepetitionPenalty != 1 {
		t.Errorf("Sampling %+v, want a greedy request with repetition_penalty 1", got.Sampling)
	}
}

// The sampling is recorded on every record: a failed verification (its
// fingerprint request used the sampling anyway), a worker restart's, and one
// whose agent could not verify.
func TestTheSamplingIsOnEveryRecord(t *testing.T) {
	failed := func(mutate func(*agentapi.VerifyResult)) (declared, *agentapi.VerifyResult, shapes, bool) {
		d, r, sh := reasoningSpec()
		r.GenerationConfig = qwen3File
		mutate(r)
		return d, r, sh, true
	}
	for name, mk := range map[string]func() (declared, *agentapi.VerifyResult, shapes, bool){
		"a failed rehash": func() (declared, *agentapi.VerifyResult, shapes, bool) {
			return failed(func(r *agentapi.VerifyResult) { r.Files[0].Outcome = agentapi.FileMismatch })
		},
		"a contradiction": func() (declared, *agentapi.VerifyResult, shapes, bool) {
			return failed(func(r *agentapi.VerifyResult) { r.EngineVersion = "0.31.0" })
		},
		"the agent could not verify": func() (declared, *agentapi.VerifyResult, shapes, bool) {
			return failed(func(r *agentapi.VerifyResult) { r.Error = "version: connection refused" })
		},
		"a worker restart": func() (declared, *agentapi.VerifyResult, shapes, bool) {
			d, r, sh, _ := failed(func(*agentapi.VerifyResult) {})
			return d, r, sh, false
		},
	} {
		t.Run(name, func(t *testing.T) {
			d, r, sh, rehash := mk()
			got := judgeVerify(d, r, rehash, sh)
			if name != "a worker restart" && got.Verdict != v1alpha1.VerdictFail {
				t.Fatalf("verdict %q, want a fail for this case", got.Verdict)
			}
			if got.Sampling == nil || !reflect.DeepEqual(*got.Sampling, *qwen3Record) {
				t.Errorf("Sampling %+v, want %+v", got.Sampling, qwen3Record)
			}
		})
	}
}

// The record's data on the wire: a sampled model's holds the values and the
// seeds, a greedy one's says greedy, and a record written before this field
// existed reads back with none (unknown), not as a sampled model.
func TestTheRecordDataCarriesTheSampling(t *testing.T) {
	d, r, sh := reasoningSpec()
	r.GenerationConfig = qwen3File
	b, err := json.Marshal(judgeVerify(d, r, true, sh))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"sampling":{"greedy":false,"temperature":0.6,"topP":0.95,`,
		`"topK":20,"minP":0,`,
		`"seeds":{"behaviour":1105,"chat":1102,"corruption":1104,"toolCall":1103,"verify":1101,"warmUp":1106}`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("record data %s lacks %s", b, want)
		}
	}
	b, err = json.Marshal(judgeVerify(judgeDeclared(), judgeResult(), true, judgeShapes()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"sampling":{"greedy":true,"temperature":0,"topP":1,"repetitionPenalty":1,"presencePenalty":0,"frequencyPenalty":0}`) {
		t.Errorf("a greedy record's data %s does not say greedy with the greedy values", b)
	}
	var old verificationData
	if err := json.Unmarshal([]byte(`{"instance":"i","verdict":"Pass","rehash":{"checked":2}}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Sampling != nil {
		t.Errorf("a record from before #151 reads back with sampling %+v, want none", old.Sampling)
	}
}

// A staged generation_config.json with a value vLLM refuses fails verification
// for every model, with the settings contradiction's cause (the stop keyed on
// the deployment ID) and a message naming the key and the value, whether the
// controller finds it in the numbers the agent recorded or the agent reports it
// (a value that is not a number reaches it only that way). No check request
// was sent, so the record holds no sampling, and it holds the refusal
// (science-agent ruling, 1 Nov 2026, N7). The refusals are written out by hand
// from vLLM's ranges (temperature [0, 2], top_p (0, 1]).
func TestARefusedGenerationConfigFailsVerificationForEveryModel(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reasoning bool
		config    map[string]string
		reported  *agentapi.GenerationConfigError
		want      agentapi.GenerationConfigError
	}{
		{"a reasoning model, temperature 2.5 in the numbers", true, map[string]string{"temperature": "2.5", "top_k": "20", "top_p": "0.95"}, nil,
			agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"}},
		{"a model without a parser, temperature 2.5 in the numbers", false, map[string]string{"temperature": "2.5", "top_k": "20", "top_p": "0.95"}, nil,
			agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"}},
		{"a model without a parser, top_p 0", false, map[string]string{"top_p": "0"}, nil,
			agentapi.GenerationConfigError{Param: "top_p", Value: "0", Want: "(0, 1]"}},
		{"temperature \"warm\", reported by the agent", true, nil,
			&agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"},
			agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"}},
		{"the same without a parser", false, nil,
			&agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"},
			agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"}},
		{"a file above 1 MiB, refused as a whole", true, nil,
			&agentapi.GenerationConfigError{Value: "larger than 1 MiB", Want: "a JSON object of at most 1 MiB"},
			agentapi.GenerationConfigError{Value: "larger than 1 MiB", Want: "a JSON object of at most 1 MiB"}},
		{"a file with NaN, refused as a whole, without a parser", false, nil,
			&agentapi.GenerationConfigError{Value: "not valid JSON", Want: "a JSON object of at most 1 MiB"},
			agentapi.GenerationConfigError{Value: "not valid JSON", Want: "a JSON object of at most 1 MiB"}},
		{"temperature 2.5, reported by the agent with the file's numbers", true, map[string]string{"temperature": "2.5", "top_p": "0.95"},
			&agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"},
			agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, r, sh := judgeDeclared(), judgeResult(), judgeShapes()
			if tc.reasoning {
				d, r, sh = reasoningSpec()
			}
			r.GenerationConfig = tc.config
			if tc.reported != nil {
				// What the agent really sends on a refusal: no engine fields.
				asTheAgentReportsARefusal(r, tc.reported)
			}
			got := judgeVerify(d, r, true, sh)
			if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeContradiction {
				t.Fatalf("verdict %q, cause %q (%s), want a fail by %s", got.Verdict, got.Cause, got.Message, causeContradiction)
			}
			if !strings.Contains(got.Message, tc.want.Param) || !strings.Contains(got.Message, tc.want.Value) {
				t.Errorf("message %q does not name %s and %s", got.Message, tc.want.Param, tc.want.Value)
			}
			if got.GenerationConfigRefused == nil || *got.GenerationConfigRefused != tc.want {
				t.Errorf("the record holds the refusal %+v, want %+v", got.GenerationConfigRefused, tc.want)
			}
			if got.Sampling != nil {
				t.Errorf("the record holds a sampling %+v, but no check request was sent", got.Sampling)
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), `"generationConfigRefused":{"param":"`+tc.want.Param+`"`) {
				t.Errorf("record data %s does not carry the refusal", b)
			}
		})
	}
}

// A valid file passes, and a rehash mismatch is judged before the file's
// values: a file that does not match the identity record is restaged, not
// blamed for what it holds. A worker restart's verification (no rehash) judges
// the values too. The refused cases are the agent's real report (no engine
// fields), whose refusal is judged before the missing-required check.
func TestTheRefusalOrderAndTheWorkerRestart(t *testing.T) {
	d, r, sh := reasoningSpec()
	r.GenerationConfig = qwen3File
	if got := judgeVerify(d, r, true, sh); got.Verdict != v1alpha1.VerdictPass || got.GenerationConfigRefused != nil || got.Sampling == nil {
		t.Errorf("a valid file: verdict %q (%s), refusal %+v, sampling %+v", got.Verdict, got.Message, got.GenerationConfigRefused, got.Sampling)
	}
	refused := func(rehash bool, outcome agentapi.FileOutcome) verificationData {
		r := asTheAgentReportsARefusal(judgeResult(), &agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"})
		r.GenerationConfig = map[string]string{"temperature": "2.5"}
		r.Files[0].Outcome = outcome
		return judgeVerify(d, r, rehash, sh)
	}
	if got := refused(true, agentapi.FileMismatch); got.Cause != causeRehash {
		t.Errorf("a mismatching file with a refused value: cause %q, want %s", got.Cause, causeRehash)
	}
	if got := refused(false, agentapi.FileMatch); got.Cause != causeContradiction || got.GenerationConfigRefused == nil {
		t.Errorf("a worker restart's verification: cause %q, refusal %+v, want %s", got.Cause, got.GenerationConfigRefused, causeContradiction)
	}
}
