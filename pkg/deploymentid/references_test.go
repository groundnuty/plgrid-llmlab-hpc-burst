package deploymentid

import (
	"errors"
	"strings"
	"testing"
)

// HBEP-3, "References to other checkpoints inside input values" (alpha):
//   - a tokenizer other than the checkpoint's needs tokenizer-revision as a
//     full commit id, or it is refused;
//   - a speculative-config that names a draft model is refused;
//   - lora-modules and config are refused (table rows, TestRefusals).
func TestTokenizerOtherThanTheCheckpointsNeedsAPin(t *testing.T) {
	commit40, commit64 := strings.Repeat("ab", 20), strings.Repeat("cd", 32)
	cases := []struct {
		name  string
		flags []Flag
		want  error // nil: accepted
	}{
		{"no tokenizer", nil, nil},
		{"the checkpoint's own tokenizer", []Flag{{"tokenizer", "example-org/example-7b"}}, nil},
		{"the checkpoint's own tokenizer with any revision", []Flag{{"tokenizer", "example-org/example-7b"}, {"tokenizer-revision", "main"}}, nil},
		{"a revision alone", []Flag{{"tokenizer-revision", "main"}}, nil},
		{"another tokenizer, unpinned", []Flag{{"tokenizer", "other-org/tok"}}, ErrRefused},
		{"another tokenizer, pinned by a branch", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", "main"}}, ErrRefused},
		{"another tokenizer, pinned by a short commit", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", "abc1234"}}, ErrRefused},
		{"another tokenizer, pinned by an upper-case commit", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", strings.ToUpper(commit40)}}, ErrRefused},
		{"another tokenizer, empty pin", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", ""}}, ErrRefused},
		{"another tokenizer, 41-character pin", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", commit40 + "a"}}, ErrRefused},
		{"another tokenizer, pinned by a 40-character commit", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", commit40}}, nil},
		{"another tokenizer, pinned by a 64-character commit", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", commit64}}, nil},
		{"the pin may come first", []Flag{{"tokenizer-revision", commit40}, {"tokenizer", "other-org/tok"}}, nil},
		{"underscore spelling is the same flag", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer_revision", "main"}}, ErrRefused},
		{"underscore spelling, pinned", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer_revision", commit40}}, nil},
		{"a local path is another tokenizer", []Flag{{"tokenizer", "/data/tok"}}, ErrRefused},
		{"an empty tokenizer is another tokenizer", []Flag{{"tokenizer", ""}}, ErrRefused},
		{"a repeated tokenizer with one other, unpinned", []Flag{{"tokenizer", "example-org/example-7b"}, {"tokenizer", "other-org/tok"}}, ErrRefused},
		{"a repeated pin with one bad value", []Flag{{"tokenizer", "other-org/tok"}, {"tokenizer-revision", commit40}, {"tokenizer-revision", "main"}}, ErrRefused},
		{"the checkpoint's name in another case is another tokenizer", []Flag{{"tokenizer", "Example-Org/Example-7b"}}, ErrRefused},
	}
	for _, c := range cases {
		spec, accel, files := baseSpec()
		spec.Flags = c.flags
		_, err := Compute(spec, accel, files)
		if c.want == nil && err != nil {
			t.Errorf("%s: got %v, want it accepted", c.name, err)
		}
		if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestSpeculativeConfigThatNamesADraftModelIsRefused(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  error
	}{
		{"ngram by method", `{"method":"ngram","num_speculative_tokens":4,"prompt_lookup_max":4}`, nil},
		{"MTP by method", `{"method":"mtp","num_speculative_tokens":1}`, nil},
		{"an empty object", `{}`, nil},
		{"a null model", `{"method":"ngram","model":null}`, nil},
		{"whitespace and order", ` { "num_speculative_tokens" : 2 , "method" : "eagle3" } `, nil},
		{"a draft model", `{"model":"other-org/draft","num_speculative_tokens":4}`, ErrRefused},
		{"a draft model after other members", `{"num_speculative_tokens":4,"method":"eagle","model":"other-org/head"}`, ErrRefused},
		{"an empty model name", `{"model":""}`, ErrRefused},
		{"a model that is ngram", `{"model":"ngram"}`, ErrRefused},
		{"a nested model is not the draft model", `{"method":"ngram","draft_load_config":{"model":"x"}}`, nil},
		{"a second member named model, last wins", `{"model":"other-org/draft","model":null}`, nil},
		{"a second member named model, last wins, the other way", `{"model":null,"model":"other-org/draft"}`, ErrRefused},
		{"not JSON", `method=ngram`, ErrInvalid},
		{"a JSON array", `[{"model":"x"}]`, ErrInvalid},
		{"JSON null", `null`, ErrInvalid},
		{"a JSON string", `"ngram"`, ErrInvalid},
		{"empty", ``, ErrInvalid},
	}
	for _, c := range cases {
		spec, accel, files := baseSpec()
		spec.Flags = []Flag{{"speculative-config", c.value}}
		_, err := Compute(spec, accel, files)
		if c.want == nil && err != nil {
			t.Errorf("%s: got %v, want it accepted", c.name, err)
		}
		if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
		if err != nil && c.value != "" && strings.Contains(err.Error(), "other-org") {
			t.Errorf("%s: the error repeats the value: %v", c.name, err)
		}
	}
	// A repeated flag is checked in every value; the underscore spelling too.
	spec, accel, files := baseSpec()
	spec.Flags = []Flag{{"speculative-config", `{"method":"ngram"}`}, {"speculative_config", `{"model":"other-org/draft"}`}}
	if _, err := Compute(spec, accel, files); !errors.Is(err, ErrRefused) {
		t.Errorf("a draft model in the second value: got %v, want ErrRefused", err)
	}
}

// The checks read input flags only: a tokenizer or speculative-config the table
// would classify otherwise is not reached, and nothing is read from env.
func TestReferenceChecksDoNotReadEnvironmentVariables(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Env = []EnvVar{{"TOKENIZER", "other-org/tok"}, {"SPECULATIVE_CONFIG", `{"model":"x"}`}}
	mustCompute(t, spec, accel, files)
}
