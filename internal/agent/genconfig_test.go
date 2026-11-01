package agent

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func writeGenerationConfig(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "generation_config.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// readGenerationConfig records only the sampling parameters vLLM v0.32.0
// applies from generation_config.json, the list in vllm/config/model.py lines
// 1869-1878 (HBEP-7, The verification record; PR #138 review, Astra B2, Opus
// 2). The file is Qwen2.5's shape, with the values #94's run saw vLLM take
// (repetition_penalty 1.1, finding 6): its token ids, do_sample and
// transformers_version are not recorded. An absent file, one that is not a
// JSON object and one that sets none of them give none.
func TestReadGenerationConfig(t *testing.T) {
	dir := t.TempDir()
	if got, _ := readGenerationConfig(dir); got != nil {
		t.Errorf("no file: %v, want none", got)
	}
	writeGenerationConfig(t, dir, `{"bos_token_id": 151643, "do_sample": true, "eos_token_id": [151645, 151643],
  "repetition_penalty": 1.1, "temperature": 0.7, "top_p": 0.8, "top_k": 20, "transformers_version": "4.37.0"}`)
	want := map[string]string{"repetition_penalty": "1.1", "temperature": "0.7", "top_p": "0.8", "top_k": "20"}
	if got, _ := readGenerationConfig(dir); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	writeGenerationConfig(t, dir, `[1, 2]`)
	if got, _ := readGenerationConfig(dir); got != nil {
		t.Errorf("not an object: %v, want none", got)
	}
	writeGenerationConfig(t, dir, `{"bos_token_id": 1, "transformers_version": "4.37.0"}`)
	if got, _ := readGenerationConfig(dir); got != nil {
		t.Errorf("no sampling parameter: %v, want none", got)
	}
}

// A file that also holds a token-like key, a local path and 40 unrelated keys,
// which sort before every sampling key, records exactly the eight sampling
// keys with their values: nothing else enters the immutable record, and no
// sampling key is crowded out (the reader took the first 32 keys in key
// order).
func TestReadGenerationConfigRecordsOnlyTheSamplingKeys(t *testing.T) {
	dir := t.TempDir()
	var b strings.Builder
	b.WriteString(`{"hf_token": "hf_0123456789abcdefABCDEF0123456789", "_auth_token": "hf_0123456789abcdef",`)
	b.WriteString(` "_name_or_path": "/home/example/models/qwen", "cache_dir": "/home/example/.cache",`)
	for i := range 40 {
		fmt.Fprintf(&b, ` "a%02d": %d,`, i, i)
	}
	b.WriteString(` "repetition_penalty": 1.05, "presence_penalty": 0.5, "frequency_penalty": -0.25, "temperature": 0.6,`)
	b.WriteString(` "top_k": 20, "top_p": 0.95, "min_p": 0, "max_new_tokens": 2048}`)
	writeGenerationConfig(t, dir, b.String())
	want := map[string]string{
		"repetition_penalty": "1.05", "presence_penalty": "0.5", "frequency_penalty": "-0.25", "temperature": "0.6",
		"top_k": "20", "top_p": "0.95", "min_p": "0", "max_new_tokens": "2048",
	}
	got, _ := readGenerationConfig(dir)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	for k, v := range got {
		if strings.Contains(v, "hf_") || strings.Contains(v, "/home") {
			t.Errorf("%s carries %q", k, v)
		}
	}
}

// Each value must be what vLLM can apply: a JSON number, not a string holding
// one, a boolean, a list or a null (which vLLM skips), and an integer for
// top_k and max_new_tokens. A value of another type is left out, and the
// others are still recorded.
func TestReadGenerationConfigChecksTheTypes(t *testing.T) {
	dir := t.TempDir()
	writeGenerationConfig(t, dir, `{"temperature": "0.7", "top_k": 20.5, "max_new_tokens": 2.0e3, "min_p": null,
  "presence_penalty": true, "frequency_penalty": [0], "repetition_penalty": {"v": 1}, "top_p": 1e-1}`)
	want := map[string]string{"top_p": "1e-1"}
	if got, _ := readGenerationConfig(dir); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	writeGenerationConfig(t, dir, `{"top_k": 40, "max_new_tokens": 9223372036854775808}`)
	if got, _ := readGenerationConfig(dir); !reflect.DeepEqual(got, map[string]string{"top_k": "40"}) {
		t.Errorf("an integer past int64: got %v, want only top_k", got)
	}
}

// The contract's value bound, 128 bytes (agentapi.MaxGenerationConfigValueBytes),
// at its edge: a 128-byte number is recorded whole, a 129-byte one is left
// out, since a cut would record another number. The contract's other two
// bounds, 32 keys of at most 64 bytes, hold for every key the reader can
// record. The numbers are the contract's, written out here (PR #138 review,
// Astra nit), so a change of a bound fails this test.
func TestReadGenerationConfigBounds(t *testing.T) {
	dir := t.TempDir()
	at, over := "0."+strings.Repeat("1", 126), "0."+strings.Repeat("1", 127)
	if len(at) != 128 || len(over) != 129 {
		t.Fatalf("fixture lengths %d %d", len(at), len(over))
	}
	writeGenerationConfig(t, dir, `{"temperature": `+at+`, "top_p": `+over+`}`)
	if got, _ := readGenerationConfig(dir); !reflect.DeepEqual(got, map[string]string{"temperature": at}) {
		t.Errorf("got %v, want the 128-byte temperature only", got)
	}
	if len(samplingParams) > 32 {
		t.Errorf("%d sampling keys, above the contract's 32", len(samplingParams))
	}
	for k := range samplingParams {
		if len(k) > 64 {
			t.Errorf("sampling key %q is above the contract's 64 bytes", k)
		}
	}
}

func boolPtr(v bool) *bool { return &v }

// do_sample counts only as a JSON boolean (agentapi.VerifyResult.DoSample):
// a string, a number, a list, a null and a missing key all leave it nil, and
// a file that sets only do_sample still reports it. The expected values are
// written out by hand.
func TestReadGenerationConfigDoSample(t *testing.T) {
	temp := map[string]string{"temperature": "0.6"}
	for _, tc := range []struct {
		name, body string
		wantCfg    map[string]string
		wantDo     *bool
	}{
		{"true with a key", `{"do_sample": true, "temperature": 0.6}`, temp, boolPtr(true)},
		{"false with a key", `{"do_sample": false, "temperature": 0.6}`, temp, boolPtr(false)},
		{"false alone", `{"do_sample": false}`, nil, boolPtr(false)},
		{"true alone", `{"do_sample": true}`, nil, boolPtr(true)},
		{"the string false", `{"do_sample": "false", "temperature": 0.6}`, temp, nil},
		{"the string true", `{"do_sample": "true", "temperature": 0.6}`, temp, nil},
		{"the number 0", `{"do_sample": 0, "temperature": 0.6}`, temp, nil},
		{"the number 1", `{"do_sample": 1, "temperature": 0.6}`, temp, nil},
		{"null", `{"do_sample": null, "temperature": 0.6}`, temp, nil},
		{"a list", `{"do_sample": [false], "temperature": 0.6}`, temp, nil},
		{"absent", `{"temperature": 0.6}`, temp, nil},
		{"not an object", `[true]`, nil, nil},
		{"not JSON", `{"do_sample": false`, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeGenerationConfig(t, dir, tc.body)
			cfg, do := readGenerationConfig(dir)
			if !reflect.DeepEqual(cfg, tc.wantCfg) || !reflect.DeepEqual(do, tc.wantDo) {
				t.Errorf("got %v, do_sample %v; want %v, %v", cfg, do, tc.wantCfg, tc.wantDo)
			}
		})
	}
	if cfg, do := readGenerationConfig(t.TempDir()); cfg != nil || do != nil {
		t.Errorf("no file: %v, %v, want none", cfg, do)
	}
	if cfg, do := readGenerationConfig(""); cfg != nil || do != nil {
		t.Errorf("no directory: %v, %v, want none", cfg, do)
	}
}

// The sampling of a verification generation's checks is computed once, from
// the file as it stood at the first check unit: a later unit of the same
// generation gets the same values even if the file has changed, so what the
// checks send and what the record reports cannot part. Expected values are
// written out by hand from the fixture (Qwen3-0.6B's keys).
func TestGenSamplingIsComputedOnce(t *testing.T) {
	dir := t.TempDir()
	writeGenerationConfig(t, dir, `{"do_sample": true, "temperature": 0.6, "top_k": 20, "top_p": 0.95}`)
	g := &genSampling{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	topK, minP := int64(20), 0.0
	want := agentapi.Sampling{
		Temperature: 0.6, TopP: 0.95, TopK: &topK, MinP: &minP, RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0,
		Seeds: map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106},
	}
	cfg, do, got := g.resolve(dir, true, log, 1)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(cfg, map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}) || do == nil || !*do {
		t.Fatalf("first resolve: %+v, %v, %v", got, cfg, do)
	}
	writeGenerationConfig(t, dir, `{"do_sample": false}`)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, do, got := g.resolve(dir, false, log, 1)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(cfg, map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}) || do == nil || !*do {
				t.Errorf("a later resolve changed: %+v, %v, %v", got, cfg, do)
			}
		}()
	}
	wg.Wait()
	// A new generation resolves again.
	_, _, fresh := (&genSampling{}).resolve(dir, true, log, 2)
	if !fresh.Greedy {
		t.Errorf("the next generation's file sets do_sample false: %+v, want greedy", fresh)
	}
}
