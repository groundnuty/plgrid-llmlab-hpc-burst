package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
	if got := readGenerationConfig(dir); got != nil {
		t.Errorf("no file: %v, want none", got)
	}
	writeGenerationConfig(t, dir, `{"bos_token_id": 151643, "do_sample": true, "eos_token_id": [151645, 151643],
  "repetition_penalty": 1.1, "temperature": 0.7, "top_p": 0.8, "top_k": 20, "transformers_version": "4.37.0"}`)
	want := map[string]string{"repetition_penalty": "1.1", "temperature": "0.7", "top_p": "0.8", "top_k": "20"}
	if got := readGenerationConfig(dir); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	writeGenerationConfig(t, dir, `[1, 2]`)
	if got := readGenerationConfig(dir); got != nil {
		t.Errorf("not an object: %v, want none", got)
	}
	writeGenerationConfig(t, dir, `{"bos_token_id": 1, "transformers_version": "4.37.0"}`)
	if got := readGenerationConfig(dir); got != nil {
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
	got := readGenerationConfig(dir)
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
	if got := readGenerationConfig(dir); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	writeGenerationConfig(t, dir, `{"top_k": 40, "max_new_tokens": 9223372036854775808}`)
	if got := readGenerationConfig(dir); !reflect.DeepEqual(got, map[string]string{"top_k": "40"}) {
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
	if got := readGenerationConfig(dir); !reflect.DeepEqual(got, map[string]string{"temperature": at}) {
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

// With no staged checkpoint directory the agent has looked at nothing, and says nothing: empty, which is not
// "absent" (issue #175).
func TestGenerationConfigFileOfNoDirectoryIsEmpty(t *testing.T) {
	if got := generationConfigFile(""); got != "" {
		t.Errorf("generationConfigFile(\"\") = %q, want empty", got)
	}
}

// A checkpoint directory that is not there cannot be looked in, so the file is not claimed absent. And the
// classification of one entry, without the harness: nothing, a dangling link and a link to a regular file.
func TestGenerationConfigFileOfADirectoryThatIsGoneAndOfLinks(t *testing.T) {
	root := t.TempDir()
	if got := generationConfigFile(filepath.Join(root, "no-such-dir")); got != "unreadable" {
		t.Errorf("a missing checkpoint directory: %q, want unreadable", got)
	}
	dir := filepath.Join(root, "ckpt")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := generationConfigFile(dir); got != "absent" {
		t.Errorf("no entry: %q, want absent", got)
	}
	link := filepath.Join(dir, "generation_config.json")
	if err := os.Symlink("nowhere.json", link); err != nil {
		t.Fatal(err)
	}
	if got := generationConfigFile(dir); got != "unreadable" {
		t.Errorf("a dangling symlink: %q, want unreadable", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "nowhere.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := generationConfigFile(dir); got != "present" {
		t.Errorf("the link once its target exists: %q, want present", got)
	}
}
