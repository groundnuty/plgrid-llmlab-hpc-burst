package deploymentid

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestRefusals(t *testing.T) {
	cases := []struct {
		name string
		flag *Flag
		env  *EnvVar
		want error
	}{
		{"short alias -tp", &Flag{"-tp", "2"}, nil, ErrShortAlias},
		{"short alias -q", &Flag{"-q", "awq"}, nil, ErrShortAlias},
		{"short alias -O3", &Flag{"-O3", ""}, nil, ErrShortAlias},
		{"long name with dashes", &Flag{"--dtype", "bfloat16"}, nil, ErrInvalid},
		{"lone dash", &Flag{"-", ""}, nil, ErrInvalid},
		{"api-key", &Flag{"api-key", "SECRET-VALUE"}, nil, ErrSecret},
		{"hf-token", &Flag{"hf-token", "SECRET-VALUE"}, nil, ErrSecret},
		{"HF_TOKEN", nil, &EnvVar{"HF_TOKEN", "SECRET-VALUE"}, ErrSecret},
		{"model", &Flag{"model", "x/y"}, nil, ErrFrameworkPassed},
		{"revision", &Flag{"revision", "main"}, nil, ErrFrameworkPassed},
		{"chat-template", &Flag{"chat-template", "{{ x }}"}, nil, ErrFrameworkPassed},
		// Credentials vLLM reads from the environment (vllm/envs.py@v0.32.0), and
		// the flags HBEP-3 refuses: config reads options from a file the ID does
		// not see, lora-modules names unpinned checkpoints (alpha).
		{"VLLM_API_KEY", nil, &EnvVar{"VLLM_API_KEY", "SECRET-VALUE"}, ErrSecret},
		{"S3_ACCESS_KEY_ID", nil, &EnvVar{"S3_ACCESS_KEY_ID", "SECRET-VALUE"}, ErrSecret},
		{"S3_SECRET_ACCESS_KEY", nil, &EnvVar{"S3_SECRET_ACCESS_KEY", "SECRET-VALUE"}, ErrSecret},
		{"config", &Flag{"config", "/data/vllm.yaml"}, nil, ErrRefused},
		{"config.x", &Flag{"config.x", "1"}, nil, ErrRefused},
		{"conf", &Flag{"conf", "/data/vllm.yaml"}, nil, ErrRefused},
		{"lora-modules", &Flag{"lora-modules", "a=org/lora"}, nil, ErrRefused},
		{"lora_modules", &Flag{"lora_modules", "a=org/lora"}, nil, ErrRefused},
		{"lora-module", &Flag{"lora-module", "a=org/lora"}, nil, ErrRefused},
		// Spellings vLLM resolves to a refused flag (vllm/utils/argparse_utils.py
		// @v0.32.0): an underscore for a dash, a dotted sub-key, a unique
		// prefix. Each is refused as the flag itself is, with no value in the
		// message (Opus's review rows).
		{"api_key", &Flag{"api_key", "SECRET-VALUE"}, nil, ErrSecret},
		{"api-k", &Flag{"api-k", "SECRET-VALUE"}, nil, ErrSecret},
		{"api-key.x", &Flag{"api-key.x", "SECRET-VALUE"}, nil, ErrSecret},
		{"api_key.x", &Flag{"api_key.x", "SECRET-VALUE"}, nil, ErrSecret},
		{"hf_token", &Flag{"hf_token", "SECRET-VALUE"}, nil, ErrSecret},
		{"hf-tok", &Flag{"hf-tok", "SECRET-VALUE"}, nil, ErrSecret},
		{"hf-token.x", &Flag{"hf-token.x", "SECRET-VALUE"}, nil, ErrSecret},
		{"revisio", &Flag{"revisio", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		{"revision.x", &Flag{"revision.x", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		{"model.x", &Flag{"model.x", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		{"chat-template.x", &Flag{"chat-template.x", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		{"chat_template", &Flag{"chat_template", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		{"chat-templ", &Flag{"chat-templ", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		// Single-letter prefixes are abbreviations too.
		{"a", &Flag{"a", "SECRET-VALUE"}, nil, ErrSecret},
		{"m", &Flag{"m", "SECRET-VALUE"}, nil, ErrFrameworkPassed},
		// A dotted sub-key of any other flag: pass the JSON under the long name.
		{"dotted sub-key", &Flag{"hf-overrides.rope_scaling", "{}"}, nil, ErrInvalid},
		{"dotted unknown flag", &Flag{"some-flag.x", "1"}, nil, ErrInvalid},
		{"dotted not-input flag", &Flag{"port.x", "1"}, nil, ErrInvalid},
		{"dot only", &Flag{".", "1"}, nil, ErrInvalid},
		{"leading dot", &Flag{".x", "1"}, nil, ErrInvalid},
		// A negated boolean: the positive name with "false" (HBEP-3).
		{"no-enable-prefix-caching", &Flag{"no-enable-prefix-caching", ""}, nil, ErrInvalid},
		{"no-enable_prefix_caching", &Flag{"no-enable_prefix_caching", ""}, nil, ErrInvalid},
		{"no-enforce-eager", &Flag{"no-enforce-eager", ""}, nil, ErrInvalid},
		// vLLM's short options, spelled without their dash (owner decision 4).
		{"tp", &Flag{"tp", "2"}, nil, ErrShortAlias},
		{"pp", &Flag{"pp", "2"}, nil, ErrShortAlias},
		{"dp", &Flag{"dp", "2"}, nil, ErrShortAlias},
		{"q", &Flag{"q", "awq"}, nil, ErrShortAlias},
		{"tp.x", &Flag{"tp.x", "2"}, nil, ErrShortAlias},
		// Names are ASCII (owner decision 1): flags [a-z0-9][a-z0-9-]*.
		{"flag with an accent", &Flag{"dtyp\u00e9", "x"}, nil, ErrInvalid},
		{"flag that is an emoji", &Flag{"\U0001F600", "x"}, nil, ErrInvalid},
		// The keys vector 05 used before names became ASCII (owner decision 1).
		{"flag U+20AC", &Flag{"\u20ac", "x"}, nil, ErrInvalid},
		{"flag U+00F6", &Flag{"\u00f6", "x"}, nil, ErrInvalid},
		{"flag U+FB33", &Flag{"\ufb33", "x"}, nil, ErrInvalid},
		{"flag U+1F600", &Flag{"\U0001F600", "x"}, nil, ErrInvalid},
		{"flag in capitals", &Flag{"DTYPE", "x"}, nil, ErrInvalid},
		{"flag in capitals, a refused one", &Flag{"API-KEY", "SECRET-VALUE"}, nil, ErrInvalid},
		{"flag with a plus", &Flag{"x+", "x"}, nil, ErrInvalid},
		{"flag with a slash", &Flag{"a/b", "x"}, nil, ErrInvalid},
		{"flag with an en dash", &Flag{"x\u2013y", "x"}, nil, ErrInvalid},
		{"empty flag name", &Flag{"", "x"}, nil, ErrInvalid},
		{"flag name with space", &Flag{"max model len", "1"}, nil, ErrInvalid},
		{"flag name with =", &Flag{"dtype=bfloat16", ""}, nil, ErrInvalid},
		{"flag name with newline", &Flag{"a\nb", ""}, nil, ErrInvalid},
		{"flag name not UTF-8", &Flag{"a\xff", ""}, nil, ErrInvalid},
		{"flag value not UTF-8", &Flag{"dtype", "a\xffb"}, nil, ErrInvalid},
		{"empty env name", nil, &EnvVar{"", "x"}, ErrInvalid},
		// Environment names are [A-Z_][A-Z0-9_]*, exact, never rewritten.
		{"env with an accent", nil, &EnvVar{"H\u00c9LLO", "x"}, ErrInvalid},
		{"env that is an emoji", nil, &EnvVar{"\U0001F600_ENV", "x"}, ErrInvalid},
		{"env U+FF5E", nil, &EnvVar{"\uff5e_ENV", "x"}, ErrInvalid},
		{"env in lower case", nil, &EnvVar{"my_var", "x"}, ErrInvalid},
		{"env starting with a digit", nil, &EnvVar{"1VAR", "x"}, ErrInvalid},
		{"env with a dash", nil, &EnvVar{"HF-TOKEN", "SECRET-VALUE"}, ErrInvalid},
		{"env with a dot", nil, &EnvVar{"A.B", "x"}, ErrInvalid},
		{"env with a space", nil, &EnvVar{"A B", "x"}, ErrInvalid},
		{"env with a newline", nil, &EnvVar{"A\nB", "x"}, ErrInvalid},
		{"env name not UTF-8", nil, &EnvVar{"A\xff", "x"}, ErrInvalid},
		{"env name with =", nil, &EnvVar{"A=B", ""}, ErrInvalid},
		{"env value not UTF-8", nil, &EnvVar{"A", "\xff"}, ErrInvalid},
	}
	for _, c := range cases {
		spec, accel, files := baseSpec()
		if c.flag != nil {
			spec.Flags = append(spec.Flags, *c.flag)
		}
		if c.env != nil {
			spec.Env = append(spec.Env, *c.env)
		}
		_, err := Compute(spec, accel, files)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
			continue
		}
		if strings.Contains(err.Error(), "SECRET-VALUE") {
			t.Errorf("%s: the error repeats the secret value: %v", c.name, err)
		}
	}
}

func TestRefusalIsNotMaskedByNonInputClass(t *testing.T) {
	// A refused name is an error wherever it appears, also before and after
	// non-input flags, and also when repeated.
	spec, accel, files := baseSpec()
	spec.Flags = []Flag{{"port", "1"}, {"api-key", "k"}, {"port", "2"}}
	if _, err := Compute(spec, accel, files); !errors.Is(err, ErrSecret) {
		t.Errorf("got %v, want ErrSecret", err)
	}
}

func TestValidationErrors(t *testing.T) {
	mut := map[string]func(*ServingSpec, *Accelerator, *[]File){
		"empty repository": func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Repository = "" },
		"short revision":   func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Revision = "abc123" },
		"41-char revision": func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Revision += "a" },
		"uppercase revision": func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Checkpoint.Revision = strings.ToUpper(s.Checkpoint.Revision)
		},
		"branch as revision":    func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Revision = "main" },
		"repository not UTF-8":  func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Repository = "a\xff" },
		"empty engine version":  func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.Version = "" },
		"digest without prefix": func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.ImageDigest = rep64("6") },
		"digest short":          func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.ImageDigest = "sha256:66" },
		"digest uppercase":      func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.ImageDigest = "sha256:" + rep64("A") },
		"digest other algo":     func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.ImageDigest = "sha512:" + rep64("6") },
		"empty template hash":   func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.ChatTemplateSHA256 = "" },
		"uppercase template":    func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.ChatTemplateSHA256 = rep64("A") },
		"empty gpu model":       func(_ *ServingSpec, a *Accelerator, _ *[]File) { a.GPUModel = "" },
		"empty cpu arch":        func(_ *ServingSpec, a *Accelerator, _ *[]File) { a.CPUArch = "" },
		"no files":              func(_ *ServingSpec, _ *Accelerator, f *[]File) { *f = nil },
		"empty file list":       func(_ *ServingSpec, _ *Accelerator, f *[]File) { *f = []File{} },
		"duplicate path":        func(_ *ServingSpec, _ *Accelerator, f *[]File) { *f = append(*f, (*f)[0]) },
		"empty path":            func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "" },
		"absolute path":         func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "/etc/x" },
		"empty segment":         func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "a//b" },
		"trailing slash":        func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "a/" },
		"dot dot":               func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "a/../b" },
		"dot":                   func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "./a" },
		"path not UTF-8":        func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].Path = "a\xff" },
		"file hash uppercase":   func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].SHA256 = rep64("A") },
		"file hash short":       func(_ *ServingSpec, _ *Accelerator, f *[]File) { (*f)[0].SHA256 = "ab" },
		"duplicate env":         func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Env = append(s.Env, s.Env[0]) },
		"duplicate non-input env": func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Env = append(s.Env, EnvVar{"HF_HOME", "other"})
		},
	}
	for name, m := range mut {
		spec, accel, files := baseSpec()
		m(&spec, &accel, &files)
		if _, err := Compute(spec, accel, files); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
}

func TestUnusualPathsAreAccepted(t *testing.T) {
	spec, accel, _ := baseSpec()
	for _, p := range []string{"a b", "q\"uote", "back\\slash", "new\nline", "tab\t", "sep\xe2\x80\xa8", "\xc3\xbc", "-dash", "a..b", ".hidden", "a/.b", "..."} {
		files := []File{{p, rep64("a")}}
		id := mustCompute(t, spec, accel, files)
		var doc struct{ Files []struct{ Path string } }
		if err := json.Unmarshal(id.Canonical, &doc); err != nil || len(doc.Files) != 1 || doc.Files[0].Path != p {
			t.Errorf("path %q did not survive: %v %+v", p, err, doc)
		}
	}
}
