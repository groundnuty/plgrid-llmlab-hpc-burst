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
		{"empty flag name", &Flag{"", "x"}, nil, ErrInvalid},
		{"flag name with space", &Flag{"max model len", "1"}, nil, ErrInvalid},
		{"flag name with =", &Flag{"dtype=bfloat16", ""}, nil, ErrInvalid},
		{"flag name with newline", &Flag{"a\nb", ""}, nil, ErrInvalid},
		{"flag name not UTF-8", &Flag{"a\xff", ""}, nil, ErrInvalid},
		{"flag value not UTF-8", &Flag{"dtype", "a\xffb"}, nil, ErrInvalid},
		{"empty env name", nil, &EnvVar{"", "x"}, ErrInvalid},
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
