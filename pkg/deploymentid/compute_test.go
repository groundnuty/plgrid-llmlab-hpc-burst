package deploymentid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"unicode/utf16"
)

func rep64(c string) string { return strings.Repeat(c, 64) }

// baseSpec is a valid served deployment that the tests mutate one field at a
// time.
func baseSpec() (ServingSpec, Accelerator, []File) {
	spec := ServingSpec{
		Checkpoint: Checkpoint{Repository: "example-org/example-7b", Revision: strings.Repeat("ab", 20)},
		Engine:     Engine{Name: "vllm", Version: "0.32.0", ImageDigest: "sha256:" + rep64("6")},
		Flags: []Flag{
			{"dtype", "bfloat16"}, {"port", "8000"}, {"lora-modules", "a"}, {"lora-modules", "b"},
		},
		Env:                []EnvVar{{"VLLM_USE_DEEP_GEMM", "1"}, {"HF_HOME", "/data/hf"}},
		ChatTemplateSHA256: rep64("1"),
	}
	files := []File{{"config.json", rep64("a")}, {"model.safetensors", rep64("b")}}
	return spec, Accelerator{GPUModel: "GH200", CPUArch: "aarch64"}, files
}

func mustCompute(t *testing.T, spec ServingSpec, accel Accelerator, files []File) ID {
	t.Helper()
	id, err := Compute(spec, accel, files)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	return id
}

// vectorInput is the structured input of a golden vector (input.json).
type vectorInput struct {
	Kind string `json:"kind"`
	Spec struct {
		Checkpoint struct{ Repository, Revision string } `json:"checkpoint"`
		Engine     struct {
			Name, Version string
			ImageDigest   string `json:"image_digest"`
		} `json:"engine"`
		Flags              []struct{ Name, Value string } `json:"flags"`
		Env                []struct{ Name, Value string } `json:"env"`
		ChatTemplateSHA256 string                         `json:"chat_template_sha256"`
	} `json:"spec"`
	ChatTemplateSources *struct {
		TokenizerConfig string `json:"tokenizer_config"`
	} `json:"chat_template_sources"`
	Accelerator struct {
		GPUModel string `json:"gpu_model"`
		CPUArch  string `json:"cpu_arch"`
	} `json:"accelerator"`
	Files   []struct{ Path, SHA256 string } `json:"files"`
	Catalog struct {
		Platform    string
		CatalogName string `json:"catalog_name"`
		Revision    string
	} `json:"catalog"`
}

func readJSONStrict(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func computeVector(t *testing.T, dir string, in vectorInput) ID {
	t.Helper()
	if in.Kind == "managed" {
		id, err := ComputeManaged(CatalogEntry(in.Catalog))
		if err != nil {
			t.Fatalf("ComputeManaged: %v", err)
		}
		return id
	}
	spec := ServingSpec{
		Checkpoint:         Checkpoint(in.Spec.Checkpoint),
		Engine:             Engine{in.Spec.Engine.Name, in.Spec.Engine.Version, in.Spec.Engine.ImageDigest},
		ChatTemplateSHA256: in.Spec.ChatTemplateSHA256,
	}
	for _, f := range in.Spec.Flags {
		spec.Flags = append(spec.Flags, Flag(f))
	}
	for _, e := range in.Spec.Env {
		spec.Env = append(spec.Env, EnvVar(e))
	}
	if src := in.ChatTemplateSources; src != nil {
		cfg, err := os.ReadFile(filepath.Join(dir, src.TokenizerConfig))
		if err != nil {
			t.Fatal(err)
		}
		text, err := ResolveChatTemplate(TemplateSources{TokenizerConfig: cfg})
		if err != nil {
			t.Fatal(err)
		}
		spec.ChatTemplateSHA256 = ChatTemplateSHA256(text)
	}
	var files []File
	for _, f := range in.Files {
		files = append(files, File(f))
	}
	return mustCompute(t, spec, Accelerator(in.Accelerator), files)
}

// The golden vectors: inputs and the expected object written by hand, and the
// canonical bytes and hash computed by gen_vectors.py with the rfc8785 package
// and hashlib, outside this package (see testdata/README.md).
func TestGoldenVectors(t *testing.T) {
	dirs, err := filepath.Glob("testdata/vectors/*")
	if err != nil || len(dirs) < 4 {
		t.Fatalf("golden vectors not found (%d, %v); HBEP-3 needs the worked example and at least three more", len(dirs), err)
	}
	for _, dir := range dirs {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			var in vectorInput
			readJSONStrict(t, filepath.Join(dir, "input.json"), &in)
			want, err := os.ReadFile(filepath.Join(dir, "canonical.json"))
			if err != nil {
				t.Fatal(err)
			}
			sumFile, err := os.ReadFile(filepath.Join(dir, "sha256.txt"))
			if err != nil {
				t.Fatal(err)
			}
			wantHash, _, _ := strings.Cut(string(sumFile), " ")

			id := computeVector(t, dir, in)
			if !bytes.Equal(id.Canonical, want) {
				t.Errorf("canonical bytes differ\n got %s\nwant %s", id.Canonical, want)
			}
			if id.SHA256 != wantHash {
				t.Errorf("SHA256 %s, want %s", id.SHA256, wantHash)
			}
			if id.Name != wantHash[:12] {
				t.Errorf("Name %s, want %s", id.Name, wantHash[:12])
			}
			// The expected bytes are an encoding of the hand-written object.
			var obj, parsed any
			readJSONStrict(t, filepath.Join(dir, "expected-object.json"), &obj)
			if err := json.Unmarshal(id.Canonical, &parsed); err != nil || !reflect.DeepEqual(obj, parsed) {
				t.Errorf("the canonical bytes do not decode to the hand-written object (%v)", err)
			}
		})
	}
}

func TestFirstVectorIsHBEPExample(t *testing.T) {
	got, err := os.ReadFile("testdata/vectors/01-hbep-example/canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, hbepExampleBytes(t)) {
		t.Error("vector 01's canonical.json is not the byte string printed in HBEP-3")
	}
}

// Keys in vector 05 that UTF-16 order sorts differently from UTF-8 byte
// order, and file paths that bytewise order sorts differently from UTF-16
// order: the vector fails a serialiser that gets either rule wrong.
func TestVector05SeparatesTheTwoOrders(t *testing.T) {
	b, err := os.ReadFile("testdata/vectors/05-unicode-order/canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Flags map[string]json.RawMessage `json:"flags"`
		Env   map[string]json.RawMessage `json:"env"`
		Files []struct{ Path string }    `json:"files"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	for name, keys := range map[string][]string{"flags": objectKeys(t, b, "flags"), "env": objectKeys(t, b, "env")} {
		if sort.StringsAreSorted(keys) {
			t.Errorf("%s keys are in bytewise order; the vector would not catch a bytewise sort: %q", name, keys)
		}
		if !slices.IsSortedFunc(keys, compareUTF16) {
			t.Errorf("%s keys are not in UTF-16 order: %q", name, keys)
		}
	}
	var paths []string
	for _, f := range doc.Files {
		paths = append(paths, f.Path)
	}
	if !sort.StringsAreSorted(paths) {
		t.Errorf("file paths are not in bytewise order: %q", paths)
	}
	if slices.IsSortedFunc(paths, compareUTF16) {
		t.Errorf("file paths are also in UTF-16 order; the vector would not catch sorting them that way: %q", paths)
	}
}

func compareUTF16(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

// objectKeys returns the keys of the named top-level object, in document order.
func objectKeys(t *testing.T, doc []byte, name string) []string {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(doc, &top); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(top[name]))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("%s is not an object", name)
	}
	var keys []string
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestIDShape(t *testing.T) {
	spec, accel, files := baseSpec()
	id := mustCompute(t, spec, accel, files)
	sum := sha256.Sum256(id.Canonical)
	if id.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("SHA256 is not the SHA-256 of Canonical")
	}
	if len(id.SHA256) != 64 || strings.ToLower(id.SHA256) != id.SHA256 {
		t.Errorf("SHA256 %q is not 64 lowercase hex characters", id.SHA256)
	}
	if len(id.Name) != NameLength || NameLength != 12 || id.Name != id.SHA256[:12] {
		t.Errorf("Name %q is not the first 12 characters of %q", id.Name, id.SHA256)
	}
}

// The inputs of an ID are exactly the fields HBEP-3 lists. Job ID, node, URL,
// lease end and keys are not fields, so they cannot change an ID. Adding a
// field is a scheme decision; this test makes it a deliberate one.
func TestInputFieldsAreExactlyThoseOfHBEP3(t *testing.T) {
	var got []string
	var walk func(prefix string, typ reflect.Type)
	walk = func(prefix string, typ reflect.Type) {
		if typ.Kind() == reflect.Slice {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct {
			got = append(got, prefix)
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			walk(prefix+"."+typ.Field(i).Name, typ.Field(i).Type)
		}
	}
	for _, v := range []any{ServingSpec{}, Accelerator{}, File{}, CatalogEntry{}} {
		walk(reflect.TypeOf(v).Name(), reflect.TypeOf(v))
	}
	want := []string{
		"ServingSpec.Checkpoint.Repository", "ServingSpec.Checkpoint.Revision",
		"ServingSpec.Engine.Name", "ServingSpec.Engine.Version", "ServingSpec.Engine.ImageDigest",
		"ServingSpec.Flags.Name", "ServingSpec.Flags.Value", "ServingSpec.Env.Name", "ServingSpec.Env.Value",
		"ServingSpec.ChatTemplateSHA256",
		"Accelerator.GPUModel", "Accelerator.CPUArch",
		"File.Path", "File.SHA256",
		"CatalogEntry.Platform", "CatalogEntry.CatalogName", "CatalogEntry.Revision",
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("input fields changed\n got %q\nwant %q", got, want)
	}
}

func TestComputeDoesNotMutateInputs(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Flags = []Flag{{"seed", "1"}, {"dtype", "x"}, {"seed", "2"}}
	spec.Env = []EnvVar{{"Z", "1"}, {"A", "2"}}
	files = []File{{"z", rep64("a")}, {"a", rep64("b")}}
	spec0 := ServingSpec{Checkpoint: spec.Checkpoint, Engine: spec.Engine, ChatTemplateSHA256: spec.ChatTemplateSHA256,
		Flags: slices.Clone(spec.Flags), Env: slices.Clone(spec.Env)}
	files0 := slices.Clone(files)
	mustCompute(t, spec, accel, files)
	if !reflect.DeepEqual(spec, spec0) || !reflect.DeepEqual(files, files0) {
		t.Error("Compute changed its arguments")
	}
}

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

func TestFlagsObjectShape(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Flags = []Flag{{"seed", "1"}, {"port", "1"}, {"lora-modules", "a"}, {"seed", "2"}, {"enforce-eager", "false"}}
	spec.Env = nil
	id := mustCompute(t, spec, accel, files)
	var doc struct {
		Flags map[string]any `json:"flags"`
		Env   map[string]any `json:"env"`
	}
	if err := json.Unmarshal(id.Canonical, &doc); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"seed": []any{"1", "2"}, "lora-modules": "a", "enforce-eager": "false"}
	if !reflect.DeepEqual(doc.Flags, want) {
		t.Errorf("flags %v, want %v (a repeated flag is an array, a single one a string, a non-input absent)", doc.Flags, want)
	}
	if doc.Env == nil || len(doc.Env) != 0 {
		t.Errorf("env %v, want {}", doc.Env)
	}
	if !bytes.Contains(id.Canonical, []byte(`"env":{}`)) {
		t.Errorf("env is not written as {}: %s", id.Canonical)
	}
}

func TestComputeManaged(t *testing.T) {
	e := CatalogEntry{Platform: "llmlab", CatalogName: "example-27b", Revision: "2026-09"}
	id, err := ComputeManaged(e)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"catalog_name":"example-27b","kind":"managed","opaque":"true","platform":"llmlab","revision":"2026-09","scheme":"v1"}`
	if string(id.Canonical) != want {
		t.Errorf("got  %s\nwant %s", id.Canonical, want)
	}
	noRev, err := ComputeManaged(CatalogEntry{Platform: "llmlab", CatalogName: "example-27b"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(noRev.Canonical, []byte("revision")) {
		t.Errorf("an empty revision must be omitted: %s", noRev.Canonical)
	}
	for name, other := range map[string]CatalogEntry{
		"platform": {"llmlab2", "example-27b", "2026-09"}, "catalog name": {"llmlab", "example-28b", "2026-09"},
		"revision": {"llmlab", "example-27b", "2026-10"}, "no revision": {"llmlab", "example-27b", ""},
	} {
		o, err := ComputeManaged(other)
		if err != nil {
			t.Fatal(err)
		}
		if o.SHA256 == id.SHA256 {
			t.Errorf("changing the %s did not change the ID", name)
		}
	}
	for _, bad := range []CatalogEntry{{"", "x", ""}, {"x", "", ""}, {"x\xff", "y", ""}} {
		if _, err := ComputeManaged(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: got %v, want ErrInvalid", bad, err)
		}
	}
}

// A catalog entry and a served deployment of the same checkpoint never get
// the same ID (HBEP-3, Test plan).
func TestManagedAndServedNeverCollide(t *testing.T) {
	spec, accel, files := baseSpec()
	served := mustCompute(t, spec, accel, files)
	managed, err := ComputeManaged(CatalogEntry{Platform: "p", CatalogName: spec.Checkpoint.Repository, Revision: spec.Checkpoint.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if served.SHA256 == managed.SHA256 {
		t.Error("a served and a managed deployment share an ID")
	}
	if !bytes.Contains(served.Canonical, []byte(`"kind":"served"`)) || !bytes.Contains(managed.Canonical, []byte(`"kind":"managed"`)) {
		t.Error("kind is not in the canonical object")
	}
}
