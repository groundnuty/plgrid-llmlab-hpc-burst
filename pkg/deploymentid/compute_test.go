package deploymentid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
