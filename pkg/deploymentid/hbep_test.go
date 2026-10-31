package deploymentid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
	"testing"
)

// hbep3Path is HBEP-3 relative to this package. The tests read the spec from
// it, so the table and the worked example are checked against the text that
// defines them rather than against a copy made by the code under test.
const hbep3Path = "../../hbep/0003-resource-model-and-naming/README.md"

func readHBEP3(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(hbep3Path)
	if err != nil {
		t.Fatalf("read HBEP-3 (the tests need the repository checkout): %v", err)
	}
	return string(b)
}

// worked example from HBEP-3, Deployment ID: 694 bytes and their SHA-256, as
// the HBEP prints them from sha256sum.
const (
	hbepExampleLen    = 694
	hbepExampleSHA256 = "0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852"
)

var printfExample = regexp.MustCompile(`(?m)^\s*\$ printf '%s' '(.*)' > v1-served-example\.json$`)

// hbepExampleBytes returns the bytes the HBEP's printf command writes: the
// text between its single quotes.
func hbepExampleBytes(t *testing.T) []byte {
	t.Helper()
	m := printfExample.FindStringSubmatch(readHBEP3(t))
	if m == nil {
		t.Fatal("HBEP-3's worked example printf command not found")
	}
	if strings.Contains(m[1], "'") {
		t.Fatal("the example text contains a single quote, so printf would not print it verbatim")
	}
	return []byte(m[1])
}

func TestHBEPExampleTextIsConsistent(t *testing.T) {
	b := hbepExampleBytes(t)
	if len(b) != hbepExampleLen {
		t.Fatalf("the example has %d bytes, HBEP-3 says %d", len(b), hbepExampleLen)
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); got != hbepExampleSHA256 {
		t.Fatalf("the example hashes to %s, HBEP-3 says %s: a spec bug, not a code bug", got, hbepExampleSHA256)
	}
	if !bytes.HasPrefix(b, []byte(`{"accelerator":`)) {
		t.Errorf("unexpected start of the example: %.30s", b)
	}
}

// The worked example's structured inputs, from HBEP-3's description of it: a
// served deployment of example-org/example-1b at a fixed commit, on vLLM
// 0.32.0, with two input flags and no input environment variable, on a GH200
// (aarch64) partition, with two files. Template, image and file hashes are one
// digit repeated 64 times.
func TestComputeReproducesHBEPWorkedExample(t *testing.T) {
	rep := func(c string) string { return strings.Repeat(c, 64) }
	spec := ServingSpec{
		Checkpoint: Checkpoint{Repository: "example-org/example-1b", Revision: "0123456789abcdef0123456789abcdef01234567"},
		Engine:     Engine{Name: "vllm", Version: "0.32.0", ImageDigest: "sha256:" + rep("2")},
		Flags:      []Flag{{"dtype", "bfloat16"}, {"max-model-len", "32768"}},
		// The HBEP says "no input environment variable"; a non-input one must
		// not change that.
		Env:                []EnvVar{{"HF_HOME", "/data/hf"}},
		ChatTemplateSHA256: rep("1"),
	}
	accel := Accelerator{GPUModel: "GH200", CPUArch: "aarch64"}
	// Given in reverse order: Compute sorts them.
	files := []File{{"model.safetensors", rep("4")}, {"config.json", rep("3")}}

	id, err := Compute(spec, accel, files)
	if err != nil {
		t.Fatal(err)
	}
	want := hbepExampleBytes(t)
	if !bytes.Equal(id.Canonical, want) {
		t.Fatalf("canonical bytes differ from HBEP-3's\n got %s\nwant %s", id.Canonical, want)
	}
	if len(id.Canonical) != hbepExampleLen {
		t.Errorf("canonical length %d, want %d", len(id.Canonical), hbepExampleLen)
	}
	if id.SHA256 != hbepExampleSHA256 {
		t.Errorf("SHA256 %s, want %s", id.SHA256, hbepExampleSHA256)
	}
	if id.Name != "0e1dd69efe8a" {
		t.Errorf("Name %q, want the display form 0e1dd69efe8a that HBEP-3 prints", id.Name)
	}
}
