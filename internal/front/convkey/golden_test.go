package convkey

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The golden vectors of HBEP-5's Test plan (Conversation key). Each case has
// a messages array as a client might send it (testdata/golden/*.input.json,
// with extra members and non-canonical spacing), the canonical bytes of its
// reduced opening written by hand (*.jcs, no trailing newline), and the
// SHA-256 of those bytes as printed by sha256sum, outside this code:
//
//	cd internal/front/convkey/testdata/golden && sha256sum *.jcs
//
// (also kept in testdata/golden/SHA256SUMS). Neither the canonical bytes nor
// the hashes come from the code under test.
var golden = []struct {
	name, sha256 string
}{
	{"one_system", "6e318fa6aaf33adedaa2ad90228ea6f6009f6cf22f64feec32a04cbea27840a3"},
	{"two_system_developer", "3d3c80d5180944c02b1345b86f178bd75529133343688b0e0439cfa62d5d4418"},
	{"no_system", "38f5b3c09554e1009261e96bf622696839cdee3c3f0126335e8224f627ca6356"},
	{"content_parts", "2252095202a00fd6fbb219f75b56e088e3d9f2d3c03ddc75a19332b7e9bca7b7"},
	{"user_after_assistant", "8b3390f0fe1779f96c80a413afa08090aa42e858f9e1dc0f05d1c6e755a7460d"},
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOpeningMatchesHandWrittenJCSAndSha256sum(t *testing.T) {
	for _, c := range golden {
		t.Run(c.name, func(t *testing.T) {
			input := readGolden(t, c.name+".input.json")
			got, err := Opening(input)
			if err != nil {
				t.Fatalf("Opening: %v", err)
			}
			if want := OpeningPrefix + c.sha256; got != want {
				t.Errorf("key = %s, want %s (sha256sum of the hand-written %s.jcs)", got, want, c.name)
			}
		})
	}
}

// TestReducedOpeningBytesAreTheHandWrittenOnes checks the bytes, not only the
// hash, so a failure shows where the serialiser differs from the hand-written
// canonical form.
func TestReducedOpeningBytesAreTheHandWrittenOnes(t *testing.T) {
	for _, c := range golden {
		t.Run(c.name, func(t *testing.T) {
			want := readGolden(t, c.name+".jcs")
			got, err := canonical(want)
			if err != nil {
				t.Fatalf("canonical(hand-written bytes): %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("canonical form of the hand-written bytes changed them:\n got %s\nwant %s", got, want)
			}
		})
	}
}

func TestSameOpeningDifferentLaterTurnsSameKey(t *testing.T) {
	a := `[{"role":"system","content":"S"},{"role":"user","content":"first"},{"role":"assistant","content":"one"}]`
	b := `[{"role":"system","content":"S"},{"role":"user","content":"first"},{"role":"assistant","content":"two"},{"role":"user","content":"more"}]`
	ka, err := Opening([]byte(a))
	if err != nil {
		t.Fatal(err)
	}
	kb, err := Opening([]byte(b))
	if err != nil {
		t.Fatal(err)
	}
	if ka != kb {
		t.Errorf("keys differ for one opening: %s and %s", ka, kb)
	}
	c := `[{"role":"system","content":"S"},{"role":"user","content":"other first"}]`
	kc, err := Opening([]byte(c))
	if err != nil {
		t.Fatal(err)
	}
	if kc == ka {
		t.Errorf("a different first user message gave the same key %s", kc)
	}
}
