package envoypin

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The pins are Envoy 1.40.2's release binaries. The expected sums are typed here from
// spikes/s13/README.md (Provenance, the Envoy 1.40.2 rows), where S13 recorded them from the
// release's signed checksums.txt.asc and GitHub's digest of each asset. They are never read from
// envoy-pins.txt, which is what is under test.
func TestEnvoyPinsAreTheSignedChecksums(t *testing.T) {
	want := []Pin{
		{Version: "1.40.2", Arch: "amd64", Asset: "envoy-1.40.2-linux-x86_64", SHA256: "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc"},
		{Version: "1.40.2", Arch: "arm64", Asset: "envoy-1.40.2-linux-aarch_64", SHA256: "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf"},
	}
	got, err := Pins()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the pins file holds\n%+v\nwant\n%+v", got, want)
	}
	for _, w := range want {
		p, err := For(w.Arch)
		if err != nil || p != w {
			t.Errorf("For(%q) = %+v, %v; want %+v", w.Arch, p, err, w)
		}
	}
	if Version != "1.40.2" {
		t.Errorf("Version = %q, want 1.40.2 (HBEP-15's Envoy row)", Version)
	}
}

func TestForAnUnpinnedArchitecture(t *testing.T) {
	for _, arch := range []string{"riscv64", "x86_64", "aarch64", ""} {
		if p, err := For(arch); err == nil {
			t.Errorf("For(%q) = %+v, want an error: only Go's amd64 and arm64 are pinned", arch, p)
		}
	}
}

// Each fault is refused with the line it is on; a file that pins nothing is refused too.
func TestEnvoyPinsRefuseAMalformedLine(t *testing.T) {
	const sum = "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc"
	good := "1.40.2 amd64 envoy-1.40.2-linux-x86_64 " + sum
	for _, c := range []struct{ name, text, want string }{
		{"three fields", "# a comment\n1.40.2 amd64 " + sum, "line 2: 3 fields"},
		{"five fields", good + " extra", "line 1: 5 fields"},
		{"a 63-character sum", "1.40.2 amd64 envoy-1.40.2-linux-x86_64 " + sum[:63], "line 1: sha256"},
		{"an uppercase sum", "1.40.2 amd64 envoy-1.40.2-linux-x86_64 " + strings.ToUpper(sum), "line 1: sha256"},
		{"a non-hex sum", "1.40.2 amd64 envoy-1.40.2-linux-x86_64 " + sum[:63] + "g", "line 1: sha256"},
		{"a v-prefixed version", "v1.40.2 amd64 envoy-1.40.2-linux-x86_64 " + sum, "line 1: version"},
		{"the kernel's name for amd64", "1.40.2 x86_64 envoy-1.40.2-linux-x86_64 " + sum, "line 1: architecture"},
		{"an unknown architecture", "1.40.2 riscv64 envoy-1.40.2-linux-riscv64 " + sum, "line 1: architecture"},
		{"the arm64 line names the x86_64 asset", "1.40.2 arm64 envoy-1.40.2-linux-x86_64 " + sum, "line 1: asset"},
		{"an asset of another version", "1.40.2 amd64 envoy-1.40.1-linux-x86_64 " + sum, "line 1: asset"},
		{"an asset with a path", "1.40.2 amd64 ../envoy-1.40.2-linux-x86_64 " + sum, "line 1: asset"},
		{"the same binary twice", good + "\n\n" + good, "line 3: Envoy 1.40.2 for amd64 is pinned already, on line 1"},
		{"no line", "# only comments\n\n", "pins no binary"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := parse(c.text)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("parse = %+v, %v; want an error containing %q", got, err, c.want)
			}
		})
	}
	if _, err := parse(good); err != nil {
		t.Errorf("a good line is refused: %v", err)
	}
}

// The expected sum is the published SHA-256 test vector for the three bytes "abc" (FIPS 180-2),
// not one computed here.
func TestCheckComparesTheSHA256WithThePin(t *testing.T) {
	const abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	p := Pin{Version: "1.40.2", Arch: "amd64", Asset: "envoy-1.40.2-linux-x86_64", SHA256: abc}
	if err := check(strings.NewReader("abc"), p); err != nil {
		t.Errorf("the bytes of the pin: %v", err)
	}
	if err := check(strings.NewReader("abd"), p); !errors.Is(err, ErrMismatch) {
		t.Errorf("other bytes: %v, want ErrMismatch", err)
	}
	// Against the real pins: three bytes are no Envoy.
	if err := Check(strings.NewReader("abc"), "amd64"); !errors.Is(err, ErrMismatch) {
		t.Errorf("Check of three bytes against the amd64 pin: %v, want ErrMismatch", err)
	}
	if err := Check(strings.NewReader("abc"), "riscv64"); err == nil || errors.Is(err, ErrMismatch) {
		t.Errorf("Check for an unpinned architecture: %v, want an error that is not a mismatch", err)
	}
}

func TestBinaryPathIsWhereTheFetchScriptInstalls(t *testing.T) {
	if got, want := BinaryPath("/opt/bin"), "/opt/bin/envoy-1.40.2/envoy"; got != want {
		t.Errorf("BinaryPath = %q, want %q (hack/fetch-envoy.sh DIR's layout)", got, want)
	}
}
