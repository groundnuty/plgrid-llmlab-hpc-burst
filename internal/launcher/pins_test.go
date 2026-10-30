package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	cases := []struct {
		in   string
		want Version
	}{
		{"v1.36.5+k3s1", Version{1, 36, 5, "k3s1"}},
		{"1.36.5", Version{1, 36, 5, ""}},
		{"v1.35.9", Version{1, 35, 9, ""}},
	}
	for _, c := range cases {
		got, err := ParseVersion(c.in)
		if err != nil || got != c.want {
			t.Errorf("ParseVersion(%q) = %+v, %v; want %+v", c.in, got, err, c.want)
		}
	}
	if s := (Version{1, 36, 5, "k3s1"}).String(); s != "v1.36.5+k3s1" {
		t.Errorf("String = %q", s)
	}
	for _, bad := range []string{"", "1.36", "1.36.x", "v1.36.5+rke2r1", "1.36.5.1", "-1.2.3"} {
		if _, err := ParseVersion(bad); err == nil {
			t.Errorf("ParseVersion(%q) accepted", bad)
		}
	}
}

func TestEmbeddedPins(t *testing.T) {
	pins, err := parsePins(pinsFile)
	if err != nil {
		t.Fatal(err)
	}
	// The expected sums are the ones in the releases' sha256sum-<arch>.txt files, which spike
	// S3 pinned independently in spikes/s3/scripts/fetch.sh (amd64).
	want := map[string]string{
		"v1.35.9+k3s1/amd64": "897acb21cca57d477a211102251a6591450ce767344a9faaa121a1007ca9fa9a",
		"v1.36.5+k3s1/amd64": "d73847bcd3c5fccef0115b372e2f9a91f3032dc84bbf71518a4617565294d313",
		"v1.35.9+k3s1/arm64": "6176dd12c512e177af998e6035920f6283c5fdbd5adfdff85c103a7fdf2a00b5",
		"v1.36.5+k3s1/arm64": "135e34cb9e8a1cfae3cb55577789e93501efe7edc080a8b2630458a85b07721e",
	}
	got := map[string]string{}
	for _, p := range pins {
		got[p.Version.String()+"/"+p.Arch] = p.SHA256
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("pin %s = %q, want %q", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d pins, want %d: %v", len(got), len(want), got)
	}
	if _, err := ParseVersion(DefaultVersion); err != nil {
		t.Errorf("default version: %v", err)
	}
	if p, err := pinFor(pins, mustVersion(t, DefaultVersion), "amd64"); err != nil || p.Asset != "k3s" {
		t.Errorf("the default version has no amd64 pin: %v", err)
	}
}

func mustVersion(t *testing.T, s string) Version {
	t.Helper()
	v, err := ParseVersion(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestParsePinsRejectsBadLines(t *testing.T) {
	for _, bad := range []string{
		"v1.36.5+k3s1 amd64 k3s",                                       // missing sum
		"v1.36.5 amd64 k3s " + strings.Repeat("a", 64),                 // no +k3sN
		"v1.36.5+k3s1 amd64 k3s " + strings.Repeat("z", 64),            // not hex
		"v1.36.5+k3s1 amd64 k3s " + strings.Repeat("a", 63),            // short
		"nonsense amd64 k3s " + strings.Repeat("a", 64),                // bad version
		"v1.36.5+k3s1 amd64 k3s " + strings.Repeat("a", 64) + " extra", // too many fields
	} {
		if _, err := parsePins(bad); err == nil {
			t.Errorf("parsePins accepted %q", bad)
		}
	}
}

func TestResolveBinary(t *testing.T) {
	// SHA-256 of the three bytes "abc" is a published test vector (FIPS 180-2).
	const abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	pins := []Pin{
		{Version: mustVersion(t, "v1.36.5+k3s1"), Arch: "amd64", Asset: "k3s", SHA256: abc},
		{Version: mustVersion(t, "v1.35.9+k3s1"), Arch: "amd64", Asset: "k3s", SHA256: strings.Repeat("0", 64)},
	}
	dir := t.TempDir()
	put := func(sub, content string) string {
		p := filepath.Join(dir, sub, "k3s")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := put("k3s-1.36.5", "abc")
	put("k3s-1.35.9", "not what the pin says")

	b, err := resolveBinary(pins, "amd64", dir, "1.36.5")
	if err != nil || b.Path != p || b.SHA256 != abc || b.Version.String() != "v1.36.5+k3s1" {
		t.Fatalf("resolve = %+v, %v", b, err)
	}
	if _, err := resolveBinary(pins, "amd64", dir, "v1.36.5+k3s1"); err != nil {
		t.Errorf("full name: %v", err)
	}
	if _, err := resolveBinary(pins, "amd64", dir, "v1.36.5+k3s2"); err == nil {
		t.Error("another +k3sN revision of a pinned version was accepted")
	}
	if _, err := resolveBinary(pins, "amd64", dir, "1.35.9"); err == nil || !strings.Contains(err.Error(), "refusing to run") {
		t.Errorf("a binary with the wrong hash must be refused, got %v", err)
	}
	if _, err := resolveBinary(pins, "amd64", dir, "1.37.0"); err == nil || !strings.Contains(err.Error(), "not pinned") {
		t.Errorf("an unpinned version must be refused, got %v", err)
	}
	if _, err := resolveBinary(pins, "arm64", dir, "1.36.5"); err == nil {
		t.Error("a version with no pin for this architecture was accepted")
	}
	if _, err := resolveBinary(pins, "amd64", t.TempDir(), "1.36.5"); err == nil || !strings.Contains(err.Error(), "fetch-k3s.sh") {
		t.Errorf("a missing binary should point at hack/fetch-k3s.sh, got %v", err)
	}
}

func TestCheckUpgradePath(t *testing.T) {
	v := func(s string) Version { return mustVersion(t, s) }
	cases := []struct {
		from, to string
		ok       bool
	}{
		{"v1.35.9+k3s1", "v1.36.5+k3s1", true},
		{"v1.35.9+k3s1", "v1.35.9+k3s1", false}, // same
		{"v1.36.5+k3s1", "v1.35.9+k3s1", false}, // downgrade
		{"v1.34.1+k3s1", "v1.36.5+k3s1", false}, // skips a minor
		{"v1.35.9+k3s1", "v2.36.0+k3s1", false}, // major
	}
	for _, c := range cases {
		if err := checkUpgradePath(v(c.from), v(c.to)); (err == nil) != c.ok {
			t.Errorf("%s -> %s: err = %v, want ok=%v", c.from, c.to, err, c.ok)
		}
	}
}
