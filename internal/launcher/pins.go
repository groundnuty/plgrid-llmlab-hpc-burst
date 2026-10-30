package launcher

import (
	"bufio"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

//go:embed k3s-pins.txt
var pinsFile string

// DefaultVersion is the k3s release the launcher starts unless told otherwise (docs/development.md).
const DefaultVersion = "v1.36.5+k3s1"

// Version is a parsed k3s release name, vMAJOR.MINOR.PATCH+k3sREV.
type Version struct {
	Major, Minor, Patch int
	Rev                 string // "k3s1"; empty when the operator gave none
}

// ParseVersion accepts "1.36.5", "v1.36.5" and "v1.36.5+k3s1".
func ParseVersion(s string) (Version, error) {
	core, rev, _ := strings.Cut(strings.TrimPrefix(s, "v"), "+")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("version %q: want MAJOR.MINOR.PATCH, optionally prefixed v and suffixed +k3sN", s)
	}
	var n [3]int
	for i, p := range parts {
		v, err := strconv.Atoi(p)
		if err != nil || v < 0 {
			return Version{}, fmt.Errorf("version %q: %q is not a number", s, p)
		}
		n[i] = v
	}
	if rev != "" && !strings.HasPrefix(rev, "k3s") {
		return Version{}, fmt.Errorf("version %q: the suffix must look like +k3s1", s)
	}
	return Version{n[0], n[1], n[2], rev}, nil
}

func (v Version) String() string {
	s := fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
	if v.Rev != "" {
		s += "+" + v.Rev
	}
	return s
}

// dirName is the directory name of the version in a binary directory: k3s-1.36.5.
func (v Version) dirName() string { return fmt.Sprintf("k3s-%d.%d.%d", v.Major, v.Minor, v.Patch) }

// Pin is one allowed release binary.
type Pin struct {
	Version Version
	Arch    string
	Asset   string
	SHA256  string
}

func parsePins(content string) ([]Pin, error) {
	var out []Pin
	sc := bufio.NewScanner(strings.NewReader(content))
	for n := 1; sc.Scan(); n++ {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.Fields(l)
		if len(f) != 4 {
			return nil, fmt.Errorf("k3s-pins.txt line %d: want 4 fields, got %d", n, len(f))
		}
		v, err := ParseVersion(f[0])
		if err != nil {
			return nil, fmt.Errorf("k3s-pins.txt line %d: %w", n, err)
		}
		if v.Rev == "" {
			return nil, fmt.Errorf("k3s-pins.txt line %d: a pin names the full release, with +k3sN", n)
		}
		if _, err := hex.DecodeString(f[3]); err != nil || len(f[3]) != 64 {
			return nil, fmt.Errorf("k3s-pins.txt line %d: %q is not a sha256", n, f[3])
		}
		out = append(out, Pin{Version: v, Arch: f[1], Asset: f[2], SHA256: f[3]})
	}
	return out, sc.Err()
}

// pinFor finds the pin for a version on this machine's architecture. A version without a +k3sN
// suffix matches the one pinned release with that MAJOR.MINOR.PATCH.
func pinFor(pins []Pin, v Version, arch string) (Pin, error) {
	var hit []Pin
	var known []string
	for _, p := range pins {
		if p.Arch != arch {
			continue
		}
		known = append(known, p.Version.String())
		if p.Version.Major == v.Major && p.Version.Minor == v.Minor && p.Version.Patch == v.Patch && (v.Rev == "" || v.Rev == p.Version.Rev) {
			hit = append(hit, p)
		}
	}
	switch len(hit) {
	case 1:
		return hit[0], nil
	case 0:
		return Pin{}, fmt.Errorf("k3s %s is not pinned for %s; pinned versions: %s", v, arch, strings.Join(known, ", "))
	}
	return Pin{}, fmt.Errorf("k3s %s matches several pins; give the +k3sN suffix", v)
}

// Binary is a verified k3s executable. file is the open descriptor whose bytes were hashed; the
// launcher executes that descriptor, not the path, so the bytes it runs are the bytes it checked.
type Binary struct {
	Version Version
	Path    string // absolute
	SHA256  string
	file    *os.File
}

// Close releases the descriptor of the verified binary.
func (b Binary) Close() {
	if b.file != nil {
		_ = b.file.Close()
	}
}

// ResolveBinary finds the pinned binary for the version under binDir (binDir/k3s-X.Y.Z/k3s,
// which is where hack/fetch-k3s.sh puts it), opens it, and checks the sha256 of what it opened
// against the pin. A missing file, or one with another hash, is an error: nothing unpinned ever
// starts. The caller closes the returned Binary.
func ResolveBinary(binDir, version string) (Binary, error) {
	pins, err := parsePins(pinsFile)
	if err != nil {
		return Binary{}, err
	}
	return resolveBinary(pins, runtime.GOARCH, binDir, version)
}

func resolveBinary(pins []Pin, arch, binDir, version string) (Binary, error) {
	v, err := ParseVersion(version)
	if err != nil {
		return Binary{}, err
	}
	pin, err := pinFor(pins, v, arch)
	if err != nil {
		return Binary{}, err
	}
	// Absolute before anything else: a relative --bin-dir means the caller's directory, and the
	// launcher later works from the state directory.
	path, err := filepath.Abs(filepath.Join(binDir, pin.Version.dirName(), "k3s"))
	if err != nil {
		return Binary{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Binary{}, fmt.Errorf("k3s %s: %w (fetch it with hack/fetch-k3s.sh)", pin.Version, err)
	}
	got, err := fdSHA256(f)
	if err != nil {
		f.Close()
		return Binary{}, fmt.Errorf("k3s %s: %w", pin.Version, err)
	}
	if got != pin.SHA256 {
		f.Close()
		return Binary{}, fmt.Errorf("k3s %s at %s has sha256 %s, but the pin says %s; refusing to run it", pin.Version, path, got, pin.SHA256)
	}
	return Binary{Version: pin.Version, Path: path, SHA256: got, file: f}, nil
}

// fdSHA256 hashes an open regular file from its start.
func fdSHA256(f *os.File) (string, error) {
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", f.Name())
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, fi.Size())); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// revNum is the number of a +k3sN suffix; 0 when there is none.
func (v Version) revNum() int {
	n, _ := strconv.Atoi(strings.TrimPrefix(v.Rev, "k3s"))
	return n
}

// compareVersions orders two releases: negative, zero or positive as a is older than, the same
// as, or newer than b.
func compareVersions(a, b Version) int {
	for _, d := range []int{a.Major - b.Major, a.Minor - b.Minor, a.Patch - b.Patch, a.revNum() - b.revNum()} {
		if d != 0 {
			return d
		}
	}
	return 0
}

// checkUpgradePath allows one step up (HBEP-11: "one minor version at a time"): to the next
// minor, or a newer patch or k3s revision of the same minor. A downgrade is never an upgrade: it
// is only possible by restoring a backup taken before the upgrade (hpcburst restore).
func checkUpgradePath(from, to Version) error {
	switch {
	case to.Major != from.Major:
		return fmt.Errorf("upgrade %s to %s changes the major version; only one minor step up is supported", from, to)
	case to.Minor == from.Minor+1:
		return nil
	case to.Minor == from.Minor && compareVersions(to, from) > 0:
		return nil
	case to.Minor == from.Minor && compareVersions(to, from) == 0:
		return fmt.Errorf("the host already runs %s", from)
	case compareVersions(to, from) < 0:
		return fmt.Errorf("%s is older than %s: a downgrade is not an upgrade; it is only possible by restoring a backup taken before the upgrade (hpcburst restore --backup DIR)", to, from)
	}
	return fmt.Errorf("upgrade %s to %s skips a minor version; upgrade one minor at a time", from, to)
}
