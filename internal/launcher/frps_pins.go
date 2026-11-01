package launcher

import (
	"bufio"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// The frps pin, the way k3s is pinned (pins.go): one embedded file names the allowed release and
// the sha256 of the executable, the launcher hashes the file it opened, and it runs that very
// descriptor. The same file pins frpc's sha256, which the controller puts in agent.json. HBEP-6 names build/dependencies.yaml (HBEP-15) as the source of frps's checksum; the
// file does not exist yet, so frp-pins.txt stands in for it, as k3s-pins.txt does for k3s.

//go:embed frp-pins.txt
var frpPinsFile string

// DefaultFrpVersion is the frp release whose frps the launcher starts (HBEP-6: frp v0.71.1).
const DefaultFrpVersion = "0.71.1"

// FrpPin is one allowed frp release, per architecture.
type FrpPin struct {
	Version       string
	Arch          string // the release's name for it: amd64, arm64
	Tarball       string // the release asset's name
	TarballSHA256 string // the asset's sum, from the release's checksums
	FrpsSHA256    string // the sum of the frps executable inside it: what is checked before a start
	FrpcSHA256    string // the sum of the frpc executable inside it: what agent.json names
}

func parseFrpPins(content string) ([]FrpPin, error) {
	var out []FrpPin
	sc := bufio.NewScanner(strings.NewReader(content))
	for n := 1; sc.Scan(); n++ {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.Fields(l)
		if len(f) != 6 {
			return nil, fmt.Errorf("frp-pins.txt line %d: want 6 fields, got %d", n, len(f))
		}
		for _, sum := range f[3:] {
			if !sha256Form.MatchString(sum) {
				return nil, fmt.Errorf("frp-pins.txt line %d: %q is not a sha256 of 64 lowercase hex characters", n, sum)
			}
		}
		out = append(out, FrpPin{Version: f[0], Arch: f[1], Tarball: f[2], TarballSHA256: f[3], FrpsSHA256: f[4], FrpcSHA256: f[5]})
	}
	return out, sc.Err()
}

// sha256Form is a sha256 as sha256sum prints it: 64 lowercase hex characters, the form agent.json's
// frpcDigest takes (agentapi.AgentConfig).
var sha256Form = regexp.MustCompile(`^[0-9a-f]{64}$`)

// cpuArchOf maps the release's architecture names to the kernel's machine names, which platform
// profiles use for cpuArch (v1alpha1.AcceleratorClass) and agent.json's frpc digest is keyed by.
var cpuArchOf = map[string]string{"amd64": "x86_64", "arm64": "aarch64"}

// FrpcDigests returns the sha256 of the pinned frpc of a version for each architecture, keyed by
// cpuArch (x86_64, aarch64): the digests agent.json names (HBEP-4, Submission: Files first). A
// pinned architecture with no cpuArch name is an error, not a digest left out.
func FrpcDigests(version string) (map[string]string, error) {
	pins, err := parseFrpPins(frpPinsFile)
	if err != nil {
		return nil, err
	}
	return frpcDigests(pins, version)
}

func frpcDigests(pins []FrpPin, version string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range pins {
		if p.Version != version {
			continue
		}
		arch, ok := cpuArchOf[p.Arch]
		if !ok {
			return nil, fmt.Errorf("frp-pins.txt pins frp %s for %s, which has no cpuArch name", p.Version, p.Arch)
		}
		out[arch] = p.FrpcSHA256
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("frpc %s is not pinned for any architecture", version)
	}
	return out, nil
}

// frpPinFor finds the pin for a version on an architecture. A version that is not pinned is an
// error that lists what is.
func frpPinFor(pins []FrpPin, version, arch string) (FrpPin, error) {
	var known []string
	for _, p := range pins {
		if p.Arch != arch {
			continue
		}
		known = append(known, p.Version)
		if p.Version == version {
			return p, nil
		}
	}
	return FrpPin{}, fmt.Errorf("frps %s is not pinned for %s; pinned versions: %s", version, arch, strings.Join(known, ", "))
}

// FrpsBinary is a verified frps executable. file is the open descriptor whose bytes were hashed;
// StartFrps executes that descriptor, not the path, so the bytes it runs are the bytes it checked.
type FrpsBinary struct {
	Version string
	Path    string // absolute
	SHA256  string
	file    *os.File
}

// Close releases the descriptor of the verified binary.
func (b FrpsBinary) Close() {
	if b.file != nil {
		_ = b.file.Close()
	}
}

// FrpDirName is the directory of one frp release in a binary directory: frp-0.71.1. It sits next
// to k3s-1.36.5 and holds frps (and frpc, which the job side uses).
func FrpDirName(version string) string { return "frp-" + version }

// ResolveFrps finds the pinned frps for the version under binDir (binDir/frp-X.Y.Z/frps), opens
// it, and checks the sha256 of what it opened against the pin. A missing file, or one with another
// hash, is an error: nothing unpinned ever starts. The caller closes the returned binary.
func ResolveFrps(binDir, version string) (FrpsBinary, error) {
	return ResolveFrpsFile(filepath.Join(binDir, FrpDirName(version), "frps"), version)
}

// ResolveFrpsFile is ResolveFrps for a caller that names the file itself.
func ResolveFrpsFile(path, version string) (FrpsBinary, error) {
	pins, err := parseFrpPins(frpPinsFile)
	if err != nil {
		return FrpsBinary{}, err
	}
	return resolveFrps(pins, runtime.GOARCH, version, path)
}

func resolveFrps(pins []FrpPin, arch, version, path string) (FrpsBinary, error) {
	pin, err := frpPinFor(pins, version, arch)
	if err != nil {
		return FrpsBinary{}, err
	}
	// Absolute before anything else: the launcher later works from the state directory.
	abs, err := filepath.Abs(path)
	if err != nil {
		return FrpsBinary{}, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return FrpsBinary{}, fmt.Errorf("frps %s: %w (its release asset is %s)", pin.Version, err, pin.Tarball)
	}
	got, err := fdSHA256(f)
	if err != nil {
		f.Close()
		return FrpsBinary{}, fmt.Errorf("frps %s: %w", pin.Version, err)
	}
	if got != pin.FrpsSHA256 {
		f.Close()
		return FrpsBinary{}, fmt.Errorf("frps %s at %s has sha256 %s, but the pin says %s; refusing to run it", pin.Version, abs, got, pin.FrpsSHA256)
	}
	return FrpsBinary{Version: pin.Version, Path: abs, SHA256: got, file: f}, nil
}
