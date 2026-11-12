// Package envoypin holds the pinned Envoy release binaries: envoy-pins.txt, embedded here, is the
// one record of their sha256 (HBEP-15, Pinned versions). Envoy 1.40.2 is HBEP-5's candidate router
// for an Instance (open question 2; Inside an Instance), run as released, with no fork (spike S13,
// PR #324). Anything that runs Envoy checks the binary against For first; hack/fetch-envoy.sh reads
// the same file to download and check the same bytes, so there is no second copy to drift.
//
// The package imports only the standard library, so the allocation agent, a static binary that
// links nothing else (internal/agent, TestAgentImportsOnlyTheStandardLibrary), can embed it.
//
// Provenance: Envoy (https://github.com/envoyproxy/envoy, Apache-2.0), the release assets
// envoy-1.40.2-linux-x86_64 and envoy-1.40.2-linux-aarch_64; their sums are the release's signed
// checksums.txt.asc, recorded in spikes/s13/README.md (Provenance). New here: the file's format and
// this reader, after internal/launcher/k3s-pins.txt's.
package envoypin

import (
	"crypto/sha256"
	_ "embed" // the pins file
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
)

//go:embed envoy-pins.txt
var pinsFile string

// Version is the Envoy release the framework runs.
const Version = "1.40.2"

// Pin is one line of the pins file: one release binary.
type Pin struct {
	Version string // the release, such as 1.40.2
	Arch    string // Go's name for the architecture (GOARCH): amd64 or arm64
	Asset   string // the release asset's file name, such as envoy-1.40.2-linux-x86_64
	SHA256  string // the asset's sha256, 64 lowercase hex characters
}

// releaseArch is Envoy's name for each architecture the file may pin, as its release assets spell
// it (envoy-<version>-linux-<name>).
var releaseArch = map[string]string{"amd64": "x86_64", "arm64": "aarch_64"}

var (
	versionForm = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	sumForm     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Pins returns every line of the embedded pins file.
func Pins() ([]Pin, error) { return parse(pinsFile) }

// For returns the pin of Version for a Go architecture (runtime.GOARCH).
func For(goarch string) (Pin, error) {
	pins, err := Pins()
	if err != nil {
		return Pin{}, err
	}
	for _, p := range pins {
		if p.Version == Version && p.Arch == goarch {
			return p, nil
		}
	}
	return Pin{}, fmt.Errorf("envoy-pins.txt pins no Envoy %s binary for %q (it pins amd64 and arm64)", Version, goarch)
}

// BinaryPath is where hack/fetch-envoy.sh DIR installs Envoy Version: DIR/envoy-<Version>/envoy.
func BinaryPath(dir string) string { return filepath.Join(dir, "envoy-"+Version, "envoy") }

// ErrMismatch marks bytes whose sha256 is not the pin's.
var ErrMismatch = errors.New("the sha256 is not the pinned one")

// Check reads r to its end and returns nil only when its sha256 is the pin of Version for goarch.
func Check(r io.Reader, goarch string) error {
	p, err := For(goarch)
	if err != nil {
		return err
	}
	return check(r, p)
}

// check reads r to its end and returns nil only when its sha256 is p's.
func check(r io.Reader, p Pin) error {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return fmt.Errorf("hashing Envoy: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != p.SHA256 {
		return fmt.Errorf("%w: Envoy %s for %s is %s, got %s", ErrMismatch, p.Version, p.Arch, p.SHA256, got)
	}
	return nil
}

// parse reads a pins file's text: one binary per line as "version arch asset sha256", with blank
// lines and lines starting with # ignored. Every line must be well formed, the asset must be the
// release's own name for its version and architecture, and no version and architecture may appear
// twice; the first fault is returned, naming its line.
func parse(text string) ([]Pin, error) {
	var out []Pin
	seen := map[string]int{}
	for i, line := range strings.Split(text, "\n") {
		n := i + 1
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 4 {
			return nil, fmt.Errorf("envoy-pins.txt line %d: %d fields, want 4 (version arch asset sha256)", n, len(f))
		}
		p := Pin{Version: f[0], Arch: f[1], Asset: f[2], SHA256: f[3]}
		ra, ok := releaseArch[p.Arch]
		switch {
		case !versionForm.MatchString(p.Version):
			return nil, fmt.Errorf("envoy-pins.txt line %d: version %q is not of the form 1.40.2", n, p.Version)
		case !ok:
			return nil, fmt.Errorf("envoy-pins.txt line %d: architecture %q is not one Go names amd64 or arm64", n, p.Arch)
		case p.Asset != "envoy-"+p.Version+"-linux-"+ra:
			return nil, fmt.Errorf("envoy-pins.txt line %d: asset %q is not Envoy %s's %s release binary, envoy-%s-linux-%s", n, p.Asset, p.Version, p.Arch, p.Version, ra)
		case !sumForm.MatchString(p.SHA256):
			return nil, fmt.Errorf("envoy-pins.txt line %d: sha256 %q is not 64 lowercase hex characters", n, p.SHA256)
		}
		key := p.Version + " " + p.Arch
		if first, dup := seen[key]; dup {
			return nil, fmt.Errorf("envoy-pins.txt line %d: Envoy %s for %s is pinned already, on line %d", n, p.Version, p.Arch, first)
		}
		seen[key] = n
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, errors.New("envoy-pins.txt pins no binary")
	}
	return out, nil
}
