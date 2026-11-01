package launcher

import (
	"bufio"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

// The frps pin, the way k3s is pinned (pins.go): one embedded file names the allowed release and
// the sha256 of the executable. The launcher copies the file it opened into a sealed memory file,
// hashes that copy, and runs that very descriptor, so no write to the file on disk, by path or in
// place, can change the bytes between the check and the exec. HBEP-6 names
// build/dependencies.yaml (HBEP-15) as the source of frps's checksum; the file does not exist yet,
// so frp-pins.txt stands in for it, as k3s-pins.txt does for k3s.

//go:embed frp-pins.txt
var frpPinsFile string

// DefaultFrpVersion is the frp release whose frps the launcher starts (HBEP-6: frp v0.71.1).
const DefaultFrpVersion = "0.71.1"

// FrpPin is one allowed frps release binary.
type FrpPin struct {
	Version       string
	Arch          string
	Tarball       string // the release asset's name
	TarballSHA256 string // the asset's sum, from the release's checksums
	FrpsSHA256    string // the sum of the frps executable inside it: what is checked before a start
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
		if len(f) != 5 {
			return nil, fmt.Errorf("frp-pins.txt line %d: want 5 fields, got %d", n, len(f))
		}
		for _, sum := range f[3:] {
			if _, err := hex.DecodeString(sum); err != nil || len(sum) != 64 {
				return nil, fmt.Errorf("frp-pins.txt line %d: %q is not a sha256", n, sum)
			}
		}
		out = append(out, FrpPin{Version: f[0], Arch: f[1], Tarball: f[2], TarballSHA256: f[3], FrpsSHA256: f[4]})
	}
	return out, sc.Err()
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

// FrpsBinary is a verified frps executable. file is a sealed memory file (sealedCopy) holding a copy
// of the file at Path, and its bytes are the ones that were hashed; StartFrps and the supervisor
// execute that descriptor, not the path, so the bytes that run are the bytes that were checked.
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
	src, err := os.Open(abs)
	if err != nil {
		return FrpsBinary{}, fmt.Errorf("frps %s: %w (its release asset is %s)", pin.Version, err, pin.Tarball)
	}
	// The file on disk can be written in place by anyone who may write it, and an ordinary
	// descriptor would run whatever it holds at the exec. The copy is sealed before it is hashed,
	// so the hash covers bytes that can no longer change.
	f, err := sealedCopy(src, "frps")
	src.Close()
	if err != nil {
		return FrpsBinary{}, fmt.Errorf("frps %s at %s: %w", pin.Version, abs, err)
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

// frpsSeals forbid every change to the copy's bytes and size, and any change to the seals.
const frpsSeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// sealedCopy copies the regular file src into a new memory file (memfd_create) and seals it with
// frpsSeals, so from then on nobody, this process included, can write to it, grow it or shrink it.
// The memory file is close-on-exec; exec.Cmd.ExtraFiles passes it to the one child that runs it.
// MFD_EXEC states that it will be executed (Linux 6.3 and later warn without it); a kernel that
// does not know the flag refuses it with EINVAL, and the copy is made without it.
func sealedCopy(src *os.File, name string) (*os.File, error) {
	fi, err := src.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", src.Name())
	}
	flags := unix.MFD_CLOEXEC | unix.MFD_ALLOW_SEALING
	fd, err := unix.MemfdCreate(name, flags|unix.MFD_EXEC)
	if errors.Is(err, unix.EINVAL) {
		fd, err = unix.MemfdCreate(name, flags)
	}
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	m := os.NewFile(uintptr(fd), "memfd:"+name)
	if _, err := io.Copy(m, src); err != nil {
		m.Close()
		return nil, fmt.Errorf("copy into a memory file: %w", err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, frpsSeals); err != nil {
		m.Close()
		return nil, fmt.Errorf("seal the memory file: %w", err)
	}
	return m, nil
}
