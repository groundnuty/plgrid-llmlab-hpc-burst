package launcher

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rendering tests below compare the text with HBEP-6's example, written out by hand for the
// block at 29244 (plugin 29252, bind 29253, tcpmux 29254, web 29255). They are the cheap extra:
// whether frps accepts the file, and does what it says, is shown by the tests that run the real
// frps (frps_real_test.go).

const testWebPassword = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func frpsTestHost() agentapi.Host {
	return agentapi.Host{StateDir: "/var/lib/hb-state", PortBase: 29244}
}

func TestRenderFrpsConfigHasEverythingTheHBEPFixes(t *testing.T) {
	got, err := RenderFrpsConfig(frpsTestHost(), testWebPassword)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(got, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			lines = append(lines, l)
		}
	}
	want := []string{
		`bindAddr = "127.0.0.1"`,
		`bindPort = 29253`,
		`proxyBindAddr = "127.0.0.1"`,
		`tcpmuxHTTPConnectPort = 29254`,
		`kcpBindPort = 0`,
		`quicBindPort = 0`,
		`vhostHTTPPort = 0`,
		`vhostHTTPSPort = 0`,
		`sshTunnelGateway.bindPort = 0`,
		`auth.method = "token"`,
		`webServer.addr = "127.0.0.1"`,
		`webServer.port = 29255`,
		`webServer.user = "hpcburst"`,
		`webServer.password = "` + testWebPassword + `"`,
		`log.to = "/var/lib/hb-state/logs/frps.log"`,
		`transport.tcpMuxKeepaliveInterval = 10`,
		`transport.heartbeatTimeout = 25`,
		`transport.tcpKeepalive = 30`,
		`[[httpPlugins]]`,
		`name = "hpcburst"`,
		`addr = "127.0.0.1:29252"`,
		`path = "/frp/v1/handler"`,
		`ops = ["Login", "NewProxy", "CloseProxy"]`,
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the rendering differs from HBEP-6's block.\ngot:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
	// No token (the plugin is the gate), and no Ping (a failed Ping call makes frpc drop its session).
	for _, absent := range []string{"auth.token", "auth.oidc", "Ping", "NewWorkConn", "NewUserConn"} {
		if strings.Contains(got, absent) {
			t.Errorf("the configuration mentions %q", absent)
		}
	}
}

func TestRenderFrpsConfigRefusesWhatItCannotWriteSafely(t *testing.T) {
	good := frpsTestHost()
	cases := []struct {
		name string
		host agentapi.Host
		pw   string
	}{
		{"password too short", good, "abc"},
		{"password with a newline", good, testWebPassword[:63] + "\n"},
		{"password in capitals", good, strings.ToUpper(testWebPassword)},
		{"empty password", good, ""},
		{"relative state directory", agentapi.Host{StateDir: "state", PortBase: 29244}, testWebPassword},
		{"double quote in the state directory", agentapi.Host{StateDir: `/x"y`, PortBase: 29244}, testWebPassword},
		{"backslash in the state directory", agentapi.Host{StateDir: `/x\y`, PortBase: 29244}, testWebPassword},
		{"newline in the state directory", agentapi.Host{StateDir: "/x\ny = 1", PortBase: 29244}, testWebPassword},
		{"block runs past 65535", agentapi.Host{StateDir: "/x", PortBase: 65530}, testWebPassword},
		{"block below 1", agentapi.Host{StateDir: "/x", PortBase: -9}, testWebPassword},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, err := RenderFrpsConfig(c.host, c.pw); err == nil {
				t.Fatalf("rendered without an error:\n%s", got)
			} else if strings.Contains(err.Error(), c.pw) && c.pw != "" {
				t.Errorf("the error repeats the password: %v", err)
			}
		})
	}
}

func TestWriteFrpsConfigIsMode600AndReplacesAtomically(t *testing.T) {
	h := agentapi.Host{StateDir: shortDir(t), PortBase: 29244}
	path, err := WriteFrpsConfig(h, testWebPassword)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(h.StateDir, "frps", "frps.toml") {
		t.Errorf("path = %s", path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %o, want 600", fi.Mode().Perm())
	}
	for _, d := range []string{filepath.Dir(path), filepath.Dir(FrpsLogFile(h))} {
		if di, err := os.Stat(d); err != nil || di.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v %v, want a directory of mode 700", d, di, err)
		}
	}
	// A second write with another password replaces the file whole, and leaves no temporary file.
	other := strings.Repeat("ab", 32)
	if _, err := WriteFrpsConfig(h, other); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), testWebPassword) || !strings.Contains(string(b), other) {
		t.Errorf("the file was not replaced:\n%s", b)
	}
	ents, _ := os.ReadDir(filepath.Dir(path))
	if len(ents) != 1 {
		t.Errorf("the frps directory holds %d entries, want only frps.toml", len(ents))
	}
}

func TestEnsureFrpsWebPassword(t *testing.T) {
	newHost := func(t *testing.T) agentapi.Host {
		return agentapi.Host{StateDir: filepath.Join(shortDir(t), "st"), PortBase: 29244}
	}
	t.Run("creates a key in a mode 600 file in a mode 700 directory", func(t *testing.T) {
		h := newHost(t)
		key, created, err := EnsureFrpsWebPassword(h)
		if err != nil || !created {
			t.Fatalf("key=%q created=%v err=%v", key, created, err)
		}
		if err := agentapi.CheckKey([]byte(key)); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Lstat(h.FrpsWebPasswordFile())
		if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o600 {
			t.Fatalf("file: %v %v", fi, err)
		}
		di, err := os.Stat(filepath.Dir(h.FrpsWebPasswordFile()))
		if err != nil || di.Mode().Perm() != 0o700 {
			t.Fatalf("directory: %v %v", di, err)
		}
		onDisk, _ := os.ReadFile(h.FrpsWebPasswordFile())
		if string(onDisk) != key {
			t.Errorf("the file holds %d bytes, not the returned key", len(onDisk))
		}
	})
	t.Run("keeps an existing one", func(t *testing.T) {
		h := newHost(t)
		first, created, err := EnsureFrpsWebPassword(h)
		if err != nil || !created {
			t.Fatal(err)
		}
		second, created, err := EnsureFrpsWebPassword(h)
		if err != nil || created || second != first {
			t.Fatalf("second call: key equal=%v created=%v err=%v", second == first, created, err)
		}
	})
	t.Run("refuses any mode but 0600 and leaves the file as it is", func(t *testing.T) {
		h := newHost(t)
		if _, _, err := EnsureFrpsWebPassword(h); err != nil {
			t.Fatal(err)
		}
		path := h.FrpsWebPasswordFile()
		before, _ := os.ReadFile(path)
		// 0400, 0500 and 0700 are owner-only, which agentapi.ReadKeyFile accepts; the launcher
		// wrote 0600 and requires exactly that.
		for _, mode := range []os.FileMode{0o644, 0o640, 0o660, 0o604, 0o607, 0o400, 0o500, 0o700} {
			if err := os.Chmod(path, mode); err != nil {
				t.Fatal(err)
			}
			key, created, err := EnsureFrpsWebPassword(h)
			if err == nil || key != "" || created {
				t.Fatalf("mode %o: key=%q created=%v err=%v, want a refusal", mode, key, created, err)
			}
			if !strings.Contains(err.Error(), "mode") {
				t.Errorf("mode %o: the error does not say why: %v", mode, err)
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(before) {
				t.Fatalf("mode %o: the file was overwritten", mode)
			}
			if fi, _ := os.Stat(path); fi.Mode().Perm() != mode {
				t.Fatalf("mode %o: the launcher changed the mode to %o", mode, fi.Mode().Perm())
			}
		}
	})
	t.Run("refuses a symbolic link and a file of another form, and writes nothing over them", func(t *testing.T) {
		h := newHost(t)
		if _, _, err := EnsureFrpsWebPassword(h); err != nil {
			t.Fatal(err)
		}
		path := h.FrpsWebPasswordFile()
		target := filepath.Join(filepath.Dir(path), "elsewhere")
		if err := os.Rename(path, target); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureFrpsWebPassword(h); err == nil {
			t.Fatal("followed a symbolic link")
		}
		if fi, err := os.Lstat(path); err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("the link was replaced: %v %v", fi, err)
		}
		os.Remove(path)
		if err := os.WriteFile(path, []byte("hunter2\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureFrpsWebPassword(h); err == nil {
			t.Fatal("accepted a password of another form")
		}
		if b, _ := os.ReadFile(path); string(b) != "hunter2\n" {
			t.Fatalf("the file was overwritten with %q", b)
		}
	})
}

func TestFrpPinsFile(t *testing.T) {
	pins, err := parseFrpPins(frpPinsFile)
	if err != nil {
		t.Fatal(err)
	}
	// The amd64 tarball's sum is the one spike S1 pinned (spikes/s1c/fetch.sh), which is also in
	// the release's frp_sha256_checksums.txt: written here from there, not from the pin file.
	const s1AMD64Tarball = "1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803"
	amd64, err := frpPinFor(pins, DefaultFrpVersion, "amd64")
	if err != nil {
		t.Fatal(err)
	}
	if amd64.TarballSHA256 != s1AMD64Tarball || amd64.Tarball != "frp_0.71.1_linux_amd64.tar.gz" {
		t.Errorf("amd64 pin = %+v", amd64)
	}
	if _, err := frpPinFor(pins, DefaultFrpVersion, "arm64"); err != nil {
		t.Error(err)
	}
	if DefaultFrpVersion != "0.71.1" {
		t.Errorf("HBEP-6 pins frp v0.71.1, the default is %s", DefaultFrpVersion)
	}
	if _, err := frpPinFor(pins, "0.70.0", "amd64"); err == nil || !strings.Contains(err.Error(), "0.71.1") {
		t.Errorf("an unpinned version: %v (the error should list what is pinned)", err)
	}
	if _, err := frpPinFor(pins, DefaultFrpVersion, "riscv64"); err == nil {
		t.Error("an architecture with no pin was accepted")
	}
	sum := strings.Repeat("a", 64)
	for _, bad := range []string{
		"0.71.1 amd64 x.tgz abc def ghi",
		"0.71.1 amd64 x.tgz",
		strings.Repeat("g", 64),
		"0.71.1 amd64 x.tgz " + sum + " " + sum, // the five fields of a file without frpc
		"0.71.1 amd64 x.tgz " + sum + " " + sum + " " + strings.Repeat("A", 64),       // upper case: not agent.json's form
		"0.71.1 amd64 x.tgz " + sum + " " + sum + " " + sum + " " + sum,               // seven fields
		"0.71.1 amd64 x.tgz " + sum + " " + sum + " " + strings.Repeat("a", 63) + "g", // not hex
	} {
		if _, err := parseFrpPins(bad); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
}

// TestFrpcDigests: the frpc digests are keyed by cpuArch, the kernel's machine name that platform
// profiles use (x86_64, aarch64), each from its own line's frpc column. The fixture's sums are
// distinct, so a column or an architecture taken from the wrong place shows.
func TestFrpcDigests(t *testing.T) {
	a, b, c, d := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)
	pins, err := parseFrpPins("0.71.1 amd64 x.tgz " + a + " " + b + " " + c + "\n0.71.1 arm64 y.tgz " + b + " " + c + " " + d + "\n0.70.0 arm64 z.tgz " + a + " " + a + " " + a + "\n")
	if err != nil {
		t.Fatal(err)
	}
	got, err := frpcDigests(pins, "0.71.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["x86_64"] != c || got["aarch64"] != d {
		t.Errorf("frpcDigests = %v, want x86_64 %s and aarch64 %s", got, c, d)
	}
	if _, err := frpcDigests(pins, "9.9.9"); err == nil {
		t.Error("a version with no pin gave digests")
	}
	riscv := append(pins, FrpPin{Version: "0.71.1", Arch: "riscv64", FrpcSHA256: a})
	if got, err := frpcDigests(riscv, "0.71.1"); err == nil || !strings.Contains(err.Error(), "riscv64") {
		t.Errorf("an architecture with no cpuArch name: %v, %v; want an error naming it, not a digest left out", got, err)
	}

	// The embedded file: both architectures of the default release, distinct from each other and
	// from the frps sums of their lines (a column mix-up).
	all, err := parseFrpPins(frpPinsFile)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := FrpcDigests(DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	if len(pinned) != 2 || pinned["x86_64"] == "" || pinned["aarch64"] == "" || pinned["x86_64"] == pinned["aarch64"] {
		t.Fatalf("FrpcDigests(%s) = %v, want two distinct digests, for x86_64 and aarch64", DefaultFrpVersion, pinned)
	}
	for _, p := range all {
		for _, v := range pinned {
			if v == p.FrpsSHA256 || v == p.TarballSHA256 {
				t.Errorf("frpc digest %s is %s's frps or tarball sum", v, p.Arch)
			}
		}
	}
}

// TestResolveFrpsChecksTheBytesItOpened gives resolveFrps a pin made from the sum of a file that
// the test hashes with crypto/sha256 itself, then a file with other bytes.
func TestResolveFrpsChecksTheBytesItOpened(t *testing.T) {
	dir := shortDir(t)
	good := filepath.Join(dir, "frps")
	content := []byte("not really frps\n")
	if err := os.WriteFile(good, content, 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	pins := []FrpPin{{Version: "9.9.9", Arch: "amd64", Tarball: "x.tar.gz", TarballSHA256: strings.Repeat("0", 64), FrpsSHA256: hex.EncodeToString(sum[:])}}

	b, err := resolveFrps(pins, "amd64", "9.9.9", good)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.SHA256 != pins[0].FrpsSHA256 || b.Path != good || b.file == nil {
		t.Errorf("binary = %+v", b)
	}

	bad := filepath.Join(dir, "other")
	if err := os.WriteFile(bad, []byte("another file\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if b, err := resolveFrps(pins, "amd64", "9.9.9", bad); err == nil || !strings.Contains(err.Error(), "refusing to run it") {
		b.Close()
		t.Errorf("a file with another hash: %v", err)
	}
	if b, err := resolveFrps(pins, "amd64", "9.9.9", filepath.Join(dir, "missing")); err == nil {
		b.Close()
		t.Error("a missing file was accepted")
	}
	if b, err := resolveFrps(pins, "amd64", "9.9.9", dir); err == nil {
		b.Close()
		t.Error("a directory was accepted")
	}
	if b, err := resolveFrps(pins, "arm64", "9.9.9", good); err == nil {
		b.Close()
		t.Error("an unpinned architecture was accepted")
	}
	// The layout: binDir/frp-X.Y.Z/frps, next to k3s-X.Y.Z.
	if got := FrpDirName("0.71.1"); got != "frp-0.71.1" {
		t.Errorf("FrpDirName = %s", got)
	}
}

// TestFrpPinsAgreeWithTheFetchScript: hack/fetch-frp.sh pins the same release as frp-pins.txt (it
// downloads what the launcher will then verify, and the frpc that agent.json names), so the two
// must not drift, in any of the three sums. The script is read as
// text; its values are not computed from the pin file.
func TestFrpPinsAgreeWithTheFetchScript(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "hack", "fetch-frp.sh"))
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{}
	for _, l := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(l, "="); ok && strings.HasPrefix(k, "FRP") && !strings.ContainsAny(k, " $") {
			vars[k] = v
		}
	}
	pins, err := parseFrpPins(frpPinsFile)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := frpPinFor(pins, DefaultFrpVersion, "amd64")
	if err != nil {
		t.Fatal(err)
	}
	if vars["FRP_VERSION"] != pin.Version || vars["FRP_TARBALL_SHA"] != pin.TarballSHA256 || vars["FRPS_SHA"] != pin.FrpsSHA256 ||
		vars["FRPC_SHA"] != pin.FrpcSHA256 {
		t.Errorf("hack/fetch-frp.sh pins %v, frp-pins.txt pins %+v", vars, pin)
	}
}
