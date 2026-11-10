package sshrun

// New for issue #343 (PR 1): RelayOptions.ConfigFile, the relay keeper's
// stable configuration file name, and a pin of alpha's derived one.

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

var exampleRelayCreds = slurm.SSHConfig{HostName: "login.example", User: "example-user", IdentityFile: "/nonexistent/key"}

// relayControlDir is a control directory of this user's, mode 700.
func relayControlDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// confFiles lists the .conf files in dir.
func confFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.conf"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// Pin: alpha's relays' configuration file (no ConfigFile) is named from the
// credentials with a random part, <16 hex>-<8 hex>.conf, directly in the
// control directory, mode 600, so two makers of the same credentials write
// two files. The positive control is TestAStableConfigFileIsTheGivenName.
func TestTheRelaysDerivedConfigFileNameHasARandomPart(t *testing.T) {
	dir := relayControlDir(t)
	shape := regexp.MustCompile(`^[0-9a-f]{16}-[0-9a-f]{8}\.conf$`)
	var names []string
	for range 2 {
		r, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if filepath.Dir(r.Config()) != dir || !shape.MatchString(filepath.Base(r.Config())) {
			t.Errorf("alpha's relay configuration file %s: want <16 hex>-<8 hex>.conf in %s", r.Config(), dir)
		}
		fi, err := os.Stat(r.Config())
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("alpha's relay configuration file: %v, mode %v; want it written, mode 600", err, fi.Mode().Perm())
		}
		names = append(names, r.Config())
	}
	if names[0] == names[1] {
		t.Errorf("two makers of the same credentials share the file %s, want a random part in each name", names[0])
	}
}

// The relay keeper's stable configuration file is the given name, written mode
// 600 with the credentials, and named by the relay's ssh -F. A second maker of
// the same name, as a keeper that restarted, writes that one file again, and
// no other. Close removes it, and a Command after Close fails.
func TestAStableConfigFileIsTheGivenName(t *testing.T) {
	dir := relayControlDir(t)
	file := filepath.Join(dir, "example-1a2b3c4d.conf")
	a, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir, ConfigFile: file})
	if err != nil {
		t.Fatal(err)
	}
	if a.Config() != file {
		t.Errorf("the configuration file is %s, want the given %s", a.Config(), file)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(file); err != nil || fi.Mode().Perm() != 0o600 || !strings.Contains(string(b), "  HostName login.example\n") {
		t.Errorf("the stable file: %v, mode %v, content %q; want mode 600 with the credentials", err, fi.Mode().Perm(), b)
	}
	again, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir, ConfigFile: file})
	if err != nil {
		t.Fatal(err)
	}
	if got := confFiles(t, dir); !slices.Equal(got, []string{file}) {
		t.Errorf("after a second maker of the stable name the directory holds %q, want only %s", got, file)
	}
	cmd, err := again.Command("true")
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.Index(cmd.Args, "-F"); i < 0 || i+1 >= len(cmd.Args) || cmd.Args[i+1] != file {
		t.Errorf("the relay's ssh %q does not name the stable file with -F", cmd.Args)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Errorf("the stable file after Close: %v, want it removed", err)
	}
	if _, err := again.Command("true"); err == nil {
		t.Error("a Command after Close succeeded")
	}
	if err := a.Close(); err != nil {
		t.Errorf("Close of a maker whose file is already gone: %v", err)
	}
}

// A stable name is refused unless it is a clean name ending .conf directly in
// the control directory, so the file is as private as the directory; nothing
// is written for a refused one, and the error names no path.
func TestAStableConfigFileOutsideItsDirectoryIsRefused(t *testing.T) {
	dir := nestedControlDir(t, "c")
	for _, f := range []string{
		"example.conf",
		filepath.Join(dir, "sub", "example.conf"),
		filepath.Join(filepath.Dir(dir), "example.conf"),
		filepath.Join(dir, ".example.conf"),
		filepath.Join(dir, "example.cfg"),
		dir + "/./example.conf",
		filepath.Join(dir, "ex ample.conf"),
	} {
		_, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir, ConfigFile: f})
		if err == nil {
			t.Errorf("ConfigFile %q was accepted", f)
			continue
		}
		if strings.Contains(err.Error(), dir) {
			t.Errorf("the refusal of %q names a path: %v", f, err)
		}
	}
	if got := confFiles(t, dir); len(got) != 0 {
		t.Errorf("refused names left files %q", got)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "example.conf")); !os.IsNotExist(err) {
		t.Errorf("a refused name outside the directory was written: %v", err)
	}
}

// nestedControlDir is a control directory, mode 700, named name inside the
// test's own temporary directory, which nothing else writes to.
func nestedControlDir(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A stable name is a regular file of its own, so it needs no control socket
// and no bound on that socket's path: it is written in a control directory
// too long for alpha's derived names, which the positive control shows
// refused in the same directory (controlPaths' 90-byte bound).
func TestAStableConfigFileNeedsNoShortControlDirectory(t *testing.T) {
	dir := nestedControlDir(t, strings.Repeat("d", 60))
	if len(dir) <= maxSocketPath {
		t.Fatalf("the control directory is %d bytes, want more than %d", len(dir), maxSocketPath)
	}
	if _, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir}); err == nil {
		t.Error("a derived name in a control directory past the socket path's bound was accepted: the control is void")
	}
	file := filepath.Join(dir, "example-1a2b3c4d.conf")
	r, err := NewRelay(RelayOptions{Name: "example", Config: exampleRelayCreds, ControlDir: dir, ConfigFile: file})
	if err != nil {
		t.Fatalf("a stable name in a long control directory: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if r.Config() != file {
		t.Errorf("the configuration file is %s, want %s", r.Config(), file)
	}
	mustBeWritten(t, file)
}

// mustBeWritten checks that a configuration file exists, mode 600.
func mustBeWritten(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the configuration file: %v, want it written, mode 600", err)
	}
}
