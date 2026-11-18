package launcher

// The audit log is readable by the host's user alone after every start, not only after the first (HBEP-10, Audit; issue #660):
// a log directory or an audit file that an older release, a umask or a restore left loose is set to owner only before k3s
// opens the file, and nothing else in the log directory is touched.

import (
	"os"
	"path/filepath"
	"testing"
)

// modeOf is the permission bits of a path, or the error.
func modeOf(path string) (os.FileMode, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Mode().Perm(), nil
}

func TestEveryStartSetsTheAuditLogAndItsDirectoryToOwnerOnly(t *testing.T) {
	s := testSpec(t, 0)
	if err := os.MkdirAll(s.l.logDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(s.l.logDir(), 0o755); err != nil { // MkdirAll's mode is cut by the umask
		t.Fatal(err)
	}
	audit := []string{"audit.log", "audit-2026-11-17T10-00-00.000.log", "audit-2026-11-17T09-00-00.000.log.gz"}
	other := []string{"k3s.log", "controller.log"}
	for _, f := range append(append([]string{}, audit...), other...) {
		if err := os.WriteFile(filepath.Join(s.l.logDir(), f), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A restart is another start: the second one finds what the first left.
	for start := 1; start <= 2; start++ {
		if err := prepareIsolation(s.l); err != nil {
			t.Fatal(err)
		}
		if m, err := modeOf(s.l.logDir()); err != nil || m != 0o700 {
			t.Errorf("start %d: the log directory is %v, %v, want mode 700", start, m, err)
		}
		for _, f := range audit {
			if m, err := modeOf(filepath.Join(s.l.logDir(), f)); err != nil || m != 0o600 {
				t.Errorf("start %d: %s is %v, %v, want mode 600", start, f, m, err)
			}
		}
		for _, f := range other {
			if m, err := modeOf(filepath.Join(s.l.logDir(), f)); err != nil || m != 0o644 {
				t.Errorf("start %d: %s is %v, %v, want it left at mode 644: only the audit files are the rule's", start, f, m, err)
			}
		}
	}
}
