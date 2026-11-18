package testenv

// The opt-in audit log of Options (issue #660, PR B): what its arguments are, and that a bad option is refused
// before anything starts. That the API server then writes the log, rotated and gzipped, is judged by the
// control plane's own audit log file, in internal/audit/e2e.

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestAuditOptionsArgs(t *testing.T) {
	dir := t.TempDir()
	policy := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policy, []byte("apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n  - level: Metadata\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "audit", "audit.log")
	got, err := (&AuditOptions{PolicyFile: policy, LogPath: logPath, MaxSizeMB: 1}).args()
	if err != nil {
		t.Fatal(err)
	}
	// Written out, as the launcher's test does: a changed flag in config/audit must fail here too.
	want := []string{
		"--audit-policy-file=" + policy, "--audit-log-path=" + logPath, "--audit-log-maxsize=1",
		"--audit-log-maxbackup=0", "--audit-log-maxage=0", "--audit-log-compress=true",
	}
	if !slices.Equal(got, want) {
		t.Errorf("audit args:\n got %v\nwant %v", got, want)
	}
	if fi, err := os.Stat(filepath.Dir(logPath)); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("the log's directory: %v, %v, want it made, mode 700", fi, err)
	}
	// MaxSizeMB 0 is the launcher's 100.
	if got, err := (&AuditOptions{PolicyFile: policy, LogPath: logPath}).args(); err != nil || !slices.Contains(got, "--audit-log-maxsize=100") {
		t.Errorf("the default size: %v, %v, want --audit-log-maxsize=100", got, err)
	}
}

func TestAuditOptionsRefuseWhatWouldStartWithoutALog(t *testing.T) {
	dir := t.TempDir()
	policy := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policy, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		why string
		o   AuditOptions
	}{
		{"no policy file", AuditOptions{LogPath: filepath.Join(dir, "a.log")}},
		{"no log path", AuditOptions{PolicyFile: policy}},
		{"a relative log path", AuditOptions{PolicyFile: policy, LogPath: "a.log"}},
		{"a relative policy path", AuditOptions{PolicyFile: "policy.yaml", LogPath: filepath.Join(dir, "a.log")}},
		{"a policy file that is not there", AuditOptions{PolicyFile: filepath.Join(dir, "missing.yaml"), LogPath: filepath.Join(dir, "a.log")}},
		{"a policy that is a directory", AuditOptions{PolicyFile: dir, LogPath: filepath.Join(dir, "a.log")}},
		{"a negative size", AuditOptions{PolicyFile: policy, LogPath: filepath.Join(dir, "a.log"), MaxSizeMB: -1}},
	} {
		if got, err := tc.o.args(); err == nil {
			t.Errorf("%s: accepted, as %v", tc.why, got)
		}
	}
}
