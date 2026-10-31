package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The command line is the launcher's contract (internal/launcher supervises
// this binary with exactly these flags).
func TestParse(t *testing.T) {
	base := []string{"--kubeconfig", "kc", "--credentials-dir", "creds", "--health-addr", "127.0.0.1:29226"}
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"minimal", base, ""},
		{"with a front", append(append([]string{}, base...), "--front-mode", "ssh", "--front-addr", "front.example:22"), ""},
		{"front mode alone", append(append([]string{}, base...), "--front-mode", "direct"), "go together"},
		{"unknown front mode", append(append([]string{}, base...), "--front-mode", "x", "--front-addr", "a:1"), "want direct or ssh"},
		{"no kubeconfig", base[2:], "--kubeconfig is required"},
		{"health on all interfaces", []string{"--kubeconfig", "kc", "--credentials-dir", "c", "--health-addr", ":29226"}, "loopback only"},
		{"zero workers", append(append([]string{}, base...), "--workers", "0"), "at least 1"},
		{"stray argument", append(append([]string{}, base...), "extra"), "unexpected arguments"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := parse(c.args, io.Discard)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				if !f.leaderElect || f.workers != 4 {
					t.Fatalf("defaults: leader-elect %v, workers %d; want true, 4", f.leaderElect, f.workers)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("parse error %v, want one containing %q", err, c.wantErr)
			}
		})
	}
}

// A credentials directory others can reach is refused (HBEP-11, Credentials);
// a private one gets its allocations and instances subdirectories.
func TestCheckCredentialsDir(t *testing.T) {
	dir := t.TempDir()
	open := filepath.Join(dir, "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := checkCredentialsDir(open); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("mode 0755: err %v, want a refusal naming 0700", err)
	}
	private := filepath.Join(dir, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := checkCredentialsDir(private); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"allocations", "instances"} {
		fi, err := os.Stat(filepath.Join(private, sub))
		if err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("%s: %v, mode %v; want a 0700 directory", sub, err, fi.Mode().Perm())
		}
	}
}
