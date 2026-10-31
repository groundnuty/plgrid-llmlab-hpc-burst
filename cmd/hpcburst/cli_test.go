package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests need no k3s: every case is decided before anything is started.

func TestUsageAndExitCodes(t *testing.T) {
	empty := shortTmp(t, false)
	cases := []struct {
		name string
		args []string
		code int
		msg  string
	}{
		{"no command", nil, exitUsage, "usage"},
		{"unknown command", []string{"frobnicate"}, exitUsage, "unknown command"},
		{"upgrade without --to", []string{"upgrade", "--state-dir", empty}, exitUsage, "--to is required"},
		{"unknown flag", []string{"up", "--nonsense"}, exitUsage, "nonsense"},
		{"stray argument", []string{"status", "--state-dir", empty, "extra"}, exitUsage, "unexpected argument"},
		{"status of an empty directory is 'stopped'", []string{"status", "--state-dir", empty}, exitStopped, "stopped"},
		{"down with nothing recorded succeeds", []string{"down", "--state-dir", empty}, exitOK, "nothing to stop"},
		{"help", []string{"help"}, exitOK, "usage"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := cli(t, c.args...)
			if r.code != c.code || !strings.Contains(r.out+r.err, c.msg) {
				t.Fatalf("exit %d, want %d; output %q / %q", r.code, c.code, r.out, r.err)
			}
		})
	}
}

// TestUpRejectsBadInputBeforeStarting: bad flags are refused at the boundary, with a message that
// names what was wrong, before any binary is looked up or any process started.
func TestUpRejectsBadInputBeforeStarting(t *testing.T) {
	state := filepath.Join(shortTmp(t, false), "st")
	for name, c := range map[string]struct {
		args []string
		msg  string
	}{
		"front address without a mode":                 {[]string{"--front-addr", "front.example.test:8443"}, "front mode"},
		"ssh without a key":                            {[]string{"--front-mode", "ssh", "--front-addr", "front.example.test"}, "--front-key"},
		"ssh on port 2222":                             {[]string{"--front-mode", "ssh", "--front-addr", "front.example.test:2222", "--front-key", "/dev/null"}, "port 22"},
		"port base too low":                            {[]string{"--port-base", "80"}, "out of range"},
		"port base leaves no room for the controllers": {[]string{"--port-base", "65530"}, "out of range"},
		"controller binary missing":                    {[]string{"--controller-bin", "/nonexistent/hpcburst-controller"}, "controller binary"},
		"controller binary is a directory":             {[]string{"--controller-bin", "/tmp"}, "not a regular file"},
		"state directory too long":                     {[]string{"--state-dir", "/tmp/" + strings.Repeat("d", 90)}, "shorter"},
	} {
		t.Run(name, func(t *testing.T) {
			args := append([]string{"up", "--state-dir", state, "--bin-dir", t.TempDir()}, c.args...)
			if strings.Contains(strings.Join(c.args, " "), "--state-dir") {
				args = append([]string{"up", "--bin-dir", t.TempDir()}, c.args...)
			}
			r := cli(t, args...)
			if r.code != exitFail || !strings.Contains(r.err, c.msg) {
				t.Fatalf("exit %d, stderr %q; want exit 1 mentioning %q", r.code, r.err, c.msg)
			}
			if _, err := os.Stat(filepath.Join(state, "state.json")); err == nil {
				t.Error("a record was written")
			}
		})
	}
}

func TestUpNeedsAPinnedBinary(t *testing.T) {
	state := filepath.Join(shortTmp(t, false), "st")
	r := cli(t, "up", "--state-dir", state, "--bin-dir", t.TempDir())
	if r.code != exitFail || !strings.Contains(r.err, "fetch-k3s.sh") {
		t.Fatalf("exit %d, stderr %q", r.code, r.err)
	}
	r = cli(t, "up", "--state-dir", state, "--bin-dir", t.TempDir(), "--version", "1.99.0")
	if r.code != exitFail || !strings.Contains(r.err, "not pinned") {
		t.Fatalf("an unpinned version: exit %d, stderr %q", r.code, r.err)
	}
	// A binary at the right place but with the wrong content is refused, not run.
	bin := t.TempDir()
	if err := os.MkdirAll(filepath.Join(bin, "k3s-1.36.5"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "k3s-1.36.5", "k3s"), []byte("#!/bin/sh\ntouch "+filepath.Join(bin, "RAN")+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r = cli(t, "up", "--state-dir", state, "--bin-dir", bin)
	if r.code != exitFail || !strings.Contains(r.err, "refusing to run") {
		t.Fatalf("a tampered binary: exit %d, stderr %q", r.code, r.err)
	}
	if _, err := os.Stat(filepath.Join(bin, "RAN")); err == nil {
		t.Fatal("the unpinned binary was executed")
	}
}

// ---- helpers shared by the integration tests ----

type stateFile struct {
	Version   string `json:"version"`
	Processes []struct {
		PID        int    `json:"pid"`
		StartTicks uint64 `json:"start_ticks"`
	} `json:"processes"`
	Front struct {
		Mode    string `json:"mode"`
		Addr    string `json:"addr"`
		KeyPath string `json:"key_path"`
	} `json:"front"`
}

func readState(t testing.TB, dir string) stateFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s stateFile
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Processes) != 1 {
		t.Fatalf("state records %d processes", len(s.Processes))
	}
	return s
}

func parseJSON(t testing.TB, s string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(s), v); err != nil {
		t.Fatalf("bad JSON: %v\n%s", err, s)
	}
}
