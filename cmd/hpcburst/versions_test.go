package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The store's release is recorded in store-version.json (kept by down). These two sequences are the
// ones that used to move a store to another k3s release without a backup: a plain start of a newer
// or an older release. Both must now be refused, with the way to do it properly named, and the
// refusal must start nothing.

// TestVersionDowngradeByUpRefused: a store written by the default release (1.36.5) is not started
// by an older release (1.35.9); only restore brings an older release back.
func TestVersionDowngradeByUpRefused(t *testing.T) {
	versionSequence(t, nil, "v1.36.5+k3s1", "1.35.9", "hpcburst restore")
}

// TestVersionUpgradeByUpRefused: a store written by 1.35.9 is not started by the newer 1.36.5 and
// not by the default either; plain up keeps starting 1.35.9, and only upgrade moves it forward.
func TestVersionUpgradeByUpRefused(t *testing.T) {
	versionSequence(t, []string{"--version", "1.35.9"}, "v1.35.9+k3s1", "1.36.5", "hpcburst upgrade")
}

// versionSequence: up (with firstArgs) -> down -> up --version other, which must exit 1 naming
// wantMsg and start nothing -> plain up, which must start the first release again -> down.
func versionSequence(t *testing.T, firstArgs []string, firstVer, other, wantMsg string) {
	const base = 29220
	h := newHost(t)
	storeFile := filepath.Join(h.dir, "store-version.json")

	if r := cli(t, h.upArgs(base, firstArgs...)...); r.code != exitOK {
		t.Fatalf("first up exited %d", r.code)
	}
	assertRunning(t, h, firstVer)
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	assertStoreVersion(t, storeFile, firstVer)
	recorded := fileOf(storeFile)

	// The refused start.
	r := cli(t, h.upArgs(base, "--version", other)...)
	if r.code != exitFail || !strings.Contains(r.err, wantMsg) {
		t.Errorf("up --version %s: exit %d, stderr %q; want exit 1 naming %q", other, r.code, r.err, wantMsg)
	}
	assertNothingStarted(t, h)
	if got := fileOf(storeFile); got != recorded {
		t.Errorf("the refused start changed store-version.json:\nwas  %s\nnow  %s", recorded, got)
	}

	// A plain up starts the release that wrote the store, not the default.
	if r := cli(t, h.upArgs(base)...); r.code != exitOK {
		t.Fatalf("plain up exited %d", r.code)
	}
	assertRunning(t, h, firstVer)
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("final down exited %d", r.code)
	}
	assertStoreVersion(t, storeFile, firstVer)
	assertNothingStarted(t, h)
}

// assertRunning checks the version in state.json and the one the server itself reports.
func assertRunning(t *testing.T, h *host, version string) {
	t.Helper()
	st := readState(t, h.dir)
	if st.Version != version {
		t.Errorf("state.json says k3s %s, want %s", st.Version, version)
	}
	if !running(st.Processes[0].PID) {
		t.Errorf("k3s (pid %d) is not running", st.Processes[0].PID)
	}
	v, err := h.clients().cs.Discovery().ServerVersion()
	if err != nil || v.GitVersion != version {
		t.Errorf("the server reports %v (%v), want %s", v, err, version)
	}
}

// assertStoreVersion reads store-version.json with the test's own parser: it must exist and name
// the release the sequence expects.
func assertStoreVersion(t *testing.T, path, version string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("store-version.json: %v", err)
		return
	}
	var sv struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(b, &sv); err != nil || sv.Version != version {
		t.Errorf("store-version.json says %q (%v), want %q:\n%s", sv.Version, err, version, b)
	}
}

// assertNothingStarted checks, with the test's own tools, that no record, no process using the
// state directory and no listener in the test port block remains.
func assertNothingStarted(t *testing.T, h *host) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(h.dir, "state.json")); err == nil {
		t.Error("state.json exists: something was started")
	}
	if pids := k3sProcessesUsing(h.dir); len(pids) != 0 {
		t.Errorf("processes use %s: %v", h.dir, pids)
	}
	if rows := listeningInRange(t, 29220, 29239); len(rows) != 0 {
		t.Errorf("listeners in the block: %v", rows)
	}
}
