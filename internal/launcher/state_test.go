package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStateRoundTripAndMode(t *testing.T) {
	l, err := newLayout(filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if st, err := l.loadState(); st != nil || err != nil {
		t.Fatalf("no record: got %v, %v", st, err)
	}
	in := &State{Version: "v1.36.5+k3s1", Binary: "/x/k3s", BinarySHA256: "ab", Nice: 10,
		Ports: Ports{1, 2, 3, 4}, Procs: []ProcRec{{Name: "k3s", PID: 4242, StartTicks: 99, BootID: "b"}},
		StartedAt: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), Front: Front{Mode: "direct", Addr: "h:1"}}
	if err := l.saveState(in); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(l.stateFile())
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("record mode: %v, %v", fi, err)
	}
	out, err := l.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != in.Version || out.Procs[0] != in.Procs[0] || out.Ports != in.Ports || out.Front != in.Front || !out.StartedAt.Equal(in.StartedAt) || out.Nice != 10 {
		t.Fatalf("round trip changed the record: %+v", out)
	}
	// No temporary files are left behind by the atomic write.
	ents, _ := os.ReadDir(l.dir)
	for _, e := range ents {
		if strings.Contains(e.Name(), ".tmp") {
			t.Errorf("leftover %s", e.Name())
		}
	}
}

func TestLoadStateRejectsDamage(t *testing.T) {
	l, _ := newLayout(t.TempDir())
	for name, content := range map[string]string{
		"not json":       "{{{",
		"wrong schema":   `{"schema": 99, "processes": [{"pid": 5}]}`,
		"no processes":   `{"schema": 1, "processes": []}`,
		"truncated file": `{"schema": 1, "proces`,
	} {
		if err := os.WriteFile(l.stateFile(), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if st, err := l.loadState(); err == nil {
			t.Errorf("%s: accepted, got %+v", name, st)
		}
	}
}

func TestLockIsExclusive(t *testing.T) {
	l, _ := newLayout(t.TempDir())
	unlock, err := l.lockState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.lockState(); err == nil || !strings.Contains(err.Error(), "another hpcburst command") {
		t.Fatalf("second lock: %v", err)
	}
	unlock()
	unlock2, err := l.lockState()
	if err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
	unlock2()
}

func TestNewLayoutRejectsLongPaths(t *testing.T) {
	if _, err := newLayout(""); err == nil {
		t.Error("empty state directory accepted")
	}
	long := "/tmp/" + strings.Repeat("d", maxStateDirLen)
	if _, err := newLayout(long); err == nil || !strings.Contains(err.Error(), "shorter") {
		t.Errorf("long path: %v", err)
	}
	l, err := newLayout("/tmp/short")
	if err != nil || l.dir != "/tmp/short" {
		t.Errorf("short path: %v, %v", l, err)
	}
	// The longest socket k3s creates inside the state directory must fit a unix socket address.
	if n := len(filepath.Join(strings.Repeat("d", maxStateDirLen), "data", "server", "kine.sock")); n >= 108 {
		t.Errorf("maxStateDirLen %d leaves a %d-byte socket path", maxStateDirLen, n)
	}
}

func TestDefaultDirs(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("XDG_CACHE_HOME", "/xdg/cache")
	if d, _ := DefaultStateDir(); d != "/xdg/state/hpcburst" {
		t.Errorf("state dir %q", d)
	}
	if d, _ := DefaultBinDir(); d != "/xdg/cache/hpcburst/k3s" {
		t.Errorf("bin dir %q", d)
	}
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("HOME", "/home/someone")
	if d, _ := DefaultStateDir(); d != "/home/someone/.local/state/hpcburst" {
		t.Errorf("state dir %q", d)
	}
	if d, _ := DefaultBinDir(); d != "/home/someone/.cache/hpcburst/k3s" {
		t.Errorf("bin dir %q", d)
	}
}
