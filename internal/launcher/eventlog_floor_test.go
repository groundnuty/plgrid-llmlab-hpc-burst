package launcher

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// The rows of HBEP-8's Test plan, "The caps fit the disk", with the arithmetic worked out by hand
// (GiB is 2^30 = 1,073,741,824 bytes, GB is 10^9): both caps 2 GiB make the caps' sum 4 GiB =
// 4,294,967,296. The log starts when free - (caps - store) - (caps - copy) >= floor, each bracket
// clamped at 0.
const (
	gib       = int64(1) << 30
	gb        = int64(1_000_000_000)
	capsFour  = 4 * gib // two caps of 2 GiB
	floor15gb = 15 * gb
)

func TestFloorNeedForTheBriefsRows(t *testing.T) {
	for name, c := range map[string]struct {
		store, prevCopy, want int64
	}{
		// 15e9 + 4,294,967,296 + 4,294,967,296 = 23,589,934,592
		"an empty store and no copy": {0, 0, 23_589_934_592},
		// 15e9 + (4 GiB - 1 GiB = 3,221,225,472) + 4,294,967,296 = 22,516,192,768
		"1 GiB in the store": {gib, 0, 22_516_192_768},
		"a 1 GiB copy":       {0, gib, 22_516_192_768},
		// a store over the caps' sum has nothing left to grow: the bracket is clamped at 0
		"a store past the caps": {5 * gib, 0, 15_000_000_000 + 4_294_967_296},
		"a copy past the caps":  {0, 5 * gib, 15_000_000_000 + 4_294_967_296},
		"both past":             {5 * gib, 5 * gib, 15_000_000_000},
	} {
		if got := floorNeed(capsFour, c.store, c.prevCopy, floor15gb); got != c.want {
			t.Errorf("%s: need %d, want %d", name, got, c.want)
		}
	}
}

func TestTheGateStartsTheLogOnlyAtTheBound(t *testing.T) {
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	l := layout{dir: "/run/st"}
	st := &State{EventLog: &cfg}
	for name, c := range map[string]struct {
		free, store, prevCopy int64
		start                 bool
	}{
		"23.6 GB free starts it":                {23_600_000_000, 0, 0, true},
		"23.5 GB free does not":                 {23_500_000_000, 0, 0, false},
		"exactly the bound starts it":           {23_589_934_592, 0, 0, true},
		"one byte under does not":               {23_589_934_591, 0, 0, false},
		"1 GiB in the store, 22.6 GB starts":    {22_600_000_000, gib, 0, true},
		"1 GiB in the store, 22.5 GB does not":  {22_500_000_000, gib, 0, false},
		"a 1 GiB copy, 22.6 GB starts":          {22_600_000_000, 0, gib, true},
		"a 1 GiB copy, 22.5 GB does not":        {22_500_000_000, 0, gib, false},
		"a full store and copy, the floor only": {15_000_000_000, 5 * gib, 5 * gib, true},
	} {
		d := newFakeDisk()
		d.set(map[string]int64{l.eventLogDir(): c.free}, map[string]int64{l.eventLogDir(): c.store, l.eventLogCopyDir(): c.prevCopy})
		ok, reason := st.eventLogGate(l, d)()
		if ok != c.start {
			t.Errorf("%s: the gate says %v (%q), want %v", name, ok, reason, c.start)
		}
		if ok && reason != "" || !ok && reason == "" {
			t.Errorf("%s: gate %v with reason %q", name, ok, reason)
		}
	}
}

// A test that reads another path's free space proves nothing: the seam is asked about <state>/eventlog
// for the free space and the store's size, and <state>/backups/eventlog for the copy's, and about
// nothing else. The double has a wrong answer at the state directory and at the backups directory, so
// a gate that asked there would start the log on the wrong figure or fail.
func TestTheGateAsksAboutTheStoresOwnPaths(t *testing.T) {
	l := layout{dir: "/run/st"}
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	d := newFakeDisk()
	d.set(map[string]int64{
		"/run/st/eventlog": 23_600_000_000,
		"/run/st":          1, // a decoy: another file system's free space
		"/run/st/backups":  2,
	}, map[string]int64{"/run/st/eventlog": 0, "/run/st/backups/eventlog": 0, "/run/st/backups": 1 << 40, "/run/st": 1 << 40})
	if ok, reason := (&State{EventLog: &cfg}).eventLogGate(l, d)(); !ok {
		t.Fatalf("the gate is closed: %s", reason)
	}
	want := []string{"Free /run/st/eventlog", "Size /run/st/eventlog", "Size /run/st/backups/eventlog"}
	if got := d.askedPaths(); !slices.Equal(got, want) {
		t.Fatalf("the seam was asked %v, want exactly %v", got, want)
	}
	// The decoys are the proof the other way: a double that knows only the wrong paths closes the gate.
	wrong := newFakeDisk()
	wrong.set(map[string]int64{"/run/st": 23_600_000_000}, map[string]int64{"/run/st": 0})
	if ok, reason := (&State{EventLog: &cfg}).eventLogGate(l, wrong)(); ok || !strings.Contains(reason, "could not be read") {
		t.Fatalf("a double with the free space at another path: gate %v, reason %q; want it closed", ok, reason)
	}
}

func TestTheGateReasonNamesTheFreeSpaceTheCapsAndTheFloor(t *testing.T) {
	l := layout{dir: "/run/st"}
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	d := newFakeDisk()
	d.set(map[string]int64{l.eventLogDir(): 23_500_000_000}, map[string]int64{l.eventLogDir(): 0, l.eventLogCopyDir(): 0})
	_, reason := (&State{EventLog: &cfg}).eventLogGate(l, d)()
	// "<free> free on the store's file system, caps <events>+<usage>, floor <floor>", the sizes in the
	// units the flags take; 23.5 GB is 23500 MB, and the need is the 23,589,934,592 worked out above.
	for _, want := range []string{"23500 MB free on the store's file system", "caps 2 GiB+2 GiB", "floor 15 GB", "needs 23589934592 B free"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the reason %q does not contain %q", reason, want)
		}
	}
}

// upgrade's own check (HBEP-8, Backup and restore): free + prevCopy - store under the floor refuses,
// naming the four sizes. 18 GB free, a 1 GiB old copy and a 4 GiB store: 18e9 + 1,073,741,824 -
// 4,294,967,296 = 14,778,774,528, under 15e9. At 19 GB it is 15,778,774,528.
func TestUpgradeRefusesAtTheFloorAndNamesTheFourSizes(t *testing.T) {
	l := layout{dir: "/run/st"}
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	d := newFakeDisk()
	d.set(map[string]int64{l.eventLogDir(): 18 * gb}, map[string]int64{l.eventLogDir(): 4 * gib, l.eventLogCopyDir(): gib})
	err := checkUpgradeFloor(d, l, cfg)
	if err == nil {
		t.Fatal("an upgrade at 18 GB free, with a 1 GiB copy and a 4 GiB store, was not refused")
	}
	for _, want := range []string{"18 GB free", "previous copy's 1 GiB", "store's 4 GiB", "floor of 15 GB", "nothing was stopped", "14432397 KiB"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not name %q", err, want)
		}
	}
	d.set(map[string]int64{l.eventLogDir(): 19 * gb}, nil)
	if err := checkUpgradeFloor(d, l, cfg); err != nil {
		t.Errorf("an upgrade at 19 GB free was refused: %v", err)
	}
	// At the bound exactly: 15e9 - 1,073,741,824 + 4,294,967,296 free is 15e9 left.
	d.set(map[string]int64{l.eventLogDir(): 15*gb - gib + 4*gib}, nil)
	if err := checkUpgradeFloor(d, l, cfg); err != nil {
		t.Errorf("an upgrade that leaves exactly the floor was refused: %v", err)
	}
	d.set(map[string]int64{l.eventLogDir(): 15*gb - gib + 4*gib - 1}, nil)
	if err := checkUpgradeFloor(d, l, cfg); err == nil {
		t.Errorf("an upgrade that leaves one byte under the floor was not refused")
	}
	// The same paths and no others.
	for _, a := range d.askedPaths() {
		if !strings.HasSuffix(a, "/run/st/eventlog") && !strings.HasSuffix(a, "/run/st/backups/eventlog") {
			t.Errorf("the seam was asked %q", a)
		}
	}
}

func TestTheOSDiskReadsTheRealFileSystem(t *testing.T) {
	dir := t.TempDir()
	if free, err := (osDisk{}).Free(dir); err != nil || free <= 0 {
		t.Fatalf("free space of %s: %d, %v", dir, free, err)
	}
	if size, err := (osDisk{}).Size(filepath.Join(dir, "absent")); err != nil || size != 0 {
		t.Fatalf("size of a path that does not exist: %d, %v, want 0", size, err)
	}
	// A tree of two files of 1 MiB and 3 MiB in a subdirectory takes at least their bytes on disk.
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, n := range map[string]int{"a/one": 1 << 20, "a/b/three": 3 << 20} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	size, err := (osDisk{}).Size(dir)
	if err != nil || size < 4<<20 || size > 4<<20+1<<20 {
		t.Fatalf("size of the tree: %d, %v, want about 4 MiB", size, err)
	}
	if _, err := (osDisk{}).Free(filepath.Join(dir, "absent")); err == nil {
		t.Error("free space of a path that does not exist was read")
	}
}
