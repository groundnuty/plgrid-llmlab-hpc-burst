package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// TestStoreFullHintOnACapVolume (#192) shows the hint's three bands on a real volume, #190's
// size-limited file system, with no k3s behind it. status asks for the hint only after its write probe
// failed or timed out, so each state below is a failed probe on a volume in that state. The volume is
// filled step by step, and at each step statfs's own count of the available blocks is checked before the
// hint is: plenty free, then 17 blocks (68 KiB), then exactly 16 blocks (64 KiB, the boundary): no hint,
// the probe failed for another reason; 15 blocks (60 KiB) and a single block (4 KiB): "store nearly
// full" with the free space; no block available, after the volume refused a write and was frozen: "store
// full". Once space is given back the hint is gone. The path is shown as given, so a link to the volume
// is named by the link and never by the mount it leads to, as status shows the store's directory in the
// state directory. TestStoreFull checks the full band in status's reasons on k3s.
func TestStoreFullHintOnACapVolume(t *testing.T) {
	const (
		limit = 2 << 20
		block = 4096
	)
	v := newCapVolume(t, limit)
	link := filepath.Join(shortTmp(t, false), "db")
	if err := os.Symlink(v.mount, link); err != nil {
		t.Fatal(err)
	}
	// available is statfs's Bavail of the link: the blocks an unprivileged process can still take.
	available := func() uint64 {
		t.Helper()
		var st syscall.Statfs_t
		if err := syscall.Statfs(link, &st); err != nil {
			t.Fatal(err)
		}
		if st.Bsize != block {
			t.Fatalf("the volume reports a block size of %d, the test counts blocks of %d", st.Bsize, block)
		}
		return st.Bavail
	}
	f, err := os.Create(filepath.Join(v.mount, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data := make([]byte, block)
	// fillTo writes one block at a time until exactly want blocks are available. A volume that has fewer,
	// or that refuses a write on the way, fails the test: the bands are checked at the states named.
	fillTo := func(want uint64) {
		t.Helper()
		for i := 0; i < 2*limit/block; i++ {
			switch got := available(); {
			case got == want:
				return
			case got < want:
				t.Fatalf("the volume has %d blocks available, fewer than the %d the step needs", got, want)
			}
			if _, err := f.Write(data); err != nil {
				t.Fatalf("filling the volume down to %d blocks available (%d now): %v", want, available(), err)
			}
		}
		t.Fatalf("the volume never came down to %d blocks available (%d now)", want, available())
	}
	check := func(wantBlocks uint64, want string) {
		t.Helper()
		fillTo(wantBlocks)
		if got := launcher.StoreFullHint(link); got != want {
			t.Errorf("with %d blocks (%d KiB) available: hint %q, want %q", wantBlocks, wantBlocks*block>>10, got, want)
		}
	}

	check(limit/block, "") // an empty volume
	check(17, "")
	check(16, "") // exactly 64 KiB: the boundary is not nearly full
	check(15, fmt.Sprintf("store nearly full (60 KiB free on %s)", link))
	check(1, fmt.Sprintf("store nearly full (4 KiB free on %s)", link))

	// No block left: the volume refuses the next write, and is frozen, as a full store is.
	fillTo(0)
	if _, err := f.Write(data); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("a write to a volume with no block available: %v, want ENOSPC", err)
	}
	v.freeze()
	full := fmt.Sprintf("store full (no free space on %s)", link)
	if got := launcher.StoreFullHint(link); got != full {
		t.Errorf("a full volume: hint %q, want %q", got, full)
	}

	v.unfreeze()
	v.grow(8 << 20)
	if got := launcher.StoreFullHint(link); got != "" {
		t.Errorf("after space was given back: hint %q, want none", got)
	}
}
