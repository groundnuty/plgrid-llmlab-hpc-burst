package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// TestStoreFullHintOnACapVolume (#192) shows the hint's judgement on a real volume, #190's size-limited
// file system, with no k3s behind it. status asks for the hint only after its write probe failed, so each
// case below is a probe that failed: an empty volume gets no hint; a volume with a few blocks left, under
// 64 KiB, gets none either, because the probe may have failed for another reason and the disk is not
// full (Astra's review of PR #229); a volume with no block available, filled to its limit and frozen,
// gets "store full" under the path it was asked about; and once space is given back the hint is gone.
// The path is shown as given, so a link to the volume is named by the link and never by the mount it
// leads to, as status shows the store's directory in the state directory. TestStoreFull checks the same
// hint in status's reasons on k3s.
func TestStoreFullHintOnACapVolume(t *testing.T) {
	const (
		limit     = 2 << 20
		remainder = 32 << 10 // 8 blocks: some space left, well under the 64 KiB an earlier threshold used
	)
	v := newCapVolume(t, limit)
	link := filepath.Join(shortTmp(t, false), "db")
	if err := os.Symlink(v.mount, link); err != nil {
		t.Fatal(err)
	}
	// available is statfs's Bavail of the link, the blocks an unprivileged process can still take, and
	// the bytes they come to.
	available := func() (blocks uint64, bytes int64) {
		t.Helper()
		var st syscall.Statfs_t
		if err := syscall.Statfs(link, &st); err != nil {
			t.Fatal(err)
		}
		return st.Bavail, int64(st.Bavail) * int64(st.Bsize)
	}
	if got := launcher.StoreFullHint(link); got != "" {
		t.Fatalf("an empty volume got the hint %q", got)
	}

	f, err := os.Create(filepath.Join(v.mount, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	block := make([]byte, 4096)
	write := func(what string) bool { // false once the volume refuses with ENOSPC
		t.Helper()
		if _, err := f.Write(block); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("%s: %v", what, err)
			}
			return false
		}
		return true
	}

	// Fill until 8 blocks are left: positive, and under 64 KiB.
	for i := 0; i < 2*limit/len(block); i++ {
		if _, bytes := available(); bytes <= remainder {
			break
		}
		if !write("filling the volume down to its remainder") {
			t.Fatal("the volume refused a write before it had the remainder left")
		}
	}
	blocks, bytes := available()
	if blocks == 0 || bytes > remainder {
		t.Fatalf("the volume has %d blocks (%d bytes) available, want a positive remainder of at most %d bytes", blocks, bytes, remainder)
	}
	if got := launcher.StoreFullHint(link); got != "" {
		t.Errorf("a volume with %d blocks (%d bytes) left got the hint %q, but the disk is not full", blocks, bytes, got)
	}

	// Fill the rest: no block is available, and the volume is frozen, as a full store is.
	for i := 0; i < 2*limit/len(block); i++ {
		if !write("filling the volume") {
			break
		}
	}
	if v.enospc.Load() == 0 {
		t.Fatal("the volume never reached its limit")
	}
	v.freeze()
	if blocks, _ := available(); blocks != 0 {
		t.Fatalf("the full volume has %d blocks available, want none", blocks)
	}
	want := "store full (no free space on " + link + ")"
	if got := launcher.StoreFullHint(link); got != want {
		t.Errorf("a full volume: hint %q, want %q", got, want)
	}

	v.unfreeze()
	v.grow(8 << 20)
	if got := launcher.StoreFullHint(link); got != "" {
		t.Errorf("after space was given back: hint %q, want none", got)
	}
}
