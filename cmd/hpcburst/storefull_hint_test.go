package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// TestStoreFullHintOnACapVolume (#192) shows the hint's judgement on a real full volume, #190's
// size-limited file system, with no k3s behind it: an empty volume gets no hint, the volume filled to its
// limit and frozen gets "store full" under the path it was asked about, and once space is given back the
// hint is gone. The path is shown as given, so a link to the volume is named by the link and never by the
// mount it leads to, as status shows the store's directory in the state directory. TestStoreFull checks
// the same hint in status's reasons on k3s.
func TestStoreFullHintOnACapVolume(t *testing.T) {
	const limit = 2 << 20
	v := newCapVolume(t, limit)
	link := filepath.Join(shortTmp(t, false), "db")
	if err := os.Symlink(v.mount, link); err != nil {
		t.Fatal(err)
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
	for i := 0; i < 2*limit/len(block); i++ {
		if _, err := f.Write(block); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("filling the volume: %v", err)
			}
			break
		}
	}
	if v.enospc.Load() == 0 {
		t.Fatal("the volume never reached its limit")
	}
	v.freeze()
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
