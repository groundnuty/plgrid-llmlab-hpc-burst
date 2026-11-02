package launcher

import (
	"fmt"
	"syscall"
)

// nearlyFullBelow is the available space under which a store counts as nearly full. SQLite needs
// whole pages for a commit, and its journal or WAL more, so a store stops accepting writes well
// before the literal last block (`@hpc-burst-science-agent`, 2 Nov 2026, on PR #229).
const nearlyFullBelow = 64 << 10

// StoreFullHint names the likely cause of a failing write probe (#192): the file system that holds
// dir, the store's directory, has no space, or almost none. kine and SQLite do not fail a write for
// lack of space but wait, so the probe times out and its error says nothing about the disk. It is
// asked only after the probe failed or timed out. The answer has three bands, by statfs(2)'s
// Bavail, the blocks an unprivileged process can take (Bfree also counts those reserved for root):
//
//	no block available            "store full (no free space on <dir>)"
//	under nearlyFullBelow         "store nearly full (<N> KiB free on <dir>)"
//	nearlyFullBelow or more       "" (the probe failed for another reason)
//
// When the free space cannot be read it says so, and it never fails the check that asked. dir is
// shown as given: the layout path under the state directory, never the directory a link in it
// leads to. The frozen volume of the store-full test (#190) reports zero blocks available.
func StoreFullHint(dir string) string {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return fmt.Sprintf("the free space on %s is unknown: %v", dir, err)
	}
	free := st.Bavail * uint64(st.Bsize)
	switch {
	case st.Bavail == 0:
		return fmt.Sprintf("store full (no free space on %s)", dir)
	case free < nearlyFullBelow:
		return fmt.Sprintf("store nearly full (%s free on %s)", kib(free), dir)
	}
	return ""
}

// kib is bytes in KiB, rounded down; a value under one KiB is said so, never as 0 KiB.
func kib(bytes uint64) string {
	if bytes < 1<<10 {
		return "under 1 KiB"
	}
	return fmt.Sprintf("%d KiB", bytes>>10)
}
