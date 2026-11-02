package launcher

import (
	"fmt"
	"syscall"
)

// noSpaceBelow is the free space under which a store's file system counts as having none. SQLite
// commits through a WAL of 4 KiB frames and a transaction touches several pages, so a file system
// with less than this left cannot take the next write although statfs still counts a few blocks
// free. The frozen volume of the store-full test (#190) stops at zero blocks free.
const noSpaceBelow = 64 << 10

// freeSpace is what a process without privilege can still take on the file system that holds dir:
// statfs(2)'s Bavail (Bfree also counts the blocks reserved for root) times the block size.
func freeSpace(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

// StoreFullHint names the likely cause of a failing write probe (#192): the file system that holds
// dir, the store's directory, has no free space. kine and SQLite do not fail a write for lack of
// space but wait, so the probe times out and its error says nothing about the disk. It returns ""
// when space is free, and says so when the free space cannot be read; it never fails the check
// that asked. dir is shown as given: the layout path under the state directory, never the
// directory a link in it leads to.
func StoreFullHint(dir string) string {
	free, err := freeSpace(dir)
	switch {
	case err != nil:
		return fmt.Sprintf("the free space on %s is unknown: %v", dir, err)
	case free < noSpaceBelow:
		return fmt.Sprintf("store full (no free space on %s)", dir)
	}
	return ""
}
