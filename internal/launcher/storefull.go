package launcher

import (
	"fmt"
	"syscall"
)

// StoreFullHint names the likely cause of a failing write probe (#192): the file system that holds
// dir, the store's directory, has no free space. kine and SQLite do not fail a write for lack of
// space but wait, so the probe times out and its error says nothing about the disk. It returns ""
// when space is free, and says so when the free space cannot be read; it never fails the check
// that asked. dir is shown as given: the layout path under the state directory, never the
// directory a link in it leads to.
//
// No free space means no block available: statfs(2)'s Bavail is zero. That counts the blocks an
// unprivileged process can take (Bfree also counts those reserved for root). A file system with
// a few blocks left is not named: a probe that fails there may have another cause, and the hint
// would blame the disk wrongly. The frozen volume of the store-full test (#190) reports zero
// blocks available.
func StoreFullHint(dir string) string {
	var st syscall.Statfs_t
	switch err := syscall.Statfs(dir, &st); {
	case err != nil:
		return fmt.Sprintf("the free space on %s is unknown: %v", dir, err)
	case st.Bavail == 0:
		return fmt.Sprintf("store full (no free space on %s)", dir)
	}
	return ""
}
