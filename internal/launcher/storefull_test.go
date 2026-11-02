package launcher

import (
	"context"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
)

// statusWithFailingWrites is Status on a fake host whose write probe is answered with a timeout, as a
// full store answers it (#190's first real run), with the store's directory in place or not.
func statusWithFailingWrites(t *testing.T, storeDir bool) (*fakeHost, *StatusReport) {
	t.Helper()
	h := newFakeHost(t)
	h.writeStatus = http.StatusGatewayTimeout
	if storeDir {
		if err := os.MkdirAll(h.l.sqliteDir(), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := Status(context.Background(), StatusOptions{StateDir: h.l.dir, NoGCProbe: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Health != Unhealthy || rep.Writes == nil || *rep.Writes {
		t.Fatalf("a failing write probe did not make the host unhealthy with writes_ok=false: %s, writes %v, %v", rep.Health, rep.Writes, rep.Reasons)
	}
	return h, rep
}

// TestStatusFailingWriteWithSpaceFreeHasNoStoreFullHint (#192): the write probe fails, but the store's
// file system has room, so the reason is the one status always gave and does not blame the disk. The
// free space is the real statfs of a real directory, and the test first shows that directory has room
// far above the nearly-full band: on a nearly full disk it would otherwise prove nothing.
func TestStatusFailingWriteWithSpaceFreeHasNoStoreFullHint(t *testing.T) {
	const room = 16 * nearlyFullBelow
	h, rep := statusWithFailingWrites(t, true)
	var st syscall.Statfs_t
	if err := syscall.Statfs(h.l.sqliteDir(), &st); err != nil || st.Bavail*uint64(st.Bsize) < room {
		t.Fatalf("the store directory %s has %d blocks of %d bytes available (%v), under %d bytes: too few to show that a failing probe on a disk with room gets no hint", h.l.sqliteDir(), st.Bavail, st.Bsize, err, room)
	}
	joined := strings.Join(rep.Reasons, "\n")
	if !strings.Contains(joined, "writes are failing: ") {
		t.Errorf("reasons %q do not say that writes are failing", joined)
	}
	for _, hint := range []string{"store full", "nearly full", "no free space", "KiB free", "free space"} {
		if strings.Contains(joined, hint) {
			t.Errorf("reasons %q carry %q although the file system has room", joined, hint)
		}
	}
}

// TestStatusSaysWhenTheFreeSpaceCannotBeRead: a store directory statfs cannot read is not "space is
// free" and not "the store is full": the reason says the free space is unknown, with the layout path,
// and keeps the write failure it explains.
func TestStatusSaysWhenTheFreeSpaceCannotBeRead(t *testing.T) {
	h, rep := statusWithFailingWrites(t, false)
	joined := strings.Join(rep.Reasons, "\n")
	if want := "the free space on " + h.l.sqliteDir() + " is unknown: "; !strings.Contains(joined, want) {
		t.Errorf("reasons %q do not contain %q", joined, want)
	}
	if !strings.Contains(joined, "writes are failing: ") {
		t.Errorf("reasons %q lost the write failure", joined)
	}
	for _, claim := range []string{"store full", "nearly full"} {
		if strings.Contains(joined, claim) {
			t.Errorf("reasons %q say %q, which the unreadable statfs does not show", joined, claim)
		}
	}
}
