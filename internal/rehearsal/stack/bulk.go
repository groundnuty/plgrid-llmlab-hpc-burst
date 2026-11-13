package stack

// The bulk path's part of the stack (HBEP-6, The bulk path; #448, part 7): up --bulk-path, and S17's
// shaped link on loopback, which the bulk path's test plan runs through.

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/linkemu"
)

// BulkPath makes StartHost's up run the bulk path (up --bulk-path): a second frps at base+13 to
// base+15, the second certificate, and the front's and the controller's bulk halves. The stack's
// block grows to base+15 with it.
func BulkPath(h *Host) {
	h.upExtra = append(h.upExtra, "--bulk-path")
	h.Ports.BulkPath = true
}

// StartLink starts S17's link in its fair mode, 48 MB/s each way and a 2.3 ms round trip
// (linkemu.S17), in front of upstream, and returns the address that reaches upstream through it and
// the link. It is closed at the end of the test.
func StartLink(t T, upstream string) (string, *linkemu.Link) {
	t.Helper()
	l, err := linkemu.New(linkemu.S17)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	addr, err := l.Listen(upstream)
	if err != nil {
		t.Fatal(err)
	}
	return addr, l
}
