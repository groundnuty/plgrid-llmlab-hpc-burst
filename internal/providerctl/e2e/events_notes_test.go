package e2e

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// No note the log keeps names a path, a user name, an account or a grant (HBEP-8,
// Allocation events, No job ID, path or name in a note; the science agent's
// ruling on #640). The Provider's credentials file here is invalid in a way
// whose error text carries a path under a synthetic home and the synthetic user
// name (the reader echoes the keyword of a line it refuses), and its account is
// invalid too, so the profile reader's InvalidProfileError reaches the
// Allocation's status.atProvider.reason. The typed AllocationRefused event in
// the API gives the fields and a fixed description, never that text. Every other
// row's notes are checked the same way by checkNotes.
func TestNoLoggedNoteNamesAPathUserAccountOrGrant(t *testing.T) {
	r := newEvRig(t, rigOpts{badProfile: true})
	r.newAlloc("a", "3h")
	a := r.until("a", "Refused", 4, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationRefused })

	// The injected values reached the source: without this the test could pass
	// because nothing was injected.
	reason := a.Status.AtProvider.Reason
	for _, injected := range []string{rowCredsPath, "provider.spec.credentialsRef.file", "provider.spec.account"} {
		if !strings.Contains(reason, injected) {
			t.Fatalf("status.atProvider.reason %q lacks %q, so the injected values do not reach the source and this test proves nothing", reason, injected)
		}
	}
	evs := r.waitEvents(a, want{"failing", "AllocationRefused"})
	r.checkNotes(evs, firstJobID)
	notes := notesOf(evs, "AllocationRefused")
	if len(notes) == 0 || !strings.HasPrefix(notes[0], "the Allocation is refused: the Provider's profile or settings are invalid at ") ||
		!strings.Contains(notes[0], "provider.spec.credentialsRef.file") || !strings.Contains(notes[0], "provider.spec.account") {
		t.Errorf("AllocationRefused notes %q, want the reason code's fixed description naming the invalid fields", notes)
	}
	// The same rule for the note the log keeps, read through a fresh client: it holds no path, user name, account or
	// grant, and its id is <uid>/AllocationRefused/<attempt>, 0 here since the Allocation was refused before any
	// submission attempt was recorded (HBEP-8, The event's id; the erratum for #625).
	r.idle()
	msgs := r.wantLog(a, eid(a, "AllocationRefused", "0"))
	r.checkLogNotes(msgs, firstJobID)
	for _, m := range msgs {
		for _, bad := range []string{rowCredsPath, rowUser, rowHome} {
			if strings.Contains(m.Data.Note, bad) {
				t.Errorf("the log's %s note %q holds %q", m.Data.Reason, m.Data.Note, bad)
			}
		}
	}
}
