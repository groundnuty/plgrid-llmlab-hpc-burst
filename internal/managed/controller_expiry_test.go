package managed

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// CredentialsExpiring follows the expiry in the credentials file (HBEP-4: "a
// day before the expiry date"). The gate lets one pass in per minute, so each
// step moves the clock two minutes and rewrites the file's expiry to land on
// the boundary under test, in place of stepping the clock by a second. The
// expected times are written out by hand.
func TestCredentialsExpiringFollowsTheFilesExpiry(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.syncAt(at(12, 0, 0))
	// The file names no expiry: False, NotExpiring (science-agent ruling, 31
	// Oct 2026).
	if c := e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring"); c.Message != "the credentials file names no expiry" {
		t.Fatalf("message = %q, want the no-expiry message", c.Message)
	}

	for i, st := range []struct {
		now     time.Time
		expiry  string
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		// Exactly 24 h before the expiry: the boundary is inside the window.
		{at(12, 2, 0), "2026-11-01T12:02:00Z", metav1.ConditionTrue, "ExpiresWithinADay", "the key expires at 2026-11-01T12:02:00Z"},
		// 24 h and 1 s before: outside.
		{at(12, 4, 0), "2026-11-01T12:04:01Z", metav1.ConditionFalse, "NotExpiring", "the key expires at 2026-11-01T12:04:01Z"},
		// An offset is read as the instant it is: 14:06:00+02:00 is 12:06:00Z, exactly 24 h on.
		{at(12, 6, 0), "2026-11-01T14:06:00+02:00", metav1.ConditionTrue, "ExpiresWithinADay", "the key expires at 2026-11-01T12:06:00Z"},
		// One second before the expiry: still not expired.
		{at(12, 8, 0), "2026-10-31T12:08:01Z", metav1.ConditionTrue, "ExpiresWithinADay", "the key expires at 2026-10-31T12:08:01Z"},
		// At the expiry, and after it.
		{at(12, 10, 0), "2026-10-31T12:10:00Z", metav1.ConditionTrue, "Expired", "the key expired at 2026-10-31T12:10:00Z"},
		{at(12, 12, 0), "2026-10-31T12:11:59Z", metav1.ConditionTrue, "Expired", "the key expired at 2026-10-31T12:11:59Z"},
	} {
		writeCreds(t, e.path, testKey, e.gw.BaseURL(), st.expiry)
		e.syncAt(st.now)
		c := e.wantCondition(v1alpha1.ConditionCredentialsExpiring, st.status, st.reason)
		if c.Message != st.message {
			t.Fatalf("step %d: message = %q, want %q", i, c.Message, st.message)
		}
	}

	// A rotation to a file with no expiry clears Expired at the next read
	// (science-agent ruling, 31 Oct 2026; review of PR #85, Opus nit 1).
	writeCreds(t, e.path, testKey, e.gw.BaseURL(), "")
	e.syncAt(at(12, 14, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring")
	if c.Message != "the credentials file names no expiry" {
		t.Fatalf("message = %q, want the no-expiry message", c.Message)
	}
	wantMT(t, "the transition", &c.LastTransitionTime, at(12, 14, 0))

	// A renewed key, far from its expiry.
	writeCreds(t, e.path, testKey, e.gw.BaseURL(), "2027-01-31T00:00:00Z")
	e.syncAt(at(12, 16, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring")
}

// A rejected key is still checked for expiry: the pass writes the status, and
// the expiry is in the file, not in the gateway's answer.
func TestRejectedKeyIsStillCheckedForExpiry(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.gw.RejectKey(testKey, 401)

	writeCreds(t, e.path, testKey, e.gw.BaseURL(), "2026-11-01T12:00:00Z") // exactly 24 h on
	e.syncAt(at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "ExpiresWithinADay")

	writeCreds(t, e.path, testKey, e.gw.BaseURL(), "2026-10-31T12:00:30Z")
	e.syncAt(at(12, 1, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "Expired")
}
