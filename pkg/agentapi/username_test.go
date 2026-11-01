package agentapi

// ValidUserName is the one portable-user-name rule for every user name that
// goes into ssh (rulings of 1 Nov 2026: PR 132 B2, extended for issue #146).
// The names are examples of the shapes, never a real account.

import (
	"strings"
	"testing"
)

func TestValidUserName(t *testing.T) {
	for _, name := range []string{
		"example-user", "Mixed_Case.Name", "_service", "a", "user.name-2",
		"A" + strings.Repeat("b", 31), // 32 characters, Linux useradd's limit
	} {
		if !ValidUserName(name) {
			t.Errorf("ValidUserName(%q) = false, want a portable name accepted", name)
		}
	}
	for _, name := range []string{
		"", "1user", "-user", "a b", "user@host", "user;x", "user$x", "üser",
		"A" + strings.Repeat("b", 32), // 33 characters
	} {
		if ValidUserName(name) {
			t.Errorf("ValidUserName(%q) = true, want it refused", name)
		}
	}
}
