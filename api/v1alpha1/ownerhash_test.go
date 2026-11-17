package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

// TestOwnerHashPins pins OwnerHash to values worked out outside the code: each was computed with python's hashlib and
// again with sha256sum (the first 12 hex characters of the SHA-256 of the name, behind "u-"), never by OwnerHash.
func TestOwnerHashPins(t *testing.T) {
	for _, tc := range []struct{ user, want string }{
		{"system:serviceaccount:hpcburst-agents:agent-a", "u-77d125ed9ec1"},
		{"system:serviceaccount:hpcburst-agents:agent-b", "u-494c5a26e66f"},
		{"", "u-e3b0c44298fc"},
		{"ExampleUser", "u-f53ee706c86f"},
		{"a:b/c", "u-fb7456513927"},
	} {
		if got := OwnerHash(tc.user); got != tc.want {
			t.Errorf("OwnerHash(%q) = %q, want %q", tc.user, got, tc.want)
		}
	}
}

// TestOwnerHashIsAlwaysAValidLabelValue: the value is 14 characters and passes the API server's label-value check for
// every name, the empty one and names with characters a label value refuses included. The control shows why the
// label carries a hash: a ServiceAccount's own name fails the same check, on its ':'.
func TestOwnerHashIsAlwaysAValidLabelValue(t *testing.T) {
	names := []string{
		"", "ExampleUser", "a:b/c", "system:serviceaccount:hpcburst-agents:agent-a",
		"Mixed_Case.Name", "name with spaces", "näme", strings.Repeat("x", 10000),
	}
	for _, n := range names {
		got := OwnerHash(n)
		if len(got) != 14 {
			t.Errorf("OwnerHash(%.20q) = %q, %d characters, want 14", n, got, len(got))
		}
		if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
			t.Errorf("OwnerHash(%.20q) = %q is not a valid label value: %v", n, got, errs)
		}
	}
	if errs := validation.IsValidLabelValue("system:serviceaccount:hpcburst-agents:agent-a"); len(errs) == 0 {
		t.Error("a ServiceAccount's user name passes IsValidLabelValue; the label would need no hash")
	}
}

// TestLabelOwnerHashName writes the label's key out, since a selector matches it exactly.
func TestLabelOwnerHashName(t *testing.T) {
	if LabelOwnerHash != "hpcburst.groundnuty.github.io/owner-hash" {
		t.Errorf("LabelOwnerHash = %q", LabelOwnerHash)
	}
}
