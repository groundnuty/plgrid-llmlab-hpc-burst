package agentapi

import (
	"regexp"
	"strings"
	"testing"
)

func TestCheckAgentInstanceID(t *testing.T) {
	for _, good := range []string{strings.Repeat("0", 32), "0123456789abcdef0123456789abcdef", strings.Repeat("f", 32)} {
		if err := CheckAgentInstanceID(good); err != nil {
			t.Errorf("%q was refused: %v", good, err)
		}
	}
	for name, bad := range map[string]string{
		"empty":                "",
		"31 characters":        strings.Repeat("a", 31),
		"33 characters":        strings.Repeat("a", 33),
		"64 characters":        strings.Repeat("a", 64),
		"uppercase":            strings.Repeat("A", 32),
		"mixed case":           strings.Repeat("a", 31) + "B",
		"a non-hex letter":     strings.Repeat("a", 31) + "g",
		"a trailing newline":   strings.Repeat("a", 32) + "\n",
		"a leading space":      " " + strings.Repeat("a", 31),
		"dashes (a UUID)":      "01234567-89ab-cdef-0123-456789abcdef",
		"a long header value":  strings.Repeat("a", 100000),
		"a NUL inside":         strings.Repeat("a", 16) + "\x00" + strings.Repeat("a", 15),
		"non-ASCII digits":     strings.Repeat("a", 30) + "٣٣",
		"a Kubernetes name":    "inst-a-x7k2p",
		"the kernel's boot ID": "6f1b2c3d-0000-4000-8000-000000000000",
	} {
		err := CheckAgentInstanceID(bad)
		if err == nil {
			t.Errorf("%s was accepted", name)
			continue
		}
		if len(bad) >= 8 && len(bad) <= 100 && strings.Contains(err.Error(), bad) {
			t.Errorf("%s: the error repeats the value: %v", name, err)
		}
		if len(err.Error()) > 200 {
			t.Errorf("%s: the error is %d bytes long, so it echoes the value", name, len(err.Error()))
		}
	}
}

func TestNewAgentInstanceID(t *testing.T) {
	// The form is checked with an expression of this test's own.
	form := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := map[string]bool{}
	for range 50 {
		id, err := NewAgentInstanceID()
		if err != nil {
			t.Fatal(err)
		}
		if !form.MatchString(id) {
			t.Fatalf("%q is not 32 lowercase hex characters", id)
		}
		if seen[id] {
			t.Fatalf("%q was drawn twice", id)
		}
		seen[id] = true
	}
}
