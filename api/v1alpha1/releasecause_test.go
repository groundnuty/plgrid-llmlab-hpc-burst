package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// A synthetic Operation UID in the API server's form.
const exampleOperationUID = types.UID("0b8f8a9e-5c1d-4e2f-9a3b-7c6d5e4f3a2b")

// The writer's form, read back: the cause is exactly the form, and parses to
// what was written.
func TestReleaseCauseOperationRoundTrips(t *testing.T) {
	got := ReleaseCauseOperation("team-a", "drain-example-1", exampleOperationUID)
	if want := "operation team-a/drain-example-1 0b8f8a9e-5c1d-4e2f-9a3b-7c6d5e4f3a2b"; got != want {
		t.Fatalf("ReleaseCauseOperation = %q, want %q", got, want)
	}
	ns, name, uid, ok := ParseReleaseCauseOperation(got)
	if !ok || ns != "team-a" || name != "drain-example-1" || uid != exampleOperationUID {
		t.Fatalf("ParseReleaseCauseOperation = %q, %q, %q, %v", ns, name, uid, ok)
	}
	// A name with dots is a valid Operation name too.
	dotted := ReleaseCauseOperation("team-a", "drain.example.1", exampleOperationUID)
	if _, name, _, ok := ParseReleaseCauseOperation(dotted); !ok || name != "drain.example.1" {
		t.Fatalf("a dotted name: %q, %v", name, ok)
	}
}

// The bound: 10 bytes of "operation ", a 63-character namespace, "/", a
// 145-character name, " " and a 36-character UID make exactly 256 bytes, the
// annotation's bound, and still parse. One more character of name would be
// cut by the writer (the case below).
func TestReleaseCauseOperationAtTheBound(t *testing.T) {
	ns := strings.Repeat("n", 63)
	name := strings.Repeat("o", 145)
	cause := ReleaseCauseOperation(ns, name, exampleOperationUID)
	if len(cause) != 256 {
		t.Fatalf("the cause is %d bytes, want 256", len(cause))
	}
	if gotNS, gotName, uid, ok := ParseReleaseCauseOperation(cause); !ok || gotNS != ns || gotName != name || uid != exampleOperationUID {
		t.Fatalf("a cause of exactly 256 bytes does not parse: %v", ok)
	}
}

// A cut cause is refused, never read as a partial name or UID. The cut causes
// are written out as the lifecycle controller's writer leaves them: 253 bytes
// of the cause, then "…" (3 bytes), 256 in all (internal/lifecycle,
// releaseCause).
func TestReleaseCauseOperationRefusesACutCause(t *testing.T) {
	// With a 63-character namespace and a 145-character name the cause is 256
	// bytes: "operation " is bytes 0-9, the namespace 10-72, "/" 73, the name
	// 74-218, the space 219 and the UID 220-255. Each case cuts it at a byte and
	// appends "…", as a writer that shortened it would.
	ns := strings.Repeat("n", 63)
	full := ReleaseCauseOperation(ns, strings.Repeat("o", 145), exampleOperationUID)
	if len(full) != 256 || full[219] != ' ' || string(full[220:]) != string(exampleOperationUID) {
		t.Fatalf("the fixture's layout is not the one worked out: %d bytes", len(full))
	}
	for name, cutAt := range map[string]int{
		"right after the slash":   74,
		"inside the name":         150,
		"after the name's last o": 219, // no space, no UID
		"after the space":         220, // the space, then no UID
		"inside the UID":          240,
		"one byte off the UID":    255, // the UID lost its last character
	} {
		t.Run(name, func(t *testing.T) {
			cut := full[:cutAt] + "…"
			gotNS, gotName, uid, ok := ParseReleaseCauseOperation(cut)
			if ok || gotNS != "" || gotName != "" || uid != "" {
				t.Fatalf("a cut cause parsed as %q, %q, %q, %v; want refused, all empty", gotNS, gotName, uid, ok)
			}
		})
	}
	// A 146-character name makes 257 bytes, which the writer cuts at 253 plus
	// "…": the UID loses its last 4 characters.
	long := ReleaseCauseOperation(ns, strings.Repeat("o", 146), exampleOperationUID)
	if len(long) != 257 {
		t.Fatalf("the fixture is %d bytes, want 257", len(long))
	}
	if _, _, _, ok := ParseReleaseCauseOperation(long[:253] + "…"); ok {
		t.Fatal("the cut 257-byte cause parsed")
	}
}

// Anything but the whole form is refused.
func TestReleaseCauseOperationRefusesOtherCauses(t *testing.T) {
	uid := string(exampleOperationUID)
	for _, cause := range []string{
		"",
		"drained: the hard end passed",
		"hedge lost to example-27b-x",
		"operation team-a/drain-example-1",                                  // no UID
		"operation team-a " + uid,                                           // no name
		"operation /drain-example-1 " + uid,                                 // no namespace
		"operation  team-a/drain-example-1 " + uid,                          // two spaces
		"operation team-a/drain-example-1  " + uid,                          // two spaces
		"Operation team-a/drain-example-1 " + uid,                           // capital
		"operation Team-A/drain-example-1 " + uid,                           // not a DNS label
		"operation team-a/Drain-Example-1 " + uid,                           // not a DNS subdomain
		"operation team-a/drain-example-1 " + uid + " ",                     // trailing space
		"operation team-a/drain-example-1 " + uid + " and more",             // a suffix
		"released: operation team-a/drain-example-1 " + uid,                 // a prefix
		"operation team-a/drain-example-1 " + strings.ToUpper(uid),          // an upper-case UID
		"operation team-a/drain-example-1 0b8f8a9e5c1d4e2f9a3b7c6d5e4f3a2b", // no hyphens
		"operation team-a/drain-example-1 ------------------------------------",
		"operation " + strings.Repeat("n", 64) + "/drain-example-1 " + uid, // a 64-character namespace
	} {
		if ns, name, got, ok := ParseReleaseCauseOperation(cause); ok || ns != "" || name != "" || got != "" {
			t.Errorf("%q parsed as %q, %q, %q, %v; want refused", cause, ns, name, got, ok)
		}
	}
}
