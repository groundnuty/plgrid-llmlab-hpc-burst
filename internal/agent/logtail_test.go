package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// leak returns the first substring of 8 or more characters of any key that s
// holds, or "". Every longer substring holds an 8-character one, so checking
// those is enough.
func leak(s string, keys ...string) string {
	for _, k := range keys {
		for i := 0; i+8 <= len(k); i++ {
			if strings.Contains(s, k[i:i+8]) {
				return k[i : i+8]
			}
		}
	}
	return ""
}

func newTestKey(t *testing.T) string {
	t.Helper()
	k, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// A key that crosses the edge of the log tail's window leaves nothing of
// itself in the tail, at every offset: o of its 64 bytes inside the window,
// from none to all (PR #138 review, Astra B1, Opus 1: the tail was cut before
// the key was replaced, and 54 of its 64 characters survived). The key
// crossing the edge is the current Instance's or an earlier one's, since the
// log is appended to across Instances. In the "shrinking" shape the window
// also holds 100 copies of the current key, each replaced by the shorter
// marker, so the cut tail reaches further back than the window: the bytes
// before the window that can hold a key's end must still be left out. The
// filler is "z", which no hex key holds.
func TestReadLogTailLeavesNoPartOfAKeyAtTheWindowEdge(t *testing.T) {
	cur, old := newTestKey(t), newTestKey(t)
	path := filepath.Join(t.TempDir(), "engine-worker-0.log")
	for _, shape := range []string{"plain", "shrinking"} {
		for _, edge := range []string{"current", "earlier"} {
			k := cur
			if edge == "earlier" {
				k = old
			}
			for o := 0; o <= len(k); o++ {
				inside := ""
				if shape == "shrinking" {
					inside = strings.Repeat("\n"+cur, 100)
				}
				inside += strings.Repeat("z", agentapi.MaxLogTailBytes-o-len(inside))
				body := strings.Repeat("z", 3000) + k + inside
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				got := readLogTail(path, []string{old, cur})
				if l := leak(got, cur, old); l != "" {
					t.Fatalf("%s, %s key, %d bytes inside: the tail holds %q of a key", shape, edge, o, l)
				}
				if len(got) > agentapi.MaxLogTailBytes {
					t.Fatalf("%s, %s key, %d bytes inside: tail is %d bytes", shape, edge, o, len(got))
				}
				if o == len(k) && !strings.Contains(got, agentapi.RedactedValue) {
					t.Fatalf("%s, %s key wholly inside: no marker in the tail", shape, edge)
				}
			}
		}
	}
}

// Why a worker could not start is cut to MaxLogTailBytes after the keys are
// replaced, so a key that the cut would split leaves nothing of itself (Opus
// 1: the key's first 20 characters survived), at every split.
func TestStartFailureTailLeavesNoPartOfAKey(t *testing.T) {
	cur, old := newTestKey(t), newTestKey(t)
	prefix := len("the worker did not start: ")
	for _, k := range []string{cur, old} {
		for o := 0; o <= len(k); o++ {
			// o bytes of the key fall inside the cut at MaxLogTailBytes.
			msg := strings.Repeat("z", agentapi.MaxLogTailBytes-prefix-o) + k + " after"
			got := startFailureTail(errors.New(msg), []string{old, cur})
			if l := leak(got, cur, old); l != "" {
				t.Fatalf("%d bytes before the cut: the text holds %q of a key", o, l)
			}
			if len(got) > agentapi.MaxLogTailBytes {
				t.Fatalf("%d bytes before the cut: text is %d bytes", o, len(got))
			}
		}
	}
	if got := startFailureTail(errors.New("exec: no such file"), []string{cur}); got != "the worker did not start: exec: no such file" {
		t.Errorf("a short text changed: %q", got)
	}
}

// redactKeys replaces overlapping and adjacent occurrences with one marker,
// replaces an occurrence that starts in the dropped prefix and ends past it,
// and leaves out the rest of the prefix.
func TestRedactKeys(t *testing.T) {
	for _, tc := range []struct {
		b    string
		drop int
		keys []string
		want string
	}{
		{"a-KEYKEY-b", 0, []string{"KEY"}, "a-[redacted]-b"},
		{"a-KEKEY-b", 0, []string{"KEK", "KEY"}, "a-[redacted]-b"},
		{"EY-b-KEY", 2, []string{"KEY"}, "-b-[redacted]"},
		{"xKEY-b", 2, []string{"KEY"}, "[redacted]-b"},
		{"no key", 0, []string{"", "KEY"}, "no key"},
	} {
		if got := string(redactKeys([]byte(tc.b), tc.drop, tc.keys)); got != tc.want {
			t.Errorf("redactKeys(%q, %d, %q) = %q, want %q", tc.b, tc.drop, tc.keys, got, tc.want)
		}
	}
}
