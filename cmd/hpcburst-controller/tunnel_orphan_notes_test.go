package main

// TunnelOrphaned's note names the proxy frps lists (HBEP-8, Allocation events, No job ID, path or name in
// a note; the science agent's ruling on #640). The name is frps's text, read from its list, and the
// event goes to a durable log that is kept and may be exported. A name in the form of an Instance UID,
// which the agent gives its proxy and the front's frps plugin demands, is the framework's own fact and
// is named; any other name is not shown, and the controller's log line has it.
//
// The test injects names that hold every synthetic value into both frps's lists, runs the real read
// into a recorder, and checks each note as TOKENS: a run of letters, digits, '_', '-', '/' and '.' is
// one token, and a value leaks only as a whole token or as a whole component of one. The expected notes
// are literals written here.

import (
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
)

const (
	synUser    = "example-user-x1"
	synAccount = "example-account-a1"
	synGrant   = "example-grant-g1"
	synJob     = "1234567"
	// synProxy is a proxy name that is a path under a synthetic home, with each synthetic value as one
	// component.
	synProxy = "/home/" + synUser + "/" + synAccount + "/" + synGrant + "/" + synJob + ".sock"
	// synProxyBulk is another, on the second frps.
	synProxyBulk = "/home/" + synUser + "/" + synGrant + "/" + synAccount + "/" + synJob + "-bulk.sock"
)

var synAll = []string{synUser, synAccount, synGrant, synJob}

// noteTokens splits s into its tokens: the maximal runs of letters, digits, '_', '-', '/' and '.'
// ('~' too, so that a home path is one token), each without trailing dots (a sentence's end).
func noteTokens(s string) []string {
	var out []string
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		if tok := strings.TrimRight(s[start:end], "."); tok != "" {
			out = append(out, tok)
		}
		start = -1
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isTok := c == '_' || c == '-' || c == '/' || c == '.' || c == '~' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if isTok && start < 0 {
			start = i
		}
		if !isTok {
			flush(i)
		}
	}
	flush(len(s))
	return out
}

// pathLooking reports whether a token looks like a path: it starts with '/', '~/', './' or '../', or it
// has two '/' or more. A Kubernetes name, namespace/name, is not one.
func pathLooking(tok string) bool {
	return strings.HasPrefix(tok, "/") || strings.HasPrefix(tok, "~/") || strings.HasPrefix(tok, "./") ||
		strings.HasPrefix(tok, "../") || strings.Count(tok, "/") >= 2
}

// noteLeaks returns what a note holds that it must not: each value that is a whole token or a whole
// component of one (a token split on '/' and '.'), and each path-looking token.
func noteLeaks(note string, values ...string) []string {
	var out []string
	for _, tok := range noteTokens(note) {
		if pathLooking(tok) {
			out = append(out, "path "+tok)
		}
		for _, comp := range strings.FieldsFunc(tok, func(r rune) bool { return r == '/' || r == '.' }) {
			for _, v := range values {
				if comp == v {
					out = append(out, v)
				}
			}
		}
	}
	return out
}

// valuesIn returns the values a text holds as whole tokens or components of one: how a test shows its
// injected text reached the source.
func valuesIn(text string, values ...string) []string {
	var out []string
	for _, v := range values {
		if len(noteLeaks(text, v)) > 0 {
			out = append(out, v)
		}
	}
	return out
}

// TestNoteTokenMatching pins the matcher the test below relies on, in both directions: a real
// occurrence is found, and a value inside a longer word is not.
func TestNoteTokenMatching(t *testing.T) {
	for _, tc := range []struct {
		name, note string
		want       []string
	}{
		{"a user as a path component", "proxy /home/example-user-x1/a.sock online", []string{"path /home/example-user-x1/a.sock", synUser}},
		{"a bare user", "of example-user-x1, a user", []string{synUser}},
		{"a job as a file's stem", "log 1234567.out", []string{synJob}},
		{"a job inside a longer number", "at 11234567 and 12345670", nil},
		{"a job with a suffix is another word", "array 1234567_3", nil},
		{"a user inside a longer name", "example-user-x10 and zexample-user-x1", nil},
		{"a Kubernetes name is not a path", "Allocation team-a/al-a", nil},
		{"a relative path with two slashes", "see a/b/c now", []string{"path a/b/c"}},
		{"an account and a grant", "example-account-a1, example-grant-g1", []string{synAccount, synGrant}},
	} {
		got := noteLeaks(tc.note, synAll...)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: noteLeaks(%q) = %q, want %q", tc.name, tc.note, got, tc.want)
		}
	}
}

// logLines collects the controller's log lines; the bulk list is read on a goroutine of its own.
type logLines struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logLines) add(prefix, args string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(prefix + args + "\n")
}

func (l *logLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// hiddenFrom splits notes into those that do not show the proxy's name and those that do.
func hiddenFrom(notes []string) (hidden, named []string) {
	for _, n := range notes {
		if strings.Contains(n, "(name not shown") {
			hidden = append(hidden, n)
		} else {
			named = append(named, n)
		}
	}
	return hidden, named
}

// messages are the notes of events.
func messages(evs []lifecycle.RecordedEvent) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Message)
	}
	return out
}

func TestTunnelOrphanedNoteNamesNoProxyThatIsNotAnInstanceUID(t *testing.T) {
	const (
		tokenHidden = "frps lists the proxy (name not shown: it is not in the form of an Instance UID) online, and no current Instance has that UID (an orphan, HBEP-6); no Allocation is known to have admitted its tunnel"
		bulkHidden  = "frps-bulk lists the proxy (name not shown: it is not in the form of an Instance UID) online, and no current Instance has that UID (an orphan, HBEP-6, The bulk path); no Allocation is known to have admitted its bulk tunnel"
		tokenNamed  = "frps lists the proxy gone-token online, and no current Instance has that UID (an orphan, HBEP-6); no Allocation is known to have admitted its tunnel"
		bulkNamed   = "frps-bulk lists the proxy gone-bulk online, and no current Instance has that UID (an orphan, HBEP-6, The bulk path); no Allocation is known to have admitted its bulk tunnel"
	)
	r := newBulkRig(t)
	logs := &logLines{}
	r.state.log = funcr.New(logs.add, funcr.Options{})
	// Each frps lists, as online, a proxy named with a path that holds every synthetic value, and a
	// proxy that is in the form of an Instance UID, the control: that one is still named.
	r.tok.list = proxies(online, synProxy, "gone-token")
	r.blk.list = proxies(online, synProxyBulk, "gone-bulk")

	r.read(-orphanGrace)
	r.read(0)
	token, bulk := r.wantSplit("both lists listed the orphans for the whole grace", 2, 2)

	// The injected text reached the source: each name holds every value, and the controller's log line
	// of each orphan holds it, where the detail stays.
	for _, name := range []string{synProxy, synProxyBulk} {
		if got := valuesIn(name, synAll...); len(got) != len(synAll) {
			t.Fatalf("control: the proxy name %q holds only %q of %q", name, got, synAll)
		}
		if !strings.Contains(logs.String(), name) {
			t.Fatalf("control: the controller's log does not hold the proxy name %q:\n%s", name, logs.String())
		}
	}

	tokHidden, tokNamed := hiddenFrom(messages(token))
	blkHidden, blkNamed := hiddenFrom(messages(bulk))
	if len(tokHidden) != 1 || tokHidden[0] != tokenHidden {
		t.Errorf("the first frps's notes for the path-named proxy: %q, want one, %q", tokHidden, tokenHidden)
	}
	if len(blkHidden) != 1 || blkHidden[0] != bulkHidden {
		t.Errorf("frps-bulk's notes for the path-named proxy: %q, want one, %q", blkHidden, bulkHidden)
	}
	// The control: a proxy in the form of an Instance UID is named, as it always was.
	if len(tokNamed) != 1 || tokNamed[0] != tokenNamed {
		t.Errorf("the first frps's notes for the UID-form proxy: %q, want one, %q", tokNamed, tokenNamed)
	}
	if len(blkNamed) != 1 || blkNamed[0] != bulkNamed {
		t.Errorf("frps-bulk's notes for the UID-form proxy: %q, want one, %q", blkNamed, bulkNamed)
	}
	for _, msg := range append(messages(token), messages(bulk)...) {
		if leaks := noteLeaks(msg, synAll...); len(leaks) > 0 {
			t.Errorf("the note %q holds %q", msg, leaks)
		}
	}
}
