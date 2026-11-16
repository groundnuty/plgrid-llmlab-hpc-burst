package managed

import (
	"os"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// No note the log keeps names a path, a user, an account, a grant or a job ID, and none copies another
// component's error text (HBEP-8, Allocation events, No job ID, path or name in a note; the science
// agent's ruling on #640). The notes of the credentials' three bad states were their conditions'
// messages, built from the errors of the file system, of the file's checks and of the gateway's answer.
// These tests inject synthetic values into the credentials file's path, run the real controller over
// its fakes into a recorder, and check each note as TOKENS: a run of letters, digits, '_', '-', '/' and
// '.' is one token, and a value leaks only as a whole token or as a whole component of one. Every
// expected note is a literal written here, never built by the code it checks.

const (
	synUser    = "example-user-x1"
	synAccount = "example-account-a1"
	synGrant   = "example-grant-g1"
	synJob     = "1234567"
	// synPath is a credentials file's path under a synthetic home that names each synthetic value as one
	// component.
	synPath = "/home/" + synUser + "/" + synAccount + "/" + synGrant + "/" + synJob + "/credentials.json"
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

// TestNoteTokenMatching pins the matcher the tests below rely on, in both directions: a real
// occurrence is found, and a value inside a longer word is not.
func TestNoteTokenMatching(t *testing.T) {
	for _, tc := range []struct {
		name, note string
		want       []string
	}{
		{"a user as a path component", "open /home/example-user-x1/credentials.json: denied", []string{"path /home/example-user-x1/credentials.json", synUser}},
		{"a bare user", "of example-user-x1, a user", []string{synUser}},
		{"a user ending a sentence", "it was example-user-x1.", []string{synUser}},
		{"a job as a whole token", "job 1234567 ended", []string{synJob}},
		{"a job as a file's stem", "log 1234567.out", []string{synJob}},
		{"a job inside a longer number", "at 11234567 and 12345670", nil},
		{"a job inside a hex word", "request ab1234567cd", nil},
		{"a job with a suffix is another word", "array 1234567_3", nil},
		{"a user inside a longer name", "example-user-x10 and zexample-user-x1", nil},
		{"a Kubernetes name is not a path", "Provider example-managed and Allocation team-a/al-a", nil},
		{"a relative path with two slashes", "see a/b/c now", []string{"path a/b/c"}},
		{"a home path", "under ~/creds now", []string{"path ~/creds"}},
		{"an account and a grant", "example-account-a1, example-grant-g1", []string{synAccount, synGrant}},
	} {
		got := noteLeaks(tc.note, synAll...)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: noteLeaks(%q) = %q, want %q", tc.name, tc.note, got, tc.want)
		}
	}
}

// The notes the controller gives, in full.
const (
	fileRefusedNote      = "CredentialsInvalid: the credentials file could not be read or was refused; the reason is in the Provider's CredentialsInvalid condition"
	noCredentialsNote    = "CredentialsInvalid: the Provider names no credentials file; a managed Provider reads one (spec.credentialsRef.file)"
	keyRejectedNote      = "CredentialsRejected: the gateway rejected the key; its answer's status is in the Provider's CredentialsRejected condition"
	readsFailingNoteHead = "Ready: reads are failing: no successful read yet"
)

// noteHoldsNothing fails the test when an event's note holds a synthetic value, any path-looking token
// or an extra text the caller names.
func noteHoldsNothing(t *testing.T, ev sentEvent, also ...string) {
	t.Helper()
	if leaks := noteLeaks(ev.Note, synAll...); len(leaks) > 0 {
		t.Errorf("the %s note %q holds %q", ev.Reason, ev.Note, leaks)
	}
	for _, text := range also {
		if strings.Contains(ev.Note, text) {
			t.Errorf("the %s note %q holds %q", ev.Reason, ev.Note, text)
		}
	}
}

// A credentials file that is refused: the condition's message keeps the error, which names the path, and
// the FileRefused event's note is the fixed description. The pass that has no read yet also turns Ready
// False, and that note names no path either.
func TestFileRefusedNoteHoldsNoPathOfTheCredentialsFile(t *testing.T) {
	// The file is absent, under a synthetic home: the error of the file system names the whole path.
	t.Run("a file that is absent", func(t *testing.T) {
		rec := &captureRecorder{}
		e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
		e.setCredentialsRef(&v1alpha1.CredentialsReference{File: synPath})
		e.syncAt(at(12, 0, 0))
		c := e.wantCondition(v1alpha1.ConditionCredentialsInvalid, "True", "FileRefused")
		// The injected text reached the source, the condition's message, as tokens and as a path.
		if got := valuesIn(c.Message, synAll...); len(got) != len(synAll) || len(noteLeaks(c.Message)) == 0 {
			t.Fatalf("control: the condition message %q holds %q of %q and a path; the test injects nothing otherwise", c.Message, got, synAll)
		}
		evs := rec.take()
		got := only(evs, "FileRefused")
		if len(got) != 1 {
			t.Fatalf("FileRefused events %+v, want one", got)
		}
		if got[0].Type != "Warning" || got[0].Action != "failing" || got[0].Note != fileRefusedNote {
			t.Errorf("FileRefused event %+v, want a Warning failing with the note %q", got[0], fileRefusedNote)
		}
		for _, ev := range evs {
			noteHoldsNothing(t, ev, synPath)
		}
		if rf := only(evs, "ReadsFailing"); len(rf) != 1 || rf[0].Note != readsFailingNoteHead {
			t.Errorf("ReadsFailing events %+v, want one with the note %q", rf, readsFailingNoteHead)
		}
	})
	// The file is there and refused for its mode: the check's error names the file, here in the test's
	// own directory, which the note must not hold either.
	t.Run("a file with a mode other than 600", func(t *testing.T) {
		rec := &captureRecorder{}
		e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
		if err := os.Chmod(e.path, 0o644); err != nil {
			t.Fatal(err)
		}
		e.syncAt(at(12, 0, 0))
		c := e.wantCondition(v1alpha1.ConditionCredentialsInvalid, "True", "FileRefused")
		if !strings.Contains(c.Message, e.path) || !strings.Contains(c.Message, "0644") {
			t.Fatalf("control: the condition message %q does not name the file %s and its mode", c.Message, e.path)
		}
		got := only(rec.take(), "FileRefused")
		if len(got) != 1 {
			t.Fatalf("FileRefused events %+v, want one", got)
		}
		if got[0].Note != fileRefusedNote {
			t.Errorf("FileRefused note %q, want %q", got[0].Note, fileRefusedNote)
		}
		noteHoldsNothing(t, got[0], e.path, "0644")
	})
}

// No file is named: the condition's message names the Provider and the field, and the note is the fixed
// description of the state.
func TestNoCredentialsFileNoteIsFixed(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
	e.setCredentialsRef(nil)
	e.syncAt(at(12, 0, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsInvalid, "True", "NoCredentialsFile")
	if !strings.Contains(c.Message, testProvider) {
		t.Fatalf("control: the condition message %q does not name the Provider %s, the text the note leaves out", c.Message, testProvider)
	}
	got := only(rec.take(), "NoCredentialsFile")
	if len(got) != 1 {
		t.Fatalf("NoCredentialsFile events %+v, want one", got)
	}
	if got[0].Type != "Warning" || got[0].Action != "failing" || got[0].Note != noCredentialsNote {
		t.Errorf("NoCredentialsFile event %+v, want a Warning failing with the note %q", got[0], noCredentialsNote)
	}
	noteHoldsNothing(t, got[0], testProvider)
}

// The gateway rejects the key: the condition's message is the error of the read, which carries the
// gateway's status; the note is the fixed description, with no gateway text. The double echoes the key
// it received in the body of its refusal, as a gateway could put any text in it.
func TestKeyRejectedNoteIsFixed(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
	e.gw.EchoKey(true)
	e.syncAt(at(12, 0, 0))
	rec.take()
	e.gw.RejectKey(e.key, 401)
	e.syncAt(at(12, 2, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, "True", "KeyRejected")
	if !strings.Contains(c.Message, "answered 401") {
		t.Fatalf("control: the condition message %q does not carry the gateway's status 401, the text the note leaves out", c.Message)
	}
	got := only(rec.take(), "KeyRejected")
	if len(got) != 1 {
		t.Fatalf("KeyRejected events %+v, want one", got)
	}
	if got[0].Type != "Warning" || got[0].Action != "failing" || got[0].Note != keyRejectedNote {
		t.Errorf("KeyRejected event %+v, want a Warning failing with the note %q", got[0], keyRejectedNote)
	}
	noteHoldsNothing(t, got[0], "401", e.key)
}
