package front

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

// Faults of the pin journal and of the managed target (HBEP-5, Workloads, Sticky routes: The journal, A target that's gone). Each row makes a fresh decision
// differ from the pin, so the ring alone cannot pass it.

// A failed append whose rollback also fails keeps the store's memory (The journal). The conversation's pin has a request in flight on b-0, held there, and a
// TTL of 1 h. The journal's handle is made read-only, so that the next write fails and its cut-back fails too; a request of a new conversation is refused, and
// one after it is served, the store having cut the journal back. 61 min later, with the stream still in flight, a second request of the first conversation
// is on its pin: the pin and its in-flight count are the ones the store held all along. A store that read the journal again after the failed rollback would
// have replaced them with the journal's, which shows the pin idle at the reload, an hour before.
func TestStickyFailedWriteWhoseRollbackFailsKeepsTheLivePinsAndTheirInFlightCount(t *testing.T) {
	f := newSkFix(t, skOpts{pinTTL: "1h", deps: []pwDep{{name: "md-a", instances: []string{"a-0"}, cap: 1}, {name: "md-b", instances: []string{"b-0"}, cap: 3}},
		edit: func(c *Config) {
			c.MaxRequestDuration, c.IdleTimeout = Duration(100*time.Hour), Duration(100*time.Hour)
		}})
	s := f.session("ns/a-0", 0)
	f.pinAway(s)
	release := f.stream(s)

	ro, err := os.Open(f.journalPath())
	if err != nil {
		t.Fatal(err)
	}
	st := f.router.pins
	st.mu.Lock()
	_ = st.f.Close()
	st.f = ro
	st.mu.Unlock()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", f.session("ns/a-0", 1))
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "capacity", detail: "pins_unavailable", retryAfter: "10"})
	f.at(f.session("ns/a-0", 2), "ns/a-0", "a new conversation, once the journal has been cut back")

	f.advance(61 * time.Minute)
	before := f.engineSaw("ns/a-0")
	second, _ := f.start(s) // b-0 holds every request, so it is seen in flight there
	errWaitFor(t, "the second request of the conversation to reach b-0, its pin's target", func() bool { return f.engines["ns/b-0"].Open() == 2 })
	if n := f.engineSaw("ns/a-0"); n != before {
		t.Errorf("a-0 saw %d requests of a conversation whose stream was still in flight on its pin", n-before)
	}
	release()
	if resp, body := second.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/b-0" {
		t.Errorf("the second request: %d from %q, want 200 from b-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
}

// A record that is not a record makes the journal unreadable (The journal). An ext record is written with its expiry, and one without it is no record: a
// journal that holds one is refused, 503 pins_unavailable, and reaches no engine, where reading it as an expired pin would have let the conversation go home.
// The control, the same journal with the expiry, puts the conversation on its pin, away from its home.
func TestStickyJournalWithAnExtRecordThatHasNoExpiryIsUnreadable(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	write := func(ext pinRecord) {
		t.Helper()
		var b []byte
		for _, r := range []pinRecord{
			{Op: "pin", Workload: f.alphaUID(), Key: keyHashByHand(s), pinTarget: pinTarget{MD: f.mdUID("md-b"), ID: f.idOf("ns/b-0")}, TTL: Duration(12 * time.Hour)}, ext} {
			line, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			b = append(append(b, line...), '\n')
		}
		if err := os.WriteFile(f.journalPath(), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ext := pinRecord{Op: "ext", Workload: f.alphaUID(), Key: keyHashByHand(s), Expiry: f.start0().Add(12 * time.Hour)}
	write(ext)
	f.restart()
	f.at(s, "ns/b-0", "the control, a journal whose ext has its expiry")

	ext.Expiry = time.Time{}
	write(ext)
	f.restart()
	before := f.totalSaw()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "capacity", detail: "pins_unavailable", retryAfter: "10"})
	if n := f.totalSaw(); n != before {
		t.Errorf("the engines saw %d requests with an ext record that has no expiry in the journal, want none", n-before)
	}
}

// A managed pin follows its own source ModelDeployment (A target that's gone). The conversation is pinned to the managed target of md-llm. A second managed-
// source ModelDeployment of another namespace lists the same Provider and catalog entry. Deleting md-llm ends the target: the conversation is 503 model_inactive
// pinned_target_gone, and the other source, which also lists the entry, does not take it over. The control, before the deletion, is served by the managed route.
func TestStickyManagedPinIsGoneWithItsSourceModelDeployment(t *testing.T) {
	f := newSkFix(t, skOpts{managed: true, deps: []pwDep{{name: "md-a", instances: []string{"a-0"}}}})
	s := f.session("ns/a-0", 0)
	resp, body := f.postWith(s)
	if scEmitterOf(resp) != "provider" || resp.StatusCode != 200 {
		t.Fatalf("the first request: %d from emitter %q, want 200 from the managed route: %s", resp.StatusCode, scEmitterOf(resp), body)
	}
	f.managedMDIn("other", "md-llm2", scModel, "llmlab", wrCatalog, "llm-a")
	f.apply()
	if resp, body := f.postWith(s); scEmitterOf(resp) != "provider" || resp.StatusCode != 200 {
		t.Fatalf("with a second source listed, the conversation went to emitter %q (%d %s), want the managed route", scEmitterOf(resp), resp.StatusCode, body)
	}
	f.removeMD("md-llm")
	resp, body = f.postWith(s)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "pinned_target_gone"})
	if n := f.engineSaw("ns/a-0"); n != 0 {
		t.Errorf("a-0 saw %d requests of a conversation pinned to a managed target", n)
	}
}

// postWith sends one request of alpha under the session key and returns its response and body.
func (f *skFix) postWith(session string) (*http.Response, string) {
	f.t.Helper()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", session)
	return resp, readAll(f.t, resp)
}
