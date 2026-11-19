package front

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A sticky pin's lifetime, its journal and its bound (HBEP-5, Workloads, Sticky routes: Lifetime, The journal, Bound). The clock is the front's fake one. A
// row that shows a pin outlives or ends a conversation makes the pin point AWAY from the key's home (pinAway), so a fresh decision would differ from the pin.

// pinAway pins the conversation s, whose home is a-0, to b-0: a-0 is held full by another conversation while s makes its first request, which spills to b-0.
// The held request is let go at the end, so a-0 is free again and s's home is where a new conversation of s would go.
func (f *skFix) pinAway(s string) {
	f.t.Helper()
	holder := f.session("ns/a-0", 7)
	release := f.hold("ns/a-0")
	held, _ := f.start(holder)
	errWaitFor(f.t, "the holder to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	if got, resp, body := f.chatAt(s); got != "ns/b-0" {
		f.t.Fatalf("the first request of the conversation went to %q (%d %s), want the spill to b-0", got, resp.StatusCode, body)
	}
	release()
	if resp, body := held.wait(f.t); resp.StatusCode != 200 {
		f.t.Fatalf("the holder: %d %s", resp.StatusCode, body)
	}
}

// at asserts that one request of the conversation is served by inst.
func (f *skFix) at(s, inst, why string) {
	f.t.Helper()
	if got, resp, body := f.chatAt(s); got != inst {
		f.t.Fatalf("%s: the conversation went to %q (%d %s), want %s", why, got, resp.StatusCode, body, inst)
	}
}

func (f *skFix) setPinTTL(ttl string) {
	for i := range f.snap.Workloads {
		if f.snap.Workloads[i].Name == "alpha" {
			d := v1alpha1.Duration(ttl)
			f.snap.Workloads[i].Spec.PinTTL = &d
		}
	}
	f.apply()
}

// A pin expires pinTTL after its last request, at the TTL in force WHEN IT WAS MADE (Lifetime). The pin is made with 2 h; the Workload's pinTTL is then edited
// to 1 h. A request 30 min later extends the pin, and 90 min after that, past the edited TTL and inside the made one, the conversation is still on its pin.
// 119 min after the next request it is still pinned, and 121 min after that it has expired, and the conversation, a new one, goes to its home, with a new pin
// made at the Workload's present TTL, 1 h.
func TestStickyPinExpiresAtTheTTLItHadWhenItWasMade(t *testing.T) {
	f := newSkFix(t, skOpts{pinTTL: "2h"})
	s := f.session("ns/a-0", 0)
	f.pinAway(s)
	f.setPinTTL("1h")
	f.advance(30 * time.Minute)
	f.at(s, "ns/b-0", "30 min after the pin, the TTL edited to 1 h")
	f.advance(90 * time.Minute)
	f.at(s, "ns/b-0", "90 min after the last request: past the edited TTL, inside the one the pin was made with")
	f.advance(119 * time.Minute)
	f.at(s, "ns/b-0", "119 min after the last request")
	f.advance(121 * time.Minute)
	f.at(s, "ns/a-0", "121 min after the last request: the pin has expired and the conversation is a new one")
	// The new pin supersedes the first, and the journal, grown past twice its live size, has been compacted to it.
	pins := f.journalOf("pin", s)
	if len(pins) != 1 || pins[0].TTL.D() != time.Hour || pins[0].MD != f.mdUID("md-a") {
		t.Errorf("the journal's pins of the key are %+v, want one: at 1h, on md-a's ModelDeployment", pins)
	}
}

// A pin never expires while a request of it is in flight, and expires pinTTL after the last request ENDS (Lifetime). The pin has pinTTL 1 h. A request is held
// on its target for 3 h, and a second request of the conversation then arrives, past the TTL, and is still sent to the pin. Both end then. 59 min later the
// pin is alive, and 61 min after that, 1 h after the last of them ended, it is not.
func TestStickyPinDoesNotExpireInFlightAndExpiresFromTheEndOfItsLastRequest(t *testing.T) {
	f := newSkFix(t, skOpts{pinTTL: "1h", deps: []pwDep{{name: "md-a", instances: []string{"a-0"}, cap: 1}, {name: "md-b", instances: []string{"b-0"}, cap: 2}},
		edit: func(c *Config) {
			c.MaxRequestDuration, c.IdleTimeout = Duration(100*time.Hour), Duration(100*time.Hour)
		}})
	s := f.session("ns/a-0", 0)
	f.pinAway(s)
	release := f.hold("ns/b-0")
	first, _ := f.start(s)
	errWaitFor(t, "the first request to reach b-0", func() bool { return f.engines["ns/b-0"].Open() == 1 })
	f.advance(3 * time.Hour)
	second, _ := f.start(s)
	errWaitFor(t, "the second request, 3 h into the first, to reach the pin's target", func() bool { return f.engines["ns/b-0"].Open() == 2 })
	if n := f.engineSaw("ns/a-0"); n != 1 {
		t.Fatalf("a-0 saw %d requests, want only the holder of the setup: the pin expired in flight and the conversation went home", n)
	}
	release()
	for _, c := range []*scCall{first, second} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/b-0" {
			t.Fatalf("a request of the pinned conversation: %d from %q: %s", resp.StatusCode, scInstanceOf(resp), body)
		}
	}
	f.advance(59 * time.Minute)
	f.at(s, "ns/b-0", "59 min after the last request ended")
	f.advance(61 * time.Minute)
	f.at(s, "ns/a-0", "61 min after the last request ended")
}

// Each request extends the pin, and the extension is journalled at most once a minute; one that can't be written fails its request (Lifetime). Requests at
// 10, 20 and 50 s after the pin write nothing. One at 61 s writes one extension, whose expiry is that time plus the TTL; one at 90 s writes none; one at
// 122 s writes the second. With the journal unwritable, a request after another minute is 503 pins_unavailable and reaches no engine, and once the journal is
// writable the conversation is on its pin. Two more conversations hold pins, so that the journal, at 5 records, is not past twice its live size, 6, and is not
// compacted: compaction would take the extensions out of the count.
func TestStickyExtensionIsJournalledAtMostOnceAMinuteAndAFailedOneFailsItsRequest(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	f.at(f.session("ns/b-0", 0), "ns/b-0", "a second conversation")
	f.at(f.session("ns/b-0", 1), "ns/b-0", "a third conversation")
	f.at(s, "ns/a-0", "the first request")
	exts := func() int { return len(f.journalOf("ext", s)) }
	for _, at := range []time.Duration{10 * time.Second, 10 * time.Second, 30 * time.Second} {
		f.advance(at)
		f.at(s, "ns/a-0", "inside the first minute")
	}
	if n := exts(); n != 0 {
		t.Fatalf("%d extensions were journalled inside the first minute, want none", n)
	}
	f.advance(11 * time.Second) // 61 s
	f.at(s, "ns/a-0", "at 61 s")
	if got := f.journalOf("ext", s); len(got) != 1 || !got[0].Expiry.Equal(f.start0().Add(61*time.Second+12*time.Hour)) {
		t.Fatalf("at 61 s the journal's extensions are %+v, want one that expires 12 h after 61 s", got)
	}
	f.advance(29 * time.Second) // 90 s
	f.at(s, "ns/a-0", "at 90 s")
	if n := exts(); n != 1 {
		t.Fatalf("at 90 s there are %d extensions, want still 1", n)
	}
	f.advance(32 * time.Second) // 122 s
	f.at(s, "ns/a-0", "at 122 s")
	if n := exts(); n != 2 {
		t.Fatalf("at 122 s there are %d extensions, want 2", n)
	}

	f.advance(time.Minute)
	f.setFailWrites(errors.New("no space left on the device"))
	before := f.totalSaw()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "capacity", detail: "pins_unavailable", retryAfter: "10"})
	if n := f.totalSaw(); n != before {
		t.Errorf("the engines saw %d requests after one whose extension could not be written, want none more", n-before)
	}
	f.setFailWrites(nil)
	f.at(s, "ns/a-0", "after the journal is writable again")
}

// The journal is reloaded before a request of a sticky Workload is served, so a restart changes no conversation's target (Sticky routes, The journal). The
// key's first request spills to b-0 while its home a-0 is full. A new router over the same journal then serves it: the key goes to b-0, not to its home, which
// is free. A store that reused the old router's memory would prove nothing, so the new router holds none: it starts from the file.
func TestStickyRestartedFrontSendsAKeyToItsPinnedTarget(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	f.pinAway(s)
	f.restart()
	f.at(s, "ns/b-0", "after a restart")
	if n := len(f.journalOf("pin", s)); n != 1 {
		t.Errorf("the journal holds %d pins of the key after the restart, want the one", n)
	}
}

// At a restart the journal is compacted, dropping the pins that have expired (The journal). Two conversations are pinned 10 h apart with a TTL of 12 h; 13 h
// after the first one's request the first has expired and the second has not. After the restart's first request the journal holds the second's pin and no
// record of the first's, and the second is still on its pin.
func TestStickyRestartDropsExpiredPinsFromTheJournal(t *testing.T) {
	f := newSkFix(t, skOpts{})
	old, live := f.session("ns/a-0", 0), f.session("ns/a-0", 1)
	f.at(old, "ns/a-0", "the first conversation")
	f.advance(10 * time.Hour)
	f.pinAway(live)
	f.advance(3 * time.Hour)
	if n := len(f.journalOf("pin", old)); n != 1 {
		t.Fatalf("before the restart the journal holds %d pins of the first conversation, want 1", n)
	}
	f.restart()
	f.at(live, "ns/b-0", "the live conversation after the restart")
	if n := len(f.journalOf("pin", old)); n != 0 {
		t.Errorf("after the restart the journal still holds %d records of the expired pin, want none", n)
	}
	if n := len(f.journalOf("pin", live)); n != 1 {
		t.Errorf("after the restart the journal holds %d pins of the live conversation, want 1", n)
	}
}

// A journal the front can't read refuses the requests of the sticky Workload with 503 capacity and detail pins_unavailable, and they reach no engine; a
// request that names no Workload is served as before. The file is not a journal here, a line that is no record in the middle of it. Once it is a journal, the
// conversation it names is on its pin. A last line a crash cut short is no error: its request was never sent, since a pin is dispatched only after its line is
// synced, so the journal loads without it and the file is cut back to its last whole line.
func TestStickyUnreadableJournalRefusesItsRequestsAndATornLastLineIsDropped(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	whole, err := json.Marshal(pinRecord{Op: "pin", Workload: f.alphaUID(), Key: "9f2b6e5d1a3c4b7e8f0a1d2c3b4a59687766554433221100ffeeddccbbaa9988",
		pinTarget: pinTarget{MD: f.mdUID("md-b"), ID: f.idOf("ns/b-0")}, TTL: Duration(12 * time.Hour), Expiry: f.start0().Add(12 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.journalPath(), append(append(append([]byte{}, whole...), '\n'), []byte("this is not a record\n"+string(whole)+"\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "capacity", detail: "pins_unavailable", retryAfter: "10"})
	if n := f.totalSaw(); n != 0 {
		t.Errorf("the engines saw %d requests with an unreadable journal, want none", n)
	}
	if got, r, body := f.postAt(s, scModel); got == "" {
		t.Errorf("a request that names no Workload was refused with an unreadable journal: %d %s", r.StatusCode, body)
	}

	// The journal of s itself, with a torn last line.
	sum := keyHashByHand(s)
	mine, _ := json.Marshal(pinRecord{Op: "pin", Workload: f.alphaUID(), Key: sum, pinTarget: pinTarget{MD: f.mdUID("md-b"), ID: f.idOf("ns/b-0")},
		TTL: Duration(12 * time.Hour), Expiry: f.start0().Add(12 * time.Hour)})
	if err := os.WriteFile(f.journalPath(), append(append(append([]byte{}, mine...), '\n'), []byte(`{"op":"pin","workload":"`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	f.at(s, "ns/b-0", "a conversation pinned by a journal whose last line was torn: its home is a-0")
	b, err := os.ReadFile(f.journalPath())
	if err != nil || len(b) == 0 || b[len(b)-1] != '\n' {
		t.Errorf("the journal after the load ends in %q (read: %v), want a whole last line", string(b[max(0, len(b)-12):]), err)
	}
}

// At most maxPins live pins per Workload (Bound). With maxPins 2, two conversations are pinned, and a third conversation's first request is 503 capacity with
// detail pins_full, reaches no engine and is not journalled; the two pinned conversations are served as before, on their pins. Pins
// that have expired are not live: after 13 h the third conversation is pinned, the first two having lapsed.
func TestStickyMaxPinsRefusesAFirstRequestAndKeepsTheLivePins(t *testing.T) {
	f := newSkFix(t, skOpts{maxPins: 2})
	one, two, three := f.session("ns/a-0", 0), f.session("ns/b-0", 0), f.session("ns/a-0", 1)
	f.at(one, "ns/a-0", "the first conversation")
	f.at(two, "ns/b-0", "the second conversation")
	before := f.totalSaw()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", three)
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "capacity", detail: "pins_full", retryAfter: "10"})
	if n := f.totalSaw(); n != before {
		t.Errorf("the engines saw %d requests of a conversation refused for a full pin table", n-before)
	}
	if n := len(f.journalOf("pin", three)); n != 0 {
		t.Errorf("the refused conversation has %d pin records", n)
	}
	f.at(one, "ns/a-0", "the first conversation after the refusal")
	f.at(two, "ns/b-0", "the second conversation after the refusal")
	if n := len(f.journal()); n < 2 {
		t.Errorf("the journal holds %d records, want the two live pins kept", n)
	}
	f.advance(13 * time.Hour)
	f.at(three, "ns/a-0", "after the first two pins lapsed")
}
