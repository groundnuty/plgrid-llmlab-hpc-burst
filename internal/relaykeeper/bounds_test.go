package relaykeeper

// New for the review of PR 354: the bounds of the expiry's arithmetic and of
// E (B5, B7), the state file's strict reading (B6), strict ping and status
// bodies, and the monotonic clock.

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// A set's listing age is compared with its validity before anything is
// converted, so no age overflows into a future expiry (the review of PR 354,
// B5): an age of the validity or more is expired on arrival, and one past
// wire.MaxListingAge, 2^58 or MaxInt64 among them, is refused as malformed.
// Below the bound, the comparison alone decides: here a set handled with no
// check of its form, whose age is MaxInt64, is expired, never fresh.
func TestAListingAgeIsComparedBeforeItIsConverted(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	for _, age := range []int64{40_000, 40_001, wire.MaxListingAge.Milliseconds()} {
		s := set("example-a", 1, true, "login.example", entry(1))
		s.ListingAgeMS = age
		if rep := accepted(t, k, s); !rep.Expired {
			t.Errorf("a set whose listing is %d ms old, valid 40000 ms: not expired", age)
		}
	}
	for _, age := range []int64{wire.MaxListingAge.Milliseconds() + 1, 1 << 58, math.MaxInt64} {
		s := set("example-a", 1, true, "login.example", entry(1))
		s.ListingAgeMS = age
		if got := refused(t, k, wire.KindSet, s); got.Reason != wire.RefusedMalformed {
			t.Errorf("a set whose listing is %d ms old: %+v, want malformed", age, got)
		}
	}
	s := set("example-a", 1, true, "login.example", entry(1))
	s.ListingAgeMS = math.MaxInt64
	kind, body := k.handleSet(s)
	if rep, ok := body.(wire.Report); kind != wire.KindReport || !ok || !rep.Expired {
		t.Errorf("an unchecked set with an age of MaxInt64: %s %+v, want expired", kind, body)
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("expired sets reached the Runner: %q", calls)
	}
}

// The state file is read as strictly as a message, and what is not a whole,
// valid state file stops the keeper and is left as it was (the review of PR
// 354, B6).
func TestTheStateFileIsReadStrictly(t *testing.T) {
	for name, content := range map[string]string{
		"no start count":      `{"schema": 1}`,
		"no schema":           `{"starts": 4}`,
		"a trailing object":   `{"schema": 1, "starts": 4} {"schema": 1, "starts": 9}`,
		"concatenated":        `{"schema":1,"starts":4}{"schema":1,"starts":9}`,
		"trailing text":       `{"schema": 1, "starts": 4} x`,
		"garbage":             `not a state file`,
		"a case variant":      `{"Schema": 1, "starts": 4}`,
		"a key twice":         `{"schema": 1, "starts": 4, "starts": 9}`,
		"a bad fence":         `{"schema": 1, "starts": 4, "providers": {"example-a": {"highestEpoch": -1}}}`,
		"a claim of nobody":   `{"schema": 1, "starts": 4, "claims": {"login.example": {"provider": "", "providerUID": "", "created": "2026-10-01T00:00:00Z", "since": "2026-11-10T12:00:00Z"}}}`,
		"an end of no kind":   `{"schema": 1, "starts": 4, "ends": [{"provider": "example-a", "end": {"uid": "0a1b", "tag": "t", "job": "5", "channel": "serving", "kind": "", "cause": "", "at": "2026-11-10T12:00:00Z", "epoch": 1, "seq": 1}}]}`,
		"a decision unreason": `{"schema": 1, "starts": 4, "ends": [{"provider": "example-a", "end": {"uid": "0a1b", "tag": "t", "job": "5", "channel": "serving", "kind": "failed", "cause": "", "at": "2026-11-10T12:00:00Z", "epoch": 1, "seq": 1}, "decided": true, "outcome": "refused"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(r.path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Options{State: r.path, Runner: func(int64) (Runner, error) {
				t.Error("a Runner was made on a bad state file")
				return &fakeRunner{}, nil
			}}); err == nil {
				t.Fatal("Open accepted it")
			}
			if b, _ := os.ReadFile(r.path); string(b) != content {
				t.Errorf("the bad state file was changed to %q", b)
			}
		})
	}
}

// An exhausted start count is refused before the file changes, so E never
// overflows into a negative epoch, which the job side would refuse (the
// review of PR 354, B7); one start short of it gives E = MaxInt64.
func TestAnExhaustedStartCountIsRefusedBeforeTheFileChanges(t *testing.T) {
	for _, c := range []struct {
		starts int64
		ok     bool
	}{{MaxStarts, false}, {MaxStarts - 1, true}} {
		r := newRig(t)
		if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
			t.Fatal(err)
		}
		content := `{"schema": 1, "starts": ` + strconv.FormatInt(c.starts, 10) + `}`
		if err := os.WriteFile(r.path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		k, err := Open(Options{State: r.path, Runner: func(int64) (Runner, error) { return &fakeRunner{}, nil }})
		switch {
		case !c.ok && err == nil:
			t.Errorf("a start count of %d was taken: E would overflow", c.starts)
		case !c.ok:
			if b, _ := os.ReadFile(r.path); string(b) != content {
				t.Errorf("the refused start changed the file to %q", b)
			}
		case err != nil:
			t.Errorf("a start count of %d: %v", c.starts, err)
		case k.Epoch() != math.MaxInt64:
			t.Errorf("E is %d, want MaxInt64", k.Epoch())
		}
	}
}

// Ping and status bodies are decoded strictly: an unknown field or another
// kind of value is refused (both reviews of PR 354).
func TestPingAndStatusBodiesAreStrict(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	for _, kind := range []wire.Kind{wire.KindPing, wire.KindStatus} {
		for _, body := range []string{`{"unknown": 1}`, `5`, `[]`} {
			got, reply := k.Handle(context.Background(), wire.Envelope{V: wire.Version, Kind: kind, Body: []byte(body)})
			if got != wire.KindRefused || reply.(wire.Refused).Reason != wire.RefusedMalformed {
				t.Errorf("a %s with the body %s: %s %+v, want malformed", kind, body, got, reply)
			}
		}
		for _, body := range []string{``, `{}`} {
			if got, reply := k.Handle(context.Background(), wire.Envelope{V: wire.Version, Kind: kind, Body: []byte(body)}); got == wire.KindRefused {
				t.Errorf("a %s with the body %q was refused: %+v", kind, body, reply)
			}
		}
	}
}

// The expiry keeps the clock's monotonic reading, which makes it immune to
// steps of the wall clock (#343's Q5): with the keeper's own clock, the
// expiry and the wish carry it. A regression to Now().UTC(), or a Round,
// would strip it.
func TestTheExpiryKeepsTheClocksMonotonicReading(t *testing.T) {
	r := newRig(t)
	k, _ := r.open(func(o *Options) { o.Now = nil })
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
	k.mu.Lock()
	expires := k.sets["example-a"].expires
	w := k.wishes[provKey{"example-a", set("example-a", 1, true, "x").ProviderUID}]
	k.mu.Unlock()
	if !strings.Contains(expires.String(), " m=+") || !strings.Contains(w.expires.String(), " m=+") {
		t.Errorf("the expiry %v and the wish's %v carry no monotonic reading", expires, w.expires)
	}
}
