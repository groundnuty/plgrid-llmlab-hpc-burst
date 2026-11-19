package front

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// The pin journal's compaction past twice its live size (HBEP-5, Sticky routes, The journal), on the store alone with a clock the test moves. One pin is
// made: the journal holds 1 record. Its expiry is journalled when its request ends, at most once a minute: one at 61 s makes 2 records, which is twice the
// live size and no more, so nothing is compacted; the in-flight mark of the next request at 122 s makes 3, past twice 1, and the journal is rewritten to the one live
// pin, written in flight, with no expiry, for that request has not ended. The rows that count records elsewhere keep other pins live so that this rewrite does not take their extensions out of the count.
func TestPinJournalIsCompactedWhenItGrowsPastTwiceItsLiveSize(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), pinJournalFile)
	s := newPinStore(path, func() time.Time { return now }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(s.close)
	const w, key = "00000000-0000-4000-8000-000000000001", "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"
	target := pinTarget{MD: "00000000-0000-4000-8000-000000000002", ID: "9f2b6e5d1a3c4b7e8f0a1d2c3b4a59687766554433221100ffeeddccbbaa9988"}
	lines := func() [][]byte {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return bytes.Split(bytes.TrimSuffix(b, []byte{'\n'}), []byte{'\n'})
	}

	p, err := s.create(w, key, target, 12*time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	s.finish(w, key, p) // its first request ends: the pin's expiry is the second record
	if n := len(lines()); n != 2 {
		t.Fatalf("after the pin and its first expiry the journal holds %d records, want 2", n)
	}
	now = now.Add(61 * time.Second)
	if p, err = s.arrive(w, key); err != nil || p == nil {
		t.Fatalf("the second request of the pin: %v, %v", p, err)
	}
	// The in-flight mark is the third record, past twice the one live pin: the journal is rewritten to that pin, in flight, with no expiry.
	if n := len(lines()); n != 1 {
		t.Fatalf("after the in-flight mark the journal holds %d records, want 1: it is past twice its live size and has been compacted", n)
	}
	var rec pinRecord
	if err := json.Unmarshal(lines()[0], &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Op != "pin" || rec.Workload != w || rec.MD != target.MD || rec.ID != target.ID || !rec.Expiry.IsZero() || rec.TTL.D() != 12*time.Hour {
		t.Errorf("the compacted journal's one record is %+v, want the pin at its TTL of 12h, with no expiry: its request is in flight", rec)
	}
	s.finish(w, key, p)
	if n := len(lines()); n != 2 {
		t.Fatalf("after the request ends the journal holds %d records, want 2: the pin and its expiry", n)
	}
}

// pinUnit is a store over a journal in a temporary directory, on a clock the test moves, with every written record kept.
type pinUnit struct {
	t       *testing.T
	now     time.Time
	path    string
	s       *pinStore
	written []pinRecord
}

const (
	unitW = "00000000-0000-4000-8000-000000000001"
	unitK = "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"
)

func newPinUnit(t *testing.T) *pinUnit {
	t.Helper()
	u := &pinUnit{t: t, now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC), path: filepath.Join(t.TempDir(), pinJournalFile)}
	u.s = newPinStore(u.path, func() time.Time { return u.now }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	u.s.appended = func(r pinRecord) { u.written = append(u.written, r) }
	t.Cleanup(u.s.close)
	return u
}

var unitTarget = pinTarget{MD: "00000000-0000-4000-8000-000000000002", ID: "9f2b6e5d1a3c4b7e8f0a1d2c3b4a59687766554433221100ffeeddccbbaa9988"}

// exts is how many expiry records the store has written for the key.
func (u *pinUnit) exts(key string) int {
	n := 0
	for _, r := range u.written {
		if r.Op == "ext" && r.Key == key {
			n++
		}
	}
	return n
}

// A compaction marks a pin journalled only once its write has succeeded (Lifetime, The journal). One pin is made and its first request ends: its expiry is
// journalled, at once. The journal's path then becomes a directory, which a rename over it cannot replace, and a request 62 s later marks the pin in flight
// again: that record is the third, past twice the one live pin, and the compaction that follows fails. The request's end must still journal the pin's second
// expiry, a minute and more after the first. A store that marked the pin journalled before the rewrite had failed would count the failed rewrite as a
// record, and skip it.
func TestPinJournalCompactionThatFailsMarksNoPinJournalled(t *testing.T) {
	u := newPinUnit(t)
	p, err := u.s.create(unitW, unitK, unitTarget, 12*time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	u.s.finish(unitW, unitK, p)
	if n := u.exts(unitK); n != 1 {
		t.Fatalf("after the first request %d expiries were journalled, want 1", n)
	}
	if err := os.Rename(u.path, u.path+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(u.path, 0o700); err != nil {
		t.Fatal(err)
	}
	u.now = u.now.Add(62 * time.Second)
	if p, err = u.s.arrive(unitW, unitK); err != nil || p == nil {
		t.Fatalf("the second request: %v, %v", p, err)
	}
	u.s.finish(unitW, unitK, p)
	if n := u.exts(unitK); n != 2 {
		t.Errorf("after a request that ended 62 s after the first expiry, %d expiries were journalled, want 2: a failed rewrite is not a record of the pin", n)
	}
}

// A compaction whose directory sync fails leaves the store appending nothing until a sync succeeds (The journal). The pin is made, ended and marked in flight
// again 61 s later; that third record makes the compaction, whose rename succeeds and whose directory sync is made to fail. A first request of another
// conversation is then refused, since its pin could be lost with the rename; with the sync working again it is written.
func TestPinJournalAppendsNothingWhileItsDirectorySyncFails(t *testing.T) {
	u := newPinUnit(t)
	p, err := u.s.create(unitW, unitK, unitTarget, 12*time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	u.s.finish(unitW, unitK, p)
	failing := true
	u.s.syncDirFn = func(dir string) error {
		if failing {
			return errors.New("input/output error")
		}
		return syncDir(dir)
	}
	u.now = u.now.Add(61 * time.Second)
	if _, err := u.s.arrive(unitW, unitK); err != nil {
		t.Fatalf("the in-flight mark that triggers the compaction: %v", err)
	}
	const other = "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"
	if _, err := u.s.create(unitW, other, unitTarget, 12*time.Hour, 10); err == nil {
		t.Fatal("a pin was written after a compaction whose directory sync failed")
	}
	failing = false
	if _, err := u.s.create(unitW, other, unitTarget, 12*time.Hour, 10); err != nil {
		t.Fatalf("with the directory syncing again the pin was refused: %v", err)
	}
}

// The pin filter of a conversation pinned to a ModelDeployment compares the FULL deployment ID (The pin). The pinned ID and another of the same ModelDeployment
// share their first 12 hex characters. An Instance of the pinned ID passes; so does none of another ID that the plan knows; and an Instance that joined after the
// plan's view, which only the display form of its ID can say anything of, is left for the next attempt and not passed on a prefix.
func TestPinnedFilterComparesTheFullDeploymentIDAndNotItsPrefix(t *testing.T) {
	const pinned = "9f2b6e5d1a3c0000000000000000000000000000000000000000000000000001"
	const sibling = "9f2b6e5d1a3c0000000000000000000000000000000000000000000000000002"
	d := &inventory.Deployment{UID: "00000000-0000-4000-8000-000000000002", Members: []*inventory.Instance{
		{Key: "ns/a-0", DeploymentID: pinned}, {Key: "ns/a-1", DeploymentID: sibling}}}
	p := &plan{n: naming.Name{Model: "m", Level: naming.LevelModel}, serving: []*inventory.Deployment{d}, pinID: pinned}
	match := p.match()
	attrs := func(id string) balancer.Attrs { return balancer.Attrs{Provider: "helios-a", DisplayID: id[:12]} }
	if !match("ns/a-0", attrs(pinned)) {
		t.Error("an Instance of the pinned ID is refused")
	}
	if match("ns/a-1", attrs(sibling)) {
		t.Error("an Instance of another ID of the same ModelDeployment passes")
	}
	if match("ns/a-2", attrs(sibling)) {
		t.Error("an Instance that joined since, whose display ID is the pinned one's, passes on its 12-character prefix")
	}
}
