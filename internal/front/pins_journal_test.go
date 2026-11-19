package front

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The pin journal's compaction past twice its live size (HBEP-5, Sticky routes, The journal), on the store alone with a clock the test moves. One pin is
// made: the journal holds 1 record. Extensions are journalled at most once a minute: one at 61 s makes 2 records, which is twice the live size and no more,
// so nothing is compacted; the next at 122 s makes 3, past twice 1, and the journal is rewritten to the one live pin, which now expires at the time of that
// last request plus its TTL. The rows that count records elsewhere keep other pins live so that this rewrite does not take their extensions out of the count.
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
	s.finish(w, key, p)
	if n := len(lines()); n != 1 {
		t.Fatalf("after the pin the journal holds %d records, want 1", n)
	}
	for i, want := range []int{2, 1} {
		now = now.Add(61 * time.Second)
		if p, err = s.arrive(w, key); err != nil || p == nil {
			t.Fatalf("request %d of the pin: %v, %v", i+2, p, err)
		}
		s.finish(w, key, p)
		if n := len(lines()); n != want {
			t.Fatalf("after the extension at %d s the journal holds %d records, want %d", (i+1)*61, n, want)
		}
	}
	var rec pinRecord
	if err := json.Unmarshal(lines()[0], &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Op != "pin" || rec.Workload != w || rec.MD != target.MD || rec.ID != target.ID || !rec.Expiry.Equal(now.Add(12*time.Hour)) || rec.TTL.D() != 12*time.Hour {
		t.Errorf("the compacted journal's one record is %+v, want the pin at its TTL of 12h, expiring 12 h after its last request", rec)
	}
}
