package front

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-8, Usage records, Rotation keeps every file, through the front: the
// records go to the file of the UTC day of the front's clock, and the counter
// runs across the files. The sink's own rotation tests are in
// internal/front/usage; these check that the front gives the sink its clock.
// File names are literals from the HBEP.

func recordsOf(t *testing.T, dir, name string) []usage.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	var out []usage.Record
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var r usage.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%s: %q: %v", name, line, err)
		}
		out = append(out, r)
	}
	return out
}

// TestRotationAcrossUTCMidnightThroughTheFront: requests before and after
// midnight UTC on the front's (fake) clock land in two files, each record in
// exactly one, with the counter continuous and the closed file untouched.
func TestRotationAcrossUTCMidnightThroughTheFront(t *testing.T) {
	f := newScFix(t, scOpts{})
	// The fake clock starts at 2026-10-31 12:00:00 UTC.
	var before []string
	for range 2 {
		resp, _ := f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
		before = append(before, resp.Header.Get("hpcburst-request-id"))
	}
	closed, err := os.ReadFile(filepath.Join(f.cfg.UsageDir, "usage-2026-10-31.jsonl"))
	if err != nil {
		t.Fatal(err)
	}

	f.clk.Advance(12*time.Hour + time.Second) // 2026-11-01 00:00:01 UTC
	var after []string
	for range 2 {
		resp, _ := f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
		after = append(after, resp.Header.Get("hpcburst-request-id"))
	}

	files, err := usage.Files(f.cfg.UsageDir)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"usage-2026-10-31.jsonl", "usage-2026-11-01.jsonl"}; !reflect.DeepEqual(files, want) {
		t.Fatalf("files = %v, want %v", files, want)
	}
	day1, day2 := recordsOf(t, f.cfg.UsageDir, files[0]), recordsOf(t, f.cfg.UsageDir, files[1])
	idsOf := func(rs []usage.Record) (out []string) {
		for _, r := range rs {
			out = append(out, r.RequestID)
		}
		return out
	}
	if got := idsOf(day1); !reflect.DeepEqual(got, before) {
		t.Errorf("the first day's file holds %v, want %v", got, before)
	}
	if got := idsOf(day2); !reflect.DeepEqual(got, after) {
		t.Errorf("the second day's file holds %v, want %v", got, after)
	}
	var counters []uint64
	for _, r := range append(day1, day2...) {
		counters = append(counters, r.Counter)
	}
	if want := []uint64{1, 2, 3, 4}; !reflect.DeepEqual(counters, want) {
		t.Errorf("counters across the files = %v, want %v", counters, want)
	}
	if now, _ := os.ReadFile(filepath.Join(f.cfg.UsageDir, "usage-2026-10-31.jsonl")); !bytes.Equal(now, closed) {
		t.Errorf("the closed file changed after midnight")
	}
	// The listing of every record, as the other tests read them.
	if n := len(f.records()); n != 4 {
		t.Errorf("%d records across the files, want 4", n)
	}
}

// TestRunNamesTheUsageFileByItsClock: Run gives the sink its clock, so on a
// fake clock at 2026-10-31 12:00 UTC the first record is in
// usage-2026-10-31.jsonl, not in a file named by the wall clock's day.
func TestRunNamesTheUsageFileByItsClock(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
	o := Options{
		Config:           Config{ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage")}.WithDefaults(),
		Clock:            clock.NewFake(t0),
		Dynamic:          fakeWatchClient(),
		Recorder:         &MemoryRecorder{},
		Log:              discardLog(),
		ClientListener:   kernelListener(t),
		DialHomeListener: kernelListener(t),
	}
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	var f *Front
	select {
	case f = <-ready:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the front did not start")
	}
	// A request with the wrong key is refused, and recorded.
	req, _ := http.NewRequest(http.MethodPost, "http://"+f.Client.String()+scChat, strings.NewReader(errChatBody(scModel, false, "hi")))
	req.Header.Set("Authorization", "Bearer not-the-key")
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, resp)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	files, err := usage.Files(o.Config.UsageDir)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"usage-2026-10-31.jsonl"}; !reflect.DeepEqual(files, want) {
		t.Errorf("files = %v, want %v", files, want)
	}
	if recs := recordsOf(t, o.Config.UsageDir, "usage-2026-10-31.jsonl"); len(recs) != 1 || recs[0].Status != 401 {
		t.Errorf("records = %+v, want the one 401", recs)
	}
}
