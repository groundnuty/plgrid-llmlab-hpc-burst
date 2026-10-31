package fakeslurm

// New. squeue's text output, which no plugin code reads: its times are local
// time in the platform's zone. The expected dates are worked out by hand.

import (
	"strings"
	"testing"
	"time"
)

// 2026-10-25 01:00 UTC is the end of daylight saving time in Europe/Warsaw
// (the last Sunday of October): 00:30 UTC is 02:30 CEST (UTC+2) and 01:30 UTC
// is 02:30 CET (UTC+1). The text output cannot tell the two apart; the epoch
// fields of the JSON can. No plugin code reads the text.
func TestSqueueTextLocalTimeAcrossDaylightSavingChange(t *testing.T) {
	r := newRig(t, Config{TimeZone: "Europe/Warsaw"})
	first := time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC)
	second := time.Date(2026, 10, 25, 1, 30, 0, 0, time.UTC)
	r.clock.Set(first)
	a := r.sbatch("--partition=p --job-name=first")
	r.clock.Set(second)
	b := r.sbatch("--partition=p --job-name=second")
	r.start(b)

	rows := strings.Split(strings.TrimSpace(r.must("squeue --me")), "\n")
	if len(rows) != 3 {
		t.Fatalf("text has %d lines, want a header and two jobs:\n%s", len(rows), strings.Join(rows, "\n"))
	}
	for _, col := range []string{"JOBID", "PARTITION", "NAME", "USER", "ST", "TIME", "NODES", "NODELIST(REASON)", "SUBMIT_TIME", "START_TIME"} {
		if !strings.Contains(rows[0], col) {
			t.Errorf("header %q lacks %s", rows[0], col)
		}
	}
	// Both jobs were submitted at the same local clock reading, an hour apart.
	for i, row := range rows[1:] {
		if !strings.Contains(row, "2026-10-25T02:30:00") {
			t.Errorf("row %d = %q, want the local submit time 2026-10-25T02:30:00", i, row)
		}
	}
	if !strings.Contains(rows[1], "PD") || !strings.Contains(rows[2], " R ") {
		t.Errorf("states in %q / %q, want PD then R", rows[1], rows[2])
	}
	if !strings.Contains(rows[1], "N/A") {
		t.Errorf("a pending job without an estimate shows no start: %q", rows[1])
	}
	subs := []float64{}
	for _, j := range jobsOf(t, r.squeue()) {
		subs = append(subs, num(t, j, "submit_time"))
	}
	if subs[1]-subs[0] != 3600 || subs[0] != float64(first.Unix()) {
		t.Errorf("submit_time epochs %v, want %d and an hour later", subs, first.Unix())
	}
	// Outside the change the zone shows plainly: noon UTC on 2026-10-30 is 13:00 CET.
	r.clock.Set(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	r.sbatch("--partition=p --job-name=noon")
	if out := r.must("squeue --me --jobs=" + itoa(b+1)); !strings.Contains(out, "2026-10-30T13:00:00") {
		t.Errorf("squeue text %q, want the local time 2026-10-30T13:00:00", out)
	}
	_ = a // job a (submitted before the change) is the pending row
}
