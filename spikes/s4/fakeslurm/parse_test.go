package fakeslurm

import (
	"testing"
	"time"
)

// Expected values follow the sbatch and sacct man pages of Slurm 24.11.
func TestParseTimeLimit(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{"30", 30},
		{"10:30", 11}, // minutes:seconds, seconds round up
		{"01:30:00", 90},
		{"2:00:00", 120},
		{"00:00:30", 1},
		{"1-12", 2160},
		{"1-02:30", 1590},
		{"1-02:03:04", 1564},
		{"0", 0},
		{"UNLIMITED", 0},
	} {
		got, err := parseTimeLimit(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseTimeLimit(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"", "abc", "1-", "1:2:3:4", "-5", "x-1"} {
		if _, err := parseTimeLimit(bad); err == nil {
			t.Errorf("parseTimeLimit(%q) succeeded, want an error", bad)
		}
	}
}

func TestParseSlurmTime(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Time
	}{
		{"now", now},
		{"now-5", now.Add(-5 * time.Second)},
		{"now+30", now.Add(30 * time.Second)},
		{"now-10seconds", now.Add(-10 * time.Second)},
		{"now-1minute", now.Add(-time.Minute)},
		{"now-15minutes", now.Add(-15 * time.Minute)},
		{"now+2hours", now.Add(2 * time.Hour)},
		{"now-1day", now.Add(-24 * time.Hour)},
		{"now-2days", now.Add(-48 * time.Hour)},
		{"now-1week", now.Add(-7 * 24 * time.Hour)},
		{"now-2Weeks", now.Add(-14 * 24 * time.Hour)},
		{"2026-10-29", time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)},
		{"2026-10-29T13:14:15", time.Date(2026, 10, 29, 13, 14, 15, 0, time.UTC)},
	} {
		got, err := parseSlurmTime(tc.in, now, time.UTC)
		if err != nil || !got.Equal(tc.want) {
			t.Errorf("parseSlurmTime(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
	// absolute times are read in the given zone
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := parseSlurmTime("2026-10-29T00:00:00", now, ny)
	if want := time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("midnight New York = %v, want %v", got, want)
	}
	for _, bad := range []string{"", "yesterday", "now-3fortnights", "now-", "2026-13-01", "nowish"} {
		if _, err := parseSlurmTime(bad, now, time.UTC); err == nil {
			t.Errorf("parseSlurmTime(%q) succeeded, want an error", bad)
		}
	}
}
