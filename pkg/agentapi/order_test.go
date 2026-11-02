package agentapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// The rule that keeps the newest report (HBEP-7, Report order). The cases are
// the ones a backend meets: the first report, a newer one, an older one and a
// repeat.
func TestReportIsNewerOnlyWhenItsCountIsAboveTheHeldOne(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seq, held uint64
		want      bool
	}{
		{"the first report, none held", 1, 0, true},
		{"the next report", 6, 5, true},
		{"a gap in the counts: reports superseded before they were sent", 9, 5, true},
		{"a repeat of the held report", 5, 5, false},
		{"an older report", 4, 5, false},
		{"a restarted instance counts from 1 against its own 0", 1, 0, true},
	} {
		if got := ReportIsNewer(tc.seq, tc.held); got != tc.want {
			t.Errorf("%s: ReportIsNewer(%d, %d) = %v, want %v", tc.name, tc.seq, tc.held, got, tc.want)
		}
	}
}

// A report with no count is refused, not dropped without a word.
func TestCheckSeqRefusesAReportWithNoCount(t *testing.T) {
	if err := CheckSeq(0); err == nil {
		t.Error("CheckSeq(0) accepted a report with no count")
	}
	for _, seq := range []uint64{1, 2, 1 << 40} {
		if err := CheckSeq(seq); err != nil {
			t.Errorf("CheckSeq(%d) = %v, want nil", seq, err)
		}
	}
}

// The field's name on the wire is "seq". Both sides marshal the same Go type,
// so a round trip through it would pass with any tag; this pins the name.
func TestTheReportCarriesItsCountAsSeqOnTheWire(t *testing.T) {
	b, err := json.Marshal(Report{Seq: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"seq":7`) {
		t.Errorf("a report counted 7 marshals as %s, want it to hold \"seq\":7", b)
	}
	var r Report
	if err := json.Unmarshal([]byte(`{"seq":12,"time":"2026-11-02T12:00:00Z"}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.Seq != 12 {
		t.Errorf("{\"seq\":12} decodes to Seq %d, want 12", r.Seq)
	}
}
