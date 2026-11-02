package agentapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// The rule that keeps the newest report (HBEP-7, Report order). The cases are
// the ones a backend meets: the first report, a newer one, an older one, a
// resend, and the unnumbered reports of an agent built before the count.
func TestKeepsReportOnlyWhenItsCountIsAboveTheHeldOneOrThereIsNone(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seq, held uint64
		want      bool
	}{
		{"the first report, none held", 1, 0, true},
		{"the next report", 6, 5, true},
		{"a gap in the counts: reports superseded before they were sent", 9, 5, true},
		{"a resend of the held report", 5, 5, false},
		{"an older report", 4, 5, false},
		{"a restarted instance counts from 1 against its own 0", 1, 0, true},
		{"an unnumbered report, none held", 0, 0, true},
		{"an unnumbered report, a count held: arrival order, as before", 0, 5, true},
	} {
		if got := KeepsReport(tc.seq, tc.held); got != tc.want {
			t.Errorf("%s: KeepsReport(%d, %d) = %v, want %v", tc.name, tc.seq, tc.held, got, tc.want)
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
