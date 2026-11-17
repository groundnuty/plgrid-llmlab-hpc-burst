package scenario_test

import (
	"testing"
	"time"
)

// TestJudgeS4 holds the verdict to S4 as revised (#448, comment 6828152670; #701) with figures typed
// from the rulings and from the runs on record. The rule: valid at a control gap p99 of 85 ms or more;
// then a pass at a bulk-on p99 of 75 ms or less and at least 30% below its control (bulk <= 70% of the
// control), a fail otherwise. The 30% is relative since `@hpc-burst-science-agent`'s ruling on #701
// (17 Nov 2026), in place of a fixed 30 ms. It needs no stack, so CI's go job runs it.
//
// Every figure is a literal, and every verdict is written by hand from the rule, never computed by the
// code under test: a row is the run's own p99s (control, bulk), parsed exactly. The runs on record are
// #448's P7 third hosted run (run 43245451281) and its final one (run 43253969665), both in #480;
// #563's three hosted runs of rehearsal-bulk; and #701's, the rehearsal-bulk run of a docs-only PR
// (run 44086778798), whose host-dialed mode missed the old fixed margin by 0.56 ms.
func TestJudgeS4(t *testing.T) {
	d := func(t *testing.T, s string) time.Duration {
		t.Helper()
		v, err := time.ParseDuration(s)
		if err != nil {
			t.Fatalf("the fixture's figure %q: %v", s, err)
		}
		return v
	}
	for _, c := range []struct {
		name          string
		control, bulk string
		want          s4Verdict
		msg           string // the whole verdict text, "" for a row that does not hold it
	}{
		// The rule's boundaries.
		{"a control of exactly 85 ms is valid", "85ms", "50ms", s4Pass, ""},
		{"a control of 84.9 ms is invalid", "84.9ms", "50ms", s4Invalid,
			"gap p99 84.9ms without the bulk path, 50ms with it, 34.9ms apart: the control is under 85ms, so this run can't show a stall to remove"},
		{"an invalid run is never a pass, however low the bulk p99", "84.9ms", "10ms", s4Invalid, ""},
		{"a bulk p99 of exactly 75 ms passes (a 37.5% cut)", "120ms", "75ms", s4Pass, ""},
		{"a bulk p99 of 76 ms fails (a 36.7% cut)", "120ms", "76ms", s4Fail,
			"gap p99 120ms without the bulk path, 76ms with it, 44ms apart: the bulk path's is above 75ms"},
		{"a bulk p99 above 75 ms fails even with a big cut (62%)", "200ms", "76ms", s4Fail, ""},
		{"exactly 30% below the control passes", "100ms", "70ms", s4Pass, ""},
		{"exactly 30% below at the lowest valid control passes (85 ms, 59.5 ms: 25.5 ms apart)", "85ms", "59.5ms", s4Pass, ""},
		{"30.1% below the control passes", "100ms", "69.9ms", s4Pass, ""},
		{"29.9% below the control fails, naming both figures and the percentage", "100ms", "70.1ms", s4Fail,
			"gap p99 100ms without the bulk path, 70.1ms with it, 29.9ms apart: the bulk path's is 29.9% below the control's, under the 30% it must be (at most 70ms)"},
		{"one nanosecond short of 30% fails, and its percentage never prints as 30.0", "100ms", "70.000001ms", s4Fail,
			"gap p99 100ms without the bulk path, 70.000001ms with it, 29.999999ms apart: the bulk path's is 29.9% below the control's, under the 30% it must be (at most 70ms)"},
		{"30.7% below a control of 88 ms passes, 27 ms apart (looser than the old 30 ms near 85 ms)", "88ms", "61ms", s4Pass, ""},
		{"29.5% below a control of 88 ms fails, 26 ms apart", "88ms", "62ms", s4Fail, ""},
		{"30 ms apart at a control of 105 ms is a 28.6% cut and fails (stricter than the old 30 ms above 100 ms)", "105ms", "75ms", s4Fail, ""},

		// The runs on record. Each is typed from where it is recorded; the verdict is the rule's.
		// #448's P7, the third hosted run (run 43245451281), four configurations: the ruling on
		// comment 6828152670 judges all four valid passes.
		{"#448 run 43245451281, runner 2, direct", "103.97418ms", "52.02896ms", s4Pass, ""},
		{"#448 run 43245451281, runner 2, host-dialed", "126.889577ms", "53.602921ms", s4Pass, ""},
		{"#448 run 43245451281, runner 1, direct", "109.269445ms", "53.188275ms", s4Pass, ""},
		{"#448 run 43245451281, runner 1, host-dialed", "90.30946ms", "52.616512ms", s4Pass, ""},
		// #480's final hosted run (run 43253969665), four configurations, figures to 0.1 ms as #480's
		// body records them; every control is above 85 ms and every cut is over 44%.
		{"#480 run 43253969665, runner 1, direct", "113.5ms", "54.3ms", s4Pass, ""},
		{"#480 run 43253969665, runner 1, host-dialed", "136.3ms", "54.4ms", s4Pass, ""},
		{"#480 run 43253969665, runner 2, direct", "119.7ms", "52.4ms", s4Pass, ""},
		{"#480 run 43253969665, runner 2, host-dialed", "96.0ms", "53.6ms", s4Pass, ""},
		// #563's three hosted runs of rehearsal-bulk (its body and review record).
		{"#563 run 43567374670, direct: a control of 79.45 ms is invalid", "79.45ms", "52.23ms", s4Invalid, ""},
		{"#563 run 43567374670, host-dialed", "102.35ms", "57.27ms", s4Pass, ""},
		{"#563 run 43569349982, direct", "109.10ms", "52.00ms", s4Pass, ""},
		{"#563 run 43569349982, host-dialed", "113.71ms", "53.48ms", s4Pass, ""},
		{"#563 run 43571487873, direct: a control of 70.98 ms is invalid", "70.98ms", "52.78ms", s4Invalid, ""},
		{"#563 run 43571487873, host-dialed", "122.57ms", "57.77ms", s4Pass, ""},
		// #701's run (docs-only PR #679's rehearsal-bulk, run 44086778798). Host-dialed: a 33.1% cut,
		// which the old fixed margin failed at 29.44 ms apart and the relative margin passes. Direct:
		// a control of 74.5 ms is invalid. The last row is the same host-dialed run as #701's text
		// gives it, to 0.1 ms.
		{"#701 run 44086778798, direct: a control of 74.5 ms is invalid", "74.519113ms", "54.191133ms", s4Invalid,
			"gap p99 74.519113ms without the bulk path, 54.191133ms with it, 20.32798ms apart: the control is under 85ms, so this run can't show a stall to remove"},
		{"#701 run 44086778798, host-dialed: a 33.1% cut passes", "88.965084ms", "59.523306ms", s4Pass,
			"gap p99 88.965084ms without the bulk path, 59.523306ms with it, 29.441778ms apart"},
		{"#701's text: host-dialed 89.0 ms with 59.5 ms, a 33% cut, passes", "89.0ms", "59.5ms", s4Pass, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, why := judgeS4(d(t, c.control), d(t, c.bulk))
			if got != c.want {
				t.Errorf("judgeS4(%s, %s) = %d (%s), want %d", c.control, c.bulk, got, why, c.want)
			}
			if c.msg != "" && why != c.msg {
				t.Errorf("judgeS4(%s, %s) says %q, want %q", c.control, c.bulk, why, c.msg)
			}
		})
	}
}
