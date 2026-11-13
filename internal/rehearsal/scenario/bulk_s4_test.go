package scenario_test

import (
	"testing"
	"time"
)

// TestJudgeS4 holds the verdict to S4 as revised (#448, comment 6828152670) with figures typed from
// the ruling: valid at a control of 85 ms or more; a pass at a bulk p99 of 75 ms or less and at least
// 30 ms below its control. The last four rows are the third hosted run's figures (run 43245451281),
// which the ruling judges four valid passes. It needs no stack, so CI's go job runs it.
func TestJudgeS4(t *testing.T) {
	ms := func(f float64) time.Duration { return time.Duration(f * float64(time.Millisecond)) }
	for _, c := range []struct {
		name          string
		control, bulk time.Duration
		want          s4Verdict
	}{
		{"a control of exactly 85 ms is valid", ms(85), ms(50), s4Pass},
		{"a control of 84.9 ms is invalid", ms(84.9), ms(50), s4Invalid},
		{"an invalid run is never a pass, however low the bulk p99", ms(84.9), ms(10), s4Invalid},
		{"a bulk p99 of exactly 75 ms passes", ms(120), ms(75), s4Pass},
		{"a bulk p99 of 76 ms fails", ms(120), ms(76), s4Fail},
		{"31 ms below the control passes", ms(100), ms(69), s4Pass},
		{"exactly 30 ms below passes", ms(100), ms(70), s4Pass},
		{"29 ms below the control fails", ms(100), ms(71), s4Fail},
		{"run 3, runner 2, direct", ms(103.97418), ms(52.02896), s4Pass},
		{"run 3, runner 2, host-dialed", ms(126.889577), ms(53.602921), s4Pass},
		{"run 3, runner 1, direct", ms(109.269445), ms(53.188275), s4Pass},
		{"run 3, runner 1, host-dialed", ms(90.30946), ms(52.616512), s4Pass},
	} {
		if got, why := judgeS4(c.control, c.bulk); got != c.want {
			t.Errorf("%s: judgeS4(%s, %s) = %d (%s), want %d", c.name, c.control, c.bulk, got, why, c.want)
		}
	}
}
