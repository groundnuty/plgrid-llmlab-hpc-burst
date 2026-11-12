package alpha

// The hour's long stream (issue #451; the science agent's ruling (b), 12 Nov 2026): the stream of check 2 of tunnel-hour-*
// ("a completed stream, within the deadline, as long as the context allows at the measured rate, capped at 4 min 30 s, and at
// least 2 minutes"). NearDeadline's 4 minutes, sized at 4 min 30 s, is 53,730 tokens at the A100's 199 tokens/s, over the
// 32,256 a context of 32768 leaves, so the stream is sized to the context at the rate measured before the hour. It shares
// FitLead's rounding (wholeSeconds) and its NOT RUN path (NotRun, NotRunNote, the script's marker).

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// LongCap is the longest the hour's long stream runs: halfway from NearDeadline to the request deadline (4 min 30 s at the
// live clock). It is the length the long stream had before it was sized to the context.
func (t Timing) LongCap() time.Duration {
	return t.NearDeadline + (t.MaxRequestDuration-t.NearDeadline)/2
}

// ErrLongTooShort is what tunnel-hour-*'s scenario returns when the stream that fits the deployment's max-model-len at the
// engine's measured rate is under the row's LongFloor: the hour cannot hold the long stream its check 2 asks for. Nothing was
// sent but the rate stream, so the run is NOT RUN (NotRun), exit 3, never a weaker pass.
var ErrLongTooShort = errors.New("the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate")

// longTooShortWords say what happened, in the marker, which hack/live-checks.sh reads for the tunnel rows. The marker adds the
// rate and the context in parentheses (LongTooShortError.counts), numbers only. The script's long_marker greps for "does not
// hold a long stream"; keep that phrase here.
const longTooShortWords = "the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate"

// LongTooShortError is ErrLongTooShort with the rate and the context it was found at. Fit is the stream that fits, in whole
// seconds, and Floor the row's LongFloor.
type LongTooShortError struct {
	Rate        float64
	MaxModelLen int
	Fit, Floor  time.Duration
}

// counts is the numbers, as the marker and the error carry them.
func (e *LongTooShortError) counts() string {
	return fmt.Sprintf("at %.0f tokens/s with max-model-len %d the stream that fits is %s, under the floor of %s", e.Rate, e.MaxModelLen, e.Fit, e.Floor)
}

func (e *LongTooShortError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrLongTooShort, e.counts())
}

// Is makes the error ErrLongTooShort.
func (e *LongTooShortError) Is(target error) bool { return target == ErrLongTooShort }

// FitLong is the length of the hour's long stream: as long as max-model-len less PromptReserve holds at rate tokens a second,
// rounded down to a whole second, and capped at the row's LongCap. A stream that fits for less than the row's LongFloor is a
// *LongTooShortError (ErrLongTooShort); at or over the cap it is the cap itself, to the nanosecond. A rate that is not a
// positive finite number is a plain error. The floor judges what fits, as FitLead's does.
func FitLong(tm Timing, maxModelLen int, rate float64) (time.Duration, error) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return 0, fmt.Errorf("a long stream cannot be sized from a measured rate of %v tokens/s", rate)
	}
	room := max(maxModelLen-PromptReserve, 0)
	secs := float64(room) / rate
	if secs < tm.LongFloor.Seconds() {
		return 0, &LongTooShortError{Rate: rate, MaxModelLen: maxModelLen, Fit: wholeSeconds(secs), Floor: tm.LongFloor}
	}
	if c := tm.LongCap(); secs >= c.Seconds() {
		return c, nil
	}
	return wholeSeconds(secs), nil
}

// LongPlan is the hour's long stream as the live row sized it: its length and its tokens (what the front is asked for), and the
// rate and the context it was sized at, which the text to post carries (issue #451: "computed once and posted with the result").
type LongPlan struct {
	Length      time.Duration `json:"length"`
	Tokens      int           `json:"tokens"`
	Rate        float64       `json:"rate"`
	MaxModelLen int           `json:"maxModelLen"`
}

// withLongPlan is the clock the hour's traffic is judged on: NearDeadline, the shortest completed stream check 2 accepts, is
// what the plan asks for. A stream planned at the cap must last NearDeadline, as it always had (4 minutes of 4 min 30 s); a
// shorter plan keeps that ratio (a stream sized for 2 min 42 s must last 2 min 24 s), the margin the sizing leaves for the rate
// the stream then runs at. A run with no plan (a dry run, or an observation that lacks it) is judged on NearDeadline, which is
// the strictest the check has.
func (t Timing) withLongPlan(p LongPlan) Timing {
	c := t.LongCap()
	if p.Length <= 0 || p.Length >= c {
		return t
	}
	// Seconds, not Durations: the product of two Durations overflows. Rounded to the millisecond.
	t.NearDeadline = time.Duration(math.Round(p.Length.Seconds()*t.NearDeadline.Seconds()/c.Seconds()*1000)) * time.Millisecond
	return t
}

// longStreamFailure is check 2's failure, in the verdict and the text to post.
const longStreamFailure = "the hour held no completed long stream, as long as the context allows at the measured rate, within the deadline"
