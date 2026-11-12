package alpha

// The lead of a handover row's streams (issue #447): how long before the drain start they open, sized to what the
// deployment's max-model-len lets one request hold at the engine's measured rate. It is the science agent's ruling (a)
// on #447, 12 Nov 2026: lead = min(MaxRequestDuration/3, (max-model-len - PromptReserve) / rate / 2), and a row whose
// fitting lead is under LeadFloor is NOT RUN, never a weaker pass. It serves every row that opens a stream across a drain
// start: alpha-athena-handover, alpha-helios-nooverlap and alpha-athena-now-nooverlap (the live steps' streamLead).

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// LeadFloor is the shortest fitting lead a row runs with. A stream opens one lead before the drain start and lasts two,
// so a lead under the floor leaves a stream that only just straddles the drain start: its end could be told from a cut by
// less than the floor's seconds, and a row that ran on that would be a weaker pass. The floor applies to the lead that
// fits the deployment's context (the second term of the minimum), not to MaxRequestDuration/3: a deadline short enough
// to make that one under the floor is the row's own timing, as it was before the lead was sized.
const LeadFloor = 30 * time.Second

// PromptReserve is how many tokens of a deployment's max-model-len a stream leaves for its prompt (tokensFor's margin).
const PromptReserve = 512

// ErrLeadTooShort is what a handover row's scenario returns when the lead that fits the deployment's max-model-len at
// the engine's measured rate is under LeadFloor: no stream long enough to tell a cut from an end can cross the drain
// start. The row was never put to the test, so the run is NOT RUN (NotRun), exit 3, and nothing past the stack, the
// first Instance ready and the rate stream was driven.
var ErrLeadTooShort = errors.New("no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate")

// leadTooShortWords say what happened, in the marker, which hack/live-checks.sh reads for the three rows that have it.
// The marker adds the rate and the context in parentheses (LeadTooShortError.counts), numbers only. hack/live-checks.sh's
// lead_marker greps for "long enough to tell a cut from an end"; keep that phrase here.
const leadTooShortWords = "no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate"

// LeadTooShortError is ErrLeadTooShort with the rate and the context it was found at, so that the error, the marker and
// the script's line name both. Fit is the lead that fits, in whole seconds, under LeadFloor.
type LeadTooShortError struct {
	Rate        float64
	MaxModelLen int
	Fit         time.Duration
}

// counts is the numbers, as the marker and the error carry them: the rate, the context, the lead that fits and the floor.
func (e *LeadTooShortError) counts() string {
	return fmt.Sprintf("at %.0f tokens/s with max-model-len %d the lead that fits is %s, under the floor of %s", e.Rate, e.MaxModelLen, e.Fit, LeadFloor)
}

func (e *LeadTooShortError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrLeadTooShort, e.counts())
}

// Is makes the error ErrLeadTooShort.
func (e *LeadTooShortError) Is(target error) bool { return target == ErrLeadTooShort }

// FitLead is the lead of the streams that cross a drain start: min(maxRequest/3, fit), where fit is the lead at which a
// stream of two leads, at rate tokens a second, fills the max-model-len less PromptReserve:
// (maxModelLen - PromptReserve) / rate / 2, rounded DOWN to a whole second. Rounding down only shortens the stream, so
// the stream still fits (a stream of 2*fit seconds holds at most the room), and the lead is a number a person can read:
// at 199 tokens a second with a context of 32768 it is 81 s (81.04 before the rounding).
//
// A fit under LeadFloor is a *LeadTooShortError (ErrLeadTooShort). A rate that is not a positive finite number, or a
// request deadline that is not positive, is a plain error: the caller measured nothing a lead can be sized from.
func FitLead(maxRequest time.Duration, maxModelLen int, rate float64) (time.Duration, error) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return 0, fmt.Errorf("a lead cannot be sized from a measured rate of %v tokens/s", rate)
	}
	if maxRequest <= 0 {
		return 0, fmt.Errorf("a lead cannot be sized from a request deadline of %s", maxRequest)
	}
	room := max(maxModelLen-PromptReserve, 0)
	secs := float64(room) / rate / 2
	if secs < LeadFloor.Seconds() {
		// Under the floor, so no overflow in the conversion.
		return 0, &LeadTooShortError{Rate: rate, MaxModelLen: maxModelLen, Fit: wholeSeconds(secs)}
	}
	// Past the deadline's third the fit does not bind. Capping the seconds first keeps a very slow rate from overflowing the Duration.
	fit := wholeSeconds(min(secs, maxRequest.Seconds()))
	return min(maxRequest/3, fit), nil
}

// wholeSeconds is secs, which is not negative, as a Duration of whole seconds, rounded down: Go's conversion of a float to an
// integer drops the fraction.
func wholeSeconds(secs float64) time.Duration { return time.Duration(secs) * time.Second }

// LeadTimes are the instants a lead sets, as offsets from the drain start (negative: before it).
type LeadTimes struct {
	// SuccessorWaitEnds is when the handover row stops waiting for its replacement to be ready before it opens its streams:
	// half a lead before they open, so that the rest of the replacement's time runs with the streams in flight.
	SuccessorWaitEnds time.Duration
	// StreamsOpen is when the streams open: a lead before the drain start (untilBefore).
	StreamsOpen time.Duration
	// StreamsEnd is when a stream of two leads, sized at the measured rate, ends: a lead after the drain start.
	StreamsEnd time.Duration
}

// LeadTimesFor is the times a lead sets: the one place the handover steps read them from.
func LeadTimesFor(lead time.Duration) LeadTimes {
	return LeadTimes{SuccessorWaitEnds: -lead - lead/2, StreamsOpen: -lead, StreamsEnd: lead}
}
