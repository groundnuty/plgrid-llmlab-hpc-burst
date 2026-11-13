package alpha

// beta-athena-now-bulk (issue #521; beta's tracker is #522): the bulk path's real-path buffer read from a loaded round trip, in host-dialed
// mode, on Athena's plgrid-now under profiles/athena-now.yaml (HBEP-6, The bulk path, S17's first condition and its Graduation criteria, Beta:
// "the real path's bottleneck buffer read from a loaded round trip, with open question 12's consequence applied if it drains in about 85 ms or
// more"). One job of 1 GPU, one worker, row A's hold of the slot, with `hpcburst up --bulk-path`.
//
// The reading is S17's (spikes/s17/README.md, the first condition): the delay a saturating download adds to the path's idle round trip is the
// buffer's drain time, and it is read here as what it does to the token streams, the gap between their tokens: eight streams of 20 tokens a
// second beside a 64 MiB reply, the p99 of the gaps that overlap the transfer. It is read twice in the one job, and the two are one pair:
//   - the control: the 64 MiB goes down the token transport, as a streamed reply (the front's request-time rule keeps a streamed request off the
//     bulk path), the bytes of the other beside the same streams, so the streams share the path with a saturating transfer;
//   - the bulk path: the same 64 MiB as a non-streamed reply, which the rule sends down the second frpc, beside eight streams of their own.
//
// The engine is the engine double, as ruled (the science agent, #521, answer 2): a real vLLM replies in a few KiB and cannot be held saturating
// the path. The post names the double and its digest, and says the engine was a double.
//
// What fails the row, as ruled: the bulk path never engaged (the front's bulk counter did not rise by exactly one across the bulk reply, or no
// Instance's bulk transport was registered), a fallback to the token transport, a stream cut or short, and a control without a stall (a control
// whose p99 is under the 85 ms below which S4 says the run can show no stall to remove). A p99 of the bulk path at or above 85 ms is a FINDING for
// HBEP-6's open question 12 and never a failure: the HBEP applies a consequence to the reading, it does not fail a build on it.

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// BetaBulkName is the bulk row's name in the script.
const BetaBulkName = "beta-athena-now-bulk"

// The measurement, as S17 ran it (HBEP-6, The bulk path, S17's figures) and the rulings fix it.
const (
	// BulkStreams token streams of BulkStreamTokens tokens each at BulkTokenDelay a token (20 a second): 30 s each, well past the transfer.
	BulkStreams      = 8
	BulkStreamTokens = 600
	BulkTokenDelay   = 50 * time.Millisecond
	// BulkReplyBytes is the transfer, a reply of 64 MiB.
	BulkReplyBytes = 64 << 20
	// BulkRule is HBEP-6's 85 ms: a layout that does not protect the token streams past it is no better than one frpc, and S4 asks the control
	// for at least this much stall to be a run at all. BulkMargin is the 30 ms S4 asks between the control's p99 and the bulk path's.
	BulkRule   = 85 * time.Millisecond
	BulkMargin = 30 * time.Millisecond
)

// BetaBulkRow is the bulk row: row A's hold of plgrid-now's one-job slot (one job, one GPU, a start wait of 10 minutes, a lease of 48 and two
// minutes of cleanup, the owner's go of plgrid-now), under athena-now.yaml, posting to the beta tracker.
func BetaBulkRow() Row {
	r := NowServeRow()
	r.Name, r.PostsTo = BetaBulkName, BetaTracker
	return r
}

// BulkPhase is one of the two measurements: the streams opened, one transfer beside them, the gaps of the streams that overlap it.
type BulkPhase struct {
	// ReplyBytes is how many bytes the transfer's reply delivered, Status its status, and Took the time from the request's send to its last byte.
	ReplyBytes int64         `json:"replyBytes"`
	Status     int           `json:"status"`
	Took       time.Duration `json:"took"`
	// Gaps is how many inter-token gaps overlap the transfer's window, across the streams; P50 and P99 are theirs.
	Gaps int           `json:"gaps"`
	P50  time.Duration `json:"p50"`
	P99  time.Duration `json:"p99"`
	// StreamsDone counts the streams that ended complete with their tokens, StreamsLong those among them that were still delivering tokens
	// after the transfer's last byte: a stream that ended before the transfer did measured nothing of it.
	StreamsDone int `json:"streamsDone"`
	StreamsLong int `json:"streamsLong"`
	// BulkSent and FallbackSent are how much the front's bulk counter rose, by path, across the transfer; -1 when the metrics could not be read.
	BulkSent     int `json:"bulkSent"`
	FallbackSent int `json:"fallbackSent"`
}

// BulkObs is a run of the bulk row.
type BulkObs struct {
	Tags       []string       `json:"tags"`
	Deployment DeploymentSpec `json:"deployment"`
	// ReadyAt is when the Instance became ready, DrainStart its drain start from its recorded lease end.
	ReadyAt    time.Time `json:"readyAt"`
	DrainStart time.Time `json:"drainStart"`
	// Registered is the front's hpcburst_front_bulk_registered before the phases: how many Instances have a registered bulk transport, -1 when
	// the series is absent (a front without the bulk path).
	Registered int `json:"registered"`
	// Control and Bulk are the two phases.
	Control BulkPhase `json:"control"`
	Bulk    BulkPhase `json:"bulk"`
	// Double names the engine double and its digest: the post says the engine was a double, and which.
	Double DoubleObs `json:"double"`
	// Responses counts the responses the run received through the front, Attributed those with HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
}

// DoubleObs names the engine double a row ran: what it is called in the post and the SHA-256 of its binary, 64 lowercase hex characters.
type DoubleObs struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

var sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// GapsOverlapping returns the inter-token gaps whose interval overlaps [start, end]: a stream stalled by the transfer gets its next token after
// it ends, and that gap is the stall.
func GapsOverlapping(ticks []time.Time, start, end time.Time) []time.Duration {
	var out []time.Duration
	for i := 1; i < len(ticks); i++ {
		if ticks[i].After(start) && ticks[i-1].Before(end) {
			out = append(out, ticks[i].Sub(ticks[i-1]))
		}
	}
	return out
}

// Percentile is the nearest-rank percentile p (0 to 1) of d, zero for none.
func Percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 || !(p > 0) || p > 1 {
		return 0
	}
	s := slices.Clone(d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(math.Ceil(p*float64(len(s))))-1]
}

// StreamBeside judges one token stream that ran beside a transfer: done says it ended complete, not cut, with exactly the BulkStreamTokens it asked
// for, and long that it was still delivering tokens after the transfer's last byte (end), since a stream that ended before the transfer did measured
// nothing of it. long needs done: a stream that was cut is no longer one.
func StreamBeside(s Stream, ticks []time.Time, end time.Time) (done, long bool) {
	done = s.Complete && s.CutBy == "" && s.Tokens == BulkStreamTokens
	long = done && len(ticks) > 0 && ticks[len(ticks)-1].After(end)
	return done, long
}

// minGaps is the fewest gaps a transfer of took must have overlapped for the phase to have measured it: half of what BulkStreams streams at
// BulkTokenDelay would give across the window.
func minGaps(took time.Duration) int { return BulkStreams * int(took/BulkTokenDelay) / 2 }

// phaseChecks: a phase's transfer delivered its bytes with a 200, the streams all finished complete and outlasted it, and enough gaps overlap it.
func phaseChecks(what string, p BulkPhase) []string {
	var f []string
	if p.Status != 200 || p.ReplyBytes < BulkReplyBytes {
		f = append(f, fmt.Sprintf("the %s transfer did not deliver its %d bytes with a 200", what, BulkReplyBytes))
	}
	if p.StreamsDone != BulkStreams {
		f = append(f, fmt.Sprintf("a stream beside the %s transfer was cut or did not finish with its tokens", what))
	} else if p.StreamsLong != BulkStreams {
		f = append(f, fmt.Sprintf("a stream beside the %s transfer ended before it did: its gaps do not cover the transfer", what))
	}
	if p.Took <= 0 || p.Gaps < minGaps(p.Took) {
		f = append(f, fmt.Sprintf("too few gaps overlap the %s transfer to measure it", what))
	}
	return f
}

// pathChecks: the control took the token transport and the bulk reply the bulk path, with no fallback, and an Instance's bulk transport was
// registered when they ran.
func pathChecks(o BulkObs) []string {
	var f []string
	if o.Registered < 1 {
		f = append(f, "no Instance's bulk transport was registered at the front")
	}
	if o.Control.BulkSent != 0 {
		f = append(f, "the control transfer took the bulk path: it is no control")
	}
	switch {
	case o.Bulk.BulkSent < 0 || o.Control.BulkSent < 0:
		f = append(f, "the front's bulk counter could not be read")
	case o.Bulk.BulkSent != 1:
		f = append(f, "the front's bulk counter did not rise by exactly one across the bulk reply: it did not take the bulk path")
	}
	if o.Bulk.FallbackSent != 0 || o.Control.FallbackSent != 0 {
		f = append(f, "a reply fell back to the token transport")
	}
	return f
}

// stallChecks: the control stalled the streams to a p99 of at least the rule, so that the run can show a stall to remove (S4's validity).
func stallChecks(o BulkObs) []string {
	if o.Control.P99 < BulkRule {
		return []string{"the control shows no stall: its gap p99 is under 85 ms, so this run cannot show a stall for the bulk path to remove"}
	}
	return nil
}

// doubleChecks: the engine double is named with its digest.
func doubleChecks(d DoubleObs) []string {
	if strings.TrimSpace(d.Name) == "" || !sha256Re.MatchString(d.Digest) {
		return []string{"the engine double was not named with the SHA-256 of its binary"}
	}
	return nil
}

// Finding is HBEP-6's open question 12's consequence, applied to the reading and never to the verdict: the bulk path's p99 is at or above the
// rule, so the layout is no better than one frpc on this path, or it is under the rule but less than the margin below the control. It names which.
func (o BulkObs) Finding() string {
	switch {
	case o.Bulk.P99 >= BulkRule:
		return fmt.Sprintf("the bulk path's gap p99 is %s, at or above the %s rule: on this path the layout is no better than one frpc, and no remedy is measured (HBEP-6, open question 12)", o.Bulk.P99, BulkRule)
	case o.Control.P99-o.Bulk.P99 < BulkMargin:
		return fmt.Sprintf("the bulk path's gap p99 is %s, under the rule but only %s below the control's %s, under the %s margin S4 asks (HBEP-6, open question 12)", o.Bulk.P99, o.Control.P99-o.Bulk.P99, o.Control.P99, BulkMargin)
	}
	return ""
}

// VerifyBetaBulk judges the bulk row. A finding is no failure.
//
//	hbep/0006-instance-reachability/README.md:146 and :711: the real path's buffer read from a loaded round trip.
func VerifyBetaBulk(row Row, o BulkObs, jobs []provider.Job) []string {
	f := deploymentChecks(row, o.Deployment)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, tagCountChecks(row, o.Tags, "the row makes exactly one Allocation")...)
	if o.ReadyAt.IsZero() {
		f = append(f, "the Instance was not seen ready")
	}
	f = append(f, pathChecks(o)...)
	f = append(f, phaseChecks("control", o.Control)...)
	f = append(f, phaseChecks("bulk", o.Bulk)...)
	f = append(f, stallChecks(o)...)
	f = append(f, doubleChecks(o.Double)...)
	if o.Responses <= 0 {
		f = append(f, "the run recorded no response, so attribution is unchecked")
	} else if o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	return dedupe(f)
}

// betaBulkPost is the bulk row's text to post: the reading, the finding if there is one, and the engine double and its digest. No job ID, no host,
// no path.
func betaBulkPost(row Row, o BulkObs, failures []string) string {
	var b strings.Builder
	mbps := func(p BulkPhase) float64 {
		if p.Took <= 0 {
			return 0
		}
		return float64(p.ReplyBytes) / p.Took.Seconds() / 1e6
	}
	fmt.Fprintf(&b, "%s\n%s\n- THE ENGINE WAS A DOUBLE, not vLLM: %s, SHA-256 %s; a real vLLM replies in a few KiB and cannot be held saturating the path (the science agent's ruling on #521)\n",
		verdictLine(row, failures), deploymentLine(o.Deployment), o.Double.Name, o.Double.Digest)
	fmt.Fprintf(&b, "- the reading: %d token streams of 20 tokens/s beside a transfer of %d bytes; the streams' inter-token gap, p99 over the gaps that overlap the transfer\n", BulkStreams, BulkReplyBytes)
	fmt.Fprintf(&b, "  - the control, the transfer down the token transport as a streamed reply: gap p99 %s (p50 %s) over %d gaps; the transfer took %s (%.1f MB/s)\n",
		o.Control.P99, o.Control.P50, o.Control.Gaps, o.Control.Took.Round(time.Millisecond), mbps(o.Control))
	fmt.Fprintf(&b, "  - the bulk path, the same bytes as a non-streamed reply: gap p99 %s (p50 %s) over %d gaps; the transfer took %s (%.1f MB/s); the front's bulk counter rose by %d, its fallback by %d\n",
		o.Bulk.P99, o.Bulk.P50, o.Bulk.Gaps, o.Bulk.Took.Round(time.Millisecond), mbps(o.Bulk), o.Bulk.BulkSent, o.Bulk.FallbackSent)
	fmt.Fprintf(&b, "  - the difference, control less bulk path: %s; HBEP-6's rule is %s and S4's margin %s\n", o.Control.P99-o.Bulk.P99, BulkRule, BulkMargin)
	if f := o.Finding(); f != "" {
		fmt.Fprintf(&b, "- FINDING (not a failure): %s\n", f)
	} else {
		fmt.Fprintf(&b, "- the layout protects the token streams on this path: the bulk path's p99 is under %s and at least %s below the control's\n", BulkRule, BulkMargin)
	}
	fmt.Fprintf(&b, "- responses with attribution: %d of %d\n", o.Attributed, o.Responses)
	return b.String()
}
