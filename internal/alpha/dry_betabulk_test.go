package alpha

// The bulk row (issue #521; betabulk.go) over the fake cluster: one job, run on the manual clock; what the stack would show is stood in for. Each
// fault departs from the healthy run in one way that must fail the row. A reading at or above the rule is no failure, a finding, and is a case of its
// own (TestABulkReadingAtOrAboveTheRuleIsAFindingNeverAFailure).

import (
	"context"
	"strings"
	"time"
)

// dryBulkPhase is a healthy phase: the 64 MiB delivered, eight streams done and outlasting the transfer, gaps enough, with the p99 given.
func dryBulkPhase(p99 time.Duration, bulkSent int) BulkPhase {
	took := 3 * time.Second
	return BulkPhase{ReplyBytes: BulkReplyBytes, Status: 200, Took: took, Gaps: minGaps(took) * 2, P50: 52 * time.Millisecond, P99: p99,
		StreamsDone: BulkStreams, StreamsLong: BulkStreams, BulkSent: bulkSent}
}

// BetaBulk is the bulk row over the fake cluster.
func (d *drySteps) BetaBulk(_ context.Context, p Plan) (BulkObs, error) {
	if d.fault("pend-past-bound") {
		return BulkObs{}, d.pendPastBound(p)
	}
	tm := p.Row.Clock()
	tag := d.tag(p, "bulk")
	n, id := d.submit(tag, d.partitionFor(true), d.cfg.Row.QOS)
	j := d.running(id)
	started := *j.StartTime
	d.extras(p, tag)
	d.tick(8 * time.Minute)
	ready := d.now()
	drain := started.Add(d.deployment().LeaseLimit - DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	obs := BulkObs{
		Deployment: d.deployment(), ReadyAt: ready, DrainStart: drain, Registered: 1,
		Control: dryBulkPhase(150*time.Millisecond, 0), Bulk: dryBulkPhase(55*time.Millisecond, 1),
		Double:    DoubleObs{Name: "the engine double (enginedouble, the stand-in's own build)", Digest: strings.Repeat("ab", 32)},
		Responses: 18, Attributed: 18,
	}
	switch {
	case d.fault("not-ready"):
		obs.ReadyAt = time.Time{}
	case d.fault("not-registered"):
		obs.Registered = 0
	case d.fault("front-without-bulk"):
		obs.Registered = -1
	case d.fault("control-took-bulk"):
		obs.Control.BulkSent = 1
	case d.fault("bulk-never-engaged"):
		obs.Bulk.BulkSent = 0
	case d.fault("bulk-twice"):
		obs.Bulk.BulkSent = 2
	case d.fault("counter-unread"):
		obs.Bulk.BulkSent = -1
	case d.fault("fallback"):
		obs.Bulk.FallbackSent = 1
	case d.fault("control-fallback"):
		obs.Control.FallbackSent = 1
	case d.fault("control-no-stall"):
		obs.Control.P99 = BulkRule - time.Nanosecond
	case d.fault("control-at-the-rule"):
		obs.Control.P99 = BulkRule // exactly the rule: the stall is there, the run is valid
	case d.fault("bulk-at-the-rule"):
		obs.Bulk.P99 = BulkRule // a finding, no failure
	case d.fault("bulk-above-the-rule"):
		obs.Bulk.P99 = 200 * time.Millisecond
	case d.fault("bulk-small-margin"):
		obs.Control.P99, obs.Bulk.P99 = 100*time.Millisecond, 80*time.Millisecond // under the rule, but 20 ms apart
	case d.fault("control-short-reply"):
		obs.Control.ReplyBytes = BulkReplyBytes - 1
	case d.fault("bulk-short-reply"):
		obs.Bulk.ReplyBytes = BulkReplyBytes - 1
	case d.fault("control-status"):
		obs.Control.Status = 502
	case d.fault("bulk-status"):
		obs.Bulk.Status = 500
	case d.fault("control-stream-cut"):
		obs.Control.StreamsDone = BulkStreams - 1
	case d.fault("bulk-stream-cut"):
		obs.Bulk.StreamsDone = BulkStreams - 1
	case d.fault("control-stream-short"):
		obs.Control.StreamsLong = BulkStreams - 1
	case d.fault("bulk-stream-short"):
		obs.Bulk.StreamsLong = BulkStreams - 1
	case d.fault("control-few-gaps"):
		obs.Control.Gaps = minGaps(obs.Control.Took) - 1
	case d.fault("bulk-few-gaps"):
		obs.Bulk.Gaps = minGaps(obs.Bulk.Took) - 1
	case d.fault("double-unnamed"):
		obs.Double.Name = " "
	case d.fault("double-digest-short"):
		obs.Double.Digest = strings.Repeat("ab", 31)
	case d.fault("double-digest-upper"):
		obs.Double.Digest = strings.Repeat("AB", 32)
	case d.fault("unattributed"):
		obs.Attributed = 17
	case d.fault("no-responses"):
		obs.Responses, obs.Attributed = 0, 0
	}
	d.tick(20 * time.Minute)
	d.tick(time.Minute)
	d.end(n, id)
	obs.Tags = append([]string(nil), d.tags...)
	return obs, nil
}

// betaBulkCases are the bulk row's dry cases. A control exactly at the rule is no failure.
var betaBulkCases = []dryCase{
	{"", ""},
	{"control-at-the-rule", ""},
	{"bulk-at-the-rule", ""},
	{"bulk-above-the-rule", ""},
	{"bulk-small-margin", ""},
	{"not-ready", "the Instance was not seen ready"},
	{"not-registered", "no Instance's bulk transport was registered at the front"},
	{"front-without-bulk", "no Instance's bulk transport was registered at the front"},
	{"control-took-bulk", "the control transfer took the bulk path: it is no control"},
	{"bulk-never-engaged", "the front's bulk counter did not rise by exactly one across the bulk reply: it did not take the bulk path"},
	{"bulk-twice", "the front's bulk counter did not rise by exactly one across the bulk reply: it did not take the bulk path"},
	{"counter-unread", "the front's bulk counter could not be read"},
	{"fallback", "a reply fell back to the token transport"},
	{"control-fallback", "a reply fell back to the token transport"},
	{"control-no-stall", "the control shows no stall: its gap p99 is under 85 ms"},
	{"control-short-reply", "the control transfer did not deliver its 67108864 bytes with a 200"},
	{"bulk-short-reply", "the bulk transfer did not deliver its 67108864 bytes with a 200"},
	{"control-status", "the control transfer did not deliver its 67108864 bytes with a 200"},
	{"bulk-status", "the bulk transfer did not deliver its 67108864 bytes with a 200"},
	{"control-stream-cut", "a stream beside the control transfer was cut or did not finish with its tokens"},
	{"bulk-stream-cut", "a stream beside the bulk transfer was cut or did not finish with its tokens"},
	{"control-stream-short", "a stream beside the control transfer ended before it did: its gaps do not cover the transfer"},
	{"bulk-stream-short", "a stream beside the bulk transfer ended before it did: its gaps do not cover the transfer"},
	{"control-few-gaps", "too few gaps overlap the control transfer to measure it"},
	{"bulk-few-gaps", "too few gaps overlap the bulk transfer to measure it"},
	{"double-unnamed", "the engine double was not named with the SHA-256 of its binary"},
	{"double-digest-short", "the engine double was not named with the SHA-256 of its binary"},
	{"double-digest-upper", "the engine double was not named with the SHA-256 of its binary"},
	{"unattributed", "a response lacked its attribution headers"},
	{"no-responses", "no response, so attribution is unchecked"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"duplicate-tag", "a tag names more than one job"},
	{"extra-allocation", "exactly one Allocation"},
}
