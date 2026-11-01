package lifecycle

import (
	"strings"
	"testing"
)

// TestDegradedRankingPairs covers HBEP-7, Events and conditions, The ranking
// (science-agent decision, 1 Nov 2026): when several reasons apply, the
// condition carries the first in HBEP-7's order and its message lists every
// one. The order is written out here by hand from the HBEP's text, never read
// from degradedOrder. Every pair of the 12 reasons is presented, the lower one
// recorded first so that recording order cannot decide.
func TestDegradedRankingPairs(t *testing.T) {
	hbep7 := []string{
		"SpecUnsupported", "PartitionTooShort", "FrontNotConfigured", "VerificationFailed",
		"IdentityUnresolved", "RestartLoop", "SubmissionRejected", "NoOverlapSlot",
		"SubmitLimitReached", "ProviderUnavailable", "ReplacementLate", "CapacityDropped",
	}
	if len(hbep7) != 12 {
		t.Fatalf("%d reasons written out, HBEP-7 ranks 12", len(hbep7))
	}
	for i, hi := range hbep7 {
		for _, lo := range hbep7[i+1:] {
			p := &pass{}
			p.degrade(lo, "message of %s", lo)
			p.degrade(hi, "message of %s", hi)
			reason, msg := p.degradedSummary()
			if reason != hi {
				t.Errorf("%s with %s: the condition carries %s, want %s", hi, lo, reason, hi)
			}
			wantMsg := hi + ": message of " + hi + " | " + lo + ": message of " + lo
			if msg != wantMsg {
				t.Errorf("%s with %s: message %q, want %q", hi, lo, msg, wantMsg)
			}
		}
	}
	// One more check that no reason is missing from the ranking: a reason
	// the ranking lacks would vanish from the message.
	p := &pass{}
	for _, r := range hbep7 {
		p.degrade(r, "m")
	}
	_, msg := p.degradedSummary()
	if got := strings.Count(msg, ": m"); got != 12 {
		t.Errorf("the message lists %d reasons of 12: %q", got, msg)
	}
}
