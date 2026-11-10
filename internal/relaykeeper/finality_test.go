package relaykeeper

// New for the review of PR 354 (B2) and its delta: a replacement's finality
// is checked when it starts, not when its Want was made; and a superseded end
// is skipped only when the keeper's own record of what it issued explains it.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// newGen is e under another credentials generation.
func newGen(e wire.Entry) wire.Entry {
	e.Gen = "2b3c4d5e6f7a8b9c"
	return e
}

// A final end that comes between a new generation's Want and its start keeps
// the Allocation down: the start, which consults the Want's Jobs as hostdial
// does, is refused, and the seeds the Runner reads then name the channel.
// Here the old generation's relay fails after the new generation's Want was
// made.
func TestAnEndBetweenAWantAndItsStartKeepsItsChannelDown(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	fr.mustStart(t, "example-a", e.Tag)
	w1, _ := fr.want("example-a", e.Tag)
	accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
	w2, _ := fr.want("example-a", e.Tag)
	w1.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 2)
	if fr.start("example-a", e.Tag) {
		t.Error("the new generation started after the old one's end was held")
	}
	if got := w2.Finals(); got[agentapi.RelayServing] != "failed: "+stepLimit {
		t.Errorf("the new generation's seeds at its start: %q, want the serving channel", got)
	}
}

// A mark that comes between a Want and its start starts nothing: the Jobs
// the start consults hide an Allocation that carries RelayEndedForGood.
func TestAMarkBetweenAWantAndItsStartStartsNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	marked := e
	marked.EndedForGood = true
	accepted(t, k, set("example-a", 1, true, "login.example", marked))
	if fr.start("example-a", e.Tag) {
		t.Error("the relays of an Allocation marked after its Want started")
	}
}

const supersededCause = "superseded (exit 5): the agent keeps a newer relay"

// A superseded end of a relay the keeper itself replaced is not recorded:
// its own record holds the (E, N) it issued the new generation on that
// channel, of its own epoch and above the ended relay's (the science agent's
// condition on the review of PR 354). A failure of the replaced generation
// is recorded all the same, and so is the current generation's own
// superseded end, which in beta is unexpected (HBEP-6).
func TestASupersededEndOfTheKeepersOwnReplacementIsNotRecorded(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	fr.mustStart(t, "example-a", e.Tag)
	w1, _ := fr.want("example-a", e.Tag)
	oldControl := fr.seqOf("example-a", e.Tag, agentapi.RelayControl)
	oldServing := fr.seqOf("example-a", e.Tag, agentapi.RelayServing)
	accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
	fr.mustStart(t, "example-a", e.Tag)
	w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), oldControl)
	if ends := r.disk().Ends; len(ends) != 0 {
		t.Errorf("the keeper's own replacement's superseded end was recorded: %+v", ends)
	}
	w1.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), oldServing)
	if ends := r.disk().Ends; len(ends) != 1 || ends[0].End.Channel != agentapi.RelayServing {
		t.Errorf("the replaced generation's failure: %+v, want it recorded", ends)
	}
	w2, _ := fr.want("example-a", e.Tag)
	w2.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), fr.seqOf("example-a", e.Tag, agentapi.RelayControl))
	if ends := r.disk().Ends; len(ends) != 2 {
		t.Errorf("the current generation's superseded end: %+v, want it recorded", ends)
	}
}

// Any superseded end the keeper's own record of what it issued does not
// explain is unexpected, and recorded for the mark (#347). The agent's
// refusal names no (E, N), so that record is all the keeper has. Recorded:
//   - with no replacement, as when a stray relay with a higher (E, N), which
//     the keeper did not issue, supersedes its only relay;
//   - with a replacement queued but not started, so the keeper has issued
//     the new generation nothing, as when another keeper's relay supersedes
//     the old one;
//   - with an ended relay above what the keeper issued the replacement;
//   - and when a stray relay outranks the replacement too: the old
//     generation's end is the replacement's in the record and is skipped,
//     but the replacement's own superseded end, of the current generation,
//     is recorded, so the stray marks the Allocation all the same.
func TestAnUnexpectedSupersededEndIsRecorded(t *testing.T) {
	for _, c := range []struct {
		name string
		do   func(t *testing.T, rg *rig, fr *fakeRunner, k *Keeper, e wire.Entry, w1 Want)
	}{
		{"no replacement", func(t *testing.T, rg *rig, fr *fakeRunner, k *Keeper, e wire.Entry, w1 Want) {
			w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), fr.seqOf("example-a", e.Tag, agentapi.RelayControl))
		}},
		{"a replacement queued, not started", func(t *testing.T, rg *rig, fr *fakeRunner, k *Keeper, e wire.Entry, w1 Want) {
			old := fr.seqOf("example-a", e.Tag, agentapi.RelayControl)
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
			w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), old)
		}},
		{"an ended relay above the replacement", func(t *testing.T, rg *rig, fr *fakeRunner, k *Keeper, e wire.Entry, w1 Want) {
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
			fr.mustStart(t, "example-a", e.Tag)
			w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), fr.seqOf("example-a", e.Tag, agentapi.RelayControl)+100)
		}},
		{"a stray relay above the replacement too", func(t *testing.T, rg *rig, fr *fakeRunner, k *Keeper, e wire.Entry, w1 Want) {
			old := fr.seqOf("example-a", e.Tag, agentapi.RelayControl)
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
			fr.mustStart(t, "example-a", e.Tag)
			w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), old)
			if ends := rg.disk().Ends; len(ends) != 0 {
				t.Fatalf("the old generation's end, which the record explains, was recorded: %+v", ends)
			}
			w2, _ := fr.want("example-a", e.Tag)
			w2.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, k.Epoch(), fr.seqOf("example-a", e.Tag, agentapi.RelayControl))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t)
			k, fr := rg.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login.example", e))
			fr.mustStart(t, "example-a", e.Tag)
			w1, _ := fr.want("example-a", e.Tag)
			c.do(t, rg, fr, k, e, w1)
			if ends := rg.disk().Ends; len(ends) != 1 || ends[0].End.Kind != wire.EndSuperseded {
				t.Errorf("the unexpected superseded end: %+v, want it recorded for the mark", ends)
			}
		})
	}
}

// The supersede check reads only the keeper's issue to the current Want,
// of its own epoch: an issue of the replaced Want, or one of another epoch,
// explains nothing, though its pair outranks the ended relay's. Each case
// isolates one guard: the issued pair outranks the ended one in both, the
// first issue carries the replaced Want's incarnation and the keeper's own
// epoch, and the second the current Want's incarnation and another epoch,
// so only the incarnation's guard, or only the epoch's, records the end (the
// delta review of PR 356). The keeper's own starts never leave these guards
// unmet, so the issue is set by hand (they survived as mutant B5 until this
// test).
func TestTheSupersedeCheckReadsOnlyTheNewGenerationsOwnIssue(t *testing.T) {
	for _, c := range []struct {
		name             string
		issue            func(k *Keeper, e wire.Entry, replaced uint64) issue
		endEpoch, endSeq func(k *Keeper) int64
	}{
		{"the replaced generation's issue",
			func(k *Keeper, e wire.Entry, replaced uint64) issue {
				return issue{entry: e, inc: replaced, epoch: k.Epoch(), seq: 99}
			},
			func(k *Keeper) int64 { return k.Epoch() }, func(*Keeper) int64 { return 1 }},
		{"another epoch's issue",
			func(k *Keeper, e wire.Entry, _ uint64) issue {
				return issue{entry: newGen(e), inc: k.wanted["example-a"][e.Tag].inc, epoch: k.Epoch() - 1, seq: 99}
			},
			func(k *Keeper) int64 { return k.Epoch() - 1 }, func(*Keeper) int64 { return 1 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login.example", e))
			fr.mustStart(t, "example-a", e.Tag)
			w1, _ := fr.want("example-a", e.Tag)
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
			k.mu.Lock()
			k.issued["example-a"][e.Tag][agentapi.RelayControl] = c.issue(k, e, w1.Incarnation)
			k.mu.Unlock()
			w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, supersededCause, c.endEpoch(k), c.endSeq(k))
			if ends := r.disk().Ends; len(ends) != 1 {
				t.Errorf("with %s: ends %+v, want the superseded end recorded", c.name, ends)
			}
		})
	}
}

// A late start of a replaced generation does not overwrite the keeper's
// record of what it issued the replacement: a start authorized before the
// replacement's Want lands after the replacement started, and the record
// still names the replacement's (E, N), which decides its supersedes.
func TestALateStartOfAReplacedGenerationKeepsTheReplacementsIssue(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	old := fr.authorize("example-a", e.Tag, agentapi.RelayControl) // the old generation's control start, in flight
	if old == nil {
		t.Fatal("the old generation's start was refused")
	}
	accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
	fr.mustStart(t, "example-a", e.Tag)
	want := fr.seqOf("example-a", e.Tag, agentapi.RelayControl)
	fr.launch(old, agentapi.RelayControl) // lands after its replacement started
	k.mu.Lock()
	is := k.issued["example-a"][e.Tag][agentapi.RelayControl]
	k.mu.Unlock()
	if is.entry != newGen(e) || is.seq != want {
		t.Errorf("the issue on record after the late start: %+v, want the replacement's, N %d", is, want)
	}
}
