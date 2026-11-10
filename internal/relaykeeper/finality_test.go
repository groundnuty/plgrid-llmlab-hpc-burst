package relaykeeper

// New for the review of PR 354 (B2): a replacement's finality is checked
// when it starts, not when its Want was made.

import (
	"context"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A final end that comes between a new generation's Want and its start keeps
// its channel down: the Runner reads the seeds (Finals) when it starts the
// relays, and the listing hides the Allocation at each start. Here the old
// generation's relay fails after the new generation's Want was made.
func TestAnEndBetweenAWantAndItsStartKeepsItsChannelDown(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w1, _ := fr.want("example-a", e.Tag)
	e2 := e
	e2.Gen = "2b3c4d5e6f7a8b9c"
	accepted(t, k, set("example-a", 1, true, "login.example", e2))
	w2, _ := fr.want("example-a", e.Tag)
	w1.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 6)
	fr.start("example-a", e.Tag)
	var serving string
	for _, rs := range fr.Relays("example-a") {
		if rs.Channel == agentapi.RelayServing {
			serving = rs.State
		}
	}
	if serving != "final" {
		t.Errorf("the new generation's serving relay, started after the old one's end: %s, want final", serving)
	}
	if jobs, err := w2.Jobs.ListByTag(context.Background(), e.Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the new generation's listing after the end: %+v, %v; want no job, so no relay of it starts", jobs, err)
	}
}

// A mark that comes between a Want and its start starts nothing: the
// listing, read at each start, hides an Allocation that carries
// RelayEndedForGood.
func TestAMarkBetweenAWantAndItsStartStartsNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w, _ := fr.want("example-a", e.Tag)
	marked := e
	marked.EndedForGood = true
	accepted(t, k, set("example-a", 1, true, "login.example", marked))
	if jobs, err := w.Jobs.ListByTag(context.Background(), e.Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the listing after the mark: %+v, %v; want no job", jobs, err)
	}
}

// A superseded end of a generation the keeper has replaced since is that
// replacement's doing, not a failure: it is not recorded, so it marks
// nothing. A failure of that generation is recorded all the same.
func TestASupersededEndOfAReplacedGenerationIsNotAFailure(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w1, _ := fr.want("example-a", e.Tag)
	e2 := e
	e2.Gen = "2b3c4d5e6f7a8b9c"
	accepted(t, k, set("example-a", 1, true, "login.example", e2))
	w1.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, "superseded (exit 5): the agent keeps a newer relay", k.Epoch(), 3)
	if ends := r.disk().Ends; len(ends) != 0 {
		t.Errorf("the replaced generation's superseded end was recorded: %+v", ends)
	}
	w1.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 4)
	if ends := r.disk().Ends; len(ends) != 1 || ends[0].End.Channel != agentapi.RelayServing {
		t.Errorf("the replaced generation's failure: %+v, want it recorded", ends)
	}
	// The current generation's own superseded end is recorded: in beta it is
	// unexpected (HBEP-6).
	w2, _ := fr.want("example-a", e.Tag)
	w2.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, "superseded (exit 5): the agent keeps a newer relay", k.Epoch(), 7)
	if ends := r.disk().Ends; len(ends) != 2 {
		t.Errorf("the current generation's superseded end: %+v, want it recorded", ends)
	}
}
