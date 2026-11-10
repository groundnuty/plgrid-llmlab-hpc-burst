package relaykeeper

// New for issue #343 (PR 2): the keeper's decisions on its state file, the
// fence, the sets and the final ends. The login-host claims are in
// claims_test.go.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// E is 2³¹ plus the keeper's start count, on disk before the Runner, which
// starts the relays, is made; each start counts once more, and the state file
// is mode 600 in a directory of mode 700.
func TestEIsTwoToThe31PlusTheStartCountOnDiskFirst(t *testing.T) {
	r := newRig(t)
	for start := int64(1); start <= 3; start++ {
		var onDisk int64 = -1
		k, fr := r.open(func(o *Options) {
			inner := o.Runner
			o.Runner = func(e int64) (Runner, error) {
				onDisk = r.disk().Starts
				return inner(e)
			}
		})
		if want := int64(1)<<31 + start; k.Epoch() != want || fr.epoch != want {
			t.Errorf("start %d: E %d, the Runner's %d, want 2^31+%d = %d", start, k.Epoch(), fr.epoch, start, want)
		}
		if onDisk != start {
			t.Errorf("start %d: the state file showed %d starts when the Runner was made, want the start counted first", start, onDisk)
		}
	}
	mustMode(t, r.path, 0o600)
	mustMode(t, filepath.Dir(r.path), 0o700)
}

// A state file that cannot be parsed, or of another schema, stops the keeper
// and is left as it was: a fresh state would lower E below the relays of the
// run before.
func TestABadStateFileIsNeverReplaced(t *testing.T) {
	for name, content := range map[string]string{
		"torn":         `{"schema": 1, "starts": 4`,
		"other schema": `{"schema": 2, "starts": 4}`,
		"unknown key":  `{"schema": 1, "starts": 4, "extra": true}`,
		"negative":     `{"schema": 1, "starts": -1}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(r.path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Open(Options{State: r.path, Runner: func(int64) (Runner, error) {
				t.Error("a Runner was made on a bad state file")
				return &fakeRunner{}, nil
			}})
			if err == nil {
				t.Fatal("Open accepted a bad state file")
			}
			if b, _ := os.ReadFile(r.path); string(b) != content {
				t.Errorf("the bad state file was changed to %q", b)
			}
		})
	}
}

// A set or an acknowledgement from a lower Lease epoch than the Provider's
// highest accepted is refused, naming the highest, and counted; the same
// epoch is accepted again.
func TestALowerLeaseEpochIsRefusedAndCounted(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 5, true, "login.example", entry(1)))
	fr.log()
	got := refused(t, k, wire.KindSet, set("example-a", 4, true, "login.example"))
	if got.Reason != wire.RefusedEpoch || got.HighestEpoch != 5 {
		t.Errorf("the lower set's refusal %+v: want epoch, naming 5", got)
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("a refused set reached the Runner: %q", calls)
	}
	accepted(t, k, set("example-a", 5, true, "login.example", entry(1)))
	got = refused(t, k, wire.KindAck, wire.Ack{Provider: "example-a", LeaseEpoch: 4, UID: entry(1).UID, JobID: entry(1).JobID,
		Channel: agentapi.RelayServing, Epoch: k.Epoch(), Seq: 1, Outcome: wire.OutcomeMarked})
	if got.Reason != wire.RefusedEpoch || got.HighestEpoch != 5 {
		t.Errorf("the lower acknowledgement's refusal %+v: want epoch, naming 5", got)
	}
	st := statusOf(t, k, "example-a")
	if st.Refused[wire.RefusedEpoch] != 2 {
		t.Errorf("refused counts %v, want 2 for epoch", st.Refused)
	}
	if d := r.disk().Providers["example-a"].HighestEpoch; d != 5 {
		t.Errorf("the highest accepted epoch on disk is %d, want 5", d)
	}
}

// A higher Lease epoch is on disk before its set takes effect: the Runner,
// at its Want, finds it in the state file.
func TestAHigherLeaseEpochIsOnDiskBeforeItsSetTakesEffect(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	var seen []int64
	fr.onWant = func(p string, _ Want) { seen = append(seen, r.disk().Providers[p].HighestEpoch) }
	accepted(t, k, set("example-a", 3, true, "login.example", entry(1)))
	accepted(t, k, set("example-a", 9, true, "login.example", entry(1)))
	if !slices.Equal(seen, []int64{3, 9}) {
		t.Errorf("the epochs on disk at each Want: %v, want [3 9]", seen)
	}
}

// After a keeper restart, a stale controller that reconnects first, with the
// lower epoch it led under before, is refused: the fence is in the state
// file. The real leader's epoch is accepted.
func TestAStaleControllerIsRefusedFirstAfterAKeeperRestart(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-a", 7, true, "login.example", entry(1)))
	k2, fr2 := r.open()
	if got := refused(t, k2, wire.KindSet, set("example-a", 6, true, "login.example", entry(1))); got.Reason != wire.RefusedEpoch {
		t.Errorf("the stale controller's set after the restart: %+v, want refused for its epoch", got)
	}
	if calls := fr2.log(); len(calls) != 0 {
		t.Errorf("the stale controller's set reached the Runner: %q", calls)
	}
	accepted(t, k2, set("example-a", 7, true, "login.example", entry(1)))
}

// ClearLeaseEpochs, the launcher's restore's, lets a lower epoch in, as a
// restored store's real leader has, and keeps the start count, the ends and
// the claims.
func TestClearLeaseEpochsLetsTheRestoredLeadersLowerEpochIn(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 9, true, "login.example", entry(1)))
	w, _ := fr.want("example-a", entry(1).Tag)
	w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, "srun refused the step: step limit reached for this job", k.Epoch(), 2)
	before := r.disk()
	if err := ClearLeaseEpochs(r.path); err != nil {
		t.Fatal(err)
	}
	after := r.disk()
	if len(after.Providers) != 0 || after.Starts != before.Starts || len(after.Ends) != 1 || len(after.Claims) != 1 {
		t.Errorf("after ClearLeaseEpochs: %+v, want no fences and the same starts, ends and claims as %+v", after, before)
	}
	mustMode(t, r.path, 0o600)
	k2, _ := r.open()
	accepted(t, k2, set("example-a", 3, true, "login.example", entry(1)))
	if k2.Epoch() <= k.Epoch() {
		t.Errorf("E after the restore %d is not above the run before's %d", k2.Epoch(), k.Epoch())
	}
	if err := ClearLeaseEpochs(filepath.Join(t.TempDir(), "none.json")); err != nil {
		t.Errorf("ClearLeaseEpochs of a missing file: %v", err)
	}
}

// A new leader's incomplete set starts its Allocations' relays and stops
// none; only a complete set stops a relay, by leaving its Allocation out.
func TestAnIncompleteSetStopsNoRelay(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2)))
	fr.log()
	accepted(t, k, set("example-a", 2, false, "login.example", entry(1)))
	if calls := fr.log(); slices.ContainsFunc(calls, func(c string) bool { return strings.HasPrefix(c, "drop") }) {
		t.Errorf("an incomplete set stopped a relay: %q", calls)
	}
	accepted(t, k, set("example-a", 2, true, "login.example", entry(1)))
	if calls := fr.log(); !slices.Contains(calls, "drop example-a "+entry(2).Tag) {
		t.Errorf("a complete set that leaves Allocation 2 out did not stop its relays: %q", calls)
	}
}

// A set's expiry: its listing shows each job RUNNING under its tag until one
// poll interval plus the renew deadline after the listing, then nothing
// starts or restarts (the listing is an error), and the relays are kept.
func TestAnExpiredSetStartsAndRestartsNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	s := set("example-a", 1, true, "login.example", entry(1))
	s.ListingAgeMS = 10_000 // the set rests on a listing 10 s old, valid 40 s from it
	accepted(t, k, s)
	w, _ := fr.want("example-a", entry(1).Tag)
	jobs, err := w.Jobs.ListByTag(context.Background(), entry(1).Tag)
	if err != nil || len(jobs) != 1 || jobs[0].ID != entry(1).JobID || jobs[0].Name != entry(1).Tag || jobs[0].Phase != "Running" {
		t.Fatalf("the unexpired set's listing: %+v, %v; want job %s running under its tag", jobs, err, entry(1).JobID)
	}
	w.StartFailed(agentapi.RelayControl) // the authorization, resolved
	if jobs, err := w.Jobs.ListByTag(context.Background(), entry(2).Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the listing of a tag the set does not name: %+v, %v; want none", jobs, err)
	}
	r.clock.advance(29_999 * time.Millisecond)
	if _, err := w.Jobs.ListByTag(context.Background(), entry(1).Tag); err != nil {
		t.Errorf("the set 1 ms before its expiry: %v", err)
	}
	w.StartFailed(agentapi.RelayControl)
	r.clock.advance(time.Millisecond)
	if _, err := w.Jobs.ListByTag(context.Background(), entry(1).Tag); err == nil {
		t.Error("the expired set's listing still shows the job: a relay would start or restart under it")
	}
	if calls := fr.log(); slices.ContainsFunc(calls, func(c string) bool { return strings.HasPrefix(c, "drop") }) {
		t.Errorf("an expiry stopped a relay: %q", calls)
	}
}

// A set that has expired on arrival moves the fence and changes nothing
// else: no relay is wanted or stopped, and its report says it expired.
func TestASetExpiredOnArrivalMovesOnlyTheFence(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
	fr.log()
	s := set("example-a", 2, true, "login.example", entry(2))
	s.ListingAgeMS = s.ValidForMS
	rep := accepted(t, k, s)
	if !rep.Expired {
		t.Error("the report of a set expired on arrival does not say so")
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("a set expired on arrival reached the Runner: %q", calls)
	}
	if d := r.disk().Providers["example-a"].HighestEpoch; d != 2 {
		t.Errorf("the fence on disk is %d, want 2: an expired set still moves it", d)
	}
}

// Before any set since the keeper started, and through a leader's outage, a
// Provider's relays start and restart nothing and are kept: an Allocation
// deleted during the outage keeps its relays until a complete set leaves it
// out.
func TestNoLeaderStartsNothingAndStopsNothing(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	if _, err := (jobs{k: k, provider: "example-a"}).ListByTag(context.Background(), entry(1).Tag); err == nil {
		t.Error("before any set since the keeper started, the listing shows a job")
	}
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2)))
	w, _ := fr.want("example-a", entry(2).Tag)
	fr.log()
	r.clock.advance(10 * time.Minute) // the outage; Allocation 2 is deleted meanwhile
	if _, err := w.Jobs.ListByTag(context.Background(), entry(2).Tag); err == nil {
		t.Error("during the outage the listing shows a job: a relay would restart with no leader")
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("the outage changed the relays: %q", calls)
	}
	accepted(t, k, set("example-a", 2, true, "login.example", entry(1)))
	if calls := fr.log(); !slices.Contains(calls, "drop example-a "+entry(2).Tag) {
		t.Errorf("the new leader's complete set did not stop the deleted Allocation's relays: %q", calls)
	}
}

// A final end that needs the mark (failed, or superseded) is on disk when
// OnFinal returns, and is in each of the Provider's reports until it is
// decided; a job's end is not recorded, and an end the keeper holds already
// is not recorded twice.
func TestAFinalEndIsSyncedAndReportedUntilDecided(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2), entry(3)))
	failed, _ := fr.want("example-a", entry(1).Tag)
	failed.OnFinal(agentapi.RelayServing, hostdial.EndFailed, "srun refused the step: step limit reached for this job", k.Epoch(), 4)
	if ends := r.disk().Ends; len(ends) != 1 || ends[0].End.Kind != wire.EndFailed || ends[0].End.Seq != 4 || ends[0].Decided {
		t.Fatalf("the state file after a failed end: %+v, want it recorded, undecided", ends)
	}
	failed.OnFinal(agentapi.RelayServing, hostdial.EndFailed, "srun refused the step: step limit reached for this job", k.Epoch(), 4)
	gone, _ := fr.want("example-a", entry(2).Tag)
	gone.OnFinal(agentapi.RelayControl, hostdial.EndJobGone, "srun refused the step: the job is not running", k.Epoch(), 5)
	sup, _ := fr.want("example-a", entry(3).Tag)
	sup.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, "superseded (exit 5): the agent keeps a newer relay", k.Epoch(), 6)
	rep := accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2), entry(3)))
	var kinds []string
	for _, e := range rep.Ends {
		kinds = append(kinds, e.Tag+" "+string(e.Channel)+" "+e.Kind)
	}
	want := []string{entry(1).Tag + " serving failed", entry(3).Tag + " control superseded"}
	if !slices.Equal(kinds, want) {
		t.Errorf("the report's ends: %q, want %q: the job's end needs no mark, and an end is recorded once", kinds, want)
	}
}

// An acknowledgement decides its end: the end leaves the reports, and stays
// on disk as decided, so its channel is seeded final in every Want, a keeper
// restart included, until its Allocation leaves a complete set (the science
// agent's ruling on #343's Q1).
func TestADecidedEndStaysUntilItsAllocationLeavesACompleteSet(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2)))
	w, _ := fr.want("example-a", entry(1).Tag)
	const cause = "srun refused the step: step limit reached for this job"
	w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, cause, k.Epoch(), 4)
	kind, body := send(t, k, wire.KindAck, wire.Ack{Provider: "example-a", LeaseEpoch: 1, UID: entry(1).UID, JobID: entry(1).JobID,
		Channel: agentapi.RelayServing, Epoch: k.Epoch(), Seq: 4, Outcome: wire.OutcomeMarked})
	if kind != wire.KindAcked || !body.(wire.Acked).Decided {
		t.Fatalf("the acknowledgement was answered %s %+v, want decided", kind, body)
	}
	if rep := accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2))); len(rep.Ends) != 0 {
		t.Errorf("a decided end is still reported: %+v", rep.Ends)
	}
	if ends := r.disk().Ends; len(ends) != 1 || !ends[0].Decided || ends[0].Outcome != wire.OutcomeMarked {
		t.Fatalf("the state file after the acknowledgement: %+v, want the end kept, decided as marked", ends)
	}
	k2, fr2 := r.open()
	accepted(t, k2, set("example-a", 1, false, "login.example", entry(1), entry(2)))
	w2, _ := fr2.want("example-a", entry(1).Tag)
	if w2.Finals()[agentapi.RelayServing] != "failed: "+cause || len(w2.Finals()) != 1 {
		t.Errorf("after a keeper restart the Want's seeds are %q, want the serving channel final", w2.Finals())
	}
	if w3, _ := fr2.want("example-a", entry(2).Tag); len(w3.Finals()) != 0 {
		t.Errorf("another Allocation was seeded %q", w3.Finals())
	}
	accepted(t, k2, set("example-a", 1, true, "login.example", entry(2)))
	if ends := r.disk().Ends; len(ends) != 0 {
		t.Errorf("the state file after Allocation 1 left a complete set: %+v, want its end gone", ends)
	}
}

// An acknowledgement of an end the keeper does not hold, or of another
// process's end of the same channel, decides nothing.
func TestAnAcknowledgementOfAnotherEndDecidesNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
	w, _ := fr.want("example-a", entry(1).Tag)
	w.OnFinal(agentapi.RelayServing, hostdial.EndSuperseded, "superseded (exit 5): the agent keeps a newer relay", k.Epoch(), 4)
	for name, a := range map[string]wire.Ack{
		"another process": {Seq: 3},
		"another channel": {Seq: 4, Channel: agentapi.RelayControl},
		"another job":     {Seq: 4, JobID: "59999"},
	} {
		a.Provider, a.LeaseEpoch, a.UID, a.Epoch, a.Outcome = "example-a", 1, entry(1).UID, k.Epoch(), wire.OutcomeRefused
		a.Reason = wire.AckDeleted
		if a.JobID == "" {
			a.JobID = entry(1).JobID
		}
		if a.Channel == "" {
			a.Channel = agentapi.RelayServing
		}
		kind, body := send(t, k, wire.KindAck, a)
		if kind != wire.KindAcked || body.(wire.Acked).Decided {
			t.Errorf("%s: answered %s %+v, want acked and not decided", name, kind, body)
		}
	}
	if ends := r.disk().Ends; len(ends) != 1 || ends[0].Decided {
		t.Errorf("the end after acknowledgements of other ends: %+v, want it undecided", ends)
	}
}

// An Allocation that carries RelayEndedForGood gets no new relay: one the
// keeper does not keep is not wanted, and its listing shows no job, so a kept
// relay of it does not restart either; one the keeper keeps is not stopped.
func TestAnAllocationWithTheMarkGetsNoNewRelay(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
	fr.log()
	marked1, marked2 := entry(1), entry(2)
	marked1.EndedForGood, marked2.EndedForGood = true, true
	accepted(t, k, set("example-a", 1, true, "login.example", marked1, marked2))
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("marked Allocations reached the Runner: %q, want neither a Want nor a Drop", calls)
	}
	w, _ := fr.want("example-a", entry(1).Tag)
	if jobs, err := w.Jobs.ListByTag(context.Background(), entry(1).Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the listing of a marked Allocation: %+v, %v; want no job, so its relays never restart", jobs, err)
	}
}

// The status counts each channel's relay starts after the first, over every
// Allocation, and the refusals by reason.
func TestTheStatusCountsRestartsAndRefusals(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 2, true, "login.example", entry(1), entry(2)))
	w1, _ := fr.want("example-a", entry(1).Tag)
	w2, _ := fr.want("example-a", entry(2).Tag)
	n := int64(0)
	start := func(f func(agentapi.RelayChannel, int64, int64), ch agentapi.RelayChannel) {
		n++
		f(ch, k.Epoch(), n)
	}
	for _, f := range []func(agentapi.RelayChannel, int64, int64){w1.OnStart, w2.OnStart} {
		start(f, agentapi.RelayControl)
		start(f, agentapi.RelayServing)
	}
	start(w1.OnStart, agentapi.RelayServing)
	start(w1.OnStart, agentapi.RelayServing)
	start(w2.OnStart, agentapi.RelayControl)
	refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example"))
	st := statusOf(t, k, "example-a")
	if st.Restarts[agentapi.RelayServing] != 2 || st.Restarts[agentapi.RelayControl] != 1 {
		t.Errorf("restarts %v, want serving 2 and control 1", st.Restarts)
	}
	if st.Refused[wire.RefusedEpoch] != 1 || len(st.Relays) != 4 {
		t.Errorf("status %+v: want one epoch refusal and four relays", st)
	}
	kind, body := send(t, k, wire.KindPing, struct{}{})
	if kind != wire.KindPong || body.(wire.Pong).KeeperEpoch != k.Epoch() {
		t.Errorf("the ping was answered %s %+v, want pong with E %d", kind, body, k.Epoch())
	}
}

// statusOf is one Provider's status.
func statusOf(t *testing.T, k *Keeper, provider string) wire.ProviderStatus {
	t.Helper()
	kind, body := send(t, k, wire.KindStatus, struct{}{})
	if kind != wire.KindStatusReport {
		t.Fatalf("the status was answered %s", kind)
	}
	for _, p := range body.(wire.StatusReport).Providers {
		if p.Provider == provider {
			return p
		}
	}
	t.Fatalf("the status has no %s", provider)
	return wire.ProviderStatus{}
}

// A keeper that cannot write its state file refuses a set that would change
// it, and nothing takes effect; it accepts it once the file can be written.
func TestAStateFileThatCannotBeWrittenRefusesTheSet(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}
	r := newRig(t)
	k, fr := r.open()
	dir := filepath.Dir(r.path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(1)))
	if got.Reason != wire.RefusedState {
		t.Errorf("the refusal %+v, want the state file's", got)
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("a set whose state could not be written reached the Runner: %q", calls)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
}

// Malformed sets and acknowledgements, and a body with an unknown field, are
// refused before they change anything.
func TestAMalformedRequestIsRefused(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	bad := set("example-a", 1, true, "login.example", entry(1))
	bad.ValidForMS = 0
	if got := refused(t, k, wire.KindSet, bad); got.Reason != wire.RefusedMalformed {
		t.Errorf("a set with no validity: %+v, want malformed", got)
	}
	if got := refused(t, k, wire.KindAck, wire.Ack{Provider: "example-a"}); got.Reason != wire.RefusedMalformed {
		t.Errorf("an empty acknowledgement: %+v, want malformed", got)
	}
	kind, body := k.Handle(context.Background(), wire.Envelope{V: wire.Version, Kind: wire.KindSet, Body: []byte(`{"provider":"example-a","extra":1}`)})
	if kind != wire.KindRefused || body.(wire.Refused).Reason != wire.RefusedMalformed {
		t.Errorf("a set with an unknown field: %s %+v, want malformed", kind, body)
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("malformed requests reached the Runner: %q", calls)
	}
	if _, err := os.Stat(r.path); err != nil || len(r.disk().Providers) != 0 {
		t.Errorf("malformed requests changed the state file: %v, %+v", err, r.disk())
	}
}

// The keeper answers over its socket: a set's report comes back through
// wire.Serve and wire.Exchange, and a refusal as a *wire.RefusedError.
func TestTheKeeperAnswersOverItsSocket(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	dir, err := os.MkdirTemp("", "rk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "keeper.sock")
	ln, err := wire.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- wire.Serve(ctx, ln, k.Handle, nil) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	var rep wire.Report
	if err := wire.Exchange(context.Background(), sock, wire.KindSet, set("example-a", 4, true, "Login.Example", entry(1)), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.KeeperEpoch != k.Epoch() || rep.LoginHost != "login.example" || len(rep.Relays) != 2 {
		t.Errorf("the report over the socket: %+v", rep)
	}
	err = wire.Exchange(context.Background(), sock, wire.KindSet, set("example-a", 3, true, "login.example"), &rep)
	var re *wire.RefusedError
	if !errors.As(err, &re) || re.Refused.Reason != wire.RefusedEpoch || re.Refused.HighestEpoch != 4 {
		t.Errorf("the lower set over the socket: %v, want refused for its epoch, naming 4", err)
	}
}

// The Wants of one Allocation carry its entry whole, so the Runner can tell a
// new identity (another Gen) from the same one, which a new leader adopts.
func TestAWantCarriesTheEntryWhole(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	e.Gen = "2b3c4d5e6f7a8b9c"
	accepted(t, k, set("example-a", 2, true, "login.example", e))
	if w, _ := fr.want("example-a", e.Tag); w.Entry != e {
		t.Errorf("the Want's entry %+v, want %+v", w.Entry, e)
	}
}

// A new credentials generation's Want keeps the seeds of the ends the keeper
// holds, so a final-ended channel stays down on a new generation (#347), and
// a marked Allocation gets no Want on one either.
func TestANewGenerationsWantKeepsTheSeeds(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e, entry(2)))
	w, _ := fr.want("example-a", e.Tag)
	w.OnFinal(agentapi.RelayControl, hostdial.EndSuperseded, "superseded (exit 5): the agent keeps a newer relay", k.Epoch(), 3)
	e.Gen = "2b3c4d5e6f7a8b9c"
	marked := entry(2)
	marked.Gen, marked.EndedForGood = "2b3c4d5e6f7a8b9c", true
	fr.log()
	accepted(t, k, set("example-a", 1, true, "login.example", e, marked))
	w2, _ := fr.want("example-a", e.Tag)
	if w2.Entry.Gen != e.Gen || w2.Finals()[agentapi.RelayControl] != "superseded: superseded (exit 5): the agent keeps a newer relay" {
		t.Errorf("the new generation's Want: %+v with seeds %q, want generation 2 with the control channel seeded", w2.Entry, w2.Finals())
	}
	if calls := fr.log(); slices.Contains(calls, "want example-a "+marked.Tag) {
		t.Errorf("a marked Allocation got a Want on a new generation: %q", calls)
	}
}
