package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Every report the agent builds carries Seq, the agent instance's own count
// (issue #221, HBEP-7 Report order): from 1, one more for each report it builds,
// periodic and last alike, and a report tried again keeps its number.

// seqRecorder records every Report call the agent makes before the Backend
// sees it, failed ones included: the Backend drops older reports, so what it
// keeps cannot show what the agent sent.
type seqRecorder struct {
	agentapi.Backend
	// failWhile, when set, makes a call fail with the error it returns for
	// that report, if not nil.
	failWhile func(agentapi.Report) error

	mu    sync.Mutex
	calls []seqCall
}

type seqCall struct {
	seq   uint64
	phase agentapi.PartPhase
	err   error
}

func (b *seqRecorder) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	var err error
	if b.failWhile != nil {
		err = b.failWhile(r)
	}
	if err == nil {
		err = b.Backend.Report(ctx, c, r)
	}
	var phase agentapi.PartPhase
	if w := worker0(r); w != nil {
		phase = w.Phase
	}
	b.mu.Lock()
	b.calls = append(b.calls, seqCall{r.Seq, phase, err})
	b.mu.Unlock()
	return err
}

func (b *seqRecorder) recorded() []seqCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]seqCall(nil), b.calls...)
}

// runUntilStopped runs the Instance to a worker running, ends the agent, by
// instruction or with its job, and waits for it to exit.
func runUntilStopped(t *testing.T, o harnessOptions, via string) *harness {
	t.Helper()
	h := newHarness(t, o)
	d := h.runState("", 1, 0, nil)
	h.set(d)
	h.waitReport("worker-0 running", 20*time.Second, running)
	if via == byJobEnd {
		h.cancel()
	} else {
		h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: d.Settings})
	}
	select {
	case err := <-h.done:
		h.done <- err // for the clean-up's wait
		if via == byInstruction && err != nil || via == byJobEnd && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v after a stop by %s", err, via)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the agent did not exit after a stop by %s", via)
	}
	return h
}

// The agent counts from 1 and never goes back; the last report is newer than
// every report before it. Between two calls the count may jump, since a
// snapshot superseded before it was sent still took a number.
func TestTheAgentCountsItsReportsAndTheLastIsTheNewest(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			rec := &seqRecorder{}
			h := runUntilStopped(t, harnessOptions{backendWrap: func(b agentapi.Backend) agentapi.Backend { rec.Backend = b; return rec }}, via)
			calls := rec.recorded()
			if len(calls) < 3 {
				t.Fatalf("%d reports reached the Backend, want at least 3", len(calls))
			}
			if calls[0].seq == 0 {
				t.Errorf("the first report carries no count")
			}
			for i := 1; i < len(calls); i++ {
				if calls[i].seq <= calls[i-1].seq {
					t.Errorf("report %d is counted %d, after %d: the count must rise with every report", i+1, calls[i].seq, calls[i-1].seq)
				}
			}
			last := calls[len(calls)-1]
			if last.phase != agentapi.PartStopped {
				t.Errorf("the last report says worker-0 %q, want %q", last.phase, agentapi.PartStopped)
			}
			if n := h.backend.Dropped(hAllocUID); n != 0 {
				t.Errorf("the Backend dropped %d reports of an agent that sent them in order", n)
			}
		})
	}
}

// A report that is tried again keeps its number, whether the agent tries it
// again after a failed send (final's loop) or after the Backend answered it
// not registered (send registers again and sends the same report). The last
// report is the one made to fail here, twice, then accepted.
func TestAReportTriedAgainKeepsItsNumber(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"after a failed send", errors.New("read: connection reset by peer")},
		{"after not registered", agentapi.Errorf(agentapi.CodeNotRegistered, "agent instance is not registered")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failed int
			rec := &seqRecorder{}
			rec.failWhile = func(r agentapi.Report) error {
				if w := worker0(r); w != nil && w.Phase == agentapi.PartStopped && failed < 2 {
					failed++
					return tc.err
				}
				return nil
			}
			runUntilStopped(t, harnessOptions{backendWrap: func(b agentapi.Backend) agentapi.Backend { rec.Backend = b; return rec }}, byInstruction)
			var lastSeqs []uint64
			var before uint64
			for _, c := range rec.recorded() {
				if c.phase == agentapi.PartStopped {
					lastSeqs = append(lastSeqs, c.seq)
				} else if c.seq > before {
					before = c.seq
				}
			}
			if len(lastSeqs) != 3 {
				t.Fatalf("the last report was sent %d times, want 3 (two refused, one accepted)", len(lastSeqs))
			}
			if lastSeqs[0] != lastSeqs[1] || lastSeqs[1] != lastSeqs[2] {
				t.Errorf("the last report was counted %v over its tries, want one number", lastSeqs)
			}
			if lastSeqs[0] <= before {
				t.Errorf("the last report is counted %d, not above the %d of the newest report before it", lastSeqs[0], before)
			}
		})
	}
}

// The count rises with every report, so it must not take part in the test of
// whether a snapshot changed: the agent offers a snapshot only when it differs
// from the last one offered, or at the report interval.
func TestTheCountIsNoPartOfTheChangeCheck(t *testing.T) {
	r := agentapi.Report{Seq: 4, Time: time.Unix(10, 0), Instance: &agentapi.InstanceReport{UID: "inst-uid-1", Generation: 1}}
	later := r
	later.Seq, later.Time = 9, time.Unix(20, 0)
	if string(agent.SnapshotKeyForTest(r)) != string(agent.SnapshotKeyForTest(later)) {
		t.Error("two snapshots that differ only in count and time have different keys")
	}
	changed := later
	changed.Instance = &agentapi.InstanceReport{UID: "inst-uid-1", Generation: 2}
	if string(agent.SnapshotKeyForTest(r)) == string(agent.SnapshotKeyForTest(changed)) {
		t.Error("two snapshots of different Instances have the same key")
	}
}
