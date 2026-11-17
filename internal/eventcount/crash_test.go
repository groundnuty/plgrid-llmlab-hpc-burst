package eventcount

// HBEP-8, Test plan, Beta: "Events survive a log crash". A test emitter, the real Events wrapper with
// the publisher over a recording inner recorder, raises 500 typed events about five synthetic objects.
// The log, a real child, is SIGKILLed through its pidfd at a seeded random point, with the seed logged,
// and restarted on its store. What the log holds is read through a fresh client. The branch that needs
// the API (the log down past the retries, with EventsNotLogged in the API) runs in
// internal/providerctl/e2e.

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	crashEvents  = 500
	crashObjects = 5
	// Kill iterations of one run: 15 back within the retries and 5 down past them, 20 in all, so that
	// the two tests together stay under 60 s in CI. A -short run does three and two.
	crashIterationsBack = 15
	crashIterationsDown = 5
)

// crashObjects are the five synthetic objects, each with a UID of its own.
func crashUIDs() []string {
	var out []string
	for i := 0; i < crashObjects; i++ {
		out = append(out, fmt.Sprintf("0000000%d-cafe-4000-8000-00000000000%d", i, i))
	}
	return out
}

func crashID(iter, n int) string { return fmt.Sprintf("crash-%02d-%03d", iter, n) }

// seedOf is the run's seed: the clock's, unless $HPCBURST_TEST_SEED fixes it, and always logged.
func seedOf(t *testing.T) int64 {
	t.Helper()
	seed := time.Now().UnixNano()
	if s := os.Getenv("HPCBURST_TEST_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("HPCBURST_TEST_SEED=%q: %v", s, err)
		}
		seed = v
	}
	t.Logf("seed %d (set HPCBURST_TEST_SEED to repeat the run)", seed)
	return seed
}

// raiseAll raises the iteration's 500 events, event n about object n mod 5, and returns the ids and
// each id's object UID.
func (r *rig) raiseAll(iter int) (idList []string, uidOf map[string]string) {
	uids := crashUIDs()
	uidOf = map[string]string{}
	for n := 0; n < crashEvents; n++ {
		id := crashID(iter, n)
		uid := uids[n%crashObjects]
		r.rec.EventfID(id, alloc(uid), nil, provider.EventKindOrdinary, "JobStarted", "ready", "event %d", n)
		idList = append(idList, id)
		uidOf[id] = uid
	}
	return idList, uidOf
}

// killAt kills the log, through its pidfd, once the stream holds at least k messages, read through a
// poller client of the test's own, and requires the wait status to say SIGKILL.
func (r *rig) killAt(k uint64) {
	r.t.Helper()
	_, js := r.d.Connect(r.t)
	eventually(r.t, fmt.Sprintf("the stream to hold %d events", k), func() bool {
		return nodetest.StreamInfo(r.t, js, eventlog.StreamEvents).State.Msgs >= k
	})
	r.proc.Kill()
}

// restart starts the log again on its store and waits until it serves.
func (r *rig) restart() {
	r.t.Helper()
	r.proc = r.d.Start(r.t)
	r.proc.Ready()
}

// Back within the retries: all 500 ids are in HB_EVENTS, each once, each on its object's subject, and
// the counter reads 0.
func TestEventsSurviveALogCrashBackWithinTheRetries(t *testing.T) {
	iters := crashIterationsBack
	if testing.Short() {
		iters = 3
	}
	rng := rand.New(rand.NewSource(seedOf(t)))
	for i := 0; i < iters; i++ {
		kill := uint64(1 + rng.Intn(crashEvents-1))
		t.Run(fmt.Sprintf("kill-%02d-at-%d", i, kill), func(t *testing.T) {
			r := newRig(t, ControllerProvider("p1"), false, bigCaps, nil, nil)
			idList, uidOf := r.raiseAll(i)
			r.killAt(kill)
			r.restart()
			r.settled()
			ms := r.read()
			if len(ms) != crashEvents {
				t.Fatalf("the stream holds %d messages after the restart, want %d: each id once", len(ms), crashEvents)
			}
			seen := map[string]bool{}
			for _, m := range ms {
				if seen[m.ID] {
					t.Fatalf("id %s is in the stream twice", m.ID)
				}
				seen[m.ID] = true
				uid, ok := uidOf[m.ID]
				if !ok {
					t.Fatalf("the stream holds %s, which the emitter did not raise", m.ID)
				}
				if want := "hb.ev.allocation." + uid; m.Subject != want {
					t.Fatalf("%s is on %q, want its object's subject %q", m.ID, m.Subject, want)
				}
			}
			for _, id := range idList {
				if !seen[id] {
					t.Fatalf("%s was raised and is not in the stream", id)
				}
			}
			if got := r.unlogged(ControllerProvider("p1")); got != 0 {
				t.Fatalf("hpcburst_events_unlogged_total reads %v, want 0: the log was back within the retries", got)
			}
		})
	}
}

// Down past the retries (the budget shortened to 2 s): the stream's ids plus the counter's value
// total at least 500, and every stream id is one the emitter raised. The log's return is followed by
// one EventsNotLogged, in the log (the API's half is the providerctl e2e test).
func TestEventsSurviveALogCrashDownPastTheRetries(t *testing.T) {
	iters := crashIterationsDown
	if testing.Short() {
		iters = 2
	}
	rng := rand.New(rand.NewSource(seedOf(t)))
	for i := 0; i < iters; i++ {
		// Early enough that events are still queued when the log dies: the poll that decides the kill
		// is a few events late.
		kill := uint64(1 + rng.Intn(crashEvents*9/10))
		t.Run(fmt.Sprintf("kill-%02d-at-%d", i, kill), func(t *testing.T) {
			ctl := ControllerProvider("p1")
			r := newRig(t, ctl, false, bigCaps, func(o *PublisherOptions) { o.Budget = 2 * time.Second }, leaseFor("provider-p1"))
			// The publisher's own record of what the log acknowledged, id by id: an event the log stored whose
			// acknowledgement the kill swallowed is counted lost though it is in the stream, so the stream
			// cannot say which events are accounted twice; the acknowledgements can.
			var ackMu sync.Mutex
			acked := map[string]bool{}
			r.pub.afterPublish = func(id string, _ *jetstream.PubAck, err error) error {
				if err == nil {
					ackMu.Lock()
					acked[id] = true
					ackMu.Unlock()
				}
				return err
			}
			idList, uidOf := r.raiseAll(i)
			r.killAt(kill)
			// Down past the retries: every event still queued is lost within the budget, and so is the
			// EventsNotLogged the first loss raises, which the log cannot take either.
			rt := r.route(ctl)
			// The first loss raises EventsNotLogged from the notifier, a goroutine of its own, and that event runs
			// out too, one budget later. Wait for the raise first: a queue that is empty before it is made is no
			// proof that the EventsNotLogged has been settled, and counting before it ran out reads one lost
			// event too few.
			eventually(t, "the loss to be raised", func() bool {
				raised := len(r.inner.byReason(ReasonEventsNotLogged))
				rt.mu.Lock()
				defer rt.mu.Unlock()
				return raised >= 1 && rt.loss == nil
			})
			eventually(t, "every event, the EventsNotLogged too, to be stored or lost", func() bool { return r.pub.Pending() == 0 })
			eventually(t, "every loss to be handled", func() bool { return rt.handled.Load() == uint64(r.unlogged(ctl)) })
			lost := r.unlogged(ctl)
			if lost < 1 {
				t.Fatalf("no event was lost with the log down past the retries (kill at %d)", kill)
			}
			// The counter also counts the EventsNotLogged that ran out against the dead log, each raised
			// while the log was down and so lost: they are not among the 500, and subtracting them takes
			// away the unit of slack they would give an event that was neither stored nor counted.
			ackMu.Lock()
			ackedEvents, ackedNotLogged := 0, 0
			for id := range acked {
				if strings.HasPrefix(id, "crash-") {
					ackedEvents++
				} else {
					ackedNotLogged++ // an EventsNotLogged the log took: it was not lost
				}
			}
			ackMu.Unlock()
			lostEvents := lost - float64(len(r.inner.byReason(ReasonEventsNotLogged))-ackedNotLogged)
			// Every one of the 500 is exactly one of two things: acknowledged by the log, or counted lost. A
			// count that is higher or lower is an event that is neither, or both.
			if float64(ackedEvents)+lostEvents != crashEvents {
				t.Fatalf("%d acknowledged + %v counted lost (less the lost EventsNotLogged) = %v, want exactly %d (kill at %d)", ackedEvents, lostEvents, float64(ackedEvents)+lostEvents, crashEvents, kill)
			}
			r.restart()
			// The log is back: the next publish that it takes closes the run of losses with one
			// EventsNotLogged, published like any event.
			r.raise(crashUIDs()[0], "after-return", "JobEnded")
			_, js := r.d.Connect(t)
			eventually(t, "the run's closing event to be stored", func() bool {
				st, err := js.Stream(t.Context(), eventlog.StreamEvents)
				if err != nil {
					return false
				}
				_, err = st.GetLastMsgForSubject(t.Context(), "hb.ev.lease.uid-of-provider-p1")
				return err == nil && r.pub.Pending() == 0
			})
			ms := r.read()
			raised := map[string]bool{"after-return": true}
			for _, id := range idList {
				raised[id] = true
			}
			var notLogged, kept int
			for _, m := range ms {
				if raised[m.ID] {
					kept++
					continue
				}
				_, d, err := Decode(m.Payload)
				if err != nil {
					t.Fatal(err)
				}
				if d.Reason != ReasonEventsNotLogged || m.Subject != "hb.ev.lease.uid-of-provider-p1" {
					t.Fatalf("the stream holds %s (%s on %s), which the emitter did not raise", m.ID, d.Reason, m.Subject)
				}
				notLogged++
				if d.Action != ActionDegraded || d.Type != provider.EventKindWarning {
					t.Errorf("EventsNotLogged is %s/%s, want degraded/Warning", d.Action, d.Type)
				}
			}
			stored := kept - 1 // the after-return event
			if float64(stored)+lostEvents < crashEvents {
				t.Fatalf("stream %d + counter %v less the lost EventsNotLogged = %v, want at least %d: an event is neither in the log nor counted", stored, lost, float64(stored)+lostEvents, crashEvents)
			}
			for _, m := range ms {
				if uid, ok := uidOf[m.ID]; ok && m.Subject != "hb.ev.allocation."+uid {
					t.Fatalf("%s is on %q", m.ID, m.Subject)
				}
			}
			if notLogged != 1 {
				t.Fatalf("the log holds %d EventsNotLogged after its return, want exactly one", notLogged)
			}
			ids := idsSorted(ms)
			for i := 1; i < len(ids); i++ {
				if ids[i] == ids[i-1] {
					t.Fatalf("id %s is in the stream twice", ids[i])
				}
			}
		})
	}
}

func idsSorted(ms []nodetest.Message) []string {
	out := ids(ms)
	sort.Strings(out)
	return out
}
