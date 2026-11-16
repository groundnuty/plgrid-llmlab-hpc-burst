package lifecycle

// A cut-short outranking (HBEP-7, Test plan, "A pass cut short in an outranking"; How it counts, What step 7 takes;
// #616, PR 3). X is chosen and ready, and P outranks it at 17.9 ms against 20 ms. The writes go in a fixed order:
// X's ranked-out mark naming P, then P's becomeReady, then, once P has been ready for the front's probe interval, X's
// drain. P's hard end, 20:10:20, is nearer than X's, 20:15:20, so without the mark step 7 would keep X, the later hard
// end, and drain P: the selection reads the mark, and a marked X is surplus once P is ready.
//
// The fixture: X's job starts at 00:15 and reaches its point at 00:40 measuring 20 ms; P's starts at 00:10, reaches
// its point at 00:45 measuring 17.9 ms, so P is the candidate.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// writeLog records, in order, the writes of the ranking: "mark" for X's ranked-out annotation, "ready" for P's
// becoming ready, and fails P's ready write while failReady is above 0.
type writeLog struct {
	mu        sync.Mutex
	events    []string
	failReady int
	candidate string
}

func (l *writeLog) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if in, ok := obj.(*v1alpha1.Instance); ok && in.Annotations[annotationRankedOut] != "" {
				l.mu.Lock()
				l.events = append(l.events, "mark")
				l.mu.Unlock()
			}
			return c.Update(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == l.candidate && in.Status.Phase == v1alpha1.InstanceReady && sub == "status" {
				l.mu.Lock()
				defer l.mu.Unlock()
				if l.failReady > 0 {
					l.failReady--
					return errors.New("injected: the candidate's ready write failed")
				}
				l.events = append(l.events, "ready")
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
}

func (l *writeLog) log() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// rkCutShort starts the row: X is chosen and ready at 00:40, and the rig stops at 00:44:50, ten seconds before P's
// ranking point. The candidate's name is told to the log so it can fail its ready write.
func rkCutShort(t *testing.T, l *writeLog) (h *rkRig, p, x string) {
	h = newRkRig(t, false, withInterceptors(l.funcs()))
	p, x = h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
	l.candidate = p
	// X serves a stream from 00:45:00 to 00:47:00, so once marked it drains for two minutes before it ends.
	busyFrom, busyTo := rkAt(0, 45, 0), rkAt(0, 47, 0)
	h.health[h.alloc(x)] = func(now time.Time, proc afake.Process) agentapi.Health {
		hl := afake.DefaultHealth(now, proc)
		if !now.Before(busyFrom) && now.Before(busyTo) {
			hl.RunningRequests, hl.GeneratedTokens = agentapi.Int64(4), agentapi.Int64(now.Unix())
		}
		return hl
	}
	h.engineOf(x, rk200, 24*time.Minute) // 08:15:20 + 24:00 + 40 s = 08:40:00
	h.engineOf(p, rk179, 34*time.Minute) // 08:10:20 + 34:00 + 40 s = 08:45:00
	h.stepTo(rkAt(0, 44, 50), 10*time.Second, nil)
	rkCalibration(t, h, p, x)
	if got := h.readyAt(x); !got.Equal(rkAt(0, 40, 0)) {
		t.Fatalf("X ready at %v, want 00:40", got)
	}
	return h, p, x
}

// TestACutShortOutrankingKeepsBothReadyAndDrainsTheMarkedPeer: P outranks X at 00:45:00. Through the 10 s window,
// every pass, once a second, keeps both ready and drains neither; X carries the mark. At 00:45:10, the front's probe
// interval after P became ready, X drains, never P. The writes went in order: X's mark, then P's ready.
func TestACutShortOutrankingKeepsBothReadyAndDrainsTheMarkedPeer(t *testing.T) {
	l := &writeLog{}
	h, p, x := rkCutShort(t, l)
	h.tick(10 * time.Second) // 00:45:00
	if got := h.readyAt(p); !got.Equal(rkAt(0, 45, 0)) {
		t.Fatalf("P ready at %v, want 00:45:00", got)
	}
	if w := l.log(); len(w) != 2 || w[0] != "mark" || w[1] != "ready" {
		t.Fatalf("writes %v, want X's mark and then P's ready, in that order", w)
	}
	h.stepTo(rkAt(0, 45, 9), time.Second, func() {
		for _, n := range []string{p, x} {
			if ph := hoPhase(h.rig, n); ph != v1alpha1.InstanceReady {
				t.Fatalf("%s is %s at %s, want both ready through the window to 00:45:10", n, ph, h.clock.Now().Format(time.TimeOnly))
			}
		}
		if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p {
			t.Fatalf("X's mark %q at %s, want %q", got, h.clock.Now().Format(time.TimeOnly), p)
		}
	})
	h.tick(time.Second) // 00:45:10
	if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceDraining {
		t.Fatalf("X is %s at 00:45:10, want draining", ph)
	}
	h.run(2*time.Minute, 10*time.Second)
	if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceReady {
		t.Fatalf("P is %s, want ready throughout: it is the one that serves", ph)
	}
	xi := h.mustInstance(x)
	if xi.Status.Phase != v1alpha1.InstanceEnded || xi.Status.EndReason != v1alpha1.ReasonRankedOut || !h.kept(x) {
		t.Fatalf("X is %s, ended for %q, Allocation kept %v: want ended for RankedOut with its Allocation kept", xi.Status.Phase, xi.Status.EndReason, h.kept(x))
	}
	if evs := h.rankedOutEvents(x); len(evs) != 1 || evs[0].RelatedName != p {
		t.Fatalf("RankedOut events regarding X: %+v, want one with %s related", evs, p)
	}
}

// TestAFailedReadyWriteIsFinishedByTheNextPass: P's becomeReady write fails at 00:45:00, after X's mark. The pass
// reports the error, X carries the mark and is still chosen and ready, and nothing drains. In the next pass, 00:45:10,
// P becomes ready (the mark is already written), and X drains when due, the probe interval later, 00:45:20.
func TestAFailedReadyWriteIsFinishedByTheNextPass(t *testing.T) {
	l := &writeLog{failReady: 1}
	h, p, x := rkCutShort(t, l)
	h.allowErr = true
	h.tick(10 * time.Second) // 00:45:00: the mark, then P's failed ready
	if h.passErr == nil {
		t.Fatal("the pass reported no error, want the injected failed ready write")
	}
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p {
		t.Fatalf("X's mark %q after the failed pass, want %q: the mark is written before P becomes ready", got, p)
	}
	if h.mustInstance(p).Status.Phase == v1alpha1.InstanceReady || hoPhase(h.rig, x) != v1alpha1.InstanceReady {
		t.Fatalf("P is %s and X %s after the failed pass, want P not ready and X ready", hoPhase(h.rig, p), hoPhase(h.rig, x))
	}
	h.tick(10 * time.Second) // 00:45:10
	if h.passErr != nil {
		t.Fatalf("the next pass failed: %v", h.passErr)
	}
	if got := h.readyAt(p); !got.Equal(rkAt(0, 45, 10)) {
		t.Fatalf("P ready at %v, want 00:45:10, the next pass", got)
	}
	if w := l.log(); len(w) != 2 || w[0] != "mark" || w[1] != "ready" {
		t.Fatalf("writes %v, want one mark and then one ready: the mark is not written twice", w)
	}
	h.stepTo(rkAt(0, 45, 19), time.Second, func() {
		if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceReady {
			t.Fatalf("X is %s at %s, want ready until P has been ready for 10 s", ph, h.clock.Now().Format(time.TimeOnly))
		}
	})
	h.tick(time.Second) // 00:45:20
	if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceDraining {
		t.Fatalf("X is %s at 00:45:20, want draining", ph)
	}
	if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceReady {
		t.Fatalf("P is %s, want ready", ph)
	}
}

// TestADrainingPeerKeepsItsMarkWhenItsCandidateRestarts: X drains from 00:45:10 with a stream in flight to 00:47:00.
// P, which outranked it, has its worker exit at 00:45:20 and is restarted at rung 1, so it is no longer passed. X's
// mark stays: a drain cannot be undone, and X ends RankedOut as it began, not Drained.
func TestADrainingPeerKeepsItsMarkWhenItsCandidateRestarts(t *testing.T) {
	l := &writeLog{}
	h, p, x := rkCutShort(t, l)
	h.tick(10 * time.Second) // 00:45:00: P is ready, X marked
	h.stepTo(rkAt(0, 45, 10), 10*time.Second, nil)
	if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceDraining {
		t.Fatalf("X is %s at 00:45:10, want draining", ph)
	}
	h.agentOf(h.alloc(p)).agent.CrashWorker("worker-0", 1)
	h.tick(10 * time.Second) // 00:45:20: P's part has exited and P restarts
	if n := len(h.mustInstance(p).Status.Restarts); n != 1 {
		t.Fatalf("P has %d restarts, want its rung-1 restart", n)
	}
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p {
		t.Fatalf("X's mark %q after P restarted, want %q: X already drains", got, p)
	}
	h.until(5*time.Minute, 10*time.Second, "X ended", func() bool { return ended(h.mustInstance(x)) })
	if xi := h.mustInstance(x); xi.Status.EndReason != v1alpha1.ReasonRankedOut {
		t.Fatalf("X ended for %q, want RankedOut, the reason it began draining for", xi.Status.EndReason)
	}
}

// TestAMarkIsClearedWhenItsCandidateRestarts: P's ready write fails after X's mark, and then P's worker exits and is
// restarted at rung 1, so P is no longer passed. The next pass clears X's mark: X stays chosen and ready, and nothing
// drains, in the minutes after. Without the clearing, P's later failure would leave X marked and surplus.
func TestAMarkIsClearedWhenItsCandidateRestarts(t *testing.T) {
	l := &writeLog{failReady: 1}
	h, p, x := rkCutShort(t, l)
	h.allowErr = true
	h.tick(10 * time.Second) // 00:45:00: the mark, then P's failed ready
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p {
		t.Fatalf("X's mark %q, want %q before the restart", got, p)
	}
	h.agentOf(h.alloc(p)).agent.CrashWorker("worker-0", 1)
	h.tick(10 * time.Second) // 00:45:10: P's part has exited
	if h.passErr != nil {
		t.Fatalf("the pass failed: %v", h.passErr)
	}
	if n := len(h.mustInstance(p).Status.Restarts); n != 1 {
		t.Fatalf("P has %d restarts, want its rung-1 restart", n)
	}
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != "" {
		t.Fatalf("X's mark is %q after P restarted, want it cleared", got)
	}
	h.stepTo(rkAt(0, 47, 0), 10*time.Second, func() {
		if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceReady {
			t.Fatalf("X is %s at %s, want ready: nothing drains without a ready candidate", ph, h.clock.Now().Format(time.TimeOnly))
		}
		if h.mustInstance(p).Status.ReadyTime != nil {
			t.Fatalf("P became ready at %s, want it restarting", h.clock.Now().Format(time.TimeOnly))
		}
	})
}
