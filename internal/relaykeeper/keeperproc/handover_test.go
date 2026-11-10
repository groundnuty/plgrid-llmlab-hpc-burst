package keeperproc

// HBEP-6's keeper items for a controller that dies or hands over, and for the
// keeper that dies (Test plan, The relay keeper): "a controller restart and a
// leadership hand-over while streams run cut no serving stream, and the
// relays' E doesn't change", and the Signals rule, "Its relays' ssh processes
// are its own children, started ... with Pdeathsig=SIGKILL, so they die with
// the keeper and never with a controller". The expected values are the
// HBEP's: E is 2^31 plus the keeper's start count, 1 here.

import (
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// firstE is a fresh state directory's first keeper's E: 2^31 plus a start
// count of 1 (HBEP-6, Fencing).
const firstE = int64(1)<<31 + 1

// leading is a test's start: one job, a keeper on a new state directory, a
// leading double at Lease epoch 1 whose complete set names the job, both
// relays up, and a serving stream across them.
type leading struct {
	w    *world
	j    job
	k    *keeper
	lead *double
	up   map[agentapi.RelayChannel]wire.RelayState
	s    *stream
}

func startLeading(t *testing.T, tag string) *leading {
	t.Helper()
	w := newWorld(t)
	j := w.submit(tag, 30*time.Second)
	k := w.startKeeper("")
	if k.epoch != firstE {
		t.Fatalf("a new state directory's keeper took E %d, want 2^31+1 = %d", k.epoch, firstE)
	}
	lead := w.startDouble("leader", k.sock)
	if r := mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j, gen1))), "the leader's set"); r.KeeperEpoch != firstE {
		t.Fatalf("the report names E %d, want %d", r.KeeperEpoch, firstE)
	}
	up := k.waitUp(provA, j)
	s := w.openStream(j, serving)
	s.mustEcho(64<<10, "before the event")
	return &leading{w: w, j: j, k: k, lead: lead, up: up, s: s}
}

// A double SIGKILLed, then a hand-over: the leader dies (KILL through its
// pidfd, possibly mid-exchange), and a second double of the same Provider,
// with a higher Lease epoch, sends its incomplete set (no Allocation observed
// yet), then its complete one, while the serving stream runs. The stream is
// not cut, and the new leader adopts the relays: their ssh PIDs, E and N are
// unchanged in the keeper's status, the job side accepted no new session and
// refused none, and the keeper's log shows no start of the Allocation's
// relays during the hand-over, nor any start refused at its gate.
//
// Mutant (ownerrunner.go, want): the identity check "if in := p.cur[e.Tag];
// in != nil && in.want.Entry == e && ..." made false, so a new leader's Want
// replaces the relays: their PIDs and N change and the stream is cut.
func TestAHandOverAfterAKilledLeaderCutsNoStream(t *testing.T) {
	l := startLeading(t, "hb-kp-hand-0123456789ab")
	before := l.j.status(t)
	m := l.k.log.mark()
	l.lead.kill()
	l.s.mustEcho(32<<10, "after the leader's KILL")
	nu := l.w.startDouble("new-leader", l.k.sock)
	r := mustReport(t, nu.set(l.w.set(provA, 2, false)), "the new leader's incomplete set")
	if r.KeeperEpoch != firstE {
		t.Errorf("the report names E %d, want %d", r.KeeperEpoch, firstE)
	}
	l.s.mustEcho(32<<10, "after the new leader's incomplete set")
	mustReport(t, nu.lead(l.w.set(provA, 2, true, l.w.entry(l.j, gen1))), "the new leader's complete set")
	for range 4 {
		l.s.mustEcho(32<<10, "after the new leader's complete set")
	}
	for _, ch := range agentapi.RelayChannels {
		r, ok := l.k.upOf(provA, l.j.tag, ch)
		switch {
		case !ok:
			t.Errorf("the %s relay is not up after the hand-over", ch)
		case r.PID != l.up[ch].PID || r.Epoch != l.up[ch].Epoch || r.Seq != l.up[ch].Seq || r.Starts != 1:
			t.Errorf("the %s relay after the hand-over: PID %d, (E, N) (%d, %d), %d starts; want it adopted: PID %d, (%d, %d), 1 start",
				ch, r.PID, r.Epoch, r.Seq, r.Starts, l.up[ch].PID, l.up[ch].Epoch, l.up[ch].Seq)
		case r.Epoch != firstE:
			t.Errorf("the %s relay's E is %d, want %d", ch, r.Epoch, firstE)
		}
		for _, msg := range []string{logStarted, logNotStarted} {
			if got := l.k.log.since(m, relayLog(msg, l.j.tag, ch)...); len(got) != 0 {
				t.Errorf("the keeper's log during the hand-over, the %s relay: %q; want no start and no refused one", ch, got)
			}
		}
	}
	if got := l.k.log.since(m, whyGate); len(got) != 0 {
		t.Errorf("a start was refused at the keeper's gate during the hand-over: %q", got)
	}
	after := l.j.status(t)
	for _, ch := range agentapi.RelayChannels {
		if after[ch] != before[ch] || !after[ch].Connected || after[ch].Sessions != 1 {
			t.Errorf("the job side's %s channel %+v after the hand-over, want the one session it kept, %+v", ch, after[ch], before[ch])
		}
	}
	l.s.whole()
}

// The keeper SIGKILLed: its relays' ssh die by Pdeathsig, and the stream is
// cut. The test holds a pidfd on each ssh, pinned and verified as the
// keeper's child (test, keeper, ssh), and first STOPs each through its pin:
// a stopped ssh cannot exit on its own, on its input's end or its pipe's, so
// only a signal that ends a stopped process, KILL, ends it once the keeper
// is gone; nothing else sends one. Each ssh is in the keeper's process group,
// with no group of its own (HBEP-6, Signals: the keeper's relays lead no
// group).
//
// Mutants: hostdial/pidfd.go's "Pdeathsig: syscall.SIGKILL" dropped (outside
// this PR's files: the stopped ssh outlive the keeper); ownerrunner.go's
// "Pidfd: true" made false (each ssh leads a group of its own, so the group
// check fails).
func TestAKilledKeepersRelaysDieByPdeathsig(t *testing.T) {
	l := startLeading(t, "hb-kp-pdks-0123456789ab")
	pins := l.k.pins(l.up)
	kpg, err := syscall.Getpgid(l.k.c.pid)
	if err != nil {
		t.Fatal(err)
	}
	for ch, p := range pins {
		if pg, err := syscall.Getpgid(p.pid); err != nil || pg != kpg || pg == p.pid {
			t.Errorf("the %s relay's ssh %d is in process group %d (%v), want the keeper's, %d, and none of its own", ch, p.pid, pg, err, kpg)
		}
	}
	for ch, p := range pins {
		if err := p.signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("STOP the %s relay's ssh through its pin: %v", ch, err)
		}
		waitFor(t, 30*time.Second, "the "+string(ch)+" relay's ssh stopped", func() bool { return procState(p.pid) == "T" })
	}
	l.k.c.kill(t)
	for ch, p := range pins {
		p.waitExit(t, 30*time.Second, "the stopped "+string(ch)+" relay's ssh after the keeper's KILL")
	}
	if !l.s.cut() {
		t.Error("the serving stream still echoes after the keeper and its relays died")
	}
}

// A double SIGKILLed instead: the relays are the keeper's, so they survive,
// and the stream goes on. The relays' ssh, pinned and verified as the
// keeper's children, are alive after the double's exit and after the stream's
// next bytes, with the same PIDs and one start each in the keeper's status.
//
// Mutant: none in keeper.go, ownerrunner.go or process.go: no line there
// ties a relay to a controller's connection. This is the control of
// TestAKilledKeepersRelaysDieByPdeathsig: it fails if the relays were the
// double's, as an alpha controller's are its own.
func TestAKilledDoubleLeavesTheRelaysAndTheirStream(t *testing.T) {
	l := startLeading(t, "hb-kp-pdcd-0123456789ab")
	pins := l.k.pins(l.up)
	l.lead.kill()
	for range 4 {
		l.s.mustEcho(32<<10, "after the leader's KILL")
	}
	for ch, p := range pins {
		if p.exited() {
			t.Errorf("the %s relay's ssh %d exited with the double", ch, p.pid)
		}
		if r, ok := l.k.upOf(provA, l.j.tag, ch); !ok || r.PID != p.pid || r.Starts != 1 {
			t.Errorf("the %s relay %+v after the double's KILL, want it up as it was, PID %d", ch, r, p.pid)
		}
	}
	l.w.roundTrip(l.j, control, 16<<10)
	l.s.whole()
}
