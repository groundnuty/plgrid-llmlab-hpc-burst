package relaykeeper

// New for issue #343 (PR 3): an old generation's attempt that passed the
// gate before its replacement was accepted, and launches after (path 5 (b),
// ruling B's case; the science agent's check item 2, in both connect
// orders), and the Runner's duty 4 for such an attempt. The old attempt's
// ssh is made to linger past its stop's TERM (lingeringSSH), so it is still
// there, and connecting, when the replacement's relay connects, as a
// process the stop has not yet ended would be.

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// jobLogHas reports whether the job side's log has a line with every part.
func (w *rkWorld) jobLogHas(jobID string, parts ...string) bool {
	for _, l := range strings.Split(w.jobLog(jobID), "\n") {
		ok := true
		for _, p := range parts {
			if !strings.Contains(l, p) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// sessionUp is the job side's line for a channel's session of (E, N).
func sessionUp(ch agentapi.RelayChannel, epoch, seq int64, replaced bool) []string {
	return []string{`msg="relay session up"`, "channel=" + string(ch) + " ", " epoch=" + strconv.FormatInt(epoch, 10) + " seq=" + strconv.FormatInt(seq, 10) + " replaced=" + strconv.FormatBool(replaced)}
}

// supersededLine is the job side's line for a refused relay of (E, N).
func supersededLine(ch agentapi.RelayChannel, epoch, seq int64) []string {
	return []string{`msg="a relay was refused as superseded"`, "channel=" + string(ch) + " ", " epoch=" + strconv.FormatInt(epoch, 10) + " seq=" + strconv.FormatInt(seq, 10) + " "}
}

// startedPIDs are the PIDs the Owner's log gives the ssh of each N.
func (rg *realRig) startedPIDs() map[string]int {
	out := map[string]int{}
	for _, r := range rg.logs.records("relay started") {
		pid, _ := strconv.Atoi(r["pid"])
		out[r["seq"]] = pid
	}
	return out
}

// Path 5 (b) and check item 2, in both connect orders. Both channels of
// generation one pass the gate (the keeper holds their starts) and are held
// before the process start; generation two is accepted.
//
//   - The old attempt connects first: the Runner's worker is held in
//     generation two's read of the seeds, so the old relays are not stopped
//     yet; they launch, connect and carry bytes. Then the worker goes on:
//     generation two's relays start with higher N, connect, and the job side
//     replaces the old sessions with them.
//   - The replacement connects first: generation two's relays start, connect
//     and carry bytes; then the old attempts launch, with their lower N, and
//     the job side refuses them as superseded.
//
// In both, the current relay is undisturbed once the old processes are gone
// (bytes cross, its PID unchanged, one start), each start is resolved once,
// the keeper holds nothing, nothing is recorded or reported (no end in the
// state file: hostdial tells no end of a relay its stop cancelled), and the
// host is freed afterwards.
func TestTheOldAttemptAndItsReplacementInBothConnectOrders(t *testing.T) {
	for _, oldFirst := range []bool{true, false} {
		name := "the replacement connects first"
		if oldFirst {
			name = "the old attempt connects first"
		}
		t.Run(name, func(t *testing.T) {
			w := newRKWorld(t)
			j := w.submit("hb-rk-order-0123456789ab", 30*time.Second)
			ssh := w.lingeringSSH()
			rg := newRealRig(t, w, rigOptions{runner: func(o *OwnerRunnerOptions) { o.SSHBinary, o.StopGrace = ssh, 10*time.Second }})
			gate, seeds := newLatch(), newLatch()
			rg.plan(func(hw *hookedWant) {
				switch hw.w.Entry.Gen {
				case genOne:
					hw.afterGate[agentapi.RelayControl], hw.afterGate[agentapi.RelayServing] = gate, gate
					hw.awaitTrap = true
				case genTwo:
					if oldFirst {
						hw.finals = seeds
					}
				}
			})
			accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
			old := rg.hooked(j.tag, genOne)
			rkWaitFor(t, time.Minute, "both old starts allowed and held", func() bool {
				return old.waitingAtGate(agentapi.RelayControl) == 1 && old.waitingAtGate(agentapi.RelayServing) == 1
			})
			if n := rg.holds(); n != 2 {
				t.Fatalf("the keeper holds %d starts with both old ones allowed, want 2", n)
			}
			oldN := map[agentapi.RelayChannel]int64{}
			for _, g := range old.gatesOf() {
				oldN[g.ch] = g.seq
				w.lingerSeq(g.seq)
			}
			E := rg.k.Epoch()
			accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genTwo)))
			newer := func(r wire.RelayState) bool { return r.Seq > oldN[r.Channel] }
			var nu map[agentapi.RelayChannel]wire.RelayState
			if oldFirst {
				gate.open()
				rkWaitFor(t, time.Minute, "both old attempts launched", func() bool { return len(old.startsOf()) == 2 })
				for _, ch := range agentapi.RelayChannels {
					rkWaitFor(t, time.Minute, "the old "+string(ch)+" session up", func() bool { return w.jobLogHas(j.id, sessionUp(ch, E, oldN[ch], false)...) })
					rkRoundTrip(t, j, ch, 16<<10)
					if r := rg.ownerRelay(rkProv, j.tag, ch); !strings.HasPrefix(r.Gen, genOne+"/") || r.Joined == 0 {
						t.Errorf("the bytes did not cross the old %s relay: %+v", ch, r)
					}
				}
				seeds.open()
				nu = rg.waitPairWhere(rkProv, j.tag, newer)
				for ch, r := range nu {
					// The job side keeps the higher (E, N): the replacement's
					// session is up, whether the old one was still there to be
					// replaced or the Runner's stop had already closed it.
					rkWaitFor(t, time.Minute, "the replacement's "+string(ch)+" session up", func() bool {
						return w.jobLogHas(j.id, sessionUp(ch, E, r.Seq, true)...) || w.jobLogHas(j.id, sessionUp(ch, E, r.Seq, false)...)
					})
				}
			} else {
				nu = rg.waitPairWhere(rkProv, j.tag, newer)
				for _, ch := range agentapi.RelayChannels {
					rkWaitFor(t, time.Minute, "the replacement's "+string(ch)+" session up", func() bool { return w.jobLogHas(j.id, sessionUp(ch, E, nu[ch].Seq, false)...) })
					rkRoundTrip(t, j, ch, 16<<10)
				}
				gate.open()
				rkWaitFor(t, time.Minute, "both old attempts launched", func() bool { return len(old.startsOf()) == 2 })
				for _, ch := range agentapi.RelayChannels {
					rkWaitFor(t, time.Minute, "the job side refusing the old "+string(ch)+" relay", func() bool { return w.jobLogHas(j.id, supersededLine(ch, E, oldN[ch])...) })
					if s := w.srunsWithSeq(j.id, oldN[ch]); len(s) != 1 {
						t.Errorf("the old %s attempt's srun lines: %q, want one: it launched", ch, s)
					}
				}
			}
			for _, s := range old.startsOf() {
				if s.seq != oldN[s.ch] || s.epoch != E || !s.shown {
					t.Errorf("the old %s start: %+v, want the gate's N %d, and listed up before the keeper was told", s.ch, s, oldN[s.ch])
				}
			}
			pids := rg.startedPIDs()
			for ch, n := range oldN {
				pid := pids[strconv.FormatInt(n, 10)]
				rkWaitFor(t, time.Minute, "the old "+string(ch)+" ssh gone", func() bool { return pid != 0 && !rkAlive(pid) })
			}
			rkWaitFor(t, time.Minute, "the old relays no longer listed", func() bool { return len(rg.upOf(rkProv)) == 2 })
			for _, ch := range agentapi.RelayChannels {
				rkRoundTrip(t, j, ch, 16<<10)
				r := rg.ownerRelay(rkProv, j.tag, ch)
				if r.PID != nu[ch].PID || r.Starts != 1 || r.Seq != nu[ch].Seq || r.Final {
					t.Errorf("the current %s relay was disturbed: %+v, was PID %d N %d with one start", ch, r, nu[ch].PID, nu[ch].Seq)
				}
			}
			if n := rg.doubleResolutions(); n != 0 {
				t.Errorf("the keeper saw %d resolutions with none in flight, want 0", n)
			}
			if n := rg.holds(); n != 0 {
				t.Errorf("the keeper holds %d starts, want 0", n)
			}
			rg.noEnds()
			rg.freeAfter()
		})
	}
}

// The Runner's duty 4: it keeps listing a replaced relay while a start of
// its is in flight, so the process that start launches is listed up once it
// runs. Generation one's control relay is up; its serving start passes the
// gate and is held before the process start. Generation two is accepted, so
// generation one's control relay is stopped, and its process exits. Then
// the held serving start launches: the Runner must list it up, with the N
// the gate saw, before the keeper's OnStart releases the start's hold, or
// the keeper counts no relay of it on the host while its process runs.
func TestAReplacedRelaysStartInFlightIsListedOnceItLaunches(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-inflt-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	gate := newLatch()
	rg.plan(func(hw *hookedWant) {
		if hw.w.Entry.Gen == genOne {
			hw.afterGate[agentapi.RelayServing] = gate
		}
	})
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	old := rg.hooked(j.tag, genOne)
	var ctl wire.RelayState
	rkWaitFor(t, time.Minute, "the old control relay up and the old serving start held", func() bool {
		for _, r := range rg.upOf(rkProv) {
			if r.Channel == agentapi.RelayControl && r.PID != 0 {
				ctl = r
			}
		}
		return ctl.PID != 0 && old.waitingAtGate(agentapi.RelayServing) == 1
	})
	rkRoundTrip(t, j, agentapi.RelayControl, 16<<10)
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genTwo)))
	rkWaitFor(t, time.Minute, "the old control relay's process gone and no longer listed", func() bool {
		if rkAlive(ctl.PID) {
			return false
		}
		for _, r := range rg.upOf(rkProv) {
			if r.PID == ctl.PID {
				return false
			}
		}
		return true
	})
	gate.open()
	var s startCall
	rkWaitFor(t, time.Minute, "the held serving start launched", func() bool {
		for _, st := range old.startsOf() {
			if st.ch == agentapi.RelayServing {
				s = st
				return true
			}
		}
		return false
	})
	if !s.shown {
		t.Errorf("the replaced relay's serving process (N %d) was not listed up when the keeper was told it started: the keeper's count of relays on the host misses a running process (duty 4)", s.seq)
	}
	rg.waitPairWhere(rkProv, j.tag, func(r wire.RelayState) bool {
		return r.Seq > s.seq || r.Channel == agentapi.RelayControl && r.Seq > ctl.Seq
	})
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, j, ch, 16<<10)
	}
	rg.noEnds()
	rg.freeAfter()
}
