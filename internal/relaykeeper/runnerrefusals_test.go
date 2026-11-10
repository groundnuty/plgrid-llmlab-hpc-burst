package relaykeeper

// New for issue #343 (PR 3): the starts that never happen on the real path,
// and that hold nothing: each refusal of the keeper's listing (path 2), a
// relay line or an ssh command that fails after the listing passed, so
// before the gate (paths 3 and 4), and a process start that fails after the
// gate allowed it (path 6). The listing holds nothing, and neither does an
// attempt that never reaches the gate; a start the gate allowed holds its
// host until StartFailed resolves it.

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// startedNothing fails the test when hw's attempts reached the gate or
// started a process, or the keeper holds a start of hw's relays. Another
// Want's starts (a replacement's, or a moved Want's on its new host) are not
// hw's.
func startedNothing(t *testing.T, rg *realRig, hw *hookedWant) {
	t.Helper()
	if g := hw.gatesOf(); len(g) != 0 {
		t.Errorf("an attempt reached the gate: %+v", g)
	}
	if s := hw.startsOf(); len(s) != 0 {
		t.Errorf("a process started: %+v", s)
	}
	rg.k.mu.Lock()
	defer rg.k.mu.Unlock()
	for key := range rg.k.inflight {
		if key.tag == hw.w.Entry.Tag && key.gen == hw.w.Entry.Gen && key.host == rg.k.runner.LoginHost(hw.p, rg.w.ssh()) {
			t.Errorf("the keeper holds a start of the Want's relays: %+v", key)
		}
	}
}

// Path 2. Each refusal of the keeper's listing starts nothing and holds
// nothing: no attempt reaches the gate, no process starts, the fake runs no
// srun line, the keeper holds no start, and the host is freed afterwards.
// Each case holds the Allocation's first listings before the keeper's
// (beforeList), changes one condition, and lets them go. The control case
// changes nothing, and its relays start and carry bytes.
func TestEachListingRefusalStartsNothingAndHoldsNothing(t *testing.T) {
	type world struct {
		w  *rkWorld
		rg *realRig
		j  rkJob
		e  wire.Entry
	}
	cases := []struct {
		name string
		// change changes one condition while the listings wait; it returns
		// the refusal the Owner should show, "" when the case refuses no
		// listing, and how many refusals to wait for.
		change func(t *testing.T, x world) (refusal string, refusals int)
	}{
		{"control: nothing changes", func(*testing.T, world) (string, int) { return "", 0 }},
		{"an expired set", func(t *testing.T, x world) (string, int) {
			x.rg.clock.advance(wire.MaxValidFor + time.Second)
			return "desired set has expired", 4
		}},
		{"the Provider moved to another host", func(t *testing.T, x world) (string, int) {
			moved := x.rg.set(rkProv, true, x.e)
			moved.SSH.HostName = "127.0.0.2"
			accepted(t, x.rg.k, moved)
			return "", 2 // the moved Want replaces the old relays: their blocked attempts are their last
		}},
		{"the claim lost", func(t *testing.T, x world) (string, int) {
			x.rg.k.mu.Lock()
			host := x.rg.k.sets[rkProv].host
			c := x.rg.k.st.Claims[host]
			lost := c
			lost.Provider = rkOther
			x.rg.k.st.Claims[host] = lost
			x.rg.k.mu.Unlock()
			x.rg.restoreClaim = func() {
				x.rg.k.mu.Lock()
				x.rg.k.st.Claims[host] = c
				x.rg.k.mu.Unlock()
			}
			return "not listed under tag", 4
		}},
		{"the Allocation marked endedForGood", func(t *testing.T, x world) (string, int) {
			marked := x.e
			marked.EndedForGood = true
			accepted(t, x.rg.k, x.rg.set(rkProv, true, marked))
			return "not listed under tag", 4
		}},
		{"a held final end", func(t *testing.T, x world) (string, int) {
			x.rg.k.onFinal(rkProv, x.e)(agentapi.RelayServing, hostdial.EndFailed, "an end the test holds", x.rg.k.Epoch(), 1)
			if n := len(x.rg.ends()); n != 1 {
				t.Fatalf("the state file holds %d ends after the injected one, want 1", n)
			}
			return "not listed under tag", 4
		}},
		{"a Want no longer current", func(t *testing.T, x world) (string, int) {
			accepted(t, x.rg.k, x.rg.set(rkProv, true, x.w.entry(x.j, genTwo)))
			return "", 2 // replaced: the blocked attempts are the old relays' last
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newRKWorld(t)
			j := w.submit("hb-rk-list-0123456789ab", 30*time.Second)
			rg := newRealRig(t, w, rigOptions{})
			hold := newLatch()
			rg.plan(func(hw *hookedWant) {
				if hw.n == 0 {
					hw.beforeList = hold
				}
			})
			e := w.entry(j, genOne)
			accepted(t, rg.k, rg.set(rkProv, true, e))
			hw := rg.hooked(j.tag, genOne)
			rkWaitFor(t, time.Minute, "both channels' first listings waiting", func() bool { return hw.count(func() int { return hw.lists }) == 2 })
			refusal, n := c.change(t, world{w: w, rg: rg, j: j, e: e})
			hold.open()
			if n == 0 {
				rg.waitPair(rkProv, j.tag)
				for _, ch := range agentapi.RelayChannels {
					rkRoundTrip(t, j, ch, 16<<10)
				}
				rg.freeAfter()
				return
			}
			rkWaitFor(t, time.Minute, "the listings refused", func() bool { return hw.count(func() int { return hw.refusals }) >= n })
			if l := hw.count(func() int { return hw.listed }); l != 0 {
				t.Errorf("%d listings showed the job, want none", l)
			}
			startedNothing(t, rg, hw)
			if refusal != "" {
				for _, ch := range agentapi.RelayChannels {
					if r := rg.ownerRelay(rkProv, j.tag, ch); r.Starts != 0 || r.PID != 0 || r.Final || !strings.Contains(r.LastRefusal, refusal) {
						t.Errorf("the %s relay: %+v, want no start, not final, refused by %q", ch, r, refusal)
					}
				}
			}
			if c.name == "a Want no longer current" {
				nu := rg.hooked(j.tag, genTwo)
				rg.waitPair(rkProv, j.tag)
				for _, ch := range agentapi.RelayChannels {
					rkRoundTrip(t, j, ch, 16<<10)
				}
				var allowed []string
				for _, g := range nu.gatesOf() {
					if g.ok {
						allowed = append(allowed, " --seq "+itoa64(g.seq))
					}
				}
				for _, l := range w.sruns(j.id) {
					if !slices.ContainsFunc(allowed, func(s string) bool { return strings.HasSuffix(l, s) }) {
						t.Errorf("the fake ran %q, not a start the replacement's gate allowed", l)
					}
				}
			} else if s := w.sruns(j.id); len(s) != 0 {
				t.Errorf("the fake ran srun for the job: %q", s)
			}
			if rg.restoreClaim != nil {
				rg.restoreClaim()
			}
			if c.name != "a held final end" {
				rg.noEnds()
			}
			rg.freeAfter()
		})
	}
}

// Path 2: the listing with no set since the keeper started. The keeper
// makes a Want only from a set, so this Want is built as handleSet builds
// it, from the keeper's own callbacks, and handed to the Runner with no set:
// its listings fail, it starts nothing and holds nothing.
func TestAListingWithNoSetSinceTheKeeperStartedStartsNothing(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-noset-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	e := w.entry(j, genOne)
	s := rg.set(rkProv, true, e)
	host := rg.host()
	k := rg.k
	rg.hr.Want(rkProv, s.SSH, Want{Entry: e, ProviderUID: s.ProviderUID, Finals: k.finalsOf(rkProv, e),
		Jobs:    jobs{k: k, provider: rkProv, uid: s.ProviderUID, host: host, entry: e},
		OnFinal: k.onFinal(rkProv, e), OnStart: k.onStart(rkProv, s.ProviderUID, host, e),
		BeforeStart: k.beforeStart(rkProv, s.ProviderUID, host, e), StartFailed: k.startFailed(rkProv, s.ProviderUID, host, e)})
	hw := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "four refused listings", func() bool { return hw.count(func() int { return hw.refusals }) >= 4 })
	startedNothing(t, rg, hw)
	if s := w.sruns(j.id); len(s) != 0 {
		t.Errorf("the fake ran srun for the job: %q", s)
	}
	for _, ch := range agentapi.RelayChannels {
		if r := rg.ownerRelay(rkProv, j.tag, ch); r.Starts != 0 || r.Final || !strings.Contains(r.LastRefusal, "no desired set") {
			t.Errorf("the %s relay: %+v, want no start, not final, refused for no set", ch, r)
		}
	}
	rg.noEnds()
	rg.freeAfter()
}

// Paths 3 and 4, and the science agent's check item 3: a start that never
// reaches the gate holds nothing, so the host is free at once. The
// listing passes, and the attempt ends before the gate: a relay line that
// agentapi.RelayLine refuses (final), the Runner's ssh maker closed, or its
// -F file gone and its directory unwritable (both retried). No attempt
// reaches the gate, no process starts, the keeper holds nothing at any
// point, and once rkProv's complete set with no Allocation is in, another
// Provider's set for the host is accepted at once, with no wait.
//
// No entry that wire.DesiredSet.Validate accepts is refused by
// agentapi.RelayLine: both check the job ID, the agent's path, the
// directory and the UID with the same patterns (validate.go, relay.go), and
// the keeper's E and hostdial's N are in range. So the relay line's case is
// injected: its set, whose agent path is relative, goes to the keeper's
// handleSet, past Handle's Validate.
func TestAStartThatNeverReachesTheGateHoldsNothingOnTheRealPath(t *testing.T) {
	cases := []struct {
		name string
		// arm makes the next attempts fail before the gate, while the first
		// listings wait; nil sends the injected set.
		arm func(t *testing.T, rg *realRig)
		// final is the relay's end: the relay line's is final, the ssh
		// command's retried.
		final bool
	}{
		{"a relay line agentapi refuses", nil, true},
		{"the ssh maker closed", func(t *testing.T, rg *realRig) {
			rg.rr.mu.Lock()
			pr := rg.rr.provs[rkProv]
			rg.rr.mu.Unlock()
			pr.mu.Lock()
			defer pr.mu.Unlock()
			for _, rl := range pr.relays {
				if err := rl.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}, false},
		{"the -F file unusable", func(t *testing.T, rg *realRig) {
			dir := filepath.Join(rg.w.base, "k")
			files, err := filepath.Glob(filepath.Join(dir, rkProv+"-*.conf"))
			if err != nil || len(files) != 1 {
				t.Fatalf("the -F file: %v %q", err, files)
			}
			if err := os.Remove(files[0]); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newRKWorld(t)
			j := w.submit("hb-rk-nogate-0123456789ab", 30*time.Second)
			rg := newRealRig(t, w, rigOptions{})
			hold := newLatch()
			rg.plan(func(hw *hookedWant) { hw.afterList = hold })
			e := w.entry(j, genOne)
			if c.arm == nil {
				e.AgentPath = "stage/agent/hb-agent"
				s := rg.set(rkProv, true, e)
				if err := s.Validate(); err == nil {
					t.Fatal("the injected set passes Validate: it is not an injection")
				}
				if kind, body := rg.k.handleSet(s); kind != wire.KindReport {
					t.Fatalf("handleSet: %s %+v", kind, body)
				}
			} else {
				accepted(t, rg.k, rg.set(rkProv, true, e))
			}
			hw := rg.hooked(j.tag, genOne)
			rkWaitFor(t, time.Minute, "both channels' first listings passed and waiting", func() bool { return hw.count(func() int { return hw.afterWait }) == 2 })
			if n := rg.holds(); n != 0 {
				t.Errorf("the keeper holds %d starts with both listings passed, want none: the listing holds nothing", n)
			}
			if c.arm != nil {
				c.arm(t, rg)
			}
			hold.open()
			if c.final {
				rkWaitFor(t, time.Minute, "both relays final", func() bool {
					return rg.ownerRelay(rkProv, j.tag, agentapi.RelayControl).Final && rg.ownerRelay(rkProv, j.tag, agentapi.RelayServing).Final
				})
			} else {
				rkWaitFor(t, time.Minute, "four attempts past the listing", func() bool { return hw.count(func() int { return hw.listed }) >= 4 })
			}
			startedNothing(t, rg, hw)
			if s := w.sruns(j.id); len(s) != 0 {
				t.Errorf("the fake ran srun for the job: %q", s)
			}
			for _, ch := range agentapi.RelayChannels {
				r := rg.ownerRelay(rkProv, j.tag, ch)
				if r.Starts != 0 || r.PID != 0 || r.Final != c.final {
					t.Errorf("the %s relay: %+v, want no start, final %v", ch, r, c.final)
				}
			}
			if c.final {
				// A relay line's failure is final, and needs the mark: one
				// end per channel. Its relay never started, so it has no (E,
				// N) of its own; the record must still be one the keeper's
				// own LoadState accepts, or the keeper cannot start again on
				// its state file.
				var raw []EndRecord
				rkWaitFor(t, time.Minute, "both relay-line ends written", func() bool { raw = rg.rawEnds(); return len(raw) == 2 })
				if _, err := LoadState(rg.state); err != nil {
					t.Errorf("after the relay-line ends %+v the keeper's state file does not load: %v", raw, err)
				}
			} else {
				rg.noEnds()
			}
			accepted(t, rg.k, rg.set(rkProv, true))
			if n := rg.holds(); n != 0 {
				t.Errorf("the keeper holds %d starts, want none", n)
			}
			if !rg.probeAccepted() {
				t.Error("another Provider's set for the host was refused at once after starts that never reached the gate")
			}
		})
	}
}

// Path 6. A process start that fails after the gate allowed it (the ssh
// binary is not there) is resolved with StartFailed, once per allowed start:
// the keeper's hold is released each time, no OnStart is told, no srun line
// runs, nothing is final or recorded, and the host is freed.
func TestAStartProcessFailureResolvesItsHoldAndFreesTheHost(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-nossh-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{runner: func(o *OwnerRunnerOptions) { o.SSHBinary = filepath.Join(w.base, "no-such-ssh") }})
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	hw := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "two failed starts of each channel", func() bool {
		return hw.count(func() int { return min(hw.failed[agentapi.RelayControl], hw.failed[agentapi.RelayServing]) }) >= 2
	})
	accepted(t, rg.k, rg.set(rkProv, true))
	rkWaitFor(t, time.Minute, "the dropped relays gone from the Owner", func() bool {
		return rg.ownerRelay(rkProv, j.tag, agentapi.RelayControl).Tag == "" && rg.ownerRelay(rkProv, j.tag, agentapi.RelayServing).Tag == ""
	})
	rkWaitFor(t, time.Minute, "every allowed start resolved", func() bool {
		allowed, failed := 0, 0
		for _, g := range hw.gatesOf() {
			if g.ok {
				allowed++
			}
		}
		failed = hw.count(func() int { return hw.failed[agentapi.RelayControl] + hw.failed[agentapi.RelayServing] })
		return allowed == failed
	})
	if s := hw.startsOf(); len(s) != 0 {
		t.Errorf("OnStart was told with no process: %+v", s)
	}
	if s := w.sruns(j.id); len(s) != 0 {
		t.Errorf("the fake ran srun: %q", s)
	}
	if n := rg.doubleResolutions(); n != 0 {
		t.Errorf("the keeper saw %d resolutions of a start with none in flight, want 0", n)
	}
	if n := rg.holds(); n != 0 {
		t.Errorf("the keeper holds %d starts after every failed start, want 0", n)
	}
	rg.noEnds()
	if !rg.probeAccepted() {
		t.Error("another Provider's set for the host was refused after every failed start was resolved")
	}
}

// itoa64 is n in decimal.
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
