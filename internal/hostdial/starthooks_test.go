package hostdial_test

// New for issue #343 (PR 3): the start hooks the relay keeper's Runner uses,
// Target.BeforeStart, Target.OnStartFailed and Target.OnExit. Each is pinned
// with a positive control: alpha's relays (the hooks nil) are unchanged, and
// each hook, set, is told at its point and only there.

import (
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// hookLog records the start hooks' calls, in order, per channel.
type hookLog struct {
	mu    sync.Mutex
	calls []string
	// up is what Relays showed of the channel when OnStart was told.
	up map[agentapi.RelayChannel]hostdial.RelayState
	// seq is the N BeforeStart was last asked about, per channel.
	seq map[agentapi.RelayChannel]int64
}

func (h *hookLog) add(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, s)
}

func (h *hookLog) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.calls)
}

// hooked sets the three hooks and OnStart on tg, recording each call; allow
// answers BeforeStart. OnStart also records what own's Relays showed then.
func hooked(tg hostdial.Target, own func() *hostdial.Owner, h *hookLog, allow func() bool) hostdial.Target {
	tg.BeforeStart = func(ch agentapi.RelayChannel, epoch, seq int64) bool {
		h.mu.Lock()
		if h.seq == nil {
			h.seq = map[agentapi.RelayChannel]int64{}
		}
		h.seq[ch] = seq
		h.mu.Unlock()
		ok := allow()
		h.add("before " + string(ch) + " " + strconv.FormatInt(epoch, 10) + " " + strconv.FormatBool(ok))
		return ok
	}
	tg.OnStart = func(ch agentapi.RelayChannel) {
		r := relayOf(own(), tg.Tag, ch)
		h.mu.Lock()
		if h.up == nil {
			h.up = map[agentapi.RelayChannel]hostdial.RelayState{}
		}
		h.up[ch] = r
		h.mu.Unlock()
		h.add("start " + string(ch))
	}
	tg.OnStartFailed = func(ch agentapi.RelayChannel) { h.add("failed " + string(ch)) }
	tg.OnExit = func(ch agentapi.RelayChannel) { h.add("exit " + string(ch)) }
	return tg
}

// count is how many of calls are want.
func count(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}

// A BeforeStart that answers false starts no process: the relay never runs
// its "ssh", and shows the refusal; OnStart, OnStartFailed and OnExit are
// never told. The positive control answers true: the relay starts, and
// OnStart is told after BeforeStart, with the relay up, its PID set and its
// Seq the one BeforeStart was asked about, by then (the keeper's duty 2).
func TestBeforeStartDecidesTheStart(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(strconv.FormatBool(allowed), func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "ran")
			script := writeScript(t, dir, "relay.sh", "echo >> "+marker+"\nread x\n")
			jobs := shellJobs{}
			var own *hostdial.Owner
			h := &hookLog{}
			tg := hooked(shellTarget(1, "hb-relay-bfs-0123456789ab", script, dir, "", jobs), func() *hostdial.Owner { return own }, h, func() bool { return allowed })
			own = shellOwner(t, hostdial.OwnerOptions{})
			own.Want(tg)
			if !allowed {
				waitFor(t, time.Minute, "two refusals of each channel", func() bool {
					c := h.list()
					return count(c, "before control 1 false") >= 2 && count(c, "before serving 1 false") >= 2
				})
				own.Drop(tg.Tag)
				for _, c := range h.list() {
					if c[:6] != "before" {
						t.Errorf("a hook other than BeforeStart was told with every start refused: %q", c)
					}
				}
				if r := relayOf(own, tg.Tag, control); r.PID != 0 {
					t.Errorf("a refused start left a process: %+v", r)
				}
				if _, err := exec.Command("test", "-e", marker).Output(); err == nil {
					t.Error("a refused start ran the relay's ssh")
				}
				return
			}
			waitFor(t, time.Minute, "both channels started", func() bool {
				c := h.list()
				return count(c, "start control") == 1 && count(c, "start serving") == 1
			})
			c := h.list()
			for _, ch := range agentapi.RelayChannels {
				b := slices.Index(c, "before "+string(ch)+" 1 true")
				s := slices.Index(c, "start "+string(ch))
				if b < 0 || s < b {
					t.Errorf("the %s channel's hooks %q: want BeforeStart, then OnStart", ch, c)
				}
				h.mu.Lock()
				up, seq := h.up[ch], h.seq[ch]
				h.mu.Unlock()
				if up.PID == 0 || up.Seq == 0 || up.Seq != seq || up.Epoch != 1 {
					t.Errorf("the %s relay when OnStart was told: %+v, want its PID set, epoch 1, and the Seq BeforeStart was asked about, %d", ch, up, seq)
				}
			}
			own.Drop(tg.Tag)
		})
	}
}

// OnStartFailed is told when the process does not start (the "ssh" is not
// there), once per attempt, and OnStart and OnExit never; each attempt that
// BeforeStart allowed is resolved by exactly one of OnStart and
// OnStartFailed. The positive control, the same target with a real "ssh",
// tells OnStart and never OnStartFailed.
func TestOnStartFailedIsToldOfAStartThatFails(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run("missing "+strconv.FormatBool(missing), func(t *testing.T) {
			dir := t.TempDir()
			script := writeScript(t, dir, "relay.sh", "read x\n")
			jobs := shellJobs{}
			var own *hostdial.Owner
			h := &hookLog{}
			tg := shellTarget(1, "hb-relay-osf-0123456789ab", script, dir, "", jobs)
			if missing {
				tg.Command = func(string) (*exec.Cmd, error) { return exec.Command(filepath.Join(dir, "no-such-ssh")), nil }
			}
			tg = hooked(tg, func() *hostdial.Owner { return own }, h, func() bool { return true })
			own = shellOwner(t, hostdial.OwnerOptions{})
			own.Want(tg)
			if missing {
				waitFor(t, time.Minute, "two failed starts of each channel", func() bool {
					c := h.list()
					return count(c, "failed control") >= 2 && count(c, "failed serving") >= 2
				})
			} else {
				waitFor(t, time.Minute, "both channels started", func() bool {
					c := h.list()
					return count(c, "start control") == 1 && count(c, "start serving") == 1
				})
			}
			own.Drop(tg.Tag)
			c := h.list()
			for _, ch := range agentapi.RelayChannels {
				allowed := count(c, "before "+string(ch)+" 1 true")
				started, failed := count(c, "start "+string(ch)), count(c, "failed "+string(ch))
				if allowed != started+failed {
					t.Errorf("the %s channel: %d starts allowed, %d started and %d failed: want each resolved once (%q)", ch, allowed, started, failed, c)
				}
				switch {
				case missing && (started != 0 || count(c, "exit "+string(ch)) != 0):
					t.Errorf("the %s channel with no ssh told OnStart or OnExit: %q", ch, c)
				case !missing && failed != 0:
					t.Errorf("the %s channel with a real ssh told OnStartFailed: %q", ch, c)
				}
			}
		})
	}
}

// OnExit is told once a started process has exited and been reaped, after
// OnStart, and with the relay's PID cleared by then: here the relay's "ssh"
// exits by itself, and the relay starts again. A Drop's stop is told too.
func TestOnExitIsToldOnceAStartedProcessIsGone(t *testing.T) {
	dir := t.TempDir()
	script := writeScript(t, dir, "relay.sh", "exit 0\n") // ends at once; retried
	jobs := shellJobs{}
	var own *hostdial.Owner
	h := &hookLog{}
	tg := hooked(shellTarget(1, "hb-relay-oex-0123456789ab", script, dir, "", jobs), func() *hostdial.Owner { return own }, h, func() bool { return true })
	var pidAtExit sync.Map
	exit := tg.OnExit
	tg.OnExit = func(ch agentapi.RelayChannel) {
		pidAtExit.Store(ch, relayOf(own, tg.Tag, ch).PID)
		exit(ch)
	}
	own = shellOwner(t, hostdial.OwnerOptions{})
	own.Want(tg)
	waitFor(t, time.Minute, "two exits of each channel", func() bool {
		c := h.list()
		return count(c, "exit control") >= 2 && count(c, "exit serving") >= 2
	})
	own.Drop(tg.Tag)
	c := h.list()
	for _, ch := range agentapi.RelayChannels {
		// Each exit follows its own start: the starts and exits alternate.
		var seq []string
		for _, x := range c {
			if x == "start "+string(ch) || x == "exit "+string(ch) {
				seq = append(seq, x[:4])
			}
		}
		for i, x := range seq {
			want := "star"
			if i%2 == 1 {
				want = "exit"
			}
			if x != want {
				t.Fatalf("the %s channel's starts and exits %q: want them alternating, a start first", ch, seq)
			}
		}
		if pid, _ := pidAtExit.Load(ch); pid != 0 {
			t.Errorf("the %s relay's PID when OnExit was told: %v, want it cleared", ch, pid)
		}
	}
}
