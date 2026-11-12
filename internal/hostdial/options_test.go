package hostdial_test

// New for issue #343 (PR 1): the relay keeper's options of hostdial.Owner and
// hostdial.Target (HBEP-6, The relay keeper), and pins of the defaults, which
// are alpha's and which the options must not move. A pin runs alpha's Owner
// as the batch Provider's controller does, with no option set. Each pin has a
// positive control, the same run with the option set, which shows the other
// behaviour, so a pin that could not fail does not pass for one.
//
// Two kinds of test:
//   - The process tests (shellOwner) run a shell double as the relay's ssh.
//     They check what the Owner does to the process it starts, its group and
//     its signals, and how it tells an end, which a double shows exactly. No
//     byte crosses them, and none claims to: HBEP-6 says a count of relays
//     with no byte crossing proves nothing about a relay.
//   - The path tests run harness_test.go's real path: ssh to the rehearsal's
//     login node, an srun -u --overlap step of this binary as "relay
//     <channel>", and the job side, with bytes crossing each relay.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// shellJobs is the process tests' listing: each tag's job is running.
type shellJobs map[string]string

func (s shellJobs) ListByTag(_ context.Context, tag string) ([]provider.Job, error) {
	if id, ok := s[tag]; ok {
		return []provider.Job{{ID: id, Name: tag, Phase: v1alpha1.AllocationRunning}}, nil
	}
	return nil, nil
}

// shellOwner starts an Owner for the process tests, stopped at the test's
// end. Its relays' doubles never write a preamble, so its ports, in issue
// #343's block, are never dialed.
func shellOwner(t *testing.T, o hostdial.OwnerOptions) *hostdial.Owner {
	t.Helper()
	o.Ports = map[agentapi.RelayChannel]int{control: 30000, serving: 30001}
	o.Epoch = epoch(1)
	if o.Slots == nil {
		o.Slots = hostdial.NewSlots(0)
	}
	o.MinBackoff, o.MaxBackoff, o.StopGrace = 50*time.Millisecond, 200*time.Millisecond, 5*time.Second
	o.Logger = slog.New(slog.NewTextHandler(testWriter{t}, nil))
	own, err := hostdial.NewOwner(o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := own.Start(ctx); err != nil {
			t.Errorf("the relay owner: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the relay owner did not stop within 30 s")
		}
	})
	return own
}

// shellTarget is the n-th process test target: its relays' "ssh" is sh
// running script with the channel, dir and arg as its arguments, and its
// standard error is kept for the end's reading, as sshrun.Stderr keeps it.
// Its job is added to jobs, running.
func shellTarget(n int, tag, script, dir, arg string, jobs shellJobs) hostdial.Target {
	id := strconv.Itoa(40_000 + n)
	jobs[tag] = id
	return hostdial.Target{Tag: tag, JobID: id, AgentPath: "/stage/agent/e/hb-agent", AllocDir: "/stage/alloc/" + tag,
		UID: fmt.Sprintf("0a1b2c3d-0000-4000-8000-%012d", n), Login: "login-" + strconv.Itoa(n) + ".example", Jobs: jobs, Gen: "1",
		Command: func(line string) (*exec.Cmd, error) {
			ch := control
			if strings.Contains(line, " "+agentapi.RelaySubcommand+" "+string(serving)+" ") {
				ch = serving
			}
			cmd := exec.Command("/bin/sh", script, string(ch), dir, arg)
			cmd.Stderr = &bytes.Buffer{}
			return cmd, nil
		},
		Stderr: func(cmd *exec.Cmd) string { return cmd.Stderr.(*bytes.Buffer).String() },
	}
}

func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The group probe's relay double starts a child that records the signals it
// gets, then waits. The child records "ready" once its traps are set, each
// TERM as "term", and "winch" when a WINCH has ended its loop; it ends by
// itself after about a minute, so a failed test leaves no process behind for
// long. It holds none of the relay's standard streams: exec.Cmd.Wait waits
// for a stream that a child still holds, so the relay's stop would wait for
// the child.
//
// The WINCH trap only sets a flag, and "winch" is written after the loop
// (issue #437). dash runs a trap's action through evaltree, which first runs
// any trap that became pending meanwhile (dotrap, eval.c, has no guard against
// re-entry): a WINCH that comes while the TERM trap runs has its trap run
// inside the TERM trap, before the TERM trap's echo. A WINCH trap that wrote
// "winch" and exited there ended the child without "term", although the TERM
// had been delivered. The flag is set only inside a trap and read only by the
// loop, which goes on only once every trap under way has returned, so the
// TERM trap of a TERM delivered before the WINCH has written "term" before
// "winch" is written.
//
// Inside a trap the double branches on values (case), never on a command's
// status: a trap that dash runs inside another trap's action leaves, as the
// status of the command it followed, the one from before the outer trap
// (dotrap restores savestatus), so a "[ ... ] ||" there can take the wrong
// branch. Outside traps a status is safe: a trap run there restores the
// status of the command it followed.
//
// The TERM trap first sources the file named by the child's second argument,
// if it is not empty: the relay double passes its own third one (the target's
// arg), which groupProbe leaves empty unless it holds the trap there
// (termHeldThenWinch).
const (
	groupRelaySh = `sh "$2/child.sh" "$2/$1.log" "$3" </dev/null >/dev/null 2>&1 &
echo $! > "$2/$1.pid.tmp" && mv "$2/$1.pid.tmp" "$2/$1.pid"
wait
`
	groupChildSh = `w=
trap 'case $2 in ?*) . "$2" ;; esac; echo term >> "$1"' TERM
trap 'w=1' WINCH
echo ready >> "$1"
i=0
while [ -z "$w" ] && [ "$i" -lt 600 ]; do sleep 0.1; i=$((i + 1)); done
[ -z "$w" ] || echo winch >> "$1"
`
	// groupTermHoldSh, sourced by the child's TERM trap, marks that the
	// trap has started and holds it, with builtins only, until the test
	// creates the release file. The WINCH trap that dash runs inside the
	// hold may leave the until test's status as it was before the TERM trap
	// (see above), which can end the hold early, but only once the WINCH
	// trap has run inside the TERM trap, which is what the hold is for. A
	// hold that runs out (some tens of seconds of builtins) says so in a
	// file of its own, counted by a case on the value, so a run whose WINCH
	// did not come inside the trap fails instead of passing.
	groupTermHoldSh = `: > "$1.in-term"
n=0
until [ -e "$1.release" ]; do
	n=$((n + 1))
	case $n in 20000000) : > "$1.held-out"; break ;; esac
done
`
)

// groupSeen is what the group probe saw of one channel's relay.
type groupSeen struct {
	relay, pgid int
	// attr is the process attributes the Owner gave the relay's command.
	setpgid   bool
	pdeathsig syscall.Signal
	pidfd     bool
	// lines is what the relay's child recorded.
	lines string
}

// groupProbe starts one Allocation's relays under an Owner with o. Each
// relay's double forks a child, which the test pins with a pidfd, verified as
// the relay's child, the relay being this test process's (pin). The probe
// records each relay's process group and the attributes its command was
// started with, then Drops the Allocation, which returns once the relays are
// gone. Then it sends each child WINCH through its verified pin and reads
// what the child recorded once it has exited. A TERM the stop sent was
// delivered before the WINCH, and the child writes "winch" only after every
// trap that ran has returned (groupChildSh), so a TERM's line comes first:
// the order is the evidence, not a wait. With heldTerm, the probe sends each
// child, after Drop, a TERM of its own through the pin, and the WINCH inside
// that TERM's trap (termHeldThenWinch), in place of the WINCH alone.
func groupProbe(t *testing.T, o hostdial.OwnerOptions, heldTerm bool) map[agentapi.RelayChannel]groupSeen {
	t.Helper()
	dir := t.TempDir()
	writeScript(t, dir, "child.sh", groupChildSh)
	hold := ""
	if heldTerm {
		hold = writeScript(t, dir, "hold.sh", groupTermHoldSh)
	}
	jobs := shellJobs{}
	tg := shellTarget(1, "hb-relay-grp-0123456789ab", writeScript(t, dir, "relay.sh", groupRelaySh), dir, hold, jobs)
	var cmdMu sync.Mutex
	cmds := map[agentapi.RelayChannel]*exec.Cmd{}
	command := tg.Command
	tg.Command = func(line string) (*exec.Cmd, error) {
		cmd, err := command(line)
		ch := control
		if strings.Contains(line, " "+agentapi.RelaySubcommand+" "+string(serving)+" ") {
			ch = serving
		}
		cmdMu.Lock()
		cmds[ch] = cmd
		cmdMu.Unlock()
		return cmd, err
	}
	own := shellOwner(t, o)
	own.Want(tg)
	seen := map[agentapi.RelayChannel]groupSeen{}
	children := map[agentapi.RelayChannel]*pinned{}
	for _, ch := range agentapi.RelayChannels {
		var relay, child int
		waitFor(t, time.Minute, "the "+string(ch)+" relay's child ready", func() bool {
			relay = relayOf(own, tg.Tag, ch).PID
			b, err := os.ReadFile(filepath.Join(dir, string(ch)+".pid"))
			if relay == 0 || err != nil {
				return false
			}
			child, err = strconv.Atoi(strings.TrimSpace(string(b)))
			l, lerr := os.ReadFile(filepath.Join(dir, string(ch)+".log"))
			return err == nil && lerr == nil && strings.HasPrefix(string(l), "ready\n")
		})
		c, err := pin(t, child, relay)
		if err != nil {
			t.Fatalf("pin the %s relay's child %d: %v", ch, child, err)
		}
		pgid, err := syscall.Getpgid(relay)
		if err != nil {
			t.Fatal(err)
		}
		// The relay's PID was read after its start, which set the attributes.
		cmdMu.Lock()
		a := cmds[ch].SysProcAttr
		cmdMu.Unlock()
		seen[ch] = groupSeen{relay: relay, pgid: pgid, setpgid: a.Setpgid, pdeathsig: a.Pdeathsig, pidfd: a.PidFD != nil}
		children[ch] = c
	}
	own.Drop(tg.Tag)
	for ch, c := range children {
		if alive(seen[ch].relay) {
			t.Errorf("the %s relay %d outlived Drop", ch, seen[ch].relay)
		}
		log := filepath.Join(dir, string(ch)+".log")
		what := "the " + string(ch) + " relay's child"
		if heldTerm {
			termHeldThenWinch(t, c, log, what)
		} else {
			if err := c.signal(syscall.SIGWINCH); err != nil {
				t.Fatalf("WINCH to %s: %v", what, err)
			}
			c.waitExit(t, 30*time.Second, what)
		}
		b, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		s := seen[ch]
		s.lines = string(b)
		seen[ch] = s
	}
	return seen
}

// termHeldThenWinch sends the pinned child (groupChildSh, started with
// groupTermHoldSh as its hook) TERM, waits until its TERM trap has started
// and is held, sends WINCH, then releases the trap and waits for the child's
// exit. The WINCH is queued before the release file exists, so the child
// takes it before it can leave the hold, and dash runs the WINCH trap inside
// the TERM trap. Each step waits on a file the child wrote, never on time.
func termHeldThenWinch(t *testing.T, c *pinned, log, what string) {
	t.Helper()
	if err := c.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("TERM to %s: %v", what, err)
	}
	waitFor(t, 30*time.Second, what+"'s TERM trap held", func() bool { return fileExists(log + ".in-term") })
	if err := c.signal(syscall.SIGWINCH); err != nil {
		t.Fatalf("WINCH to %s: %v", what, err)
	}
	if err := os.WriteFile(log+".release", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	c.waitExit(t, 30*time.Second, what)
	if fileExists(log + ".held-out") {
		t.Fatalf("%s's TERM trap ran out of its hold before the test released it, so the WINCH did not come inside the trap: this run proves nothing", what)
	}
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// waitPidfdExit waits until the pidfd's process has exited.
func waitPidfdExit(t *testing.T, fd int, d time.Duration, what string) {
	t.Helper()
	end := time.Now().Add(d)
	for {
		pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(pfd, 100)
		if err != nil && err != unix.EINTR {
			t.Fatalf("poll %s's pidfd: %v", what, err)
		}
		if n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("%s did not exit within %s", what, d)
		}
	}
}

// Pin: alpha's relay process (no option set) leads a process group of its
// own, with a KILL when this process dies (Pdeathsig), and its stop signals
// that group, so a child its ssh forked gets the TERM too (PR #301). The
// positive control is TestAPidfdRelayLeadsNoGroupAndItsStopReachesOnlyIt.
func TestAlphasRelayLeadsItsOwnGroupAndItsStopSignalsIt(t *testing.T) {
	for ch, s := range groupProbe(t, hostdial.OwnerOptions{}, false) {
		if !s.setpgid || s.pdeathsig != syscall.SIGKILL || s.pidfd {
			t.Errorf("alpha's %s relay started with Setpgid %v, Pdeathsig %v and a clone pidfd %v; want a group of its own, SIGKILL and none", ch, s.setpgid, s.pdeathsig, s.pidfd)
		}
		if s.pgid != s.relay {
			t.Errorf("alpha's %s relay %d is in process group %d, want a group of its own", ch, s.relay, s.pgid)
		}
		if s.lines != "ready\nterm\nwinch\n" {
			t.Errorf("alpha's %s relay's child recorded %q, want the stop's TERM before the test's WINCH", ch, s.lines)
		}
	}
}

// The relay keeper's relay process (OwnerOptions.Pidfd) leads no process
// group of its own, starts with a pidfd from clone and a KILL when this
// process dies, and its stop, through its pidfd, reaches it alone: its child
// never gets a TERM (HBEP-6, The relay keeper, Signals). Its positive
// control, that a TERM which does reach the child is recorded, is
// TestAKeeperRelaysChildRecordsATermThatReachesIt.
func TestAPidfdRelayLeadsNoGroupAndItsStopReachesOnlyIt(t *testing.T) {
	for ch, s := range groupProbe(t, hostdial.OwnerOptions{Pidfd: true}, false) {
		if s.setpgid || s.pdeathsig != syscall.SIGKILL || !s.pidfd {
			t.Errorf("the keeper's %s relay started with Setpgid %v, Pdeathsig %v and a clone pidfd %v; want no group, SIGKILL and a pidfd", ch, s.setpgid, s.pdeathsig, s.pidfd)
		}
		if s.pgid == s.relay || s.pgid != syscall.Getpgrp() {
			t.Errorf("the keeper's %s relay %d is in process group %d, want the test's %d", ch, s.relay, s.pgid, syscall.Getpgrp())
		}
		if s.lines != "ready\nwinch\n" {
			t.Errorf("the keeper's %s relay's child recorded %q, want no TERM, only the test's WINCH", ch, s.lines)
		}
	}
}

// The positive control of TestAPidfdRelayLeadsNoGroupAndItsStopReachesOnlyIt
// (the science agent's condition on #437's fix): in the same probe, a TERM
// that does reach a child is recorded, also when a WINCH comes inside its
// trap, so the "no TERM" there is the absence of a TERM, not a lost record.
// After Drop the probe sends each child TERM through its verified pin, holds
// the trap and sends the WINCH inside it (termHeldThenWinch).
//
// The TERM is not a group signal, and must never be one: the keeper's relay
// leads no group, so the child's process group is this test process's own,
// and a TERM to it would reach the test binary and, under go test, the go
// command and the other packages' binaries. The guard not-our-own-group
// (groupsig.go) refuses that group for the same reason, and the rule made
// after the signal sweep of 10 Nov 2026 (#308) allows a test's signals only
// through a verified pidfd. The control needs no group signal either: what
// the negative control relies on is that the double records a TERM it
// receives, and the kernel queues a group's signal and a pidfd's on the same
// shared pending set of each process, so the child takes either alike.
func TestAKeeperRelaysChildRecordsATermThatReachesIt(t *testing.T) {
	for ch, s := range groupProbe(t, hostdial.OwnerOptions{Pidfd: true}, true) {
		if s.pgid == s.relay || s.pgid != syscall.Getpgrp() {
			t.Errorf("the keeper's %s relay %d is in process group %d, want the test's %d, as in the negative control", ch, s.relay, s.pgid, syscall.Getpgrp())
		}
		if s.lines != "ready\nterm\nwinch\n" {
			t.Errorf("the keeper's %s relay's child recorded %q for a TERM sent to it and a WINCH inside its trap, want \"ready\\nterm\\nwinch\\n\"", ch, s.lines)
		}
	}
}

// The group probe's child keeps the record of a TERM whose trap a WINCH
// interrupts (issue #437). The test starts the child itself, sends it TERM
// through its verified pin and the WINCH inside that TERM's trap
// (termHeldThenWinch). A child whose WINCH trap wrote "winch" and exited there
// recorded "ready\nwinch\n" for a TERM it was sent: the false failure of
// TestAlphasRelayLeadsItsOwnGroupAndItsStopSignalsIt, and, read the other way,
// a stray TERM that TestAPidfdRelayLeadsNoGroupAndItsStopReachesOnlyIt would
// not have seen.
func TestTheGroupChildKeepsATermWhoseTrapAWinchInterrupts(t *testing.T) {
	dir := t.TempDir()
	child := writeScript(t, dir, "child.sh", groupChildSh)
	hold := writeScript(t, dir, "hold.sh", groupTermHoldSh)
	log := filepath.Join(dir, "child.log")
	cmd := exec.Command("/bin/sh", child, log, hold)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Registered before the pin, so it reaps the child after the pin's KILL.
	t.Cleanup(func() { _ = cmd.Wait() })
	c, err := pin(t, cmd.Process.Pid)
	if err != nil {
		t.Fatalf("pin the child %d: %v", cmd.Process.Pid, err)
	}
	waitFor(t, 30*time.Second, "the child ready", func() bool {
		b, err := os.ReadFile(log)
		return err == nil && strings.HasPrefix(string(b), "ready\n")
	})
	termHeldThenWinch(t, c, log, "the child")
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "ready\nterm\nwinch\n" {
		t.Errorf("the child recorded %q for a TERM and a WINCH that came while the TERM's trap ran, want \"ready\\nterm\\nwinch\\n\"", b)
	}
}

// The end probe's relay double ends its relay as $3 says, as ssh passes on
// what srun and the relay step said: srun's step-limit refusal (failed), the
// relay's superseded exit code 5, or srun's refusal that the job does not
// exist (the job's end).
const endsRelaySh = `case "$3" in
failed) echo "srun: error: Unable to create step for job 40000: Step limit reached for this job" >&2; exit 1 ;;
superseded) exit 5 ;;
gone) echo "srun: error: Unable to create step for job 40000: Invalid job id specified" >&2; exit 1 ;;
esac
exit 2
`

// toldEnds records what OnFailed and OnFinal were told.
type toldEnds struct {
	mu             sync.Mutex
	failed, finals []string
}

func (e *toldEnds) hook(tg *hostdial.Target, onFinal bool) {
	tag := tg.Tag
	tg.OnFailed = func(ch agentapi.RelayChannel, cause string) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.failed = append(e.failed, tag+" "+string(ch)+": "+cause)
	}
	if onFinal {
		tg.OnFinal = func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.finals = append(e.finals, tag+" "+string(ch)+" "+kind.String()+": "+cause)
		}
	}
}

func (e *toldEnds) lists() (failed, finals []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	failed, finals = slices.Clone(e.failed), slices.Clone(e.finals)
	slices.Sort(failed)
	slices.Sort(finals)
	return failed, finals
}

// endsProbe runs three Allocations whose relays end failed, superseded and
// with the job's end, and returns what OnFailed (and, with onFinal, OnFinal)
// were told once all six relays ended for good and their hooks returned,
// with each relay's final cause as the Owner shows it. A relay shows Final
// before its hooks run, so the probe then Drops each Allocation: Drop returns
// once its relays' loops have, and the hooks run inside the loop.
func endsProbe(t *testing.T, onFinal bool) (failed, finals []string, causes map[string]string) {
	t.Helper()
	dir := t.TempDir()
	script := writeScript(t, dir, "relay.sh", endsRelaySh)
	jobs := shellJobs{}
	var told toldEnds
	var tgs []hostdial.Target
	for i, how := range []string{"failed", "superseded", "gone"} {
		tg := shellTarget(i+1, "hb-relay-end-"+how, script, dir, how, jobs)
		told.hook(&tg, onFinal)
		tgs = append(tgs, tg)
	}
	own := shellOwner(t, hostdial.OwnerOptions{})
	for _, tg := range tgs {
		own.Want(tg)
	}
	causes = map[string]string{}
	waitFor(t, time.Minute, "all six relays ended for good", func() bool {
		for _, tg := range tgs {
			for _, ch := range agentapi.RelayChannels {
				r := relayOf(own, tg.Tag, ch)
				if !r.Final {
					return false
				}
				causes[tg.Tag+" "+string(ch)] = r.FinalCause
			}
		}
		return true
	})
	for _, tg := range tgs {
		own.Drop(tg.Tag)
	}
	failed, finals = told.lists()
	return failed, finals, causes
}

// checkEndCauses checks that the probe's relays ended as their doubles said,
// so a callback that was not told of an end is not mistaken for an end that
// never happened.
func checkEndCauses(t *testing.T, causes map[string]string) {
	t.Helper()
	want := map[string]string{
		"hb-relay-end-failed":     "failed: srun refused the step: step limit reached for this job",
		"hb-relay-end-superseded": "superseded: superseded (exit 5)",
		"hb-relay-end-gone":       "the job's end: srun refused the step: the job is not running",
	}
	for tag, prefix := range want {
		for _, ch := range agentapi.RelayChannels {
			if got := causes[tag+" "+string(ch)]; !strings.HasPrefix(got, prefix) {
				t.Errorf("%s's %s relay ended %q, want %q", tag, ch, got, prefix)
			}
		}
	}
}

// Pin: alpha's OnFailed is told only of a failed end, never of a superseded
// one or of the job's end, which also ended their relays for good here
// (HBEP-6, What ends a relay: "A final end other than superseded or the job's
// end raises a Warning event, reason RelayFailed"). The positive control is
// TestOnFinalIsToldOfEveryFinalEndAndOnFailedStaysAsItWas.
func TestOnFailedIsToldOnlyOfAFailedEnd(t *testing.T) {
	failed, finals, causes := endsProbe(t, false)
	checkEndCauses(t, causes)
	want := []string{
		"hb-relay-end-failed control: srun refused the step: step limit reached for this job",
		"hb-relay-end-failed serving: srun refused the step: step limit reached for this job",
	}
	if !slices.Equal(failed, want) {
		t.Errorf("OnFailed was told %q, want only the failed Allocation's two ends %q", failed, want)
	}
	if len(finals) != 0 {
		t.Errorf("OnFinal, unset, was told %q", finals)
	}
}

// The relay keeper's OnFinal is told of every final end, of each kind, while
// OnFailed is told exactly what it is told without it.
func TestOnFinalIsToldOfEveryFinalEndAndOnFailedStaysAsItWas(t *testing.T) {
	failed, finals, causes := endsProbe(t, true)
	checkEndCauses(t, causes)
	if want := []string{
		"hb-relay-end-failed control: srun refused the step: step limit reached for this job",
		"hb-relay-end-failed serving: srun refused the step: step limit reached for this job",
	}; !slices.Equal(failed, want) {
		t.Errorf("OnFailed, beside OnFinal, was told %q, want %q", failed, want)
	}
	want := []string{
		"hb-relay-end-failed control failed: srun refused the step: step limit reached for this job",
		"hb-relay-end-failed serving failed: srun refused the step: step limit reached for this job",
		"hb-relay-end-gone control the job's end: srun refused the step: the job is not running",
		"hb-relay-end-gone serving the job's end: srun refused the step: the job is not running",
		"hb-relay-end-superseded control superseded: superseded (exit 5): the agent keeps a newer relay",
		"hb-relay-end-superseded serving superseded: superseded (exit 5): the agent keeps a newer relay",
	}
	if !slices.Equal(finals, want) {
		t.Errorf("OnFinal was told %q, want %q", finals, want)
	}
}

// srunsOn counts the srun lines the fake ran for a job's channel.
func (w *world) srunsOn(jobID string, ch agentapi.RelayChannel) int {
	n := 0
	for _, l := range w.sruns(jobID) {
		if strings.Contains(l, " "+agentapi.RelaySubcommand+" "+string(ch)+" ") {
			n++
		}
	}
	return n
}

// The keeper's relays (OwnerOptions.Pidfd) carry bytes over the real path,
// lead no process group of their own, and are stopped by Drop through their
// pidfds: the ssh processes and their relay steps end, and the job runs on.
func TestAPidfdOwnersRelaysCarryBytesAndStopThroughTheirPidfds(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-pfd-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{Pidfd: true})
	own.Want(w.target(j))
	up := waitUp(t, own, j.tag)
	roundTrip(t, j, control, "control", payload(10_000))
	roundTrip(t, j, serving, "serving", payload(10_000))
	for ch, r := range up {
		if pgid, err := syscall.Getpgid(r.PID); err != nil || pgid == r.PID || pgid != syscall.Getpgrp() {
			t.Errorf("the keeper's %s relay %d is in process group %d (%v), want the test's %d", ch, r.PID, pgid, err, syscall.Getpgrp())
		}
	}
	own.Drop(j.tag)
	for ch, r := range up {
		if alive(r.PID) {
			t.Errorf("the keeper's %s relay %d outlived Drop", ch, r.PID)
		}
		waitFor(t, time.Minute, "the "+string(ch)+" relay step ended with its ssh", func() bool { return len(stepPIDs(t, j.id, ch)) == 0 })
	}
	if s, ok := w.plat.Job(atoi(t, j.id)); !ok || s.State != "RUNNING" {
		t.Errorf("job %s is %v after Drop, want it running: the stop is the relays', not the job's end", j.id, s.State)
	}
}

// A channel seeded as ended for good (Target.Final) is never started: no srun
// line for it reaches the login node, the Owner shows it final with the
// seeded cause, and neither OnFailed nor OnFinal is told of it again. The
// other channel starts and carries bytes. The positive control: the same
// Allocation wanted with no seed starts that channel, which carries bytes.
func TestASeededFinalChannelIsNeverStarted(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-sfn-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	const cause = "failed: srun refused the step: step limit reached for this job"
	tg := w.target(j)
	tg.Final = map[agentapi.RelayChannel]string{serving: cause}
	var told toldEnds
	told.hook(&tg, true)
	own.Want(tg)
	waitFor(t, time.Minute, "the control relay up and the serving relay final", func() bool {
		return relayOf(own, j.tag, control).PID != 0 && relayOf(own, j.tag, serving).Final
	})
	roundTrip(t, j, control, "control", payload(10_000))
	if r := relayOf(own, j.tag, serving); r.FinalCause != cause || r.Starts != 0 || r.PID != 0 {
		t.Errorf("the seeded serving relay %+v: want final with the seeded cause, never started", r)
	}
	if n := w.srunsOn(j.id, serving); n != 0 {
		t.Errorf("%d srun lines for the seeded serving channel reached the login node, want none", n)
	}
	if failed, finals := told.lists(); len(failed) != 0 || len(finals) != 0 {
		t.Errorf("a seeded end was told again: OnFailed %q, OnFinal %q", failed, finals)
	}
	own.Drop(j.tag)
	own.Want(w.target(j))
	waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(10_000))
}

// A new credentials generation restarts the live channel on the new Gen and
// keeps a seeded final channel down (the science agent's ruling on #343's
// Q7, 10 Nov 2026). A Want that changes only the seed restarts nothing,
// since same ignores Final: seeding the live channel later neither stops nor
// ends it.
func TestANewGenRestartsTheLiveChannelAndKeepsASeededFinalOneDown(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-gen-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	const cause = "superseded: superseded (exit 5): the agent keeps a newer relay"
	tg := w.target(j)
	tg.Final = map[agentapi.RelayChannel]string{serving: cause}
	own.Want(tg)
	waitFor(t, time.Minute, "the control relay up", func() bool { return relayOf(own, j.tag, control).PID != 0 })
	first := relayOf(own, j.tag, control)
	roundTrip(t, j, control, "control", payload(1000))
	tg.Gen = "2"
	own.Want(tg)
	waitFor(t, time.Minute, "the control relay up again on generation 2", func() bool {
		r := relayOf(own, j.tag, control)
		return r.PID != 0 && r.PID != first.PID && r.Gen == "2"
	})
	roundTrip(t, j, control, "control", payload(1000))
	if r := relayOf(own, j.tag, serving); !r.Final || r.FinalCause != cause || r.Starts != 0 || r.Gen != "2" {
		t.Errorf("the seeded serving relay on generation 2 %+v: want final with the seeded cause, never started", r)
	}
	if n := w.srunsOn(j.id, serving); n != 0 {
		t.Errorf("%d srun lines for the seeded serving channel reached the login node, want none", n)
	}
	second := relayOf(own, j.tag, control)
	tg.Final = map[agentapi.RelayChannel]string{serving: cause, control: cause}
	own.Want(tg)
	roundTrip(t, j, control, "control", payload(1000))
	if r := relayOf(own, j.tag, control); r.PID != second.PID || r.Final || r.Starts != second.Starts {
		t.Errorf("a Want that changed only the seed moved the live control relay: %+v, was %+v", r, second)
	}
}

// OnStart is told of each ssh start of a channel's relay: once per channel at
// the start, and once more for a relay started again after its ssh was
// killed. The fake's own log of srun lines, one per start that reached the
// login node, is the independent count; it lags the relay's session, so the
// test waits for it.
func TestOnStartIsToldOfEveryStart(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-ost-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	var mu sync.Mutex
	starts := map[agentapi.RelayChannel]int{}
	tg := w.target(j)
	tg.OnStart = func(ch agentapi.RelayChannel) {
		mu.Lock()
		defer mu.Unlock()
		starts[ch]++
	}
	own.Want(tg)
	up := waitUp(t, own, j.tag)
	// Killed only once its session is up, so its start reached the login node.
	waitFor(t, time.Minute, "both sessions up and both starts logged", func() bool {
		return relayOf(own, j.tag, control).Sessions == 1 && relayOf(own, j.tag, serving).Sessions == 1 &&
			w.srunsOn(j.id, control) == 1 && w.srunsOn(j.id, serving) == 1
	})
	ssh, err := pin(t, up[serving].PID)
	if err != nil {
		t.Fatalf("pin the serving relay's ssh: %v", err)
	}
	if err := ssh.signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the serving relay started again", func() bool {
		r := relayOf(own, j.tag, serving)
		return r.PID != 0 && r.PID != up[serving].PID
	})
	roundTrip(t, j, serving, "serving", payload(1000))
	roundTrip(t, j, control, "control", payload(1000))
	waitFor(t, time.Minute, "the login node's log of the three starts", func() bool {
		return w.srunsOn(j.id, control) == 1 && w.srunsOn(j.id, serving) == 2
	})
	mu.Lock()
	got := map[agentapi.RelayChannel]int{control: starts[control], serving: starts[serving]}
	mu.Unlock()
	for ch, want := range map[agentapi.RelayChannel]int{control: 1, serving: 2} {
		if got[ch] != want || w.srunsOn(j.id, ch) != want {
			t.Errorf("the %s channel: OnStart told %d times, %d srun lines on the login node, want %d of each", ch, got[ch], w.srunsOn(j.id, ch), want)
		}
	}
}

// Over the real path, an old copy's relays that a newer one supersedes end
// for good, and OnFinal is told of each as superseded, with no OnFailed: the
// end the keeper reports for the mark in beta (HBEP-6, The relay keeper: "In
// beta a superseded end is unexpected ... so the keeper reports it as a final
// end needing the mark").
func TestOnFinalIsToldOfASupersededEndOverTheRealPath(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-sup-0123456789ab", 30*time.Second)
	nu, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(7)})
	nu.Want(w.target(j))
	waitUp(t, nu, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	old, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(6)})
	var told toldEnds
	tg := w.target(j)
	told.hook(&tg, true)
	old.Want(tg)
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the old copy's "+string(ch)+" relay superseded", func() bool { return relayOf(old, j.tag, ch).Final })
	}
	roundTrip(t, j, serving, "serving", payload(1000))
	old.Drop(j.tag) // once its relays' loops, and so their hooks, have returned
	failed, finals := told.lists()
	want := []string{
		j.tag + " control superseded: superseded (exit 5): the agent keeps a newer relay",
		j.tag + " serving superseded: superseded (exit 5): the agent keeps a newer relay",
	}
	if !slices.Equal(finals, want) || len(failed) != 0 {
		t.Errorf("OnFinal %q and OnFailed %q, want OnFinal %q and no OnFailed", finals, failed, want)
	}
}

// The pidfd stop's grace, then its KILL: a keeper relay's ssh that is wedged
// (stopped, through its verified pin, so neither its pipes' closing nor a
// TERM ends it) is ended by the KILL the stop sends through its pidfd after
// StopGrace, and Drop returns. Without that KILL, Drop and Stop would never
// return. This is TestHostDialedDropStopsAFrozenRelayByPID with Pidfd set
// (#349's review round).
func TestAPidfdOwnerStopsAFrozenRelayWithAKILLAfterItsGrace(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-pfz-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{Pidfd: true, StopGrace: time.Second})
	own.Want(w.target(j))
	up := waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	ssh, err := pin(t, up[serving].PID)
	if err != nil {
		t.Fatalf("pin the serving relay's ssh: %v", err)
	}
	if err := ssh.signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "the serving relay's ssh stopped", func() bool { return procState(up[serving].PID) == "T" })
	dropped := make(chan struct{})
	go func() { own.Drop(j.tag); close(dropped) }()
	select {
	case <-dropped:
	case <-time.After(30 * time.Second):
		_ = ssh.signal(syscall.SIGKILL)
		t.Fatal("Drop did not return within 30 s with the keeper relay's ssh stopped: the stop sent no KILL after its grace")
	}
	if !ssh.exited() {
		t.Errorf("the serving relay's ssh %d is alive after Drop", up[serving].PID)
	}
	if alive(up[control].PID) {
		t.Errorf("the control relay's ssh %d is alive after Drop", up[control].PID)
	}
	if s, ok := w.plat.Job(atoi(t, j.id)); !ok || s.State != "RUNNING" {
		t.Errorf("job %s is %v: Drop must stop the relays, not the job", j.id, s.State)
	}
}

// A seeded channel reads as final from the Want on, while its Allocation
// still waits for a pair of slots on its login node; the other channel waits
// with no process. Once the slots free, the other channel starts, and the
// seeded one never does.
func TestASeededChannelReadsFinalWhileItsPairWaitsForSlots(t *testing.T) {
	dir := t.TempDir()
	script := writeScript(t, dir, "relay.sh", "read x\n") // holds until its stop
	jobs := shellJobs{}
	held := shellTarget(1, "hb-relay-hld-0123456789ab", script, dir, "", jobs)
	seeded := shellTarget(2, "hb-relay-sed-0123456789ab", script, dir, "", jobs)
	seeded.Login = held.Login
	const cause = "failed: srun refused the step: step limit reached for this job"
	seeded.Final = map[agentapi.RelayChannel]string{serving: cause}
	own := shellOwner(t, hostdial.OwnerOptions{Slots: hostdial.NewSlots(2)})
	own.Want(held)
	waitFor(t, time.Minute, "the first Allocation's relays up", func() bool {
		return relayOf(own, held.Tag, control).PID != 0 && relayOf(own, held.Tag, serving).PID != 0
	})
	own.Want(seeded)
	if r := relayOf(own, seeded.Tag, serving); !r.Final || r.FinalCause != cause || !r.Waiting || r.Starts != 0 {
		t.Errorf("the seeded serving relay while its pair waits for slots: %+v, want final with the seeded cause, waiting, never started", r)
	}
	if r := relayOf(own, seeded.Tag, control); r.Final || !r.Waiting || r.PID != 0 {
		t.Errorf("the other channel while its pair waits for slots: %+v, want waiting with no process", r)
	}
	own.Drop(held.Tag)
	waitFor(t, time.Minute, "the other channel started once the slots freed", func() bool { return relayOf(own, seeded.Tag, control).PID != 0 })
	if r := relayOf(own, seeded.Tag, serving); !r.Final || r.Starts != 0 || r.PID != 0 {
		t.Errorf("the seeded serving relay after the slots freed: %+v, want final, never started", r)
	}
}
