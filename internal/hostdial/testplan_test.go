package hostdial_test

// HBEP-6's host-dialed Test plan (Test plan, Host-dialed mode), each item
// through the real path (harness_test.go): "The fake Slurm's srun --overlap
// carries the relays." The expected values are the HBEP's rules and the
// bytes and names this file sends; none is read back from the code under
// test. Every wait is on observable state (a PID, a greeting, a count) with a
// generous deadline.

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	control = agentapi.RelayControl
	serving = agentapi.RelayServing
)

// payload is n bytes of every value, so a byte that is changed, dropped or
// added in transit shows.
func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

// Channels stay apart: each relay is its own ssh process and srun step, a
// connection to the job's control listener reaches only the control target
// (the agent endpoint's place) and one to the serving listener only the
// serving target (the dial-home listener's), each relay counting only its
// own streams.
func TestHostDialedChannelsStayApart(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-apart-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	up := waitUp(t, own, j.tag)
	if up[control].PID == up[serving].PID {
		t.Fatalf("both channels run in one process %d: the two must never share a connection", up[control].PID)
	}
	roundTrip(t, j, control, "control", payload(100_000))
	roundTrip(t, j, serving, "serving", payload(300_000))
	roundTrip(t, j, control, "control", []byte("GET /agent/v1/desired HTTP/1.1\r\n\r\n"))
	if c, s := relayOf(own, j.tag, control), relayOf(own, j.tag, serving); c.Joined != 2 || s.Joined != 1 {
		t.Errorf("the control relay joined %d streams and the serving relay %d, want 2 and 1: a stream crossed to the other channel", c.Joined, s.Joined)
	}
	if got := w.targets[control].received(); len(got) != 2 || len(got[0]) != 100_000 {
		t.Errorf("the control target received %d connections, want 2, the first of 100000 bytes", len(got))
	}
	if got := w.targets[serving].received(); len(got) != 1 || !bytes.Equal(got[0], payload(300_000)) {
		t.Errorf("the serving target did not receive exactly the serving bytes")
	}
	if n := len(w.sruns(j.id)); n != 2 {
		t.Errorf("the fake ran %d srun steps for job %s, want 2, one per channel: %q", n, j.id, w.sruns(j.id))
	}
	for _, line := range w.sruns(j.id) {
		if !strings.HasPrefix(line, "srun -u --overlap --jobid="+j.id+" --ntasks=1 ") {
			t.Errorf("a relay ran %q, not HBEP-6's srun -u --overlap line", line)
		}
	}
}

// A stream on either relay can reach only its channel's port: bytes that name
// another port (the plugin endpoint's, here the decoy's), as a CONNECT line or
// a bare port, reach the channel's own target verbatim, and the decoy gets no
// connection.
func TestHostDialedStreamReachesOnlyItsChannelsPort(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-port-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	waitUp(t, own, j.tag)
	decoy := []byte(w.decoy.ln.Addr().String())
	hostile := [][]byte{
		[]byte("CONNECT " + string(decoy) + " HTTP/1.1\r\nHost: " + string(decoy) + "\r\n\r\n"),
		[]byte(strings.Split(string(decoy), ":")[1] + "\n"),
		append([]byte{0, 0, 0, 2}, decoy...), // a frame header and an address
	}
	for _, ch := range agentapi.RelayChannels {
		for _, h := range hostile {
			roundTrip(t, j, ch, string(ch), h)
		}
	}
	for _, ch := range agentapi.RelayChannels {
		got := w.targets[ch].received()
		if len(got) != len(hostile) {
			t.Fatalf("the %s target received %d connections, want %d", ch, len(got), len(hostile))
		}
		for i, h := range hostile {
			if !bytes.Equal(got[i], h) {
				t.Errorf("the %s target received %q, want the bytes sent, %q, verbatim", ch, got[i], h)
			}
		}
	}
	if n := w.decoy.accepted.Load(); n != 0 {
		t.Errorf("the decoy on another port accepted %d connections: a stream reached a port that is not its channel's", n)
	}
}

// A relay killed mid-stream: the stream in flight is cut (the client sees its
// connection end), the relay starts again within its backoff, and the next
// connection completes. (frpc logging in again and the front's count of the
// cut stream are TestRehearsalHostDialed's: they need frpc, frps and the
// front.)
func TestHostDialedRelayKilledMidStream(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-kill-0123456789ab", 30*time.Second)
	const maxBackoff = 2 * time.Second
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 500 * time.Millisecond, MaxBackoff: maxBackoff})
	own.Want(w.target(j))
	ups := waitUp(t, own, j.tag)
	first := ups[serving]
	c, err := dial(j, serving, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if err := c.echo(payload(64<<10), 30*time.Second); err != nil {
		t.Fatalf("the stream before the kill: %v", err)
	}
	killed := time.Now() // before the kill: the relay may record its end before Kill returns
	if err := syscall.Kill(first.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the serving relay's ssh %d: %v", first.PID, err)
	}
	// The stream in flight is cut: an echo cannot complete.
	cut := false
	for range 50 {
		if err := c.echo(payload(1024), 10*time.Second); err != nil {
			cut = true
			break
		}
	}
	if !cut {
		t.Fatal("the stream in flight still echoes after its relay's ssh was killed: it was not cut")
	}
	var again hostdial.RelayState
	waitFor(t, time.Minute, "the serving relay started again", func() bool {
		again = relayOf(own, j.tag, serving)
		return again.PID != 0 && again.PID != first.PID
	})
	if again.Starts != first.Starts+1 {
		t.Errorf("the serving relay has %d starts after the kill, want %d", again.Starts, first.Starts+1)
	}
	// Within the backoff: the wait the relay chose after the end is at most
	// MaxBackoff, and the new start came after the end and that wait (the
	// owner's own records, not a window measured from outside).
	if again.LastWait <= 0 || again.LastWait > maxBackoff {
		t.Errorf("the relay waited %s after the kill, want within its backoff, (0, %s]", again.LastWait, maxBackoff)
	}
	if again.LastEnded.Before(killed) || again.Since.Before(again.LastEnded.Add(again.LastWait)) {
		t.Errorf("the relay ended at %s and started again at %s, before its wait of %s had passed", again.LastEnded, again.Since, again.LastWait)
	}
	if c := relayOf(own, j.tag, control); c.PID != ups[control].PID || c.Starts != 1 {
		t.Errorf("the control relay restarted (%d starts) when only the serving relay was killed", c.Starts)
	}
	roundTrip(t, j, serving, "serving", payload(200_000))
}

// A restart of the Provider's controller: when the old owner stops, its relays
// are stopped by PID while the jobs still run, and a new owner (the new
// leader, told of every running Allocation by its first observations) starts
// both relays of every one of them.
func TestHostDialedControllerRestart(t *testing.T) {
	w := newWorld(t)
	jobs := []job{w.submit("hb-relay-rsa-0123456789ab", 30*time.Second), w.submit("hb-relay-rsb-0123456789ab", 30*time.Second)}
	old, cancel, done := w.owner(hostdial.OwnerOptions{})
	var before []int
	for _, j := range jobs {
		old.Want(w.target(j))
	}
	for _, j := range jobs {
		for _, r := range waitUp(t, old, j.tag) {
			before = append(before, r.PID)
		}
		roundTrip(t, j, control, "control", payload(10_000))
		roundTrip(t, j, serving, "serving", payload(10_000))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the old owner did not stop within 30 s")
	}
	for _, pid := range before {
		if alive(pid) {
			t.Errorf("the old owner's relay %d outlived it: relays are stopped by PID", pid)
		}
	}
	for _, j := range jobs {
		if s, ok := w.plat.Job(atoi(t, j.id)); !ok || s.State != "RUNNING" {
			t.Fatalf("job %s is %v, not running: the stop must be the owner's, not the job's end", j.id, s.State)
		}
	}
	// The new leader's epoch is higher: its Lease changed hands.
	nu, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(2)})
	for _, j := range jobs {
		nu.Want(w.target(j))
	}
	for _, j := range jobs {
		for ch, r := range waitUp(t, nu, j.tag) {
			for _, pid := range before {
				if r.PID == pid {
					t.Errorf("the new owner's %s relay of %s has the old PID %d", ch, j.tag, pid)
				}
			}
		}
		roundTrip(t, j, control, "control", payload(10_000))
		roundTrip(t, j, serving, "serving", payload(10_000))
	}
}

// Drop stops a relay by its PID even when its ssh is wedged: the relay's ssh
// process is stopped (SIGSTOP), so neither its pipes' closing nor a TERM can
// end it, and only the KILL that the stop sends after its grace does. Drop
// returns once the process is gone; the job runs on.
func TestHostDialedDropStopsAFrozenRelayByPID(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-frz-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{StopGrace: time.Second})
	own.Want(w.target(j))
	up := waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	ssh := up[serving].PID
	t.Cleanup(func() { _ = syscall.Kill(ssh, syscall.SIGKILL) })
	if err := syscall.Kill(ssh, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "the serving relay's ssh stopped", func() bool { return procState(ssh) == "T" })
	dropped := make(chan struct{})
	go func() { own.Drop(j.tag); close(dropped) }()
	select {
	case <-dropped:
	case <-time.After(30 * time.Second):
		for _, r := range up {
			_ = syscall.Kill(r.PID, syscall.SIGKILL)
		}
		t.Fatal("Drop did not return within 30 s with the relay's ssh stopped: the relay was not stopped by its PID")
	}
	for ch, r := range up {
		if alive(r.PID) {
			t.Errorf("the %s relay's ssh %d is alive after Drop", ch, r.PID)
		}
	}
	if s, ok := w.plat.Job(atoi(t, j.id)); !ok || s.State != "RUNNING" {
		t.Errorf("job %s is %v: Drop must stop the relays, not the job", j.id, s.State)
	}
}

// procState is a process's state letter from /proc ("" when it is gone).
func procState(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return ""
	}
	return s[i+2 : i+3]
}

// Two relays for one channel: the agent keeps the newer, the one with the
// higher (E, N). Two owners stand for an unfenced hand-over of the
// Provider's controller, the newer copy with the higher epoch. Once its
// relays connect, the older copy's are closed by the agent, and the older
// copy's next start is refused as superseded and never made again, so the
// two do not take turns (HBEP-6, Fencing); new connections cross the newer
// copy's relays.
func TestHostDialedNewerRelayReplacesOlder(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-new-0123456789ab", 30*time.Second)
	older, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(1), MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	older.Want(w.target(j))
	a := waitUp(t, older, j.tag)
	roundTrip(t, j, control, "control", payload(1000))
	roundTrip(t, j, serving, "serving", payload(1000))
	newer, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(2)})
	newer.Want(w.target(j))
	b := waitUp(t, newer, j.tag)
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the older "+string(ch)+" relay closed by the agent", func() bool { return !alive(a[ch].PID) })
		waitFor(t, time.Minute, "the older "+string(ch)+" relay superseded", func() bool { return relayOf(older, j.tag, ch).Final })
		if r := relayOf(older, j.tag, ch); !strings.HasPrefix(r.FinalCause, "superseded") {
			t.Errorf("the older %s relay ended %q, want superseded", ch, r.FinalCause)
		}
	}
	for _, ch := range agentapi.RelayChannels {
		before := relayOf(older, j.tag, ch).Joined
		roundTrip(t, j, ch, string(ch), payload(5000))
		if got := relayOf(newer, j.tag, ch).Joined; got != 1 {
			t.Errorf("the newer %s relay joined %d streams, want 1: the agent did not use the newer session", ch, got)
		}
		if got := relayOf(older, j.tag, ch).Joined; got != before {
			t.Errorf("the older %s relay joined a stream after the newer one connected", ch)
		}
		if r := relayOf(newer, j.tag, ch); r.PID != b[ch].PID || r.Starts != 1 {
			t.Errorf("the newer %s relay was replaced (%+v, was %d)", ch, r, b[ch].PID)
		}
	}
}

// A fifth relay on one login node waits: with three running Allocations on
// one login node, four relays run and carry bytes, and the other two have no
// process; a connection on a waiting channel gets no byte back within the
// job's wait. When one Allocation's relays stop, the waiting ones start and
// carry bytes.
func TestHostDialedFifthRelayWaits(t *testing.T) {
	w := newWorld(t)
	var jobs []job
	for _, tag := range []string{"hb-relay-cpa-0123456789ab", "hb-relay-cpb-0123456789ab", "hb-relay-cpc-0123456789ab"} {
		jobs = append(jobs, w.submit(tag, 3*time.Second))
	}
	slots := hostdial.NewSlots(hostdial.DefaultRelaysPerLogin)
	own, _, _ := w.owner(hostdial.OwnerOptions{Slots: slots})
	for _, j := range jobs {
		own.Want(w.target(j))
	}
	running := func() (up, waiting []hostdial.RelayState) {
		for _, r := range own.Relays() {
			if r.PID != 0 {
				up = append(up, r)
			} else {
				waiting = append(waiting, r)
			}
		}
		return up, waiting
	}
	waitFor(t, time.Minute, "four relays up", func() bool { up, _ := running(); return len(up) >= 4 })
	byTag := map[string]job{}
	for _, j := range jobs {
		byTag[j.tag] = j
	}
	up, waiting := running()
	for _, r := range up {
		roundTrip(t, byTag[r.Tag], r.Channel, string(r.Channel), payload(2000))
	}
	if len(waiting) != 2 {
		t.Fatalf("%d relays wait, want 2: %d run on one login node, the cap is %d", len(waiting), len(up), hostdial.DefaultRelaysPerLogin)
	}
	for _, r := range waiting {
		if c, err := dial(byTag[r.Tag], r.Channel, 20*time.Second); err == nil {
			c.close()
			t.Errorf("a connection on the waiting %s relay of %s got a greeting back: a fifth relay ran", r.Channel, r.Tag)
		}
		if r.Starts != 0 {
			t.Errorf("the waiting %s relay of %s started %d times", r.Channel, r.Tag, r.Starts)
		}
	}
	if n := slots.InUse(w.login()); n != 4 {
		t.Errorf("%d slots in use on the login node, want 4", n)
	}
	// Free the slots of an Allocation whose two relays both run.
	counts := map[string]int{}
	for _, r := range up {
		counts[r.Tag]++
	}
	var free string
	for tag, n := range counts {
		if n == 2 {
			free = tag
		}
	}
	if free == "" {
		t.Fatalf("no Allocation has both relays up: %+v", up)
	}
	own.Drop(free)
	for _, r := range waiting {
		waitFor(t, time.Minute, "the waiting "+string(r.Channel)+" relay of "+r.Tag+" up", func() bool { return relayOf(own, r.Tag, r.Channel).PID != 0 })
		roundTrip(t, byTag[r.Tag], r.Channel, string(r.Channel), payload(2000))
	}
	if n := slots.InUse(w.login()); n != 4 {
		t.Errorf("%d slots in use after the hand-over of slots, want 4", n)
	}
}

// The job-is-ours check: a Target whose recorded job is another running job of
// the same user (with another tag) starts no relay; nothing reaches that job.
// The same job under its own tag is a positive control.
func TestHostDialedRelayStartsOnlyOnTheAllocationsJob(t *testing.T) {
	w := newWorld(t)
	mine := w.submit("hb-relay-own-0123456789ab", 3*time.Second)
	other := w.submit("hb-relay-oth-0123456789ab", 3*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	wrong := w.target(mine)
	wrong.JobID = other.id // the Allocation's record names a job that is not under its tag
	own.Want(wrong)
	waitFor(t, time.Minute, "both relays refused", func() bool {
		for _, ch := range agentapi.RelayChannels {
			if !strings.Contains(relayOf(own, mine.tag, ch).LastRefusal, "not listed under tag") {
				return false
			}
		}
		return true
	})
	for _, ch := range agentapi.RelayChannels {
		r := relayOf(own, mine.tag, ch)
		if r.Starts != 0 || r.PID != 0 {
			t.Errorf("the %s relay started (%d starts) on a job not listed under its tag", ch, r.Starts)
		}
		if c, err := dial(other, ch, 15*time.Second); err == nil {
			c.close()
			t.Errorf("the other job's %s listener got a greeting: a relay landed on another job", ch)
		}
	}
	if s := w.sruns(other.id); len(s) != 0 {
		t.Errorf("the fake ran srun in the other job: %q", s)
	}
	// The positive control: under its own tag the same job gets its relays.
	own.Want(w.target(other))
	waitUp(t, own, other.tag)
	roundTrip(t, other, serving, "serving", payload(1000))
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("%q is not a job ID", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}
