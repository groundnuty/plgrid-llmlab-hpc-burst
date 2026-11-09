package hostdial_test

// The review of PR 301's tests, each through the real path
// (harness_test.go), each failing without its fix: the pair of slots (F2),
// rotated credentials (F1), a squatted relay name (F5), the job-is-ours read
// before every start (N2), the hand-over's flapping ending with the old owner
// (N6), and a named failure for a relay that finds no agent (N7).

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// F2: a relay that drops never gives its slot to another Allocation. A and B
// hold the login node's four slots and C waits; A's serving relay is killed
// and starts again, and C still waits, with no process and no byte crossing.
// Once B's relays stop, C gets both.
func TestHostDialedABlipNeverGivesASlotToAnotherAllocation(t *testing.T) {
	w := newWorld(t)
	a := w.submit("hb-relay-pra-0123456789ab", 3*time.Second)
	b := w.submit("hb-relay-prb-0123456789ab", 3*time.Second)
	c := w.submit("hb-relay-prc-0123456789ab", 3*time.Second)
	slots := hostdial.NewSlots(hostdial.DefaultRelaysPerLogin)
	own, _, _ := w.owner(hostdial.OwnerOptions{Slots: slots, MinBackoff: 500 * time.Millisecond, MaxBackoff: time.Second})
	own.Want(w.target(a))
	own.Want(w.target(b))
	upA := waitUp(t, own, a.tag)
	waitUp(t, own, b.tag)
	own.Want(w.target(c))
	waitFor(t, time.Minute, "C waiting for a pair of slots", func() bool { return relayOf(own, c.tag, control).Waiting })
	if err := syscall.Kill(upA[serving].PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "A's serving relay up again", func() bool {
		r := relayOf(own, a.tag, serving)
		return r.PID != 0 && r.PID != upA[serving].PID
	})
	roundTrip(t, a, serving, "serving", payload(2000))
	roundTrip(t, a, control, "control", payload(2000))
	for _, ch := range agentapi.RelayChannels {
		r := relayOf(own, c.tag, ch)
		if !r.Waiting || r.PID != 0 || r.Starts != 0 {
			t.Errorf("C's %s relay %+v: a blip of A's gave C a slot", ch, r)
		}
		if conn, err := dial(c, ch, 15*time.Second); err == nil {
			conn.close()
			t.Errorf("C's %s listener got a greeting while C waits", ch)
		}
	}
	if n := slots.InUse(w.relay.Login()); n != 4 {
		t.Errorf("%d slots in use, want 4", n)
	}
	own.Drop(b.tag)
	waitUp(t, own, c.tag)
	roundTrip(t, c, control, "control", payload(2000))
	roundTrip(t, c, serving, "serving", payload(2000))
}

// F1: rotated credentials reach running relays. The relays run on a key at
// one path; the key moves to another (the old one is removed, as a revoked
// key would be) and the target names the new credentials' generation: both
// relays start again on the new configuration, carry bytes, and the old
// configuration file is gone and is never written again.
func TestHostDialedRotatedCredentialsReachRunningRelays(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-rot-0123456789ab", 30*time.Second)
	key, err := os.ReadFile(w.creds.IdentityFile)
	if err != nil {
		t.Fatal(err)
	}
	relayOn := func(path string) *sshrun.Relay {
		if err := os.WriteFile(path, key, 0o600); err != nil {
			t.Fatal(err)
		}
		c := w.creds
		c.IdentityFile = path
		r, err := sshrun.NewRelay(sshrun.RelayOptions{Name: "rotation", Config: c, ControlDir: filepath.Join(w.base, "c")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	old := relayOn(filepath.Join(w.base, "key-1"))
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	tg := w.target(j)
	tg.Command, tg.Gen = old.Command, "gen-1"
	own.Want(tg)
	before := waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	// The rotation, as the Provider controller makes it: a new Relay for the
	// new credentials, the old one closed, the old key gone.
	nu := relayOn(filepath.Join(w.base, "key-2"))
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(w.base, "key-1")); err != nil {
		t.Fatal(err)
	}
	tg.Command, tg.Gen = nu.Command, "gen-2"
	own.Want(tg)
	for _, ch := range agentapi.RelayChannels {
		var r hostdial.RelayState
		waitFor(t, time.Minute, "the "+string(ch)+" relay on the new credentials", func() bool {
			r = relayOf(own, j.tag, ch)
			return r.Gen == "gen-2" && r.PID != 0 && r.PID != before[ch].PID
		})
		args, err := os.ReadFile("/proc/" + strconv.Itoa(r.PID) + "/cmdline")
		if err != nil || !strings.Contains(string(args), nu.Config()+"\x00") {
			t.Errorf("the %s relay %d does not run on the new configuration %s: %q", ch, r.PID, nu.Config(), args)
		}
		if alive(before[ch].PID) {
			t.Errorf("the %s relay %d on the old credentials still runs", ch, before[ch].PID)
		}
	}
	roundTrip(t, j, control, "control", payload(1000))
	roundTrip(t, j, serving, "serving", payload(1000))
	if _, err := os.Stat(old.Config()); !os.IsNotExist(err) {
		t.Errorf("the old credentials' configuration file %s is left: %v", old.Config(), err)
	}
	if _, err := old.Command("true"); err == nil {
		t.Error("a closed Relay still makes ssh clients on its credentials")
	}
}

// F5: a squatted relay name does not stop the agent. Before the job starts,
// the names a predictable scheme would give its relay directory are taken
// (the plain name as a link to elsewhere, which the agent refuses as another
// user's directory, and the first random-looking one as a file); the agent
// makes its own unpredictable directory and both channels carry bytes.
func TestHostDialedASquattedRelayNameDoesNotStopTheAgent(t *testing.T) {
	w := newWorld(t)
	id := strconv.Itoa(w.firstID)
	squatted := filepath.Join(w.relays, "hb-relay-"+id)
	if err := os.Symlink("/nonexistent", squatted); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(squatted+"-0", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	j := w.submit("hb-relay-sqt-0123456789ab", 30*time.Second)
	if j.id != id {
		t.Fatalf("the job is %s, not the squatted ID %s", j.id, id)
	}
	if j.ports.Dir == squatted || !strings.HasPrefix(filepath.Base(j.ports.Dir), "hb-relay-"+id+"-") {
		t.Fatalf("the job's relay directory is %s, want an unpredictable one beside %s", j.ports.Dir, squatted)
	}
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	waitUp(t, own, j.tag)
	roundTrip(t, j, control, "control", payload(1000))
	roundTrip(t, j, serving, "serving", payload(1000))
}

// N2: the job-is-ours read happens before every start, not only the first.
// Once the relays run, the job is listed under another name; the serving
// relay is killed, and its next start is refused: no srun runs again.
func TestHostDialedEveryStartReadsTheJobAgain(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-rea-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	up := waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(w.sruns(j.id)) == 2 })
	if err := w.plat.RenameListed(atoi(t, j.id), "hb-relay-oth-0123456789ab"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(up[serving].PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the serving relay's next start refused", func() bool {
		return strings.Contains(relayOf(own, j.tag, serving).LastRefusal, "not listed under tag")
	})
	if r := relayOf(own, j.tag, serving); r.PID != 0 || r.Starts != 1 {
		t.Errorf("the serving relay %+v started again on a job no longer under its tag", r)
	}
	if n := len(w.sruns(j.id)); n != 2 {
		t.Errorf("the fake ran %d srun steps in the job, want the first 2 only: %q", n, w.sruns(j.id))
	}
}

// N6: an unfenced hand-over flaps, and the flapping ends with the old owner.
// Two owners keep relays for one Allocation, and the agent keeps the newer
// session each time, so each owner's relays are closed and started again in
// turn. Once the old owner stops (its Provider manager lost its Lease, as it
// does within the Lease's duration), the other's relays stay up and carry
// bytes.
func TestHostDialedAStoppedOldOwnerEndsTheFlapping(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-flp-0123456789ab", 30*time.Second)
	opts := hostdial.OwnerOptions{MinBackoff: 200 * time.Millisecond, MaxBackoff: 400 * time.Millisecond}
	older, stopOlder, olderDone := w.owner(opts)
	newer, _, _ := w.owner(opts)
	older.Want(w.target(j))
	newer.Want(w.target(j))
	waitFor(t, 2*time.Minute, "both owners' serving relays started at least twice: the flapping", func() bool {
		return relayOf(older, j.tag, serving).Starts >= 2 && relayOf(newer, j.tag, serving).Starts >= 2
	})
	stopOlder()
	select {
	case <-olderDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the old owner did not stop")
	}
	up := waitUp(t, newer, j.tag)
	starts := map[agentapi.RelayChannel]int{}
	for ch, r := range up {
		starts[ch] = r.Starts
	}
	for range 3 {
		roundTrip(t, j, control, "control", payload(2000))
		roundTrip(t, j, serving, "serving", payload(2000))
	}
	for _, ch := range agentapi.RelayChannels {
		if r := relayOf(newer, j.tag, ch); r.PID != up[ch].PID || r.Starts != starts[ch] {
			t.Errorf("the %s relay was replaced after the old owner stopped (%+v, was %+v)", ch, r, up[ch])
		}
	}
}

// N7: a relay that finds no agent says so by name. The job runs no agent, so
// no pointer is published: each relay step exits with hostdial.RelayMessage's
// line, which the owner records as the relay's failure through Classify.
func TestHostDialedARelayWithNoAgentIsNamed(t *testing.T) {
	w := newWorld(t)
	j := job{tag: "hb-relay-noa-0123456789ab"}
	j.id = w.sbatch(j.tag, "exec sleep 600")
	waitFor(t, time.Minute, "the job running", func() bool {
		s, ok := w.plat.Job(atoi(t, j.id))
		return ok && s.State == "RUNNING"
	})
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	want := "hb-relay: no relay pointer for job " + j.id + ":"
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the "+string(ch)+" relay's failure named", func() bool {
			return strings.HasPrefix(relayOf(own, j.tag, ch).LastFailure, want)
		})
	}
}
