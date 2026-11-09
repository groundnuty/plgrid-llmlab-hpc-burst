package hostdial_test

// HBEP-6's host-dialed Test plan items that PR 298's final text (b12aa9f)
// and PR 301's review round add, each through the real path
// (harness_test.go), each failing without its rule: the pair of slots,
// rotated credentials, the rendezvous (different TMPDIRs, a stale
// relay-socket), the job-is-ours read before every start, fencing, and what
// ends a relay (a step-limit refusal, the job's end, too many restarts, a
// missing preamble).

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// failures records the targets' OnFailed calls: the controller's RelayFailed.
type failures struct {
	mu  sync.Mutex
	got []string
}

func (f *failures) hook(t *hostdial.Target) {
	t.OnFailed = func(ch agentapi.RelayChannel, cause string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.got = append(f.got, string(ch)+": "+cause)
	}
}

func (f *failures) list() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.got...)
}

// A relay that drops never gives its slot to another Allocation (HBEP-6,
// Capacity: "a pair is reserved whole, and neither relay of a running
// Allocation's pair is ever missing while the other runs"). A and B hold the
// login node's four slots and C waits; A's serving relay is killed and starts
// again, and C still waits, with no process and no byte crossing. Once B's
// relays stop, C gets both.
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

// Rotated credentials reach running relays (PR 301's review, F1): the relays
// run on a key at one path; the key moves to another (the old one removed, as
// a revoked key would be) and the target names the new credentials'
// generation; both relays start again on the new configuration and carry
// bytes, and the old configuration file is gone and never written again.
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
			t.Errorf("the %s relay %d does not run on the new configuration %s", ch, r.PID, nu.Config())
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

// The rendezvous: a batch step and a relay step with different TMPDIR values
// still meet through relay-socket (HBEP-6, Test plan). The job side's socket
// is under the batch step's TMPDIR; the relay steps get a TMPDIR of their own
// (fakeslurm.RunMode.StepEnv), as a step srun starts from the login node may.
func TestHostDialedRendezvousAcrossDifferentTMPDIRs(t *testing.T) {
	stepTmp, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(stepTmp) })
	w := newWorld(t, func(c *worldConfig) { c.stepEnv = map[string]string{"TMPDIR": stepTmp} })
	j := w.submit("hb-relay-tmp-0123456789ab", 30*time.Second)
	if strings.HasPrefix(j.ports.Socket, stepTmp) || !strings.HasPrefix(j.ports.Socket, w.jobDir()) {
		t.Fatalf("the agent's socket %s is not under the batch step's TMPDIR (in %s): the TMPDIRs do not differ as the test needs", j.ports.Socket, w.jobDir())
	}
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	waitUp(t, own, j.tag)
	roundTrip(t, j, control, "control", payload(1000))
	roundTrip(t, j, serving, "serving", payload(1000))
}

// A stale relay-socket (its socket has no listener) ends the relay as "agent
// not ready", which is retried, never final.
func TestHostDialedAStaleRendezvousIsAgentNotReady(t *testing.T) {
	w := newWorld(t)
	j := w.allocation("hb-relay-stl-0123456789ab")
	stale := filepath.Join(w.base, "gone.sock")
	if err := os.WriteFile(agentapi.RelaySocketFile(j.allocDir), []byte(stale+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	j.id = w.sbatch(j.tag, "exec sleep 600")
	waitRunning(t, w, j)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	own.Want(w.target(j))
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the "+string(ch)+" relay retried after agent not ready", func() bool {
			r := relayOf(own, j.tag, ch)
			return r.Starts >= 2 && strings.Contains(r.LastEnd, "agent not ready")
		})
		if r := relayOf(own, j.tag, ch); r.Final {
			t.Errorf("the %s relay ended for good (%s) on agent not ready, which is retried", ch, r.FinalCause)
		}
	}
}

// The job-is-ours read happens before every start, not only the first
// (review N2), from the Provider's listing. Once the relays run, the job is
// listed under another name; the serving relay is killed, and its next start
// is refused, with no srun, no RelayFailed and no final end, since a listing
// can miss a running job for a while. Once the listing shows the job under
// its tag again, the relay starts again and bytes cross it.
func TestHostDialedEveryStartReadsTheJobAgain(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-rea-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	var f failures
	tg := w.target(j)
	f.hook(&tg)
	own.Want(tg)
	up := waitUp(t, own, j.tag)
	roundTrip(t, j, serving, "serving", payload(1000))
	waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(w.sruns(j.id)) == 2 })
	listed := func(want bool) func() bool {
		return func() bool {
			jobs, err := w.jobs.ListByTag(t.Context(), j.tag)
			return err == nil && (len(jobs) == 1) == want
		}
	}
	if err := w.plat.RenameListed(atoi(t, j.id), "hb-relay-oth-0123456789ab"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the Provider's listing to show the job under another name", listed(false))
	if err := syscall.Kill(up[serving].PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the serving relay's next start refused", func() bool {
		return strings.Contains(relayOf(own, j.tag, serving).LastRefusal, "not listed under tag")
	})
	if r := relayOf(own, j.tag, serving); r.Final || r.PID != 0 || r.Starts != 1 {
		t.Errorf("the serving relay %+v: want refused, not final, after its one start", r)
	}
	if n := len(w.sruns(j.id)); n != 2 {
		t.Errorf("the fake ran %d srun steps in the job, want the first 2 only: %q", n, w.sruns(j.id))
	}
	if err := w.plat.RenameListed(atoi(t, j.id), j.tag); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the Provider's listing to show the job under its tag again", listed(true))
	waitFor(t, time.Minute, "the serving relay up again", func() bool {
		r := relayOf(own, j.tag, serving)
		return r.PID != 0 && r.Starts == 2
	})
	roundTrip(t, j, serving, "serving", payload(1000))
	if got := f.list(); len(got) != 0 {
		t.Errorf("a job missing from the listing for a while raised RelayFailed: %q", got)
	}
}

// Fencing (HBEP-6): an old copy's relay that arrives after the new copy's,
// with a lower epoch, is refused as superseded and is not started again; the
// new copy's relays stay and carry bytes, and no RelayFailed is raised.
func TestHostDialedALowerEpochIsSuperseded(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-fen-0123456789ab", 30*time.Second)
	nu, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(7)})
	nu.Want(w.target(j))
	up := waitUp(t, nu, j.tag)
	roundTrip(t, j, control, "control", payload(1000))
	old, _, _ := w.owner(hostdial.OwnerOptions{Epoch: epoch(6)})
	var f failures
	tg := w.target(j)
	f.hook(&tg)
	old.Want(tg)
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the old copy's "+string(ch)+" relay superseded", func() bool { return relayOf(old, j.tag, ch).Final })
		r := relayOf(old, j.tag, ch)
		if !strings.HasPrefix(r.FinalCause, "superseded") || r.Starts != 1 || r.Epoch != 6 {
			t.Errorf("the old copy's %s relay %+v: want superseded after one start, epoch 6", ch, r)
		}
	}
	for range 3 {
		roundTrip(t, j, control, "control", payload(1000))
		roundTrip(t, j, serving, "serving", payload(1000))
	}
	for _, ch := range agentapi.RelayChannels {
		if r := relayOf(nu, j.tag, ch); r.PID != up[ch].PID || r.Starts != 1 || r.Epoch != 7 {
			t.Errorf("the new copy's %s relay %+v was disturbed by the old copy's", ch, r)
		}
	}
	if got := f.list(); len(got) != 0 {
		t.Errorf("a superseded relay raised RelayFailed: %q", got)
	}
}

// What ends a relay: srun's step-limit refusal ends it for good, with
// RelayFailed naming the channel and the cause.
func TestHostDialedAStepLimitRefusalEndsTheRelayWithRelayFailed(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-lim-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	var f failures
	tg := w.target(j)
	f.hook(&tg)
	own.Want(tg)
	up := waitUp(t, own, j.tag)
	waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(w.sruns(j.id)) == 2 })
	if err := w.plat.SetStepLimit(atoi(t, j.id), 2); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(up[serving].PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the serving relay ended for good", func() bool { return relayOf(own, j.tag, serving).Final })
	if r := relayOf(own, j.tag, serving); !strings.Contains(r.FinalCause, "step limit") {
		t.Errorf("the serving relay ended %q, want srun's step-limit refusal", r.FinalCause)
	}
	waitFor(t, time.Minute, "RelayFailed raised", func() bool { return len(f.list()) == 1 })
	if got := f.list()[0]; !strings.HasPrefix(got, "serving: ") || !strings.Contains(got, "step limit") {
		t.Errorf("RelayFailed %q: want the serving channel and the step limit", got)
	}
	if r := relayOf(own, j.tag, control); r.Final || r.PID != up[control].PID {
		t.Errorf("the control relay %+v was disturbed by the serving relay's end", r)
	}
}

// What ends a relay: more restarts of one channel than the limit within the
// window (120 an hour; 3 here) ends it for good, with RelayFailed.
func TestHostDialedTooManyRestartsEndTheRelayWithRelayFailed(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-res-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 50 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, RestartLimit: 3})
	var f failures
	tg := w.target(j)
	f.hook(&tg)
	own.Want(tg)
	killed := map[int]bool{}
	waitFor(t, 2*time.Minute, "the serving relay ended for good after repeated kills", func() bool {
		r := relayOf(own, j.tag, serving)
		if r.Final {
			return true
		}
		if r.PID != 0 && !killed[r.PID] {
			_ = syscall.Kill(r.PID, syscall.SIGKILL)
			killed[r.PID] = true
		}
		return false
	})
	r := relayOf(own, j.tag, serving)
	// More than 3 restarts is the fourth restart: the fifth start.
	if !strings.Contains(r.FinalCause, "more than 3 restarts") || r.Starts != 5 {
		t.Errorf("the serving relay %+v: want a final end after 5 starts, more than 3 restarts", r)
	}
	if got := f.list(); len(got) != 1 || !strings.Contains(got[0], "more than 3 restarts") {
		t.Errorf("RelayFailed %q: want one, naming the restarts", got)
	}
}

// What ends a relay: a missing preamble or unexpected output first is
// refused, counted and retried, never final (HBEP-6, The wire).
func TestHostDialedUnexpectedOutputIsRetried(t *testing.T) {
	w := newWorld(t, func(c *worldConfig) { c.stepEnv = map[string]string{"HB_TEST_RELAY_GARBAGE": "1"} })
	j := w.submit("hb-relay-gar-0123456789ab", 3*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	own.Want(w.target(j))
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the "+string(ch)+" relay refused for its output and retried", func() bool {
			r := relayOf(own, j.tag, ch)
			return r.Starts >= 2 && strings.Contains(r.LastEnd, "unexpected output before the preamble")
		})
		if r := relayOf(own, j.tag, ch); r.Final || r.Joined != 0 {
			t.Errorf("the %s relay %+v: unexpected output must be retried, and no stream joined", ch, r)
		}
	}
	if c, err := dial(j, serving, 10*time.Second); err == nil {
		c.close()
		t.Error("a byte crossed a relay whose output was refused")
	}
}

// What ends a relay: the job ending while a relay starts ends it for good as
// the job's end, with no RelayFailed.
func TestHostDialedTheJobsEndEndsTheRelayWithoutRelayFailed(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-end-0123456789ab", 30*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	var f failures
	tg := w.target(j)
	f.hook(&tg)
	own.Want(tg)
	waitUp(t, own, j.tag)
	if err := w.plat.End(atoi(t, j.id), "CANCELLED"); err != nil {
		t.Fatal(err)
	}
	// The job's end stops each relay's process with its srun step; a later
	// start is refused or ends for good, by the listing or by srun, and
	// never starts a step.
	for _, ch := range agentapi.RelayChannels {
		waitFor(t, time.Minute, "the "+string(ch)+" relay stopped by the job's end", func() bool {
			r := relayOf(own, j.tag, ch)
			return r.PID == 0 && (strings.HasPrefix(r.FinalCause, "the job's end") ||
				strings.Contains(r.LastRefusal, "not listed under tag") || strings.Contains(r.LastRefusal, "not running"))
		})
		if r := relayOf(own, j.tag, ch); r.Final && !strings.HasPrefix(r.FinalCause, "the job's end") {
			t.Errorf("the %s relay ended %q, want the job's end", ch, r.FinalCause)
		}
	}
	if got := f.list(); len(got) != 0 {
		t.Errorf("the job's end raised RelayFailed: %q", got)
	}
}

// waitRunning waits until a job's state is RUNNING.
func waitRunning(t *testing.T, w *world, j job) {
	t.Helper()
	waitFor(t, time.Minute, "job "+j.id+" running", func() bool {
		s, ok := w.plat.Job(atoi(t, j.id))
		return ok && s.State == "RUNNING"
	})
}

// A killed job side's directory goes with its job (science-agent ruling on
// #301): the socket's directory is under the job's TMPDIR, which the site
// removes when the job ends, so a SIGKILLed job side, which cannot clean up,
// leaves nothing behind once its job has ended. The fake Slurm removes a
// job's execution directory, TMPDIR included, once its processes are gone,
// as a site's per-job TMPDIR goes.
func TestHostDialedAKilledJobSidesDirectoryGoesWithItsJob(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-kil-0123456789ab", 30*time.Second)
	dir := filepath.Dir(j.ports.Socket)
	if !strings.HasPrefix(dir, w.jobDir()+"/") || !strings.HasPrefix(filepath.Base(dir), "hb-relay-") {
		t.Fatalf("the socket's directory %s is not a hb-relay-* directory under the job's TMPDIR, in %s", dir, w.jobDir())
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the socket's directory %s: %v", dir, err)
	}
	if err := syscall.Kill(j.ports.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Minute, "the killed job's socket directory gone with the job", func() bool {
		_, err := os.Stat(dir)
		return os.IsNotExist(err)
	})
	if s, ok := w.plat.Job(atoi(t, j.id)); !ok || s.State == "RUNNING" {
		t.Errorf("job %s is %+v after its batch process was killed", j.id, s)
	}
	if left, _ := filepath.Glob(filepath.Join(w.base, "s", "hb-relay-*")); len(left) != 0 {
		t.Errorf("the fallback directory holds %v: the job side fell back from TMPDIR", left)
	}
}
