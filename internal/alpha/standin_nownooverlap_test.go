package alpha

// Row B over the stand-in (issue #376; nownooverlap.go): the LIVE steps over the real stack against the fake Slurm behind
// the rehearsal's sshd, in the one-slot shape (standInWorldShaped: a partition QOS of one submitted job and one GPU per
// user, as athena-now.yaml types plgrid-now). Both jobs start warm from the pre-staging row's staging (preStage). The
// owner's own job is the fake's OutsideJob, under the cluster's user, which the run's squeue --me lists. It proves the
// wiring, not a cluster: no stand-in pass is cluster evidence.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// standInOwnerJob is the owner's own job's name on the stand-in: no tag of the run's.
const standInOwnerJob = "owner-own-work"

// The ways the owner's job meets row B's successor on the stand-in.
const (
	ownerNone = iota
	// ownerSeenFirst: the successor's sbatch is held, the owner's job takes the slot, and the held sbatch goes only once
	// the run's read of the slot has seen that job (its slotTakenFn): the successor never makes a job.
	ownerSeenFirst
	// ownerRefusesSuccessor: the successor's sbatch is held, the owner's job takes the slot, and the held sbatch goes at
	// once, so that the scheduler refuses it for the submit limit, whatever the run read first.
	ownerRefusesSuccessor
)

// standInNowNoOverlap runs row B's live steps over the stand-in, warm, with the owner's job as owner says.
func standInNowNoOverlap(t *testing.T, owner int) (secondHeldRun, *stack.Cluster) {
	t.Helper()
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the live steps runs only with %s=1 (internal/alpha, standin_test.go)", envStandIn)
	}
	row := standInRow(t, "alpha-athena-now-nooverlap")
	id, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Row: row, StandIn: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Minute, RunID: id, FrontLabel: standInFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	w, cl := standInWorldShaped(t, row, nil)
	preStage(t, cl, w)
	lr := newLiveRun(t, cfg, w)
	lr.stop = &stackStop{}
	lr.agentLogFn = func(path string) string { return "tail -c 65536 " + path }
	if owner != ownerNone {
		release := ownerTakesTheSlot(t, cl, TagPrefix(id), owner == ownerRefusesSuccessor)
		lr.slotTakenFn = release
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, lr, rec, t.Logf)
	return secondHeldRun{row: row, cfg: cfg, cl: cl, prefix: TagPrefix(id), failures: failures, err: err, stop: *lr.stop}, cl
}

// ownerTakesTheSlot arms the fake for the owner's job: once the run's first job runs, the next sbatch (the successor's,
// which the controller sends only after the predecessor's job has left the slot) is held at a gate; while it is held the
// owner's own job is registered in the slot. now lets the held sbatch go at once; otherwise the returned function, the
// run's slotTakenFn, lets it go. The owner's job is the test's: nothing of the run touches it.
func ownerTakesTheSlot(t *testing.T, cl *stack.Cluster, prefix string, now bool) func() {
	t.Helper()
	var gate *fakeslurm.Gate
	armed := make(chan struct{})
	var once sync.Once
	release := func() {
		<-armed
		once.Do(gate.Release)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The run's first job running: the gate then holds the controller's next submission, the successor's.
		for !firstJobRunning(cl, prefix) {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		gate = cl.Plat.Gate("sbatch")
		close(armed)
		select {
		case <-stop:
			return
		case <-gate.Blocked():
		}
		id := cl.Plat.OutsideJob(stack.ClusterUser, standInOwnerJob, stack.Partition, "", 1)
		t.Logf("stand-in: the successor's sbatch is held; the owner's own job (fake ID %d) takes the slot", id)
		if now {
			once.Do(gate.Release)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		select {
		case <-armed:
			once.Do(gate.Release)
		default:
		}
	})
	return func() {
		t.Log("stand-in: the run has seen the owner's job in the slot; the held sbatch goes")
		release()
	}
}

// firstJobRunning reports whether the fake shows a job of the run running.
func firstJobRunning(cl *stack.Cluster, prefix string) bool {
	for _, j := range cl.Plat.Jobs() {
		if strings.HasPrefix(j.Name, prefix) && j.State == fakeslurm.Running {
			return true
		}
	}
	return false
}

// ownerJobUntouched fails the test unless the fake's record of the owner's job shows it never cancelled, signalled or
// ended by anyone: the fake runs it on, and the run's cleanup cancels by its prefix and tags alone.
func ownerJobUntouched(t *testing.T, cl *stack.Cluster) {
	t.Helper()
	found := false
	for _, j := range cl.Plat.Jobs() {
		if j.Name != standInOwnerJob {
			continue
		}
		found = true
		if j.State == fakeslurm.Cancelled || j.Finished {
			t.Errorf("the owner's job is %s (finished %t): the run must never touch it", j.State, j.Finished)
		}
	}
	if !found {
		t.Error("the fake holds no owner's job")
	}
	for _, c := range cl.Plat.Calls() {
		if len(c.Words) > 0 && (c.Words[0] == "scancel" || c.Words[0] == "scontrol") && strings.Contains(strings.Join(c.Words, " "), standInOwnerJob) {
			t.Errorf("a call named the owner's job: %v", c.Words)
		}
	}
}

// TestStandInNowNoOverlap is alpha-athena-now-nooverlap's live steps over the stand-in, warm, with nobody else in the slot:
// the predecessor serves a stream across its drain start, ReleaseWhenIdle frees the slot, the controller creates the
// successor only then, and the row PASSES. The fake's own record then shows the two jobs one after the other.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNowNoOverlap(t *testing.T) {
	s, cl := standInNowNoOverlap(t, ownerNone)
	if err := s.stop.problem(); err != nil {
		t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
	}
	post, _ := os.ReadFile(filepath.Join(s.cfg.Out, PostsFile))
	t.Logf("%s's text to post:\n%s", s.row.Name, post)
	if s.err != nil || len(s.failures) > 0 {
		t.Fatalf("%s over the stand-in: failures %v, err %v; want a PASS", s.row.Name, s.failures, s.err)
	}
	var ours []fakeslurm.Job
	for _, j := range cl.Plat.Jobs() {
		if strings.HasPrefix(j.Name, s.prefix) {
			ours = append(ours, j)
		}
	}
	if len(ours) != 2 {
		t.Fatalf("the fake cluster holds %d jobs of the run, want two", len(ours))
	}
	if pre, suc := ours[0], ours[1]; pre.End.IsZero() || suc.Submit.Before(pre.End) || suc.Start.IsZero() {
		t.Errorf("the fake's record: the predecessor ended %s, the successor submitted %s and started %s; want it submitted after the end", pre.End, suc.Submit, suc.Start)
	}
	record, err := os.ReadFile(filepath.Join(s.cfg.Out, RecordFile))
	if err != nil || !strings.Contains(string(record), "STAND-IN RUN") || strings.Contains(string(record), "LIVE RUN") {
		t.Errorf("the record does not say the run was a stand-in one (%v)", err)
	}
}

// TestStandInNowNoOverlapSlotTaken is row B over the stand-in with the owner's own job taking the slot between the jobs,
// seen by the run's read before the successor's sbatch goes: the run ends its deployment, the row is NOT RUN with no
// failure, its marker and text say so, and the owner's job is never touched (Q7).
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNowNoOverlapSlotTaken(t *testing.T) {
	s, cl := standInNowNoOverlap(t, ownerSeenFirst)
	standInSlotTakenIsNotRun(t, s, cl)
}

// TestStandInNowNoOverlapSuccessorRefused is row B over the stand-in with the owner's job taking the slot and the
// successor's sbatch let go at once, so that the scheduler refuses it for the submit limit: NOT RUN, never a FAIL, the
// owner's job untouched, and no job of the successor's in the fake's record.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNowNoOverlapSuccessorRefused(t *testing.T) {
	s, cl := standInNowNoOverlap(t, ownerRefusesSuccessor)
	standInSlotTakenIsNotRun(t, s, cl)
	sbatches := 0
	for _, c := range cl.Plat.Calls() {
		if len(c.Words) > 0 && c.Words[0] == "sbatch" && strings.Contains(strings.Join(c.Words, " "), s.prefix) {
			sbatches++
		}
	}
	if sbatches < 2 {
		t.Errorf("the fake saw %d sbatch calls of the run, want the predecessor's and at least one of the successor's", sbatches)
	}
}

// standInSlotTakenIsNotRun is what both owner stand-ins require: NOT RUN for the slot taken, no failure, a marker and
// text that say so and name no tag or job, one job of the run in the fake's record, ended, and the owner's untouched.
func standInSlotTakenIsNotRun(t *testing.T, s secondHeldRun, cl *stack.Cluster) {
	t.Helper()
	if err := s.stop.problem(); err != nil {
		t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
	}
	if len(s.failures) != 0 || !errors.Is(s.err, ErrSlotTaken) || !NotRun(s.row, s.err) {
		t.Fatalf("failures %v, err %v; want the NOT RUN of the owner's job in the slot", s.failures, s.err)
	}
	note := NotRunNote(s.row, s.cfg.PendingMax, s.err)
	t.Logf("the marker would say: %s", strings.TrimSpace(note))
	if !strings.Contains(note, slotTakenWords) || strings.Contains(note, s.prefix) || strings.Contains(note, standInOwnerJob) {
		t.Errorf("the marker's note %q does not say the slot was taken, or names a tag or the owner's job", note)
	}
	post, err := os.ReadFile(filepath.Join(s.cfg.Out, PostsFile))
	if err != nil || !strings.Contains(string(post), s.row.Name+": NOT RUN") || strings.Contains(string(post), ": PASS") || strings.Contains(string(post), standInOwnerJob) {
		t.Errorf("the text to post does not say NOT RUN, or names the owner's job (%v):\n%s", err, post)
	}
	t.Logf("%s's text to post:\n%s", s.row.Name, post)
	var ours []fakeslurm.Job
	for _, j := range cl.Plat.Jobs() {
		if strings.HasPrefix(j.Name, s.prefix) {
			ours = append(ours, j)
		}
	}
	if len(ours) != 1 || !ours[0].Finished {
		t.Errorf("the fake holds %d jobs of the run, want the predecessor's alone, ended", len(ours))
	}
	ownerJobUntouched(t, cl)
}
