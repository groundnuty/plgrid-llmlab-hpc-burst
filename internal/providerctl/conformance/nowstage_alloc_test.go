package conformance

// The pre-staging row's own Allocation directory (issue #376; the review of PR #400). The row keeps the profile's
// staging directory for row A, so the run-directory wipe of the other rows is off (worldKit.keepStaging), and the
// Allocation's own directory, alloc/<tag>, with its agent.json, its staged credential (internal/providerctl/create.go,
// the credential file of the Allocation's files) and the job's output, would stay in $SCRATCH on every exit before the
// body's Delete. removeOwnAllocation, registered by stageBody before the submit, removes it on every exit, the
// credential first. The tests below take each exit over the fake Slurm and read its file system afterwards: no
// alloc/<tag>, no credential of the run's, and the staged image and checkpoint, and another tag's directory, still there.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ownTagForm is the form of a tag newAlloc writes down (v1alpha1.JobTag): hb-, the name, a dash and 12 hex digits.
var ownTagForm = regexp.MustCompile(`^hb-[a-z0-9.-]+-[0-9a-f]{12}$`)

// removeOwnAllocation removes the Allocation directory of tag from the Provider's staging directory, over the run's own
// transport. The tag must be one this run wrote down before its submit (newAlloc), of a tag's form; any other is refused
// before anything runs. Then it cancels the run's jobs by tag, so that no job of the tag still runs to write into the
// directory (RemoveAllocationFiles' rule); removes the staged credential alone, so that it goes whatever happens to the
// rest; removes the directory by the Provider's own call; and checks that the credential is gone. It touches nothing
// outside alloc/<tag>: not the staging directory, its images or checkpoints, nor another tag's directory. It runs as a
// cleanup, after a pass, a Fatal or a NOT RUN alike, so every failure is an Errorf.
func (w *world) removeOwnAllocation(tag string) {
	w.t.Helper()
	if !slices.Contains(w.p.tags, tag) || !ownTagForm.MatchString(tag) || strings.Contains(tag, "..") {
		w.t.Errorf("an Allocation directory that is not this run's was not removed")
		return
	}
	if w.batch == nil || w.cap == nil {
		return // the controller never reached the platform: nothing was staged
	}
	ctx := context.Background()
	if err := w.cancelByTag(ctx); err != nil {
		w.t.Errorf("before the Allocation's directory was removed, cancelling the run's jobs by tag failed: %v", err)
	}
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		w.t.Errorf("the Allocation's directory was not removed: the staging directory could not be resolved: %v", err)
		return
	}
	cred := paths.Staging + "/" + agentapi.StagedCredentialPath(tag)
	if res, err := w.cap.Run(ctx, slurm.Command{Line: "rm -f " + cred}); err != nil || res.ExitCode != 0 {
		w.t.Errorf("the Allocation's staged credential was not removed: exit %d, %v", res.ExitCode, err)
	}
	if err := w.batch.RemoveAllocationFiles(ctx, tag); err != nil {
		w.t.Errorf("the Allocation's directory was not removed: %v", err)
	}
	if res, err := w.cap.Run(ctx, slurm.Command{Line: "test -f " + cred}); err != nil || res.ExitCode != 1 {
		w.t.Errorf("the Allocation's staged credential may still be there: test -f exit %d, %v", res.ExitCode, err)
	}
}

// exitRecorder is a stopRecorder whose cleanups are recorded and run by finish, as testing runs a test's after its body
// ends, so that a test can read what they left.
type exitRecorder struct {
	stopRecorder
	cleanups []func()
}

func newExitRecorder(t testing.TB) *exitRecorder {
	return &exitRecorder{stopRecorder: stopRecorder{TB: t}}
}

func (r *exitRecorder) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

// The rest of testing.TB's stops and failures, which stopRecorder leaves to the test (the body's own t.Fatal of a line of
// another form among them), recorded the same way.
func (r *exitRecorder) Fatal(a ...any) { r.Fatalf("%s", fmt.Sprint(a...)) }
func (r *exitRecorder) FailNow()       { r.Fatalf("FailNow") }
func (r *exitRecorder) Skip(a ...any)  { r.Skipf("%s", fmt.Sprint(a...)) }
func (r *exitRecorder) SkipNow()       { r.Skipf("SkipNow") }
func (r *exitRecorder) Error(a ...any) { r.Errorf("%s", fmt.Sprint(a...)) }
func (r *exitRecorder) Fail()          { r.Errorf("Fail") }
func (r *exitRecorder) Log(a ...any)   {}

// finish runs the cleanups, last first, in a goroutine of their own, as ends runs a body.
func (r *exitRecorder) finish() {
	ends(func() {
		for i := len(r.cleanups) - 1; i >= 0; i-- {
			r.cleanups[i]()
		}
	})
}

// What a cleanup must leave, relative to the staging directory: an image and a checkpoint, and another Allocation's
// directory with its credential, of a tag this run never wrote down.
var (
	keptImage      = filepath.Join(agentapi.StagedImagePath("sha256:"+strings.Repeat("cd", 32), agentapi.ImageFormatSandbox), "sentinel")
	keptCheckpoint = filepath.Join(agentapi.StagedCheckpointDir("example/kept", strings.Repeat("e", 40)), "config.json")
	foreignTag     = "hb-other-run-0123456789ab"
	keptForeign    = agentapi.StagedCredentialPath(foreignTag)
)

// stageExitWorld is the pre-staging row's dry world over the fake Slurm, with agent as its Allocation's agent build and
// the files a cleanup must keep laid out in the staging directory, the fake cluster's $SCRATCH, which it returns.
func stageExitWorld(t *testing.T, fault string, agent []byte, jobEnv map[string]string, kit func(*worldKit)) (*world, *fakeslurm.Platform, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if err := os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	// A short job directory: the helper's run directory is in it, and the agent's own rule holds its socket paths to 107
	// bytes (agent.MakeRunDir), as in TestTheStageRowsDryRunStagesWhereTheAgentStages.
	jobs, err := os.MkdirTemp("", "d")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(jobs) })
	w, plat := dryWorld(t, alpha.NowStageRow(), fault, out, dryShape{cpus: "16", memory: "125Gi"}, agent, jobEnv, dryExtra{
		run: func(rm *fakeslurm.RunMode) { rm.JobDir = jobs },
		kit: func(k *worldKit) {
			k.keepStaging = true
			if kit != nil {
				kit(k)
			}
		},
		settings: func(s settings) settings { return withProfileStaging(t, s) },
	})
	scratch := filepath.Join(w.artefacts[len(w.artefacts)-1], "root", "scratch")
	for _, p := range []string{keptImage, keptCheckpoint, keptForeign} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(scratch, p)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(scratch, p), []byte("kept\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return w, plat, scratch
}

// runStageExit runs the row's body under an exitRecorder until it ends, by a pass, a Fatal or a skip, and returns the
// recorder, its cleanups not yet run, and the run's one tag.
func runStageExit(t *testing.T, w *world) (*exitRecorder, string) {
	t.Helper()
	rec := newExitRecorder(t)
	w.t = rec
	ends(func() { stageBody(rec, w, alpha.NowStageRow().Name) })
	if len(w.p.tags) != 1 {
		t.Fatalf("the run wrote down %d tags, want its one", len(w.p.tags))
	}
	return rec, w.p.tags[0]
}

// credentialStaged fails unless the Provider staged the Allocation's credential: before the cleanup it is there to be
// removed, so the test of its removal is not one of a file that never was.
func credentialStaged(t *testing.T, scratch, tag string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(scratch, agentapi.StagedCredentialPath(tag))); err != nil {
		t.Fatalf("the Allocation's credential was not staged before the cleanup: %v", err)
	}
}

// allocationGone fails unless the run's Allocation directory is gone, with no credential of the run's left anywhere
// under alloc/, while the image, the checkpoint and the other tag's directory are all still there.
func allocationGone(t *testing.T, scratch, tag string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(scratch, agentapi.AllocationDir(tag))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the run's Allocation directory is still in the staging directory (%v)", err)
	}
	_ = filepath.WalkDir(filepath.Join(scratch, "alloc"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "credential" && p != filepath.Join(scratch, keptForeign) {
			t.Errorf("a credential is left under alloc/: %s", strings.TrimPrefix(p, scratch))
		}
		return nil
	})
	for _, p := range []string{keptImage, keptCheckpoint, keptForeign} {
		if _, err := os.Stat(filepath.Join(scratch, p)); err != nil {
			t.Errorf("the cleanup removed %s, which is not the run's Allocation's: %v", p, err)
		}
	}
}

// noRunJobLive fails when a job of the run is still running: the cleanup ends it before it removes the directory.
func noRunJobLive(t *testing.T, plat *fakeslurm.Platform) {
	t.Helper()
	for _, j := range plat.Jobs() {
		if strings.HasPrefix(j.Name, "hb-") && (j.State == fakeslurm.Running || j.State == fakeslurm.Pending) {
			t.Errorf("the run's job is still %s after the cleanup", j.State)
		}
	}
}

// The start wait's NOT RUN (the owner's other job holds the slot past the 10 minutes): the job is cancelled by its tag and
// the run skips, and the Allocation's directory, staged before the submit, goes with its credential.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsNotRunLeavesNoAllocationDirectory(t *testing.T) {
	w, plat, scratch := stageExitWorld(t, "slot-held", []byte("#!/bin/sh\nexit 0\n"), nil, nil)
	rec, tag := runStageExit(t, w)
	if rec.failed != "" || !strings.HasPrefix(rec.skipped, "NOT RUN: the job: a job did not start within 10m0s") {
		t.Fatalf("failed %q, skipped %q; want the start wait's NOT RUN", rec.failed, rec.skipped)
	}
	credentialStaged(t, scratch, tag)
	rec.finish()
	if len(rec.errors) != 0 {
		t.Errorf("the cleanup reported %q", rec.errors)
	}
	allocationGone(t, scratch, tag)
	noRunJobLive(t, plat)
}

// A result of another form than the helper's lines is a Fatal after the job ended: the directory goes all the same. An
// Allocation directory of a tag this run never wrote down is refused before anything runs, and stays.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsFatalOnAForeignLineLeavesNoAllocationDirectory(t *testing.T) {
	agent := []byte("#!/bin/sh\nprintf 'outcome=done\\n' > \"$1/" + stageResultFile + "\"\n")
	w, plat, scratch := stageExitWorld(t, "", agent, nil, nil)
	rec, tag := runStageExit(t, w)
	if !strings.Contains(rec.failed, "not one of the stage helper's lines") || rec.skipped != "" {
		t.Fatalf("failed %q, skipped %q; want the Fatal of a line of another form", rec.failed, rec.skipped)
	}
	credentialStaged(t, scratch, tag)
	rec.finish()
	if len(rec.errors) != 0 {
		t.Errorf("the cleanup reported %q", rec.errors)
	}
	allocationGone(t, scratch, tag)
	noRunJobLive(t, plat)

	rec.errors = nil
	ends(func() { w.removeOwnAllocation(foreignTag) })
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "not this run's") {
		t.Errorf("the removal of another tag's directory reported %q, want its refusal", rec.errors)
	}
	allocationGone(t, scratch, tag)
}

// A Fatal while the job still runs (the wait for its end runs out, shortened here): the cleanup cancels the job by its tag
// first, then removes the directory.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsFatalMidRunLeavesNoAllocationDirectory(t *testing.T) {
	w, plat, scratch := stageExitWorld(t, "", []byte("#!/bin/sh\nexec sleep 600\n"), nil, nil)
	w.shorten = func(time.Duration) time.Duration { return 2 * time.Second }
	rec, tag := runStageExit(t, w)
	if !strings.Contains(rec.failed, "waiting until the job ends (phase Running)") || rec.skipped != "" {
		t.Fatalf("failed %q, skipped %q; want the Fatal of the end wait, the job running", rec.failed, rec.skipped)
	}
	credentialStaged(t, scratch, tag)
	rec.finish()
	if len(rec.errors) != 0 {
		t.Errorf("the cleanup reported %q", rec.errors)
	}
	allocationGone(t, scratch, tag)
	noRunJobLive(t, plat)
}

// treeRemovalDropped is the run's transport with every rm -rf lost, as a dropped connection loses it.
type treeRemovalDropped struct{ slurm.Runner }

func (r treeRemovalDropped) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if strings.HasPrefix(cmd.Line, "rm -rf ") {
		return slurm.Result{}, errors.New("the connection dropped")
	}
	return r.Runner.Run(ctx, cmd)
}

// The credential goes first, alone: with the directory's removal lost, the credential is gone all the same, and the lost
// removal is reported.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsCredentialGoesFirst(t *testing.T) {
	agent := []byte("#!/bin/sh\nprintf 'outcome=done\\n' > \"$1/" + stageResultFile + "\"\n")
	w, _, scratch := stageExitWorld(t, "", agent, nil, func(k *worldKit) {
		dial := k.dial
		k.dial = func(v slurm.Validated) (slurm.Runner, io.Closer, error) {
			r, c, err := dial(v)
			return treeRemovalDropped{r}, c, err
		}
	})
	rec, tag := runStageExit(t, w)
	if !strings.Contains(rec.failed, "not one of the stage helper's lines") {
		t.Fatalf("failed %q, skipped %q; want the Fatal of a line of another form", rec.failed, rec.skipped)
	}
	credentialStaged(t, scratch, tag)
	rec.finish()
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "the Allocation's directory was not removed") {
		t.Errorf("the cleanup reported %q, want the lost removal of the directory alone", rec.errors)
	}
	if _, err := os.Stat(filepath.Join(scratch, agentapi.StagedCredentialPath(tag))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the credential is still there with the directory's removal lost (%v)", err)
	}
	if _, err := os.Stat(filepath.Join(scratch, agentapi.StagedConfigPath(tag))); err != nil {
		t.Errorf("the directory's removal was not lost, so this test shows nothing: %v", err)
	}
}

// stagedByTheHelper is what the dry helper stages, which the pass's cleanup leaves: its image and its checkpoint.
func stagedByTheHelper() []string {
	return []string{
		agentapi.StagedImagePath("sha256:"+strings.Repeat("ab", 32), agentapi.ImageFormatSandbox),
		agentapi.StagedCheckpointDir(stack.CheckpointRepo, stack.CheckpointRev),
	}
}
