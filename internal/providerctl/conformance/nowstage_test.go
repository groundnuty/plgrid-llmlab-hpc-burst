package conformance

// The pre-staging row of issue #376, alpha-athena-now-stage (internal/alpha, NowStageRow; the science agent's rulings 1
// to 3 of 11 Nov 2026): one job on Athena's plgrid-now, under profiles/athena-now.yaml, whose agent build is the
// test-only helper stagehelper. The helper calls the real stager (internal/agent/stage) with the agent's own
// configuration, so it stages the sandbox image and the checkpoint where the agent stages, under the digest's lock with
// the integrity records the agent writes: the Provider's staging directory as the profile names it, $SCRATCH, which the
// agent of row A then reads, and which this run keeps (worldKit.keepStaging). It runs through the Provider's controller
// like the reach rows (reach_run_test.go), with no stack and no Instance, under the row's one-job slot and its start wait
// (NOT RUN when the slot's owner keeps it waiting), and is cancelled by its tag.
//
// A cold build on A100 is unmeasured, so this row is the cold-start measurement: a staging that has not ended by the
// job's 30-minute time limit is a FAIL that names that. Slurm's TERM at the limit cancels the staging, and the helper
// writes its signalled line (outcome=signalled, never a step's error line), or, when the KILL comes first, nothing. The
// result is the helper's one line of fixed words and numbers, read back and held to its form (stagehelper, writeResult),
// so no path or other text of the node reaches the test or the post.
//
// The staging directory is kept, for row A; the Allocation's own directory in it, alloc/<tag>, with its agent.json,
// its staged credential and the job's output, is not: it goes on every exit of the run, the credential first
// (removeOwnAllocation, registered before the submit), as the run-directory wipe of the other rows would take it. That row A starts warm is shown by row A's own agent log,
// never by this row's line (internal/alpha, warmStartShownByTheAgentsOwnLog).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The row's own bounds and files.
const (
	// stageTimeLimit is the job's time limit, the row's lease (alpha.NowStageLease); hack/live-checks.sh's shape carries
	// it as 00:30:00.
	stageTimeLimit = "30m"
	// stageEndWait is how long the job may take once it has started: its time limit and a minute for the scheduler. Slurm's
	// KillWait (180 s on Athena) does not add to it: at the limit the job's base state is TIMEOUT, which is Ended while the
	// job is still COMPLETING (provider.SlurmPhase; AllocationObservation.JobCompleting), and the Allocation's deletion,
	// bounded by stageDeleteWait, is what waits for the COMPLETING to clear. A dry run shortens it (bound).
	stageEndWait = 31 * time.Minute
	// stageDeleteWait bounds the deletion of the run's Allocation, whose finalizer has the Provider remove its files.
	stageDeleteWait = 5 * time.Minute
	// stageResultFile is the helper's line in the Allocation's directory (stagehelper's resultName).
	stageResultFile = "stage-result"
	// stageHelperSetting names the helper build hack/live-checks.sh made (hack/build-stage-helper.sh).
	stageHelperSetting = "HPCBURST_CONF_STAGE_HELPER"
)

// The helper's lines (stagehelper's result.line), and nothing else.
var (
	stageOKLine    = regexp.MustCompile(`^outcome=ok image_seconds=(\d+) image_built=(true|false) checkpoint_seconds=(\d+) files=(\d+) mismatched=(\d+)\n$`)
	stageErrorLine = regexp.MustCompile(`^outcome=error step=(image|files|checkpoint|config)\n$`)
	// stageSignalledLine is a step cut off by the signal: Slurm's TERM at the time limit, or a cancel.
	stageSignalledLine = regexp.MustCompile(`^outcome=signalled step=(image|files|checkpoint|config)\n$`)
)

// stageResult is the helper's line as read.
type stageResult struct {
	present, ok                     bool
	signalled                       bool
	step                            string
	imageSeconds, checkpointSeconds int
	imageBuilt                      bool
	files, mismatched               int
}

// parseStageResult reads the helper's line; anything but one of its lines is an error.
func parseStageResult(b []byte) (stageResult, error) {
	if m := stageOKLine.FindSubmatch(b); m != nil {
		n := func(i int) int { v, _ := strconv.Atoi(string(m[i])); return v }
		return stageResult{present: true, ok: true, imageSeconds: n(1), imageBuilt: string(m[2]) == "true", checkpointSeconds: n(3), files: n(4), mismatched: n(5)}, nil
	}
	if m := stageErrorLine.FindSubmatch(b); m != nil {
		return stageResult{present: true, step: string(m[1])}, nil
	}
	if m := stageSignalledLine.FindSubmatch(b); m != nil {
		return stageResult{present: true, signalled: true, step: string(m[1])}, nil
	}
	return stageResult{}, fmt.Errorf("the stage helper's result is not one of its lines")
}

// stageVerdict is the row's verdict from the helper's line and the job's time on the cluster (its start to its end, the
// scheduler's): PASS for a staging that ended ok with nothing mismatched, else a FAIL with its reason. A job that ran its
// whole time limit and whose helper was signalled, or left no line, is the cold staging past its lease, the cold-start
// FAIL; a signalled helper whose job ended sooner was cancelled. Neither is ever a step's error.
func stageVerdict(r stageResult, ran, limit time.Duration) (pass bool, why string) {
	switch {
	case r.signalled && ran >= limit-time.Minute:
		return false, fmt.Sprintf("the cold staging did not finish within the row's %s lease: the job ran %s and the time limit's signal ended the helper in its %s step (the cold-start measurement: past its lease it is a FAIL)", limit, ran.Round(time.Second), r.step)
	case r.signalled:
		return false, fmt.Sprintf("a signal ended the stage helper in its %s step after %s, within the row's %s lease: a cancel, not its time limit", r.step, ran.Round(time.Second), limit)
	case !r.present && ran >= limit-time.Minute:
		return false, fmt.Sprintf("the cold staging did not finish within the row's %s lease: the job ran %s and the helper was ended before it wrote its result (the cold-start measurement: past its lease it is a FAIL)", limit, ran.Round(time.Second))
	case !r.present:
		return false, fmt.Sprintf("the stage helper left no result, and the job ended after %s, within its lease", ran.Round(time.Second))
	case !r.ok:
		return false, "the staging failed at its " + r.step + " step"
	case r.mismatched > 0:
		return false, fmt.Sprintf("%d checkpoint files were present and did not match their identity", r.mismatched)
	}
	return true, ""
}

// stageExampleInputs are the served deployment's image and checkpoint, read as a ModelDeployment from the file row A
// serves (examples/modeldeployment-qwen25-instruct.yaml), and its load format, auto when it names none.
func stageExampleInputs(t testing.TB) (image, digest, repo, rev, load string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "examples", "modeldeployment-qwen25-instruct.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var md v1alpha1.ModelDeployment
	if err := yaml.UnmarshalStrict(b, &md); err != nil {
		t.Fatal(err)
	}
	sv := md.Spec.Source.Served
	if sv == nil {
		t.Fatal("the example deployment is not a served one")
	}
	load = "auto"
	for _, f := range sv.Flags {
		if f.Name == "load-format" {
			load = f.Value
		}
	}
	return sv.Engine.Image, sv.Engine.ImageDigest, sv.Checkpoint.Repository, sv.Checkpoint.Revision, load
}

// stageHelperBinary is the helper build the script made, held to the example deployment: each of its compiled-in values
// is in the binary, as -X writes them, so a build for another image or checkpoint is refused before anything is
// submitted.
func stageHelperBinary(t testing.TB, path string) []byte {
	t.Helper()
	if path == "" {
		t.Fatalf("%s is not set: hack/live-checks.sh builds the helper (hack/build-stage-helper.sh) before the row takes its slot", stageHelperSetting)
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		t.Fatalf("%s names no helper build: %v", stageHelperSetting, err)
	}
	image, digest, repo, rev, _ := stageExampleInputs(t)
	for _, v := range []string{image, digest, repo, rev} {
		if !bytes.Contains(b, []byte(v)) {
			t.Fatal("the stage helper was not built with the example deployment's image and checkpoint (hack/build-stage-helper.sh)")
		}
	}
	return b
}

// shippedStagingDir is the profile's own stagingDir, as the file ships it: where the agent stages.
func shippedStagingDir(t testing.TB, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(profilesDir(), name+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(b, &pp); err != nil {
		t.Fatal(err)
	}
	if pp.Spec.StagingDir == "" {
		t.Fatalf("profiles/%s.yaml names no stagingDir", name)
	}
	return pp.Spec.StagingDir
}

// withProfileStaging is the run's settings with the Provider staging where the agent does, the profile's own stagingDir,
// in place of a run's own directory below it: the cleanup then keeps it (worldKit.keepStaging).
func withProfileStaging(t testing.TB, s settings) settings {
	t.Helper()
	s.profile = s.profile.DeepCopy()
	s.profile.Spec.StagingDir = shippedStagingDir(t, s.profile.Name)
	s.runStaging = ""
	return s
}

// TestNowStage is the pre-staging row on Athena plgrid-now (issue #376): the stage helper as the Allocation's agent build,
// through the Provider's controller. The settings of TestConformance apply, with HPCBURST_CONF_STAGE_HELPER.
//
//livecheck:alpha-athena-now-stage
func TestNowStage(t *testing.T) {
	if os.Getenv("HPCBURST_ALPHA_DRY") == "1" {
		t.Fatal("the pre-staging row has no dry run through the script; its dry run is TestTheStageRowsDryRunStagesWhereTheAgentStages")
	}
	s := withProfileStaging(t, load(t))
	if s.profile.Name != "athena-now" || s.part.Name != "plgrid-now" {
		t.Fatalf("the pre-staging row runs under athena-now on plgrid-now, not %s on %s", s.profile.Name, s.part.Name)
	}
	helper := stageHelperBinary(t, os.Getenv(stageHelperSetting))
	w := buildWorld(t, s, worldKit{agent: helper, keepStaging: true, dial: func(v slurm.Validated) (slurm.Runner, io.Closer, error) {
		ssh, err := sshrunNew(s, v)
		return ssh, ssh, err
	}})
	stageBody(t, w, alpha.NowStageRow().Name)
}

// stageResultOf reads the helper's line from the Allocation's directory, its last bytes (the one read both a login node
// and the fake have), held to its form. A file that is not there is no result.
func (w *world) stageResultOf(ctx context.Context, tag string) stageResult {
	w.t.Helper()
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		w.t.Fatalf("the staging directory could not be resolved: %v", err)
	}
	res, err := w.cap.Run(ctx, slurm.Command{Line: "tail -c 256 " + paths.Staging + "/alloc/" + tag + "/" + stageResultFile})
	if err != nil {
		w.t.Fatalf("the read of the stage helper's result: %v", err)
	}
	if res.ExitCode != 0 {
		return stageResult{}
	}
	r, err := parseStageResult(res.Stdout)
	if err != nil {
		w.t.Fatal("the job's result file holds something that is not one of the stage helper's lines")
	}
	return r
}

// stageBody is the run, live or dry: one job through the Provider's controller, the helper's line, and the Allocation
// deleted, so that the Provider removes its own files from the kept staging directory.
func stageBody(t testing.TB, w *world, rowName string) {
	t.Helper()
	ctx := context.Background()
	row, ok := alpha.RowByName(rowName)
	if !ok || rowName != alpha.NowStageRow().Name {
		t.Fatalf("%s is not the pre-staging row", rowName)
	}
	name := "stage"
	w.timeLimit = stageTimeLimit
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	// Registered before the submit (the first reconcile of untilRuns): whatever ends the run from here, a NOT RUN, a Fatal or
	// a pass, the Allocation's own directory goes, its credential first.
	t.Cleanup(func() { w.removeOwnAllocation(tag) })
	// The job waits in the queue (the start wait, the first clock: past it the job is cancelled by its tag and the run is
	// NOT RUN, with no text to post; startwait_test.go), runs, and ends by itself or at its time limit.
	started := func(a *v1alpha1.Allocation) bool { return accountingStarted(t, a) }
	w.untilRuns("the job", name, "the job runs", w.at(), "", started)
	w.until(name, "the job ends", w.bound(stageEndWait), func(a *v1alpha1.Allocation) bool {
		accountingNotFailed(t, a)
		return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded
	})
	var ran time.Duration
	if obs := w.get(name).Status.AtProvider; obs.StartTime != nil && obs.EndTime != nil {
		ran = obs.EndTime.Sub(obs.StartTime.Time)
	}
	res := w.stageResultOf(ctx, tag)
	if !res.ok {
		// The helper's own standard error, to the test's private log only (it can name a path): the post names the step.
		if paths, err := w.batch.Paths(ctx); err == nil {
			if out, err := w.cap.Run(ctx, slurm.Command{Line: "tail -c 4096 " + paths.Staging + "/alloc/" + tag + "/job.err"}); err == nil && out.ExitCode == 0 {
				t.Logf("the stage helper's standard error, its end:\n%s", out.Stdout)
			}
		}
	}
	if err := w.c.Delete(ctx, w.get(name)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(stageDeleteWait)
	for w.get(name) != nil && time.Now().Before(deadline) {
		w.reconcile(name)
		time.Sleep(2 * time.Second)
	}
	if w.get(name) != nil {
		t.Fatal("the deleted Allocation still holds its finalizer")
	}
	limit, err := time.ParseDuration(stageTimeLimit)
	if err != nil {
		t.Fatal(err)
	}
	pass, why := stageVerdict(res, ran, limit)
	verdict := "PASS"
	if !pass {
		verdict = "FAIL"
	}
	posts := fmt.Sprintf("%s: %s (posts to %s)\n- the cold staging on partition %s, qos %s, in the Provider's staging directory as the profile names it: the job ran %s of its %s lease\n",
		row.Name, verdict, row.PostsTo, row.Partition, qosUsed(w.s.qos, w.s.part.QOS), ran.Round(time.Second), limit)
	if res.ok {
		posts += fmt.Sprintf("- the sandbox image: built %t, in %d s; the checkpoint: %d files in %d s, %d mismatched\n", res.imageBuilt, res.imageSeconds, res.files, res.checkpointSeconds, res.mismatched)
	}
	if !pass {
		posts += "- " + why + "\n"
	}
	w.p.save(alpha.PostsFile, []byte(posts))
	w.p.save(alpha.RecordFile, []byte(fmt.Sprintf("{\"row\": %q, \"verdict\": %q, \"ranSeconds\": %d, \"result\": {\"present\": %t, \"ok\": %t, \"signalled\": %t, \"step\": %q, \"imageBuilt\": %t, \"imageSeconds\": %d, \"files\": %d, \"checkpointSeconds\": %d, \"mismatched\": %d}}\n",
		row.Name, verdict, int(ran.Seconds()), res.present, res.ok, res.signalled, res.step, res.imageBuilt, res.imageSeconds, res.files, res.checkpointSeconds, res.mismatched)))
	if !pass {
		t.Fatalf("%s: %s", row.Name, why)
	}
	t.Logf("%s passed; the text to post is in %s", row.Name, alpha.PostsFile)
}

// dryStageHelper is the helper built for this machine with the Hub double's checkpoint and an image of the right shape
// compiled in, once per process, into a directory of the test's own.
func dryStageHelper(t testing.TB, dir string) []byte {
	t.Helper()
	out := filepath.Join(dir, "stagehelper")
	flags := "-X main.image=example.org/engine/server -X main.imageDigest=sha256:" + strings.Repeat("ab", 32) +
		" -X main.checkpointRepo=" + stack.CheckpointRepo + " -X main.checkpointRev=" + stack.CheckpointRev + " -X main.loadFormat=auto"
	cmd := exec.Command("go", "build", "-ldflags", flags, "-o", out, "./internal/providerctl/conformance/stagehelper")
	cmd.Dir = filepath.Join("..", "..", "..")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build the stage helper: %v\n%s", err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The pre-staging row's dry run: the helper as the agent build of a job of the fake Slurm in run mode, on this machine,
// with the engine double's fake Apptainer and the rehearsal's Hub double. It stages where the Provider stages, the
// profile's own staging directory (the fake cluster's $SCRATCH), and the run passes with the helper's line read back;
// then a fresh real stager, configured as the agent's, over that directory finds the image and every listed file already
// staged, by its own lines. That row A's agent does on a cluster is row A's own agent log's to show.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsDryRunStagesWhereTheAgentStages(t *testing.T) {
	bin := t.TempDir()
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	hub := stack.StartHub(t)
	helper := dryStageHelper(t, t.TempDir())
	r := alpha.NowStageRow()
	jobEnv := map[string]string{"HPCBURST_APPTAINER": filepath.Join(bin, "apptainer"), "HF_ENDPOINT": hub.URL}
	// The fake cluster's $SCRATCH, with what a cleanup must keep laid out in it (nowstage_alloc_test.go).
	w, plat, scratch := stageExitWorld(t, "", helper, jobEnv, nil)
	out := w.p.dir
	rec, tag := runStageExit(t, w)
	if rec.failed != "" || rec.skipped != "" || len(rec.errors) != 0 {
		t.Fatalf("the dry run did not pass: failed %q, skipped %q, errors %q", rec.failed, rec.skipped, rec.errors)
	}
	// The pass's exit: the Allocation's directory gone with its credential, by its deletion and by the cleanup alike, and
	// the image and checkpoint the helper staged kept.
	rec.finish()
	if len(rec.errors) != 0 {
		t.Errorf("the cleanup reported %q", rec.errors)
	}
	allocationGone(t, scratch, tag)
	noRunJobLive(t, plat)
	for _, p := range stagedByTheHelper() {
		if _, err := os.Stat(filepath.Join(scratch, p)); err != nil {
			t.Errorf("the cleanup removed the staged %s: %v", p, err)
		}
	}
	// What the agent of row A would then find: a fresh real stager, configured as cmd/hb-agent configures it, over the same
	// staging directory (the fake cluster's $SCRATCH), says by its own lines (PR #393) that the image is already staged
	// and verified and that every listed file is present and matching, and builds nothing.
	run, err := os.MkdirTemp("", "r")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(run) })
	var log bytes.Buffer
	st, err := stage.New(stage.Config{StagingDir: scratch, Apptainer: filepath.Join(bin, "apptainer"), TmpDir: run, HubEndpoint: hub.URL, BaseEnv: os.Environ(), Log: &log})
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("ab", 32)
	built := false
	ctx := agent.WithImageBuild(context.Background(), func(string) { built = true })
	if _, err := st.Image(ctx, "example.org/engine/server", digest, agentapi.ImageFormatSandbox); err != nil {
		t.Fatal(err)
	}
	files := dryHubFiles(t, hub.URL)
	if _, err := st.Checkpoint(ctx, agent.CheckpointRequest{Repository: stack.CheckpointRepo, Revision: stack.CheckpointRev, Files: files}); err != nil {
		t.Fatal(err)
	}
	text := log.String()
	if built || !strings.Contains(text, "image "+digest+": "+alpha.StagedAlreadyVerified) {
		t.Errorf("the agent's stager would build the image again (built %t):\n%s", built, text)
	}
	skip := stage.SkippedFiles(files, "")
	for i, f := range files {
		if !skip[i] && !strings.Contains(text, "checkpoint file "+f.Path+": "+alpha.StagedPresentMatching) {
			t.Errorf("the agent's stager does not find %s present and matching:\n%s", f.Path, text)
		}
	}
	posts, err := os.ReadFile(filepath.Join(out, alpha.PostsFile))
	if err != nil || !strings.Contains(string(posts), r.Name+": PASS") || !strings.Contains(string(posts), "built true") {
		t.Errorf("the dry run's text to post does not say a cold build passed (%v):\n%s", err, posts)
	}
	if regexp.MustCompile(`/|sha256`).Match(posts) {
		t.Errorf("the text to post names a path or a digest:\n%s", posts)
	}
}

// The verdict from the helper's line and the job's time, by hand: a staging that ended ok passes; a signalled line or a
// missing one from a job that ran its whole 30 minutes is the cold staging past its lease, a FAIL that names it and never
// a step's error; a signalled line from a job that ended sooner is a cancel; a failed step and a mismatched file are
// FAILs; a line of another form is refused.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageRowsVerdict(t *testing.T) {
	limit := 30 * time.Minute
	ok, err := parseStageResult([]byte("outcome=ok image_seconds=600 image_built=true checkpoint_seconds=40 files=10 mismatched=0\n"))
	if err != nil || !ok.ok || ok.files != 10 || !ok.imageBuilt {
		t.Fatalf("the ok line read as %+v, %v", ok, err)
	}
	if pass, why := stageVerdict(ok, 12*time.Minute, limit); !pass {
		t.Errorf("an ok staging fails: %s", why)
	}
	if pass, why := stageVerdict(stageResult{}, 30*time.Minute, limit); pass || !strings.Contains(why, "did not finish within the row's 30m0s lease") {
		t.Errorf("a staging past the lease: %t, %q", pass, why)
	}
	if pass, why := stageVerdict(stageResult{}, 5*time.Minute, limit); pass || !strings.Contains(why, "left no result") {
		t.Errorf("a helper that left nothing within the lease: %t, %q", pass, why)
	}
	cut, err := parseStageResult([]byte("outcome=signalled step=image\n"))
	if err != nil || !cut.present || cut.ok || !cut.signalled || cut.step != "image" {
		t.Fatalf("the signalled line read as %+v, %v", cut, err)
	}
	if pass, why := stageVerdict(cut, 30*time.Minute, limit); pass || !strings.Contains(why, "did not finish within the row's 30m0s lease") ||
		!strings.Contains(why, "time limit's signal ended the helper in its image step") || strings.Contains(why, "failed at") {
		t.Errorf("a staging cut off by the time limit: %t, %q", pass, why)
	}
	if pass, why := stageVerdict(cut, 4*time.Minute, limit); pass || !strings.Contains(why, "a cancel, not its time limit") || strings.Contains(why, "failed at") {
		t.Errorf("a staging cancelled within its lease: %t, %q", pass, why)
	}
	failed, _ := parseStageResult([]byte("outcome=error step=checkpoint\n"))
	if pass, why := stageVerdict(failed, 3*time.Minute, limit); pass || !strings.Contains(why, "checkpoint step") {
		t.Errorf("a failed staging: %t, %q", pass, why)
	}
	mism := ok
	mism.mismatched = 1
	if pass, _ := stageVerdict(mism, 12*time.Minute, limit); pass {
		t.Error("a mismatched file passes")
	}
	for _, bad := range []string{"", "outcome=ok\n", "outcome=error step=/scratch\n", "outcome=signalled step=/scratch\n", "outcome=signalled\n", "outcome=ok image_seconds=1 image_built=true checkpoint_seconds=1 files=1 mismatched=0", "x\noutcome=error step=image\n"} {
		if _, err := parseStageResult([]byte(bad)); err == nil {
			t.Errorf("%q read as a line of the helper's", bad)
		}
	}
}

// The script's build of the helper takes the example deployment's values: its --print is the -ldflags of the image, the
// digest, the checkpoint and the load format that the file, read as a ModelDeployment, holds.
//
//livecheck:none a dry test on this host, no cluster
func TestTheStageHelpersBuildIsTheExampleDeployments(t *testing.T) {
	cmd := exec.Command("bash", filepath.Join("..", "..", "..", "hack", "build-stage-helper.sh"), "--print")
	b, err := cmd.Output()
	if err != nil {
		t.Fatalf("hack/build-stage-helper.sh --print: %v", err)
	}
	image, digest, repo, rev, load := stageExampleInputs(t)
	want := fmt.Sprintf("-X main.image=%s -X main.imageDigest=%s -X main.checkpointRepo=%s -X main.checkpointRev=%s -X main.loadFormat=%s\n", image, digest, repo, rev, load)
	if string(b) != want {
		t.Errorf("the build's flags are %q, want %q", b, want)
	}
}

// dryHubFiles is the Hub double's checkpoint as an identity record lists it, read here over plain net/http and hashed by
// crypto/sha256, by none of the code under test.
func dryHubFiles(t testing.TB, hubURL string) []agentapi.FileDigest {
	t.Helper()
	get := func(url string) []byte {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d %v", url, resp.StatusCode, err)
		}
		return b
	}
	var listing []struct{ Path, Type string }
	if err := json.Unmarshal(get(hubURL+"/api/models/"+stack.CheckpointRepo+"/tree/"+stack.CheckpointRev), &listing); err != nil {
		t.Fatal(err)
	}
	var files []agentapi.FileDigest
	for _, e := range listing {
		if e.Type == "file" {
			sum := sha256.Sum256(get(hubURL + "/" + stack.CheckpointRepo + "/resolve/" + stack.CheckpointRev + "/" + e.Path))
			files = append(files, agentapi.FileDigest{Path: e.Path, SHA256: hex.EncodeToString(sum[:])})
		}
	}
	if len(files) < 2 {
		t.Fatalf("the Hub double lists %d files", len(files))
	}
	return files
}
