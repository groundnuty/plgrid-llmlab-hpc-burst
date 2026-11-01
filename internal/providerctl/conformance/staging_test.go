package conformance

// The run's staging directory. A conformance run stages the agent stand-in, frpc
// and each Allocation's files on the real platform's scratch space, whose inodes
// are shared with the owner's work (97% used on 1 Nov 2026). So every run stages
// into a directory of its own, run-<12 hex> below HPCBURST_CONF_STAGING, and
// removes the whole directory when the run ends, pass or fail (testing.T's
// Cleanup runs either way). The removal is refused for any directory that is
// not of that form, so it can never take the operator's own directory with it.
// The cleanup is tested against a fake Slurm, since the run itself needs a real
// cluster.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// runStagingForm is the resolved path of a run's staging directory: an absolute
// path of safe characters ending in run-<12 hex>.
var runStagingForm = regexp.MustCompile(`^/[A-Za-z0-9._/-]+/run-[0-9a-f]{12}$`)

// newRunStaging is a staging directory for one run, below base (the operator's
// HPCBURST_CONF_STAGING): base/run-<12 random hex characters>.
func newRunStaging(base string) (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("the run's staging directory name: %w", err)
	}
	return base + "/run-" + hex.EncodeToString(b[:]), nil
}

// removeStaging removes the run's whole staging directory on the platform: the
// resolved directory of b, which must be of the run-specific form, with rm -rf
// through run. It names no path in its errors, which reach the test log.
func removeStaging(ctx context.Context, b *slurm.Batch, run slurm.Runner) error {
	paths, err := b.Paths(ctx)
	if err != nil {
		return fmt.Errorf("the staging directory could not be resolved: %w", err)
	}
	if !runStagingForm.MatchString(paths.Staging) {
		return fmt.Errorf("the staging directory is not a run-specific one (run-<12 hex>); it was not removed")
	}
	res, err := run.Run(ctx, slurm.Command{Line: "rm -rf " + paths.Staging})
	switch {
	case err != nil:
		return fmt.Errorf("remove the staging directory: %w", err)
	case res.ExitCode != 0:
		return fmt.Errorf("remove the staging directory: exit %d", res.ExitCode)
	}
	return nil
}

// cleanupRecorder stands in for a test: Cleanup is recorded, and the functions
// run, last first, when the test body ends by any means, as testing's do after
// a pass, after FailNow and after a panic.
type cleanupRecorder struct {
	testing.TB
	funcs []func()
}

func (r *cleanupRecorder) Cleanup(f func()) { r.funcs = append(r.funcs, f) }

func (r *cleanupRecorder) finish() {
	for i := len(r.funcs) - 1; i >= 0; i-- {
		r.funcs[i]()
	}
}

// stagedRun is a fake platform with a run's staging directory on it: the
// operator's directory base holds a file of its own and another run's directory,
// and this run's directory holds what a run stages (the agent stand-in, frpc, an
// Allocation's files).
type stagedRun struct {
	sl           *fakeslurm.Platform
	session      *fakeslurm.Session
	batch        *slurm.Batch
	base, runDir string
}

func newStagedRun(t testing.TB, staging func(base string) string) *stagedRun {
	t.Helper()
	clock := fake.NewClock(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))
	sl := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: "plgrid"}}})
	session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	const base = "/scratch/example-user/hpc-burst-conformance"
	run := staging(base)
	pp, part, err := loadProfile(profilesDir(), "helios", "plgrid", strings.Replace(run, "/scratch/example-user", "$SCRATCH", 1))
	if err != nil {
		t.Fatal(err)
	}
	b := slurm.NewBatch(session, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: part},
		provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid"}, slurm.Options{Clock: clock})
	s := &stagedRun{sl: sl, session: session, batch: b, base: base, runDir: run}
	for p, content := range map[string]string{base + "/operators-own-file": "keep", base + "/run-aaaaaaaaaaaa/other-run": "keep"} {
		if err := sl.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// stage writes what a run stages, through the real EnsureFile, into the run's
// directory.
func (s *stagedRun) stage(t testing.TB) {
	t.Helper()
	tag := v1alpha1.JobTag("conformance", "example", "example-uid")
	for _, rel := range []string{"agent/0123/hb-agent", "frpc/4567/frpc", "alloc/" + tag + "/credential", "alloc/" + tag + "/dial-home.crt"} {
		content := []byte("staged " + rel)
		if err := s.batch.EnsureFile(context.Background(), provider.File{Path: rel, SHA256: sha256Of(content), Mode: 0o600, Content: content}); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.under(s.runDir); n < 4 {
		t.Fatalf("precondition: %d files staged in the run's directory, want 4", n)
	}
}

// under counts the files below dir on the fake platform.
func (s *stagedRun) under(dir string) int {
	n := 0
	for p := range s.sl.Files() {
		if strings.HasPrefix(p, dir+"/") {
			n++
		}
	}
	return n
}

func sha256Of(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// world returns a world holding only what the cleanup reads, over this run.
func (s *stagedRun) world(t testing.TB) *world {
	t.Helper()
	p := newPrivate(t, t.TempDir())
	return &world{t: t, p: p, jobIDs: map[string]bool{}, batch: s.batch, cap: &capture{Runner: s.session, p: p, submitted: func(string) {}}}
}

// checkGone fails unless the run's directory, and everything in it, is gone from
// the platform while the operator's own file and the other run's directory stay.
func (s *stagedRun) checkGone(t *testing.T, when string) {
	t.Helper()
	if _, ok := s.sl.Dirs()[s.runDir]; ok || s.under(s.runDir) != 0 {
		t.Errorf("%s: the run's staging directory is still on the platform (%d files)", when, s.under(s.runDir))
	}
	files := s.sl.Files()
	for _, keep := range []string{s.base + "/operators-own-file", s.base + "/run-aaaaaaaaaaaa/other-run"} {
		if _, ok := files[keep]; !ok {
			t.Errorf("%s: %s was removed with the run's directory", when, keep)
		}
	}
}

// After a pass, the run's directory and every file staged in it are gone; the
// operator's own file and another run's directory, beside it, stay.
func TestTheRunsStagingDirectoryIsRemovedAfterAPass(t *testing.T) {
	runDir := "/scratch/example-user/hpc-burst-conformance/run-0123456789ab"
	s := newStagedRun(t, func(string) string { return runDir })
	t.Run("the run", func(t *testing.T) {
		w := s.world(t)
		t.Cleanup(w.cleanup)
		s.stage(t)
	})
	s.checkGone(t, "after a pass")
}

// After a forced failure, in the middle of the run and after files were staged,
// the same cleanup removes the directory: it is registered with Cleanup, which
// runs when the body ends by FailNow or a panic. The stand-in for the test runs
// its Cleanup functions as testing does.
func TestTheRunsStagingDirectoryIsRemovedAfterAForcedFailure(t *testing.T) {
	runDir := "/scratch/example-user/hpc-burst-conformance/run-0123456789ab"
	s := newStagedRun(t, func(string) string { return runDir })
	rec := &cleanupRecorder{TB: t}
	func() {
		defer func() {
			if r := recover(); r != "forced failure" {
				t.Fatalf("the run ended with %v, want the forced failure", r)
			}
			rec.finish()
		}()
		w := s.world(rec)
		rec.Cleanup(w.cleanup)
		s.stage(rec)
		panic("forced failure")
	}()
	s.checkGone(t, "after a forced failure")
}

// A run that fails before its platform exists (no batch, nothing staged) has
// nothing to remove, and its cleanup neither panics nor touches the platform.
func TestTheCleanupOfARunWithNoPlatformRemovesNothing(t *testing.T) {
	s := newStagedRun(t, func(string) string { return "/scratch/example-user/hpc-burst-conformance/run-0123456789ab" })
	rec := &cleanupRecorder{TB: t}
	p := newPrivate(rec, t.TempDir())
	w := &world{t: rec, p: p, jobIDs: map[string]bool{}}
	rec.Cleanup(w.cleanup)
	rec.finish()
	if n := len(s.sl.Calls()); n != 0 {
		t.Errorf("%d platform calls from the cleanup of a run with no platform, want 0", n)
	}
}

// The removal takes only a directory of the run-specific form: the operator's
// own directory (the base) and a directory merely named like one are refused,
// and nothing is removed.
func TestTheRemovalRefusesADirectoryThatIsNotRunSpecific(t *testing.T) {
	for _, dir := range []string{
		"/scratch/example-user/hpc-burst-conformance",
		"/scratch/example-user/hpc-burst-conformance/run-0123456789",
		"/scratch/example-user/hpc-burst-conformance/run-0123456789AB",
		"/scratch/example-user/hpc-burst-conformance/my-run-0123456789ab",
	} {
		t.Run(dir, func(t *testing.T) {
			s := newStagedRun(t, func(string) string { return dir })
			before := len(s.sl.Files())
			if err := s.sl.WriteFile(dir+"/precious", []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := removeStaging(context.Background(), s.batch, s.session); err == nil || strings.Contains(err.Error(), "example-user") {
				t.Errorf("removeStaging = %v, want a refusal that names no path", err)
			}
			if n := s.sl.Count("rm"); n != 0 {
				t.Errorf("%d rm calls, want 0", n)
			}
			if got := len(s.sl.Files()); got != before+1 {
				t.Errorf("the platform holds %d files, want the %d it held plus the one planted: nothing is removed", got, before+1)
			}
		})
	}
}

// Every run's directory is its own: two runs with the same base get different
// ones, of the form the removal accepts, and the profile's stagingDir is the
// run's.
func TestEachRunStagesInADirectoryOfItsOwn(t *testing.T) {
	a, err := newRunStaging("$SCRATCH/hpc-burst-conformance")
	if err != nil {
		t.Fatal(err)
	}
	b, err := newRunStaging("$SCRATCH/hpc-burst-conformance")
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !regexp.MustCompile(`^\$SCRATCH/hpc-burst-conformance/run-[0-9a-f]{12}$`).MatchString(a) {
		t.Errorf("run directories %q and %q, want two different ones of the form base/run-<12 hex>", a, b)
	}
	m := full()
	s1, err := readSettings(env(m))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := readSettings(env(m))
	if err != nil {
		t.Fatal(err)
	}
	if s1.profile.Spec.StagingDir != s1.runStaging || s1.runStaging == s2.runStaging || !strings.HasPrefix(s1.runStaging, "$SCRATCH/hpc-burst-conformance/run-") {
		t.Errorf("run staging %q and %q, profile stagingDir %q: want each run its own directory, and the profile's stagingDir the run's", s1.runStaging, s2.runStaging, s1.profile.Spec.StagingDir)
	}
}
