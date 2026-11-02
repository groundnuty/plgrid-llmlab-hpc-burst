package conformance

// A late duplicate under GH200's effective limits (PR 210's review, B2). On
// Helios plgrid-gpu-gh200 the partition's QOS plgrid sets 2000 submitted jobs
// and comes before QOS now's 1, so a held job and a late duplicate of it are
// both accepted; now's cap of one GPU limits running jobs only. The run keeps
// HBEP-4's rules for an ambiguous submission (the controller resolves it by tag
// and cancels a held duplicate found beside the recorded job, event
// HeldDuplicateCancelled), changes nothing in production, and asserts at the
// end that no duplicate is left (settleDuplicates). The fake Slurm models the
// limits and the late registration (fakeslurm.LostReplyLate); the "controller"
// of these tests is a nudge that cancels the held duplicate, as Observe does.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// gh200Fake is a fake Slurm shaped like Helios plgrid-gpu-gh200: QOS now allows
// one submitted job and one running GPU per user, and the partition's QOS, when
// partitionQOS is set, allows 2000 submitted jobs and comes first. A batch for
// the shipped helios profile, partition plgrid-gpu-gh200, under qos now, is over it.
type gh200Fake struct {
	t     *testing.T
	sl    *fakeslurm.Platform
	ft    fakeTime
	batch *slurm.Batch
	w     *world
	rec   *stopRecorder
	tag   string
}

func newGH200Fake(t *testing.T, partitionQOS bool) *gh200Fake {
	t.Helper()
	ft := newFakeTime()
	part := fakeslurm.Partition{Name: "plgrid-gpu-gh200", GPUOnly: true}
	if partitionQOS {
		part.QOS = "plgrid"
	}
	sl := fakeslurm.New(ft.c, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{part},
		QOS: []fakeslurm.QOS{
			{Name: "now", MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)},
			{Name: "plgrid", MaxSubmitPerUser: fakeslurm.Cap(2000)},
		},
	})
	session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user", "MEMFS": "/memfs/example"}})
	pp, p, err := loadProfile(profilesDir(), "helios", "plgrid-gpu-gh200", "$SCRATCH/example-run")
	if err != nil {
		t.Fatal(err)
	}
	b := slurm.NewBatch(session, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: p},
		provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid-gpu-gh200", QOS: "now"}, slurm.Options{Clock: ft.c})
	rec := &stopRecorder{TB: t}
	w := &world{t: rec, p: newPrivate(t, t.TempDir()), batch: b, jobIDs: map[string]bool{}, now: ft.now, sleep: ft.sleep, began: ft.now()}
	w.s.account, w.s.qos = "example-account", "now"
	g := &gh200Fake{t: t, sl: sl, ft: ft, batch: b, w: w, rec: rec}
	g.tag = v1alpha1.JobTag("conformance", "conformance-lost-record-found-by-tag-observe-end", "uid-3")
	w.p.tag(g.tag)
	return g
}

// submit is one held submission under the tag, through the plugin's real check.
func (g *gh200Fake) submit() (string, provider.SubmitOutcome, error) {
	g.t.Helper()
	params := v1alpha1.BatchParameters{Partition: "plgrid-gpu-gh200", TimeLimit: "5m", GPUs: 1, CPUsPerTask: 1, Memory: resource.MustParse("1Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64)}
	return g.batch.Submit(context.Background(), heldRequest(g.w.s, g.tag, params))
}

func (g *gh200Fake) job(t *testing.T, id string) fakeslurm.Job {
	t.Helper()
	n, _ := strconv.Atoi(id)
	j, ok := g.sl.Job(n)
	if !ok {
		t.Fatalf("the fake holds no job %s", id)
	}
	return j
}

// runToTheEnd lifts the hold of a job, starts it and ends it, as the first job of a step runs.
func (g *gh200Fake) runToTheEnd(t *testing.T, id string) {
	t.Helper()
	n, _ := strconv.Atoi(id)
	if err := g.batch.LiftHold(context.Background(), id); err != nil {
		t.Fatalf("lift: %v", err)
	}
	if err := g.sl.Start(n); err != nil {
		t.Fatal(err)
	}
	if err := g.sl.End(n, "COMPLETED"); err != nil {
		t.Fatal(err)
	}
}

// lateDuplicate plays out Astra's scenario: attempt 1's reply is lost and its job
// registers late, at the next sbatch; attempt 2 is the controller's retry after the
// in-flight window. It returns both job IDs, and the retry's outcome.
func (g *gh200Fake) lateDuplicate(t *testing.T) (first, second string, outcome provider.SubmitOutcome) {
	t.Helper()
	g.sl.Script(fakeslurm.LostReplyLate(slurm.ErrTimeout))
	if id, out, _ := g.submit(); id != "" || out != provider.Unknown {
		t.Fatalf("attempt 1: job %q, outcome %s; want an unknown outcome and no ID", id, out)
	}
	g.ft.c.Advance(6 * time.Minute) // the in-flight window passes with nothing found
	id2, out, _ := g.submit()
	jobs := g.sl.Jobs()
	if len(jobs) == 0 {
		t.Fatalf("no job registered at all: the late job never registered, so the fixture proves nothing")
	}
	return strconv.Itoa(jobs[0].ID), id2, out
}

// The premise of the finding, and its control. Under GH200's effective limits the
// retry is accepted beside the late original: two held jobs of one tag. Under
// QOS now alone the retry is refused for the submit limit, which is why now's own
// limit of one is no guard on this partition.
func TestUnderGH200sLimitsALateOriginalAndItsRetryAreBothAccepted(t *testing.T) {
	g := newGH200Fake(t, true)
	first, second, out := g.lateDuplicate(t)
	if out != provider.Accepted || second == "" || first == second {
		t.Fatalf("retry: job %q, outcome %s; want it accepted beside the late original %s", second, out, first)
	}
	if jobs := g.sl.Jobs(); len(jobs) != 2 || !jobs[0].Held || !jobs[1].Held || jobs[0].Name != jobs[1].Name || jobs[0].Name != g.tag {
		t.Fatalf("jobs %+v; want two held jobs with the one tag", jobs)
	}

	c := newGH200Fake(t, false)
	_, second, out = c.lateDuplicate(t)
	if out == provider.Accepted || second != "" {
		t.Errorf("control, QOS now alone: the retry was accepted (job %q, %s); the fixture does not tell the two limits apart", second, out)
	}
	if n := len(c.sl.Jobs()); n != 1 {
		t.Errorf("control: %d jobs, want only the late original", n)
	}
}

// What the controller does with that duplicate: it cancels the held one found beside the
// recorded job. The run gives it until the end of the run and asserts that it did.
func TestAnEndOfRunWithALateDuplicateCancelledByTheControllerPasses(t *testing.T) {
	g := newGH200Fake(t, true)
	first, second, _ := g.lateDuplicate(t)
	g.runToTheEnd(t, first)
	nudges := 0
	g.w.nudge = func(string) {
		nudges++
		if r := g.batch.Cancel(context.Background(), []string{second}); r[0].Outcome == provider.CancelError {
			t.Errorf("cancel: %v", r[0].Err)
		}
	}
	began := g.ft.now()
	g.w.settleDuplicates([]string{"alloc"}, duplicateWait)
	if len(g.rec.errors) != 0 || g.rec.failed != "" {
		t.Fatalf("errors %v, failure %q; want none: the duplicate was cancelled", g.rec.errors, g.rec.failed)
	}
	if nudges == 0 || g.ft.since(began) >= duplicateWait {
		t.Errorf("%d nudges in %s; want the duplicate cancelled within the wait, by a pass of the controller", nudges, g.ft.since(began))
	}
	if st := g.job(t, second); st.State != fakeslurm.Cancelled || !st.Start.IsZero() {
		t.Errorf("the duplicate is %s (started %v), want cancelled while held", st.State, st.Start)
	}
}

// A duplicate nobody cancels is a failure of the run after the wait, never a pass, and
// the cleanup then finds it by its tag; two jobs of a tag that both started is a failure at once.
func TestAnEndOfRunWithALiveDuplicateFailsAfterTheWait(t *testing.T) {
	g := newGH200Fake(t, true)
	first, second, _ := g.lateDuplicate(t)
	g.runToTheEnd(t, first)
	g.w.nudge = func(string) {} // the controller does nothing
	began := g.ft.now()
	g.w.settleDuplicates([]string{"alloc"}, duplicateWait)
	if len(g.rec.errors) != 1 || !strings.Contains(g.rec.errors[0], "still live at the end") {
		t.Fatalf("errors %v; want one naming the live duplicate", g.rec.errors)
	}
	if waited := g.ft.since(began); waited < duplicateWait || waited > duplicateWait+10*time.Second {
		t.Errorf("waited %s, want the whole %s and no more", waited, duplicateWait)
	}
	if !g.w.jobIDs[second] {
		t.Errorf("the live duplicate %s was not noted for the cleanup", second)
	}
}

func TestAnEndOfRunWhereTwoJobsOfATagStartedFailsAtOnce(t *testing.T) {
	g := newGH200Fake(t, true)
	first, second, _ := g.lateDuplicate(t)
	g.runToTheEnd(t, first)
	g.runToTheEnd(t, second)
	g.w.nudge = func(string) { t.Error("the controller was nudged: two jobs started, the run fails at once") }
	g.w.settleDuplicates([]string{"alloc"}, duplicateWait)
	if len(g.rec.errors) != 1 || !strings.Contains(g.rec.errors[0], "2 jobs of one of this run's tags started") {
		t.Errorf("errors %v; want one naming the two started jobs", g.rec.errors)
	}
}

// A run with one job per tag, all ended, passes without a pass of the controller.
func TestAnEndOfRunWithNoDuplicatePassesWithoutWaiting(t *testing.T) {
	g := newGH200Fake(t, true)
	id, out, err := g.submit()
	if out != provider.Accepted || err != nil {
		t.Fatalf("submit: %s %v", out, err)
	}
	if r := g.batch.Cancel(context.Background(), []string{id}); r[0].Outcome != provider.Cancelled {
		t.Fatalf("cancel: %+v", r[0])
	}
	g.w.nudge = func(string) { t.Error("the controller was nudged for a run with no live job") }
	began := g.ft.now()
	g.w.settleDuplicates([]string{"alloc"}, duplicateWait)
	if len(g.rec.errors) != 0 || g.ft.since(began) != 0 {
		t.Errorf("errors %v after %s; want none and no wait", g.rec.errors, g.ft.since(began))
	}
}

// An unresolved submission is written down once, as a tag, in a file of mode 600 that
// hack/live-checks.sh reads.
func TestAnUnresolvedSubmissionIsWrittenDownOnce(t *testing.T) {
	dir := t.TempDir()
	p := newPrivate(t, dir)
	tag := v1alpha1.JobTag("conformance", "example", "uid-9")
	p.unresolve(tag)
	p.unresolve(tag)
	raw, err := os.ReadFile(filepath.Join(dir, unresolvedFile))
	if err != nil || strings.TrimSpace(string(raw)) != tag {
		t.Fatalf("unresolved.txt = %q, %v; want the tag once", raw, err)
	}
	if info, _ := os.Stat(filepath.Join(dir, unresolvedFile)); info.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 600", info.Mode().Perm())
	}
}
