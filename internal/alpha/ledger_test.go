package alpha

// The live Ledger and EndJobs over the stand-in cluster: the fake Slurm in run
// mode behind the rehearsal's login node, a real sshd reached by the system ssh
// client through the plugin's transport (sshrun), as the Provider's controller
// reaches a real login node. What the cluster holds is read straight from the
// fake (Platform.Jobs), never through the code under test.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	standInPartition = "stand-in-gpu"
	standInProvider  = "stand-in"
	// standInUser is the fake cluster's own user, never this host's.
	standInUser = "example-user"
	// standInWait bounds a wait on the fake cluster's state.
	standInWait = 30 * time.Second
)

// standIn is the fake Slurm in run mode behind the rehearsal's login node.
type standIn struct {
	t     *testing.T
	dir   string
	plat  *fakeslurm.Platform
	login *rehearsal.LoginNode
	creds string
	ctl   string
}

// shortTempDir is a scratch directory with a short path: the login node's
// relay socket and the ssh master's must fit in about 100 bytes.
func shortTempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "al")
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 48 {
		t.Fatalf("scratch directory %s is longer than 48 bytes; set TMPDIR to a shorter path", d)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func startStandIn(t *testing.T) *standIn {
	t.Helper()
	rehearsal.RequireTools(t)
	s := &standIn{t: t, dir: shortTempDir(t)}
	root := filepath.Join(s.dir, "c")
	home, scratch := filepath.Join(root, "home", standInUser), filepath.Join(root, "scratch", standInUser)
	for _, d := range []string{home, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{filepath.Join(s.dir, "ln"), filepath.Join(s.dir, "ctl")} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	s.ctl = filepath.Join(s.dir, "ctl")
	s.plat = fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: standInPartition, GPUOnly: true}},
		Run:        &fakeslurm.RunMode{Root: root, JobDir: filepath.Join(s.dir, "j"), KillWait: 5 * time.Second},
	})
	t.Cleanup(func() {
		if err := s.plat.Close(); err != nil {
			t.Errorf("the fake cluster's Close: %v", err)
		}
	})
	login, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(s.dir, "ln"), Platform: s.plat,
		Login: fakeslurm.Login{User: standInUser, Env: map[string]string{"HOME": home, "SCRATCH": scratch}},
	})
	if err != nil {
		t.Fatalf("the login node: %v", err)
	}
	// Registered after the Platform's, so it runs first: the login node closes before its cluster.
	t.Cleanup(func() {
		if err := login.Close(); err != nil {
			t.Errorf("the login node's Close: %v", err)
		}
	})
	s.login = login
	s.creds = filepath.Join(s.dir, "provider-ssh")
	if err := login.WriteCredentials(s.creds); err != nil {
		t.Fatal(err)
	}
	return s
}

// release is the file a stand-in job waits for.
func (s *standIn) release(tag string) string { return filepath.Join(s.dir, "release-"+tag) }

// submit sends a job under tag straight to the fake cluster, past the code
// under test: it runs until its release file exists.
func (s *standIn) submit(tag string, minutes int) int {
	s.t.Helper()
	line := fmt.Sprintf("sbatch --parsable --no-requeue --job-name=%s --partition=%s --account=example-account --time=%d --gres=gpu:1 --cpus-per-task=1 --mem=1024 '--wrap=while [ ! -e %s ]; do sleep 0.05; done'",
		tag, standInPartition, minutes, s.release(tag))
	res, err := s.login.Session().Run(context.Background(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		s.t.Fatalf("sbatch %s: %v exit %d %s", tag, err, res.ExitCode, res.Stderr)
	}
	id, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(string(res.Stdout), ";", 2)[0]))
	if err != nil {
		s.t.Fatalf("sbatch printed %q", res.Stdout)
	}
	return id
}

// job is the fake cluster's own record of a job.
func (s *standIn) job(id int) fakeslurm.Job {
	j, ok := s.plat.Job(id)
	if !ok {
		s.t.Fatalf("the fake cluster has no job %d", id)
	}
	return j
}

func (s *standIn) waitJob(what string, id int, cond func(fakeslurm.Job) bool) {
	s.t.Helper()
	deadline := time.Now().Add(standInWait)
	for !cond(s.job(id)) {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %s waiting for job %d %s (state %s)", standInWait, id, what, s.job(id).State)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// apiWith is an API holding the stand-in's PlatformProfile and Provider, read
// as the Provider's controller reads them (slurm.ProfileReader).
func (s *standIn) apiWith() client.Reader {
	s.t.Helper()
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		s.t.Fatal(err)
	}
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
	d := func(v string) *v1alpha1.Duration { x := v1alpha1.Duration(v); return &x }
	one, two := int32(1), int32(2)
	prof := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: standInProvider}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42", StagingDir: "$SCRATCH/stage", InflightWindow: d("5m"),
		Partitions: []v1alpha1.PartitionProfile{{Name: standInPartition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu", CPUArch: arch},
			MaxTime: d("30m"), GPUsPerNode: &one, CPUsPerGPU: &two, MemPerGPU: ptrTo(resource.MustParse("2Gi"))}},
	}}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: standInProvider}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: standInProvider}, AllowedPartitions: []string{standInPartition},
		CredentialsRef: &v1alpha1.CredentialsReference{File: s.creds},
	}}
	return crfake.NewClientBuilder().WithScheme(sch).WithObjects(prof, prov).Build()
}

func ptrTo[T any](v T) *T { return &v }

// standInBatch is the run's batch client over the stand-in login node.
func (s *standIn) standInBatch(ctx context.Context) *slurm.Batch {
	s.t.Helper()
	b, closer, err := SSHBatch(ctx, s.apiWith(), standInProvider, s.ctl, filepath.Join(s.dir, "host.lock"))
	if err != nil {
		s.t.Fatalf("SSHBatch: %v", err)
	}
	s.t.Cleanup(func() {
		if err := closer.Close(); err != nil {
			s.t.Errorf("the run's ssh transport's Close: %v", err)
		}
	})
	return b
}

func TestBatchLedgerReadsTheClusterByTagOverSSH(t *testing.T) {
	s := startStandIn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	start := time.Now()
	ended, running, other := "hb-run-1-aaaa", "hb-run-2-bbbb", "hb-other-cccc"
	idEnded := s.submit(ended, 7)
	idRunning := s.submit(running, 9)
	idOther := s.submit(other, 7)
	s.waitJob("to run", idRunning, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	if err := os.WriteFile(s.release(ended), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.waitJob("to end", idEnded, func(j fakeslurm.Job) bool { return j.State == "COMPLETED" && j.Finished })
	t.Cleanup(func() {
		for _, tg := range []string{running, other} {
			_ = os.WriteFile(s.release(tg), nil, 0o600)
		}
	})

	l := &BatchLedger{Finder: s.standInBatch(ctx), Since: start.Add(-AccountingSlack)}
	jobs, err := l.Jobs(ctx, []string{ended, running})
	if err != nil {
		t.Fatalf("Jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("the ledger read %d jobs, want the two of the run's tags: %+v", len(jobs), jobs)
	}
	want := map[string]struct {
		id      int
		minutes time.Duration
		ended   bool
	}{ended: {idEnded, 7 * time.Minute, true}, running: {idRunning, 9 * time.Minute, false}}
	for _, j := range jobs {
		w, ok := want[j.Name]
		switch {
		case !ok:
			t.Errorf("the ledger read a job named %q, not one of the run's tags", j.Name)
		case j.ID != strconv.Itoa(w.id):
			t.Errorf("tag %s: job %s, want %d", j.Name, j.ID, w.id)
		case j.Partition != standInPartition || j.TimeLimit != w.minutes:
			t.Errorf("tag %s: partition %q time limit %s, want %q %s", j.Name, j.Partition, j.TimeLimit, standInPartition, w.minutes)
		case j.SubmitTime.IsZero() || j.StartTime == nil:
			t.Errorf("tag %s: no submission or start time: %+v", j.Name, j)
		case w.ended != (j.EndTime != nil):
			t.Errorf("tag %s: end time %v, want ended %v", j.Name, j.EndTime, w.ended)
		}
	}
	_ = idOther
}

func TestEndJobsCancelsOnlyTheRunsTags(t *testing.T) {
	s := startStandIn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mine, pending, other := "hb-run-1-dddd", "hb-run-2-eeee", "hb-other-ffff"
	idMine := s.submit(mine, 7)
	idOther := s.submit(other, 7)
	s.waitJob("to run", idMine, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	s.waitJob("to run", idOther, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	t.Cleanup(func() { _ = os.WriteFile(s.release(other), nil, 0o600) })
	b := s.standInBatch(ctx)
	rec, err := NewRecorder(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range []string{mine, pending} {
		if err := rec.Tag(tg); err != nil {
			t.Fatal(err)
		}
	}
	if err := EndJobs(ctx, b, RunOwned("", rec), 100*time.Millisecond); err != nil {
		t.Fatalf("EndJobs: %v", err)
	}
	if j := s.job(idMine); j.State != "CANCELLED" {
		t.Errorf("the run's job is %s after EndJobs, want CANCELLED", j.State)
	}
	if j := s.job(idOther); j.State != "RUNNING" {
		t.Errorf("a job of another tag is %s after EndJobs, want RUNNING: EndJobs ends only the run's tags", j.State)
	}
}

// enderDouble is a JobEnder whose cancels take effect after a number of passes, or never.
type enderDouble struct {
	listErr  error
	cancelOK bool
	// liveFor is how many listings still show the job live after its cancel.
	liveFor   int
	cancelled atomic.Bool
	lists     atomic.Int32
	cancels   atomic.Int32
}

func (e *enderDouble) Queue(context.Context) ([]provider.Job, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	n := e.lists.Add(1)
	state := "RUNNING"
	if e.cancelled.Load() && int(n) > e.liveFor+1 {
		state = "CANCELLED"
	}
	return []provider.Job{{ID: "1", Name: "hb-x-1", State: []string{state}}}, nil
}

func (e *enderDouble) Cancel(_ context.Context, ids []string) []provider.CancelResult {
	e.cancels.Add(1)
	if !e.cancelOK {
		return []provider.CancelResult{{JobID: ids[0], Outcome: provider.CancelError, Err: errors.New("refused")}}
	}
	e.cancelled.Store(true)
	return []provider.CancelResult{{JobID: ids[0], Outcome: provider.Cancelled}}
}

// ownedX owns the doubles' job by its prefix.
var ownedX = RunOwned("hb-x-", nil)

func TestEndJobsWaitsForTheSchedulerToAct(t *testing.T) {
	// The scheduler lists the job running for two more reads after the cancel: EndJobs must keep reading.
	e := &enderDouble{cancelOK: true, liveFor: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := EndJobs(ctx, e, ownedX, time.Millisecond); err != nil {
		t.Fatalf("EndJobs: %v", err)
	}
	if n := e.lists.Load(); n < 4 {
		t.Errorf("EndJobs read the queue %d times; the job left the live states only at the 4th read", n)
	}
}

func TestEndJobsReportsWhatIsLeft(t *testing.T) {
	for name, e := range map[string]*enderDouble{
		"cancel refused":   {cancelOK: false},
		"queue unreadable": {listErr: errors.New("ssh: connection lost")},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			err := EndJobs(ctx, e, ownedX, 10*time.Millisecond)
			if err == nil {
				t.Fatal("EndJobs returned nil with the job never ended")
			}
			if strings.Contains(err.Error(), "hb-x-1") {
				t.Errorf("EndJobs's error names a tag: %v", err)
			}
		})
	}
}

// A job's liveness is the scheduler's (review of PR 255, B4): squeue's job states, from squeue(1), and its
// COMPLETING flag. A cancelled job whose processes are still ending, and a requeued job that runs again (the
// plugin's phase Failed), are alive; a job whose end state is set and that is not completing is not.
func TestLiveJobIsTheSchedulersLiveness(t *testing.T) {
	for _, c := range []struct {
		name       string
		state      []string
		completing bool
		live       bool
	}{
		{"pending", []string{"PENDING"}, false, true},
		{"running", []string{"RUNNING"}, false, true},
		{"suspended", []string{"SUSPENDED"}, false, true},
		{"requeued, pending again", []string{"PENDING", "REQUEUED"}, false, true},
		{"requeued, running again", []string{"RUNNING"}, false, true},
		{"configuring, a state this does not name", []string{"CONFIGURING"}, false, true},
		{"no state", nil, false, true},
		{"cancelled, still completing", []string{"CANCELLED", "COMPLETING"}, true, true},
		{"completed, still completing", []string{"COMPLETED", "COMPLETING"}, true, true},
		{"cancelled", []string{"CANCELLED"}, false, false},
		{"completed", []string{"COMPLETED"}, false, false},
		{"timeout", []string{"TIMEOUT"}, false, false},
		{"node fail", []string{"NODE_FAIL"}, false, false},
		{"failed", []string{"FAILED"}, false, false},
		{"preempted", []string{"PREEMPTED"}, false, false},
		{"out of memory", []string{"OUT_OF_MEMORY"}, false, false},
		{"boot fail", []string{"BOOT_FAIL"}, false, false},
		{"deadline", []string{"DEADLINE"}, false, false},
	} {
		if got := liveJob(provider.Job{State: c.state, Completing: c.completing}); got != c.live {
			t.Errorf("%s: liveJob = %v, want %v", c.name, got, c.live)
		}
	}
}

// EndJobs over the fake Slurm (not in run mode, on a manual clock) and the plugin's real batch client: the job
// states are the fake scheduler's, read through squeue as the run reads a real one. A job cancelled while its
// processes still end is COMPLETING: EndJobs keeps it alive, and does not return until the flag clears; a requeued
// job that runs again is cancelled. The phase alone (Ended, and Failed for a requeue) would let both through.
func TestEndJobsWaitsOutACompletingJobAndCancelsARequeuedOne(t *testing.T) {
	clock := pfake.NewClock(time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC))
	plat := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: standInPartition}}})
	sess := plat.Login(fakeslurm.Login{User: standInUser, Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	prof := provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: "/scratch/example-user"},
		Partition: v1alpha1.PartitionProfile{Name: standInPartition}}
	b := slurm.NewBatch(sess, prof, provider.Settings{Provider: "example", Account: "example-account", Partition: standInPartition},
		slurm.Options{Clock: clock, PollInterval: time.Nanosecond, BusyInterval: time.Nanosecond})
	prefix := "hb-alpha-0a1b2c3d4e-"
	done := plat.OutsideJob(standInUser, prefix+"done1-000000000001", standInPartition, "", 1)
	requeued := plat.OutsideJob(standInUser, prefix+"reque-000000000002", standInPartition, "", 1)
	other := plat.OutsideJob(standInUser, "hb-alpha-ffffffffff-other-000000000003", standInPartition, "", 1)
	for _, id := range []int{done, requeued, other} {
		if err := plat.Start(id); err != nil {
			t.Fatal(err)
		}
	}
	// The first: cancelled, its processes still ending. The second: requeued, and running again.
	if err := plat.End(done, "CANCELLED"); err != nil {
		t.Fatal(err)
	}
	if err := plat.HoldCompleting(done, true); err != nil {
		t.Fatal(err)
	}
	if err := plat.Requeue(requeued); err != nil {
		t.Fatal(err)
	}
	if err := plat.Start(requeued); err != nil {
		t.Fatal(err)
	}
	owned := RunOwned(prefix, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := EndJobs(ctx, b, owned, 10*time.Millisecond)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "still alive") {
		t.Fatalf("EndJobs = %v while a job of the run is still completing, want the jobs left alive", err)
	}
	if j, _ := plat.Job(requeued); j.State != "CANCELLED" {
		t.Errorf("the requeued job running again is %s, want CANCELLED", j.State)
	}
	if j, _ := plat.Job(other); j.State != "RUNNING" {
		t.Errorf("another run's job is %s, want RUNNING: it is not this run's", j.State)
	}
	if err := plat.HoldCompleting(done, false); err != nil {
		t.Fatal(err)
	}
	// The cancelled requeue may still be listed COMPLETING: the fake clears that as the clock moves.
	clock.Advance(time.Minute)
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := EndJobs(ctx, b, owned, 10*time.Millisecond); err != nil {
		t.Fatalf("EndJobs once the flag cleared: %v", err)
	}
}

func TestBatchLedgerWithoutAClientIsNamed(t *testing.T) {
	var l *BatchLedger
	if _, err := l.Jobs(context.Background(), []string{"hb-x-1"}); !errors.Is(err, ErrNoLedger) {
		t.Errorf("a ledger with no client returned %v, want ErrNoLedger", err)
	}
	if _, err := (&BatchLedger{}).Jobs(context.Background(), nil); !errors.Is(err, ErrNoLedger) {
		t.Errorf("a ledger with no Finder returned %v, want ErrNoLedger", err)
	}
}

// Close runs on every exit of Run, a run past its deadline included, so it ends
// the run's jobs on a time of its own: here the run's context is already over,
// and the run's job, which the fake shows running, must end cancelled.
func TestCloseEndsTheRunsJobsUnderADeadContext(t *testing.T) {
	s := startStandIn(t)
	tag := "hb-run-1-gggg"
	id := s.submit(tag, 7)
	s.waitJob("to run", id, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	t.Cleanup(func() { _ = os.WriteFile(s.release(tag), nil, 0o600) })
	ctx, cancelMake := context.WithTimeout(context.Background(), time.Minute)
	b, tr, err := SSHBatch(ctx, s.apiWith(), standInProvider, s.ctl, filepath.Join(s.dir, "host.lock"))
	cancelMake()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := NewRecorder(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Tag(tag); err != nil {
		t.Fatal(err)
	}
	r := &liveRun{t: t, batch: b, tr: tr, tags: rec, guardFail: make(chan struct{})}
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(dead); err != nil {
		t.Fatalf("Close under a dead context: %v", err)
	}
	if j := s.job(id); j.State != "CANCELLED" {
		t.Errorf("the run's job is %s after Close, want CANCELLED", j.State)
	}
}

// Close finds the run's jobs by the run's own prefix, not only by the tags the recorder holds (review of PR 255,
// B3): a job whose Allocation the watch never delivered, or whose tag could not be written down, is still the
// run's. Here the recorder holds nothing (its tag write failed: its directory is gone); the job of the run's prefix
// must end cancelled, and a job of another run's prefix must keep running.
func TestCloseEndsAJobOfTheRunsPrefixThatWasNeverWrittenDown(t *testing.T) {
	s := startStandIn(t)
	id := "0a1b2c3d4e"
	mine, other := TagPrefix(id)+"x7k2p-0123456789ab", TagPrefix("ffffffffff")+"x7k2p-0123456789ab"
	idMine, idOther := s.submit(mine, 7), s.submit(other, 7)
	s.waitJob("to run", idMine, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	s.waitJob("to run", idOther, func(j fakeslurm.Job) bool { return j.State == "RUNNING" })
	t.Cleanup(func() {
		for _, tg := range []string{mine, other} {
			_ = os.WriteFile(s.release(tg), nil, 0o600)
		}
	})
	ctx, cancelMake := context.WithTimeout(context.Background(), time.Minute)
	b, tr, err := SSHBatch(ctx, s.apiWith(), standInProvider, s.ctl, filepath.Join(s.dir, "host.lock"))
	cancelMake()
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out")
	rec, err := NewRecorder(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(out); err != nil {
		t.Fatal(err)
	}
	if err := rec.Tag(mine); err == nil {
		t.Fatal("a tag write into a directory that is gone succeeded")
	}
	r := &liveRun{t: t, batch: b, tr: tr, tags: rec, prefix: TagPrefix(id), guardFail: make(chan struct{})}
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if j := s.job(idMine); j.State != "CANCELLED" {
		t.Errorf("the run's job, never written down, is %s after Close, want CANCELLED", j.State)
	}
	if j := s.job(idOther); j.State != "RUNNING" {
		t.Errorf("another run's job is %s after Close, want RUNNING", j.State)
	}
}
