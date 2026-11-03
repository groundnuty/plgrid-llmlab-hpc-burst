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
	if err := EndJobs(ctx, b, []string{mine, pending}, 100*time.Millisecond); err != nil {
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

func (e *enderDouble) ListByTag(context.Context, string) ([]provider.Job, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	n := e.lists.Add(1)
	phase := v1alpha1.AllocationRunning
	if e.cancelled.Load() && int(n) > e.liveFor+1 {
		phase = v1alpha1.AllocationEnded
	}
	return []provider.Job{{ID: "1", Name: "hb-x-1", Phase: phase}}, nil
}

func (e *enderDouble) Cancel(_ context.Context, ids []string) []provider.CancelResult {
	e.cancels.Add(1)
	if !e.cancelOK {
		return []provider.CancelResult{{JobID: ids[0], Outcome: provider.CancelError, Err: errors.New("refused")}}
	}
	e.cancelled.Store(true)
	return []provider.CancelResult{{JobID: ids[0], Outcome: provider.Cancelled}}
}

func TestEndJobsWaitsForTheSchedulerToAct(t *testing.T) {
	// The scheduler lists the job running for two more reads after the cancel: EndJobs must keep reading.
	e := &enderDouble{cancelOK: true, liveFor: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := EndJobs(ctx, e, []string{"hb-x-1"}, time.Millisecond); err != nil {
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
			err := EndJobs(ctx, e, []string{"hb-x-1"}, 10*time.Millisecond)
			if err == nil {
				t.Fatal("EndJobs returned nil with the job never ended")
			}
			if strings.Contains(err.Error(), "hb-x-1") {
				t.Errorf("EndJobs's error names a tag: %v", err)
			}
		})
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
	r := &liveRun{t: t, batch: b, tr: tr, tags: rec}
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(dead); err != nil {
		t.Fatalf("Close under a dead context: %v", err)
	}
	if j := s.job(id); j.State != "CANCELLED" {
		t.Errorf("the run's job is %s after Close, want CANCELLED", j.State)
	}
}
