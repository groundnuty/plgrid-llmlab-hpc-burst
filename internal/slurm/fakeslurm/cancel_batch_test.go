package fakeslurm_test

// New (#199). Batch.Cancel classifies by the job's state, read before the
// scancel, never by what scancel prints: real Slurm 24.11's scancel prints
// nothing and exits 0 for an ended job and for an unknown one, which is what
// the fake does by default. Every test here runs against that quiet fake; a
// test against a fake that still prints the error text would pass on the old
// classification, which is the behaviour that hid the fault. The one noisy test
// says so by name.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// hookRunner runs a command through the session after before has seen its line;
// an error from before is the command's transport error.
type hookRunner struct {
	slurm.Runner
	before func(line string) error
	// reply, when it says ok for a line, answers the line itself and the session never sees it.
	reply func(line string) (slurm.Result, bool)
}

func (h hookRunner) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if h.reply != nil {
		if res, ok := h.reply(cmd.Line); ok {
			return res, nil
		}
	}
	if h.before != nil {
		if err := h.before(cmd.Line); err != nil {
			return slurm.Result{}, err
		}
	}
	return h.Runner.Run(ctx, cmd)
}

type cancelRig struct {
	t     *testing.T
	batch *slurm.Batch
	plat  *fakeslurm.Platform
	clock *fake.Clock
	hook  *hookRunner
	n     int
}

// newCancelRig is a Batch over a fake Slurm of the given queue shape: in
// Helios's, a job that ended is not listed at once (only accounting shows it);
// in Athena's, it stays listed, with its end time, until MinJobAge.
func newCancelRig(t *testing.T, cfg fakeslurm.Config) *cancelRig {
	t.Helper()
	clock := fake.NewClock(smokeStart)
	cfg.Partitions = []fakeslurm.Partition{{Name: smokePart}}
	parser := "v0.0.42"
	if cfg.NotListed == fakeslurm.NotListedAthena {
		parser = "v0.0.40"
	}
	cfg.DataParser = parser
	plat := fakeslurm.New(clock, cfg)
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": smokeHome, "SCRATCH": smokeScratch}})
	gpus := int32(4)
	maxTime := v1alpha1.Duration("1h")
	profile := provider.Profile{
		Name: "example",
		Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: parser, StagingDir: "$SCRATCH/hpcburst"},
		Partition: v1alpha1.PartitionProfile{
			Name:             smokePart,
			AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu-model", CPUArch: "aarch64"},
			GPUsPerNode:      &gpus, MaxTime: &maxTime,
		},
	}
	settings := provider.Settings{Provider: "example", Account: smokeAccount, Partition: smokePart}
	hook := &hookRunner{Runner: sess}
	batch := slurm.NewBatch(hook, profile, settings, slurm.Options{Clock: clock})
	return &cancelRig{t: t, batch: batch, plat: plat, clock: clock, hook: hook}
}

// submit submits a job under a fresh tag, held or not, and returns its ID.
func (r *cancelRig) submit(hold bool) string {
	r.t.Helper()
	ctx := context.Background()
	r.n++
	tag := "hb-" + strings.Repeat(strconv.Itoa(r.n), 12)
	agent := []byte("example agent binary")
	digest := fileOf(agent, "", 0).SHA256
	for _, f := range []provider.File{
		fileOf(agent, agentapi.StagedAgentPath(digest), 0o755),
		fileOf([]byte("example-credential-"+tag), agentapi.StagedCredentialPath(tag), 0o600),
	} {
		if err := r.batch.EnsureFile(ctx, f); err != nil {
			r.t.Fatal(err)
		}
	}
	req := smokeRequest(digest, hold)
	req.Tag = tag
	id, outcome, err := r.batch.Submit(ctx, req)
	if err != nil || outcome != provider.Accepted {
		r.t.Fatalf("Submit = (%q, %s, %v)", id, outcome, err)
	}
	return id
}

func (r *cancelRig) start(id string) {
	r.t.Helper()
	n, _ := strconv.Atoi(id)
	if err := r.plat.Start(n); err != nil {
		r.t.Fatal(err)
	}
}

func (r *cancelRig) end(id, state string) {
	r.t.Helper()
	n, _ := strconv.Atoi(id)
	if err := r.plat.End(n, state); err != nil {
		r.t.Fatal(err)
	}
}

func (r *cancelRig) state(id string) string {
	r.t.Helper()
	n, _ := strconv.Atoi(id)
	j, ok := r.plat.Job(n)
	if !ok {
		r.t.Fatalf("job %s is unknown to the fake", id)
	}
	return j.State
}

func (r *cancelRig) cancel(id string) provider.CancelResult {
	r.t.Helper()
	res := r.batch.Cancel(context.Background(), []string{id})
	if len(res) != 1 || res[0].JobID != id {
		r.t.Fatalf("Cancel(%s) = %+v, want one result for the job", id, res)
	}
	return res[0]
}

// A job live when it is cancelled reports Cancelled and is cancelled, held,
// pending or running.
func TestCancelOfALiveJobIsCancelled(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	held, pending, running := r.submit(true), r.submit(false), r.submit(false)
	r.start(running)
	for _, c := range []struct{ name, id string }{{"held", held}, {"pending", pending}, {"running", running}} {
		if res := r.cancel(c.id); res.Outcome != provider.Cancelled || res.Err != nil {
			t.Errorf("Cancel of a %s job = %+v, want Cancelled", c.name, res)
		}
		if got := r.state(c.id); got != fakeslurm.Cancelled {
			t.Errorf("the %s job is %s after the cancel, want %s", c.name, got, fakeslurm.Cancelled)
		}
	}
	if n := r.plat.Count("scancel"); n != 3 {
		t.Errorf("%d scancel calls for three live jobs, want 3", n)
	}
}

// A job that has ended reports AlreadyEnded, listed with its end time in the
// queue (Athena) or shown only by accounting (Helios), and no scancel runs: the
// outcome is the job's state, not what scancel would say. The quiet fake says
// nothing for an ended job, as real Slurm does, so a Cancel that took scancel's
// silence for success reports Cancelled here.
func TestCancelOfAnEndedJobIsAlreadyEnded(t *testing.T) {
	for _, mode := range []fakeslurm.NotListedMode{fakeslurm.NotListedHelios, fakeslurm.NotListedAthena} {
		t.Run(string(mode), func(t *testing.T) {
			r := newCancelRig(t, fakeslurm.Config{NotListed: mode})
			id := r.submit(false)
			r.start(id)
			r.clock.Advance(time.Minute)
			r.end(id, fakeslurm.Completed)
			r.clock.Advance(time.Second)
			// Which read answers: the queue in Athena's shape, accounting in Helios's.
			if _, listed, err := r.batch.QueueJob(context.Background(), id); err != nil || listed != (mode == fakeslurm.NotListedAthena) {
				t.Fatalf("QueueJob of the ended job = listed %v, %v; the %s shape lists an ended job: %v", listed, err, mode, mode == fakeslurm.NotListedAthena)
			}
			if res := r.cancel(id); res.Outcome != provider.AlreadyEnded || res.Err != nil {
				t.Errorf("Cancel of an ended job = %+v, want AlreadyEnded", res)
			}
			if got := r.state(id); got != fakeslurm.Completed {
				t.Errorf("the ended job is %s after the cancel, want %s untouched", got, fakeslurm.Completed)
			}
			if n := r.plat.Count("scancel"); n != 0 {
				t.Errorf("%d scancel calls for an ended job, want 0", n)
			}
		})
	}
}

// A job slurmctld has purged (MinJobAge) but accounting still shows has ended.
func TestCancelOfAPurgedJobThatAccountingShowsIsAlreadyEnded(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena})
	id := r.submit(false)
	r.start(id)
	r.clock.Advance(time.Minute)
	r.end(id, fakeslurm.Completed)
	r.clock.Advance(time.Minute) // past MinJobAge: no longer listed
	if res := r.cancel(id); res.Outcome != provider.AlreadyEnded || res.Err != nil {
		t.Errorf("Cancel of a purged job = %+v, want AlreadyEnded", res)
	}
	if n := r.plat.Count("scancel"); n != 0 {
		t.Errorf("%d scancel calls, want 0", n)
	}
}

// A job neither the queue nor accounting shows is unknown: ErrNotFound, not
// Cancelled, and no scancel runs.
func TestCancelOfAnUnknownJobIsNotFound(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	res := r.cancel("424242")
	if res.Outcome != provider.CancelError || !errors.Is(res.Err, provider.ErrNotFound) {
		t.Errorf("Cancel of an unknown job = %+v, want CancelError wrapping ErrNotFound", res)
	}
	if n := r.plat.Count("scancel"); n != 0 {
		t.Errorf("%d scancel calls for an unknown job, want 0", n)
	}
}

// Results come back one per job, in order, each by its own state.
func TestCancelOfSeveralJobsReportsEachInOrder(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	live, ended := r.submit(false), r.submit(false)
	r.start(ended)
	r.clock.Advance(time.Minute)
	r.end(ended, fakeslurm.Completed)
	r.clock.Advance(time.Second)
	res := r.batch.Cancel(context.Background(), []string{live, "424242", ended, live})
	want := []provider.CancelOutcome{provider.Cancelled, provider.CancelError, provider.AlreadyEnded, provider.AlreadyEnded}
	if len(res) != len(want) {
		t.Fatalf("%d results, want %d: %+v", len(res), len(want), res)
	}
	for i, w := range want {
		if res[i].Outcome != w {
			t.Errorf("result %d = %+v, want %s", i, res[i], w)
		}
	}
	if !errors.Is(res[1].Err, provider.ErrNotFound) {
		t.Errorf("result 1 error = %v, want ErrNotFound", res[1].Err)
	}
	if n := r.plat.Count("scancel"); n != 1 {
		t.Errorf("%d scancel calls, want 1: only the first, live job needs one (the second time it has ended)", n)
	}
}

// A read that fails leaves the job's state unknown: an error, and no scancel,
// since cancelling a job that may have ended would be reported as a cancel.
func TestCancelWhenTheStateCannotBeReadIsAnError(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	id := r.submit(false)
	r.hook.before = func(line string) error {
		if strings.HasPrefix(line, "squeue") {
			return slurm.ErrTransport
		}
		return nil
	}
	res := r.cancel(id)
	if res.Outcome != provider.CancelError || !errors.Is(res.Err, slurm.ErrTransport) || errors.Is(res.Err, provider.ErrNotFound) {
		t.Errorf("Cancel with the queue unreadable = %+v, want CancelError wrapping the transport error", res)
	}
	if n := r.plat.Count("scancel"); n != 0 {
		t.Errorf("%d scancel calls with the state unknown, want 0", n)
	}
	if got := r.state(id); got == fakeslurm.Cancelled {
		t.Error("the job was cancelled with its state unknown")
	}
}

// The queue does not show the job, so accounting decides, and accounting cannot be read: the
// state is unknown, so an error, never ErrNotFound (the job may exist) and never AlreadyEnded,
// and no scancel runs. Three ways a read can fail, for a job that has ended and for one that
// never existed.
func TestCancelWhenTheQueueDoesNotShowTheJobAndAccountingCannotBeReadIsAnError(t *testing.T) {
	failures := map[string]func(line string) (slurm.Result, bool){
		"sacct fails to connect": nil, // handled by before, below
		"sacct exits 1": func(line string) (slurm.Result, bool) {
			if strings.HasPrefix(line, "sacct") {
				return slurm.Result{ExitCode: 1, Stderr: []byte("sacct: error: Problem talking to the database\n")}, true
			}
			return slurm.Result{}, false
		},
		"sacct prints what cannot be parsed": func(line string) (slurm.Result, bool) {
			if strings.HasPrefix(line, "sacct") {
				return slurm.Result{Stdout: []byte("this is not json")}, true
			}
			return slurm.Result{}, false
		},
	}
	for name, reply := range failures {
		for _, job := range []string{"ended", "unknown"} {
			t.Run(name+"/"+job, func(t *testing.T) {
				r := newCancelRig(t, fakeslurm.Config{})
				id := "424242"
				if job == "ended" {
					id = r.submit(false)
					r.start(id)
					r.clock.Advance(time.Minute)
					r.end(id, fakeslurm.Completed)
					r.clock.Advance(time.Second)
				}
				if _, listed, err := r.batch.QueueJob(context.Background(), id); err != nil || listed {
					t.Fatalf("QueueJob = listed %v, %v; the Helios shape does not list the job, so accounting decides", listed, err)
				}
				if reply == nil {
					r.hook.before = func(line string) error {
						if strings.HasPrefix(line, "sacct") {
							return slurm.ErrTransport
						}
						return nil
					}
				} else {
					r.hook.reply = reply
				}
				res := r.cancel(id)
				if res.Outcome != provider.CancelError || res.Err == nil {
					t.Fatalf("Cancel = %+v, want a CancelError", res)
				}
				if errors.Is(res.Err, provider.ErrNotFound) {
					t.Errorf("Cancel = %+v: an unreadable accounting must not read as ErrNotFound", res)
				}
				if n := r.plat.Count("scancel"); n != 0 {
					t.Errorf("%d scancel calls with the job's state unknown, want 0", n)
				}
				if job == "ended" {
					if got := r.state(id); got != fakeslurm.Completed {
						t.Errorf("the ended job is %s, want %s untouched", got, fakeslurm.Completed)
					}
				}
			})
		}
	}
}

// A job that ends in the instant between the read and the scancel still reports
// Cancelled on a Slurm that is quiet about it, as 24.11 is. This is the known
// limit of reading the state before the scancel; the test pins it so that it
// stays a conscious choice. It also checks the order that makes it the limit and
// not a skipped observation: the fresh single-job read came before the scancel,
// the end happened in between (the hook fired), and the job stayed Completed.
func TestACancelOfAJobThatEndsAfterTheReadIsCancelledOnAQuietSlurm(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	id := r.submit(false)
	r.start(id)
	r.clock.Advance(time.Minute)
	fired := false
	r.hook.before = func(line string) error {
		if strings.HasPrefix(line, "scancel") {
			r.hook.before = nil
			fired = true
			r.end(id, fakeslurm.Completed)
		}
		return nil
	}
	if res := r.cancel(id); res.Outcome != provider.Cancelled {
		t.Errorf("Cancel of a job that ended after the read = %+v, want Cancelled on a quiet Slurm", res)
	}
	if !fired {
		t.Fatal("the hook never fired: no scancel ran, so the job did not end after the read and this test proves nothing")
	}
	if got := r.state(id); got != fakeslurm.Completed {
		t.Errorf("the job is %s, want %s: it ended before the scancel, which left it alone", got, fakeslurm.Completed)
	}
	read, scancel := -1, -1
	for i, c := range r.plat.Calls() {
		switch {
		case len(c.Words) > 0 && c.Words[0] == "squeue" && strings.Contains(strings.Join(c.Words, " "), "--jobs="+id) && read < 0:
			read = i
		case len(c.Words) > 0 && c.Words[0] == "scancel":
			scancel = i
		}
	}
	if read < 0 || scancel < 0 || read > scancel {
		t.Errorf("the single-job read was call %d and the scancel call %d, want the read first: the outcome must come from observing the job", read, scancel)
	}
}

// The same race on a Slurm that does say so (Config.NoisyScancel): the job was live at the read
// and ended before the scancel, which then fails. The outcome is not read from the text of the
// failure: it is a CancelError, and the caller's next pass reads the job, finds it ended and
// goes on. This is the only test that runs the noisy fake.
func TestACancelOfAJobThatEndsAfterTheReadIsAnErrorOnANoisySlurm(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{NoisyScancel: true})
	id := r.submit(false)
	r.start(id)
	r.clock.Advance(time.Minute)
	r.hook.before = func(line string) error {
		if strings.HasPrefix(line, "scancel") {
			r.hook.before = nil
			r.end(id, fakeslurm.Completed)
		}
		return nil
	}
	res := r.cancel(id)
	if res.Outcome != provider.CancelError || errors.Is(res.Err, provider.ErrNotFound) {
		t.Errorf("Cancel of a job that ended after the read, on a noisy Slurm = %+v, want a plain CancelError", res)
	}
	if again := r.cancel(id); again.Outcome != provider.AlreadyEnded {
		t.Errorf("the next Cancel = %+v, want AlreadyEnded: the job is read as ended", again)
	}
}

// Another user's job is in neither this user's queue listing (squeue --me) nor
// this user's accounting, so it is unknown to the plugin: ErrNotFound, and no
// scancel runs. Before #199 the scancel ran and Slurm's refusal was an error.
func TestCancelOfAnotherUsersJobIsNotFoundAndLeftAlone(t *testing.T) {
	r := newCancelRig(t, fakeslurm.Config{})
	other := r.plat.Login(fakeslurm.Login{User: "other-user", Env: map[string]string{"HOME": "/home/other-user", "SCRATCH": "/scratch/other-user"}})
	res, err := other.Run(context.Background(), slurm.Command{Line: "sbatch --parsable --job-name=hb-999999999999 --no-requeue --partition=" +
		smokePart + " --time=30 --nodes=1 --gres=gpu:1 --cpus-per-task=8 --mem=16384M --wrap=true"})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("the other user's sbatch: %v %d %s", err, res.ExitCode, res.Stderr)
	}
	id := strings.TrimSpace(string(res.Stdout))
	got := r.cancel(id)
	if got.Outcome != provider.CancelError || !errors.Is(got.Err, provider.ErrNotFound) {
		t.Errorf("Cancel of another user's job = %+v, want CancelError wrapping ErrNotFound", got)
	}
	if n := r.plat.Count("scancel"); n != 0 {
		t.Errorf("%d scancel calls, want 0", n)
	}
	if s := r.state(id); s == fakeslurm.Cancelled {
		t.Errorf("another user's job is %s", s)
	}
}
