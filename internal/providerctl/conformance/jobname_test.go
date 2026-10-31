package conformance

// The job-name round trip (HBEP-4, Risks, "A site rewrites the job name", and
// Graduation criteria, alpha: "a 56-character job name comes back unchanged on
// each alpha partition"). The provider finds a job by its tag, which it sends
// as the job name, so a platform that shortens or rewrites a 56-character name
// loses every job it submits. The step submits a job under a name of exactly 56
// characters and reads the name back twice: from squeue while the job is
// queued, and from sacct after it has left the queue.
//
// The job is submitted held and cancelled before it is ever released, so it
// never starts and bills nothing. That is what lets the step run on a
// partition that cannot start jobs at the moment, as well as on one that can.
// A run's evidence for jobName.maxLength in a shipped profile is this step.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// nameLength is the length the step checks: HBEP-3's job tag, hb-<40>-<12 hex>.
// It is written out here and not taken from the code under test.
const nameLength = 56

// nameReadBack is what each reader returned for the job's name.
type nameReadBack struct{ queue, accounting string }

// roundTripJobName submits req held, reads the job's name from the queue,
// cancels the job, waits until the queue no longer lists it, reads the name
// from accounting, and returns an error unless both equal req.Tag. req.Tag must
// have nameLength characters. It returns the job ID as soon as it is known,
// also with an error, so a caller can see to the job. pause is called between
// polls; at most polls polls of each reader are made.
func roundTripJobName(ctx context.Context, b *slurm.Batch, req provider.SubmitRequest, polls int, pause func()) (string, nameReadBack, error) {
	var got nameReadBack
	if len(req.Tag) != nameLength {
		return "", got, fmt.Errorf("the test name has %d characters, want %d", len(req.Tag), nameLength)
	}
	if !req.Hold {
		return "", got, fmt.Errorf("the round trip submits held, so that the job never starts")
	}
	id, outcome, err := b.Submit(ctx, req)
	if err != nil || outcome != provider.Accepted {
		return id, got, fmt.Errorf("submit: %s: %v", outcome, err)
	}
	// The job is cancelled on every way out, so a failed step leaves none live.
	cancelled := false
	defer func() {
		if !cancelled {
			b.Cancel(ctx, []string{id})
		}
	}()

	// squeue, while the job is queued (held).
	for i := 0; ; i++ {
		j, listed, err := b.QueueJob(ctx, id)
		if err != nil {
			return id, got, fmt.Errorf("squeue: %w", err)
		}
		if listed {
			got.queue = j.Name
			break
		}
		if i >= polls {
			return id, got, fmt.Errorf("squeue never listed the held job in %d polls", polls)
		}
		pause()
	}
	if err := sameName("squeue", req.Tag, got.queue); err != nil {
		return id, got, err
	}

	// sacct, once the queue has dropped the cancelled job.
	cancelled = true
	for _, r := range b.Cancel(ctx, []string{id}) {
		if r.Outcome != provider.Cancelled && r.Outcome != provider.AlreadyEnded {
			return id, got, fmt.Errorf("cancel: %s: %v", r.Outcome, r.Err)
		}
	}
	for i := 0; ; i++ {
		_, listed, qerr := b.QueueJob(ctx, id)
		j, found, err := b.Observe(ctx, id)
		if qerr == nil && !listed && err == nil && found && j.Source == provider.SourceAccounting {
			got.accounting = j.Name
			break
		}
		if i >= polls {
			return id, got, fmt.Errorf("sacct did not return the cancelled job in %d polls (queue error %v, accounting error %v, found %v)", polls, qerr, err, found)
		}
		pause()
	}
	return id, got, sameName("sacct", req.Tag, got.accounting)
}

// sameName compares without printing the names: the lengths and the first
// position at which they differ say what happened.
func sameName(reader, want, got string) error {
	if got == want {
		return nil
	}
	at := 0
	for at < len(want) && at < len(got) && want[at] == got[at] {
		at++
	}
	return fmt.Errorf("%s returned a name of %d characters, want %d: it differs from the submitted name at character %d", reader, len(got), len(want), at+1)
}

// jobNameRoundTrip is the step on the real platform. The job ID is private; the
// test log says only that the name came back unchanged.
func (w *world) jobNameRoundTrip() {
	w.t.Helper()
	a := w.newAlloc("conformance-job-name-round-trip-of-56-chars") // 44 characters, cut to 40 in the tag
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	req := provider.SubmitRequest{Tag: tag, Hold: true, Account: w.s.account, Params: *a.Spec.ForProvider.Batch}
	id, got, err := roundTripJobName(context.Background(), w.batch, req, 36, func() { time.Sleep(5 * time.Second) })
	if id != "" {
		w.jobIDs[id] = true
		w.p.log("job-name round trip: job %s, queue name %d characters, accounting name %d characters, err %v", id, len(got.queue), len(got.accounting), err)
	}
	if err != nil {
		w.t.Fatalf("the 56-character job name did not come back unchanged: %v", err)
	}
	w.t.Logf("a job name of %d characters came back unchanged from squeue and from sacct", nameLength)
}

// TestJobNameRoundTrip runs the step alone. The job is never released, so this
// works on a partition that cannot start jobs at the moment.
func TestJobNameRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	w := newWorld(t)
	w.jobNameRoundTrip()
}

// The step against the fake Slurm, on the shipped Helios plgrid profile, so it
// is exercised without a platform: a name that comes back unchanged passes, and
// a name shortened by either reader fails, naming the reader.

type rewrite struct {
	slurm.Runner
	// tag is the submitted name.
	tag string
	// sbatchTo is the length the name is cut to in the sbatch line; 0 keeps it.
	sbatchTo int
	// sacctTo is the length the name is cut to in sacct's reply; 0 keeps it.
	sacctTo int
}

func (r *rewrite) Run(ctx context.Context, c slurm.Command) (slurm.Result, error) {
	if r.sbatchTo > 0 && strings.HasPrefix(c.Line, "sbatch") {
		c.Line = strings.Replace(c.Line, "--job-name="+r.tag, "--job-name="+r.tag[:r.sbatchTo], 1)
	}
	res, err := r.Runner.Run(ctx, c)
	if r.sacctTo > 0 && strings.HasPrefix(c.Line, "sacct") {
		res.Stdout = bytes.ReplaceAll(res.Stdout, []byte(r.tag), []byte(r.tag[:r.sacctTo]))
	}
	return res, err
}

func TestRoundTripJobNameAgainstTheFakePlatform(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rewrite rewrite
		wantErr string // "" for a name that comes back unchanged
	}{
		{"unchanged", rewrite{}, ""},
		{"shortened by the scheduler", rewrite{sbatchTo: 55}, "squeue returned a name of 55 characters, want 56"},
		{"shortened in accounting", rewrite{sacctTo: 40}, "sacct returned a name of 40 characters, want 56"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
			sl := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: "plgrid"}}})
			session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
			pp, part, err := loadProfile(profilesDir(), "helios", "plgrid", "$SCRATCH/example-run")
			if err != nil {
				t.Fatal(err)
			}
			name := "conformance-job-name-round-trip-of-56-chars"
			tag := v1alpha1.JobTag("conformance", name, "example-uid")
			rw := tc.rewrite
			rw.Runner, rw.tag = session, tag
			b := slurm.NewBatch(&rw, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: part},
				provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid"}, slurm.Options{Clock: clock})
			digest := sha256.Sum256([]byte("example agent"))
			req := provider.SubmitRequest{Tag: tag, Hold: true, Account: "example-account", Params: v1alpha1.BatchParameters{
				Partition: "plgrid", TimeLimit: "5m", CPUsPerTask: 1, Memory: resource.MustParse("1Gi"), Nodes: 1, AgentDigest: hex.EncodeToString(digest[:]),
			}}
			id, got, err := roundTripJobName(context.Background(), b, req, 20, func() { clock.Advance(5 * time.Second) })
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("a name that comes back unchanged failed: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
			if tc.wantErr == "" && (got.queue != tag || got.accounting != tag) {
				t.Errorf("names read back: %d and %d characters, want the submitted 56", len(got.queue), len(got.accounting))
			}
			// Counted from the fake's log: one sbatch, no release (the job never
			// starts), and one cancel whatever the outcome: the step leaves no
			// live job.
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
			if n := sl.Count("scontrol"); n != 0 {
				t.Errorf("%d scontrol calls, want 0: the job must never be released", n)
			}
			if id == "" {
				t.Fatal("no job ID returned")
			}
			if n := sl.Count("scancel"); n != 1 {
				t.Errorf("%d scancel calls, want 1", n)
			}
			if j := slurmJobState(t, sl, id); j != fakeslurm.Cancelled {
				t.Errorf("the job is %s at the end, want %s", j, fakeslurm.Cancelled)
			}
		})
	}
}

// slurmJobState is the fake's own record of the job's state.
func slurmJobState(t *testing.T, sl *fakeslurm.Platform, id string) string {
	t.Helper()
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatalf("job ID %q: %v", id, err)
	}
	j, ok := sl.Job(n)
	if !ok {
		t.Fatalf("the fake Slurm has no job %s", id)
	}
	return j.State
}
