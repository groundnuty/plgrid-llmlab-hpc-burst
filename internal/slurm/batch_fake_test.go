package slurm_test

// Tests of the Batch's health counters and its cached paths over the fake
// Slurm. They live in the external test package because fakeslurm imports
// internal/slurm. Expected values are worked out from HBEP-4 (Provider status
// and Operational readiness; Transport) and from the fake's environment, never
// read back from the Batch under test.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	fakePart    = "example-gpu"
	fakeHome    = "/home/example-user"
	fakeScratch = "/scratch/example-user"
)

var fakeStart = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// sacctReply is what a sacctRunner returns for a sacct line.
type sacctReply struct {
	res slurm.Result
	err error
}

// sacctRunner is a Session whose sacct lines can be made to fail while every
// other command, squeue included, still reaches the fake cluster. FailNext
// cannot do this: it takes the next command whatever it is.
type sacctRunner struct {
	*fakeslurm.Session
	reply atomic.Pointer[sacctReply]
}

func (s *sacctRunner) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if r := s.reply.Load(); r != nil && strings.HasPrefix(cmd.Line, "sacct ") {
		return r.res, r.err
	}
	return s.Session.Run(ctx, cmd)
}

type fakeRig struct {
	batch  *slurm.Batch
	plat   *fakeslurm.Platform
	runner *sacctRunner
	clock  *fake.Clock
}

func exampleProfile(stagingDir string) provider.Profile {
	gpus := int32(4)
	maxTime := v1alpha1.Duration("1h")
	return provider.Profile{
		Name: "example",
		Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: stagingDir},
		Partition: v1alpha1.PartitionProfile{
			Name:             fakePart,
			AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu-model", CPUArch: "aarch64"},
			GPUsPerNode:      &gpus,
			MaxTime:          &maxTime,
		},
	}
}

func newFakeRig(t *testing.T, stagingDir string) *fakeRig {
	t.Helper()
	clock := fake.NewClock(fakeStart)
	plat := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: fakePart}}})
	sess := plat.Login(fakeslurm.Login{
		User: "example-user",
		Env:  map[string]string{"HOME": fakeHome, "SCRATCH": fakeScratch},
	})
	runner := &sacctRunner{Session: sess}
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart}
	batch := slurm.NewBatch(runner, exampleProfile(stagingDir), settings, slurm.Options{Clock: clock})
	return &fakeRig{batch: batch, plat: plat, runner: runner, clock: clock}
}

// unknownStateReply is a good sacct reply whose one job has a state the plugin
// does not know, so the listing parses and the merge fails.
func unknownStateReply(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "v0.0.42", "sacct-completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, j := range doc["jobs"].([]any) {
		j.(map[string]any)["state"].(map[string]any)["current"] = []any{"EXOTIC"}
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// An accounting read that fails while the queue listing succeeds is counted in
// ReadFailures, and ListingFailed stays false: the one signal that shows it.
func TestReadFailuresCountAccountingFailures(t *testing.T) {
	cases := []struct {
		name  string
		reply func(t *testing.T) sacctReply
	}{
		{"runner error", func(*testing.T) sacctReply { return sacctReply{err: slurm.ErrTransport} }},
		{"non-zero exit", func(*testing.T) sacctReply {
			return sacctReply{res: slurm.Result{ExitCode: 1, Stderr: []byte("sacct: error: Problem talking to the database\n")}}
		}},
		{"unparseable reply", func(*testing.T) sacctReply {
			return sacctReply{res: slurm.Result{Stdout: []byte("not json")}}
		}},
		{"unknown state in the merge", func(t *testing.T) sacctReply {
			return sacctReply{res: slurm.Result{Stdout: unknownStateReply(t)}}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			r := newFakeRig(t, "$SCRATCH/stage")
			if err := r.batch.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			first := r.batch.Health().LastListing
			// Past the poll interval, so FindByTag reads the listing again.
			r.clock.Advance(time.Minute)
			reply := c.reply(t)
			r.runner.reply.Store(&reply)

			_, err := r.batch.FindByTag(ctx, "hb-example-0123456789ab", r.clock.Now().Add(-time.Hour))
			if err == nil {
				t.Fatal("FindByTag succeeded although sacct failed")
			}
			h := r.batch.Health()
			if h.ReadFailures != 1 {
				t.Errorf("ReadFailures = %d, want 1", h.ReadFailures)
			}
			if h.ListingFailed {
				t.Error("ListingFailed is true although the listing was read")
			}
			if !h.LastListing.After(first) || !h.LastListing.Equal(r.clock.Now()) {
				t.Errorf("LastListing = %v, want it advanced from %v to now (%v)", h.LastListing, first, r.clock.Now())
			}
		})
	}
}

// A failed queue listing is counted too, and ListingFailed says so.
func TestReadFailuresCountListingFailures(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t, "$SCRATCH/stage")
	for want := uint64(1); want <= 2; want++ {
		r.plat.FailNext(1, slurm.ErrTransport)
		if err := r.batch.Poll(ctx); !errors.Is(err, slurm.ErrTransport) {
			t.Fatalf("Poll = %v, want the transport error", err)
		}
		h := r.batch.Health()
		if h.ReadFailures != want || !h.ListingFailed {
			t.Errorf("after failure %d: ReadFailures = %d, ListingFailed = %v; want %d and true", want, h.ReadFailures, h.ListingFailed, want)
		}
	}
	// A good read clears ListingFailed and leaves the count where it was.
	if err := r.batch.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	h := r.batch.Health()
	if h.ReadFailures != 2 || h.ListingFailed {
		t.Errorf("after a good read: ReadFailures = %d, ListingFailed = %v; want 2 and false", h.ReadFailures, h.ListingFailed)
	}
}

// Reads that succeed leave the count at zero, and so does a cache hit.
func TestReadFailuresStayZeroOnSuccess(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t, "$SCRATCH/stage")
	if err := r.batch.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.batch.Poll(ctx); err != nil { // within the interval: a cache hit
		t.Fatal(err)
	}
	if _, err := r.batch.FindByTag(ctx, "hb-example-0123456789ab", r.clock.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	// 4242 is in neither the listing nor accounting: both reads succeed.
	if _, found, err := r.batch.Observe(ctx, "4242"); err != nil || found {
		t.Fatalf("Observe = found %v, %v; want not found and no error", found, err)
	}
	if h := r.batch.Health(); h.ReadFailures != 0 || h.ListingFailed {
		t.Errorf("ReadFailures = %d, ListingFailed = %v; want 0 and false", h.ReadFailures, h.ListingFailed)
	}
}

func pathsOf(t *testing.T, b *slurm.Batch) slurm.Paths {
	t.Helper()
	p, err := b.Paths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Configure with a new stagingDir makes the next Paths resolve it: the
// connection's generation did not change, so the cache alone would serve the
// old directory. printenv runs again for it (HOME and SCRATCH, two calls per
// resolution), and a Configure that keeps the stagingDir keeps the cache.
func TestConfigureResetsPathsOnANewStagingDir(t *testing.T) {
	r := newFakeRig(t, "$SCRATCH/stage-a")
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart}

	if got := pathsOf(t, r.batch); got.Staging != fakeScratch+"/stage-a" || got.Home != fakeHome {
		t.Fatalf("Paths = %+v, want staging %s/stage-a and home %s", got, fakeScratch, fakeHome)
	}
	if n := r.plat.Count("printenv"); n != 2 {
		t.Fatalf("printenv ran %d times for the first resolution, want 2", n)
	}

	r.batch.Configure(exampleProfile("$SCRATCH/stage-a"), settings)
	if got := pathsOf(t, r.batch); got.Staging != fakeScratch+"/stage-a" {
		t.Errorf("Paths after a Configure with the same stagingDir = %+v", got)
	}
	if n := r.plat.Count("printenv"); n != 2 {
		t.Errorf("printenv ran %d times in all, want 2: the same stagingDir keeps the cache", n)
	}

	r.batch.Configure(exampleProfile("$SCRATCH/stage-b"), settings)
	if got := pathsOf(t, r.batch); got.Staging != fakeScratch+"/stage-b" {
		t.Errorf("Paths after a Configure with a new stagingDir = %+v, want staging %s/stage-b", got, fakeScratch)
	}
	if n := r.plat.Count("printenv"); n != 4 {
		t.Errorf("printenv ran %d times in all, want 4: the new stagingDir is resolved again", n)
	}
}

// A Paths call that is resolving the old stagingDir when Configure changes it
// must not put the old directory back in the cache.
func TestConfigureDuringPathsDoesNotRecacheTheOldDir(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t, "$SCRATCH/stage-a")
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart}
	gate := r.plat.Gate("printenv")

	done := make(chan error, 1)
	go func() {
		_, err := r.batch.Paths(ctx)
		done <- err
	}()
	gate.Wait()
	r.batch.Configure(exampleProfile("$SCRATCH/stage-b"), settings)
	gate.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := pathsOf(t, r.batch); got.Staging != fakeScratch+"/stage-b" {
		t.Errorf("Paths = %+v, want staging %s/stage-b: the in-flight call re-cached the old directory", got, fakeScratch)
	}
}

// A partition with a tmpDir and its tmpDirFeature submits with --constraint
// and a batch script that sets TMPDIR from the job's own variable. When that
// script stops (TestBootstrapStopsOnAnUnusableTmpDir runs it in /bin/sh: an
// unset or unusable variable exits non-zero), Slurm ends the job FAILED, and
// the Provider reads it as AllocationFailed (science-agent rulings, 1 Nov
// 2026, issue #125 and the #127 review).
func TestTmpDirJobThatEndsFailedIsAllocationFailed(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t, "$SCRATCH/stage")
	profile := exampleProfile("$SCRATCH/stage")
	profile.Partition.TmpDir, profile.Partition.TmpDirFeature = "$SCRATCH_LOCAL", "localfs"
	r.batch.Configure(profile, provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart})
	id, outcome, err := r.batch.Submit(ctx, provider.SubmitRequest{
		Tag: "hb-example-0123456789ab", Account: "example-account",
		Params: v1alpha1.BatchParameters{
			Partition: fakePart, TimeLimit: "1h", GPUs: 1, CPUsPerTask: 8,
			Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64),
		},
	})
	if err != nil || outcome != provider.Accepted {
		t.Fatalf("Submit = %q, %s, %v", id, outcome, err)
	}
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatal(err)
	}
	job, ok := r.plat.Job(n)
	if !ok {
		t.Fatalf("the fake has no job %d", n)
	}
	if job.Constraint != "localfs" {
		t.Errorf("--constraint %q, want localfs", job.Constraint)
	}
	if !strings.Contains(job.Wrap, "TMPDIR=${SCRATCH_LOCAL:?}") {
		t.Errorf("the batch script does not set TMPDIR from SCRATCH_LOCAL: %s", job.Wrap)
	}
	if err := r.plat.Start(n); err != nil {
		t.Fatal(err)
	}
	if err := r.plat.End(n, fakeslurm.Failed); err != nil {
		t.Fatal(err)
	}
	r.clock.Advance(time.Minute)
	got, found, err := r.batch.Observe(ctx, id)
	if err != nil || !found {
		t.Fatalf("Observe = %+v, %v, %v", got, found, err)
	}
	if got.Phase != v1alpha1.AllocationFailed {
		t.Errorf("phase %s (state %v), want %s", got.Phase, got.State, v1alpha1.AllocationFailed)
	}
}
