package slurm_test

// The plugin's own submission over the fake Slurm in run mode: the staged
// agent, the held submission, the release and the bootstrap all run for real.
// Expected values come from HBEP-4 (Submission: the bootstrap sets TMPDIR from
// the partition's tmpDir, expanded in the job; an unset variable stops the job
// before the agent starts, and Slurm records it FAILED) and from RunMode's
// documented layout (a job variable is <JobDir>/<job ID>/<name in lower case>).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestBootstrapRunsInRunMode(t *testing.T) {
	for _, c := range []struct {
		name  string
		vars  []fakeslurm.JobVar
		phase v1alpha1.AllocationPhase
		state string
		ran   bool
	}{
		{"the job sets MEMFS", []fakeslurm.JobVar{{Name: "MEMFS", Feature: "memfs"}}, v1alpha1.AllocationEnded, "COMPLETED", true},
		{"MEMFS is unset", nil, v1alpha1.AllocationFailed, "FAILED", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			home := filepath.Join(root, "home", "example-user")
			if err := os.MkdirAll(home, 0o755); err != nil {
				t.Fatal(err)
			}
			clock := fake.NewClock(fakeStart)
			plat := fakeslurm.New(clock, fakeslurm.Config{
				Partitions: []fakeslurm.Partition{{Name: fakePart}},
				Run:        &fakeslurm.RunMode{Root: root, JobVars: c.vars},
			})
			t.Cleanup(func() {
				if err := plat.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home}})
			profile := exampleProfile("$HOME/hpc-burst")
			profile.Partition.TmpDir, profile.Partition.TmpDirFeature = "$MEMFS", "memfs"
			settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart}
			batch := slurm.NewBatch(sess, profile, settings, slurm.Options{Clock: clock})

			// The stand-in agent records the TMPDIR it was started with in its
			// Allocation's directory, its one argument.
			agent := []byte("#!/bin/sh\necho \"$TMPDIR\" > \"$1/seen-tmpdir\"\n")
			sum := sha256.Sum256(agent)
			digest := hex.EncodeToString(sum[:])
			const tag = "hb-example-0001"
			cred := []byte("example-credential")
			credSum := sha256.Sum256(cred)
			for _, f := range []provider.File{
				{Path: agentapi.StagedAgentPath(digest), SHA256: digest, Mode: 0o700, Content: agent},
				{Path: agentapi.StagedCredentialPath(tag), SHA256: hex.EncodeToString(credSum[:]), Mode: 0o600, Content: cred},
			} {
				if err := batch.EnsureFile(ctx, f); err != nil {
					t.Fatalf("EnsureFile %s: %v", f.Path, err)
				}
			}
			id, outcome, err := batch.Submit(ctx, provider.SubmitRequest{
				Tag: tag, Hold: true, Account: "example-account",
				Params: v1alpha1.BatchParameters{
					Partition: fakePart, TimeLimit: "10m", GPUs: 1, CPUsPerTask: 1,
					Memory: resource.MustParse("1Gi"), AgentDigest: digest,
				},
			})
			if err != nil || outcome != provider.Accepted {
				t.Fatalf("Submit: %s, %v", outcome, err)
			}
			n, _ := strconv.Atoi(id)
			if j, _ := plat.Job(n); j.PID != 0 || !j.Held {
				t.Fatalf("the held job: held %v, PID %d; want held with no process", j.Held, j.PID)
			}
			if err := batch.LiftHold(ctx, id); err != nil {
				t.Fatalf("LiftHold: %v", err)
			}
			deadline := time.Now().Add(60 * time.Second)
			for j, _ := plat.Job(n); !j.Finished; j, _ = plat.Job(n) {
				if time.Now().After(deadline) {
					t.Fatalf("job %s still has processes after 60 s: %+v", id, j)
				}
				time.Sleep(5 * time.Millisecond)
			}
			job, found, err := batch.Observe(ctx, id)
			if err != nil || !found || job.Phase != c.phase || len(job.State) == 0 || job.State[0] != c.state {
				t.Errorf("Observe: %+v, found %v, %v; want phase %s, state %s", job, found, err, c.phase, c.state)
			}
			seen := filepath.Join(home, "hpc-burst", agentapi.AllocationDir(tag), "seen-tmpdir")
			b, err := os.ReadFile(seen)
			switch {
			case c.ran && (err != nil || string(b) != filepath.Join(root, id, "memfs")+"\n"):
				t.Errorf("the agent saw TMPDIR %q (%v), want the job's MEMFS %s", b, err, filepath.Join(root, id, "memfs"))
			case !c.ran && !errors.Is(err, os.ErrNotExist):
				t.Errorf("the agent ran with MEMFS unset: %q, %v", b, err)
			}
		})
	}
}
