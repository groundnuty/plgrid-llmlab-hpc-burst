package providerctl

// The protections of a completing job, over the real Batch and the fake Slurm in
// RUN MODE (HBEP-4, Phases, A completing job; science-agent ruling for #198,
// 1 Nov 2026): the Allocation is deleted while its job runs, scancel ends the
// job CANCELLED and its processes are gone at once, but the epilog
// (RunMode.Epilog) keeps the flag, so the job is COMPLETING, listed, and not
// purged, long past MinJobAge. The finalizer stays, the Allocation's files stay,
// the accounting record is not written, and nothing is cancelled again. Once the
// epilog ends, all three happen, once. A fixture whose flag ends with the
// processes proves nothing here: the processes are gone before the first
// assertion, and the flag stands.
//
// The clock starts at 12:00:00 and the job starts then; the controller cancels it
// at 12:00:40 after one poll of 40 s. Expected values are literals.

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// withRunModeLogin makes the controller log in under root, whose home and
// scratch directories it creates, and stage a stand-in agent that records that
// it started (an up file in its Allocation's directory, its one argument),
// and runs until a go file appears there.
func withRunModeLogin(root string) realOption {
	return func(e *realEnv) {
		home, scratch := filepath.Join(root, "home", e.user), filepath.Join(root, "scratch", e.user)
		for _, d := range []string{home, scratch} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				e.t.Fatal(err)
			}
		}
		e.session = e.slurm.Login(fakeslurm.Login{User: e.user, Env: map[string]string{"HOME": home, "SCRATCH": scratch}})
		agent := []byte("#!/bin/sh\ntouch \"$1/up\"\ni=0\nwhile [ ! -e \"$1/go\" ] && [ $i -lt 1200 ]; do sleep 0.05; i=$((i+1)); done\n")
		sum := sha256.Sum256(agent)
		e.digest = hex.EncodeToString(sum[:])
		if err := os.WriteFile(filepath.Join(e.builds, agentapi.AgentBuildName(testArch)), agent, 0o700); err != nil {
			e.t.Fatal(err)
		}
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 60 s waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCompletingJobKeepsFinalizerFilesAndRecordPastMinJobAgeInRunMode(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	root := t.TempDir()
	sl := newSlurm(clock, fakeslurm.Config{
		NotListed: fakeslurm.NotListedHelios, MinJobAge: 10 * time.Second,
		Run: &fakeslurm.RunMode{Root: root, Epilog: 10 * time.Minute},
	})
	t.Cleanup(func() {
		if err := sl.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withRunModeLogin(root))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if id != "1001" {
		t.Fatalf("precondition: job ID %q, want 1001", id)
	}
	allocDir := filepath.Join(root, "scratch", "user-a", "stage", agentapi.AllocationDir(jobTag(e, "alloc-a")))
	waitUntil(t, "the stand-in agent to start", func() bool { _, err := os.Stat(filepath.Join(allocDir, "up")); return err == nil })
	e.poll("alloc-a", 1) // 12:00:40
	if p := e.get("alloc-a").Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
		t.Fatalf("precondition: phase %s, want Running", p)
	}

	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	e.run("alloc-a", 1) // Delete cancels the job at 12:00:40
	if j := slurmJob(t, sl, id); j.State != fakeslurm.Cancelled {
		t.Fatalf("precondition: job %s is %s, want CANCELLED", id, j.State)
	}
	waitUntil(t, "the job's processes to be gone", func() bool { return slurmJob(t, sl, id).Finished })
	if j := slurmJob(t, sl, id); !j.Completing {
		t.Fatalf("processes gone: Completing false; want true (the epilog still runs)")
	}
	scancels := sl.Count("scancel")

	// Long past MinJobAge, with the processes gone and the epilog running.
	for i := 0; i < 4; i++ {
		e.clock.Advance(time.Minute)
		e.run("alloc-a", 2)
		if e.gone("alloc-a") {
			t.Fatalf("pass %d: the Allocation is gone while its job is completing", i)
		}
		if _, ok := e.record("alloc-a"); ok {
			t.Fatalf("pass %d: the accounting record was written while the job is completing", i)
		}
		if n := sl.Count("rm"); n != 0 {
			t.Fatalf("pass %d: %d file removals while the job is completing, want 0", i, n)
		}
		if _, err := os.Stat(allocDir); err != nil {
			t.Fatalf("pass %d: the Allocation's directory is gone while the job is completing: %v", i, err)
		}
	}
	if n := sl.Count("scancel"); n != scancels {
		t.Errorf("%d scancel calls after the job ended, want none beyond the first (a completing job is not cancelled again)", n-scancels)
	}
	if a := e.get("alloc-a"); !ptHasFinalizer(a) {
		t.Error("the finalizer is gone while the job is completing")
	}

	// The epilog ends (12:00:40 plus 10 min): the record, the files and the
	// finalizer follow, once.
	e.clock.Advance(10 * time.Minute)
	e.run("alloc-a", 3)
	if !e.gone("alloc-a") {
		t.Fatalf("the Allocation is still there after the epilog; job %s Completing %v", id, slurmJob(t, sl, id).Completing)
	}
	if n := sl.Count("rm"); n != 1 {
		t.Errorf("%d file removals after the epilog, want 1", n)
	}
	if _, err := os.Stat(allocDir); !os.IsNotExist(err) {
		t.Errorf("the Allocation's directory is still there after the epilog: %v", err)
	}
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":40,`+
		`"startTime":"2026-10-30T12:00:00Z","endTime":"2026-10-30T12:00:40Z","basis":"allocated"}`)
}
