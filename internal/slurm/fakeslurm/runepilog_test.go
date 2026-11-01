package fakeslurm

// New. #198 (science-agent ruling, 1 Nov 2026): the COMPLETING flag outlasts
// the job's processes, for Slurm's epilog (RunMode.Epilog), and Slurm frees the
// job's submit slot and its GPUs when it sets the flag. A fake whose flag ends
// the moment the processes exit proves nothing about a consumer that releases
// at that moment, so these tests hold a job in COMPLETING after its processes
// are gone, past MinJobAge, and then let it clear, in both NotListed modes.
//
// Expected values are Slurm's and the ruling's, written by hand. The clock is
// manual, so MinJobAge and the epilog pass only when a test advances it; every
// wait is on the job's own state, never a sleep.

import (
	"os"
	"strings"
	"testing"
	"time"
)

// newEpilogRig is a run-mode platform in the given NotListed mode with
// MinJobAge minAge and an epilog of epilog after the processes are gone.
func newEpilogRig(t *testing.T, mode NotListedMode, minAge, epilog time.Duration, cfg Config) *runRig {
	t.Helper()
	cfg.Run = &RunMode{KillWait: 10 * time.Minute, Minute: 50 * time.Millisecond, Epilog: epilog}
	cfg.NotListed, cfg.MinJobAge = mode, minAge
	return newRunRigCfg(t, cfg, true)
}

// A job whose processes are gone is still COMPLETING until its epilog ends: it
// is listed with the flag in every form, inside MinJobAge and long past it, in
// both modes (the flag outlasts the processes, which Finished does not say),
// and when the epilog ends after end + MinJobAge, the purge, which counts from
// the end, takes it at once.
func TestEpilogKeepsTheFlagAfterTheProcessesAreGoneAndPastMinJobAge(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		for _, way := range endWays {
			t.Run(string(mode)+"/"+way.name, func(t *testing.T) {
				r := newEpilogRig(t, mode, 10*time.Second, 10*time.Minute, Config{})
				id, release := r.endStubborn(way)
				release()
				if j := r.job(id); !j.Finished || !j.Completing {
					t.Fatalf("processes gone: Finished %v, Completing %v; want true, true (the epilog still runs)", j.Finished, j.Completing)
				}
				completing := []string{way.state, Completing}
				r.wantSqueue("processes gone, epilog running", id, listed, completing, "CG")
				r.clock.Advance(time.Minute) // past MinJobAge, inside the epilog
				r.wantSqueue("end + 1 min, epilog running", id, listed, completing, "CG")
				_, stderr, exit := r.run("scancel " + itoa(id))
				if want := "scancel: error: Kill job error on job id " + itoa(id) + ": Job/step already completing or completed\n"; exit != 1 || stderr != want {
					t.Errorf("scancel of the completing job: exit %d stderr %q, want exit 1 stderr %q", exit, stderr, want)
				}
				r.clock.Advance(8*time.Minute + 59*time.Second) // 1 s short of the epilog's end
				r.wantSqueue("epilog 1 s short of its end", id, listed, completing, "CG")
				if j := r.job(id); !j.Completing {
					t.Errorf("1 s before the epilog ends: Completing false, want true")
				}
				r.clock.Advance(2 * time.Second) // the epilog has ended, long after end + MinJobAge
				if j := r.job(id); j.Completing {
					t.Errorf("after the epilog: Completing true, want false")
				}
				r.wantSqueue("epilog over, end + 10 min", id, invalidID, nil, "")
			})
		}
	}
}

// An epilog that ends inside MinJobAge leaves the job to the mode's rules from
// then on, the two shapes of #198's ruling: Helios drops it from the --me
// listing and answers an empty list for it by --jobs, Athena lists it in its
// end state without the flag until end + MinJobAge.
func TestEpilogEndingInsideMinJobAgeFollowsTheMode(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		for _, way := range endWays {
			t.Run(string(mode)+"/"+way.name, func(t *testing.T) {
				r := newEpilogRig(t, mode, 10*time.Minute, 5*time.Second, Config{})
				id, release := r.endStubborn(way)
				release()
				r.clock.Advance(4 * time.Second)
				r.wantSqueue("epilog 1 s short of its end", id, listed, []string{way.state, Completing}, "CG")
				r.clock.Advance(2 * time.Second) // the epilog ended at 5 s
				kind := emptyList
				if mode == NotListedAthena {
					kind = listed
				}
				r.wantSqueue("epilog over, end + 6 s", id, kind, []string{way.state}, way.code)
				if j := r.job(id); j.Completing {
					t.Errorf("after the epilog: Completing true, want false")
				}
				r.clock.Advance(10 * time.Minute)
				r.wantSqueue("end + 10 min 6 s", id, invalidID, nil, "")
			})
		}
	}
}

// With no epilog the flag ends with the processes, as R5 has it.
func TestNoEpilogEndsTheFlagWithTheProcesses(t *testing.T) {
	r := newEpilogRig(t, NotListedAthena, 10*time.Minute, 0, Config{})
	id, release := r.endStubborn(endWays[0])
	if j := r.job(id); !j.Completing || j.Finished {
		t.Fatalf("processes alive: Completing %v, Finished %v; want true, false", j.Completing, j.Finished)
	}
	release()
	if j := r.job(id); j.Completing || !j.Finished {
		t.Errorf("processes gone, no epilog: Completing %v, Finished %v; want false, true", j.Completing, j.Finished)
	}
	r.wantSqueue("processes gone, no epilog", id, listed, []string{Cancelled}, "CA")
}

// A negative epilog is a test bug, refused as New refuses the rest.
func TestNegativeEpilogPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "negative") {
			t.Errorf("recover() = %v, want a panic naming the negative epilog", r)
		}
	}()
	normalizeRun(RunMode{Root: t.TempDir(), Epilog: -time.Second})
}

// helios-like limits: one submitted job and one GPU per user under QOS now.
func nowQOS(submit, gpus *int) Config {
	return Config{
		Partitions: []Partition{{Name: "gh", GPUOnly: true}},
		QOS:        []QOS{{Name: "now", MaxSubmitPerUser: submit, MaxGPUsPerUser: gpus}},
	}
}

// Slurm frees the user's GPUs when it sets the flag, not when the processes
// are gone: a job held back by the GPU cap starts while the job that held the
// GPU is still COMPLETING, with its processes alive and its epilog to run.
func TestCompletingJobFreesItsGPUWhenTheFlagIsSet(t *testing.T) {
	r := newEpilogRig(t, NotListedHelios, 10*time.Second, 10*time.Minute, nowQOS(nil, Cap(1)))
	up, goFile, goB := r.home+"/up", r.home+"/go", r.home+"/goB"
	a := r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(goFile), "--time=6000", "--partition=gh", "--qos=now", "--gres=gpu:1")
	r.waitFile(up)
	b := r.submit(waitLoop(goB), "--time=6000", "--partition=gh", "--qos=now", "--gres=gpu:1")
	if j := r.job(b); j.State != Pending {
		t.Fatalf("job B is %s beside the running job A, want PENDING (the GPU cap holds it)", j.State)
	}
	r.must("scancel " + itoa(a))
	waitFor(t, "the TERM to the processes of job A", func() bool { return !r.job(a).TermAt.IsZero() })
	if j := r.job(a); j.State != Cancelled || !j.Completing || j.Finished {
		t.Fatalf("job A: state %s, Completing %v, Finished %v; want CANCELLED, true, false (its processes ignore TERM)", j.State, j.Completing, j.Finished)
	}
	waitFor(t, "job B to start while job A completes", func() bool { return r.job(b).State == Running })
	for _, f := range []string{goFile, goB} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	r.waitFinished(a)
	r.waitFinished(b)
}

// Slurm frees the user's submit slot when it sets the flag: with one job
// allowed, a second sbatch is accepted while the first is COMPLETING, and
// refused while the first still runs.
func TestCompletingJobFreesItsSubmitSlotWhenTheFlagIsSet(t *testing.T) {
	r := newEpilogRig(t, NotListedHelios, 10*time.Second, 10*time.Minute, nowQOS(Cap(1), nil))
	up, goFile, goB := r.home+"/up", r.home+"/go", r.home+"/goB"
	a := r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(goFile), "--time=6000", "--partition=gh", "--qos=now", "--gres=gpu:1")
	r.waitFile(up)
	_, errOut, code := r.run("sbatch --parsable --partition=gh --qos=now --gres=gpu:1 --wrap=true")
	if code != 1 || !strings.Contains(errOut, "QOSMaxSubmitJobPerUserLimit") {
		t.Fatalf("a second sbatch beside the running job: exit %d stderr %q, want the submit-limit refusal", code, errOut)
	}
	r.must("scancel " + itoa(a))
	waitFor(t, "the TERM to the processes of job A", func() bool { return !r.job(a).TermAt.IsZero() })
	if j := r.job(a); !j.Completing || j.Finished {
		t.Fatalf("job A: Completing %v, Finished %v; want true, false", j.Completing, j.Finished)
	}
	b := r.submit(waitLoop(goB), "--time=6000", "--partition=gh", "--qos=now", "--gres=gpu:1")
	if j := r.job(b); j.State != Running {
		t.Errorf("job B accepted beside the completing job A is %s, want RUNNING", j.State)
	}
	for _, f := range []string{goFile, goB} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	r.waitFinished(a)
	r.waitFinished(b)
}
