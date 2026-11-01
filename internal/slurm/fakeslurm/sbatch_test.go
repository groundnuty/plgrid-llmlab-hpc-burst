package fakeslurm

// New, with the cases of spikes/s4/fakeslurm (holds_test.go, faults_test.go)
// that concern sbatch. Expected texts are the ones S4 and the brief of this
// fake quote from Slurm.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

const submitFailedPrefix = "sbatch: error: Batch job submission failed: "

// transientReasons are the texts after which the plugin reads a failure as a
// possibly registered job (HBEP-4, internal/slurm/dialect.go). A definite
// rejection of the fake must hold none of them.
var transientReasons = []string{
	"Socket timed out", "temporarily unavailable", "Unable to contact slurm controller",
	"Communication connection failure", "Zero Bytes were transmitted", "Connection refused",
	"Connection reset", "Transport endpoint", "Slurm backup controller in standby mode",
}

func TestSbatchRegistersJobs(t *testing.T) {
	r := newRig(t, Config{})
	line := "sbatch --parsable --job-name=hb-0123456789ab --no-requeue --partition=example-gpu " +
		"--account=example-account --time=30 --nodes=1 --gres=gpu:2 --cpus-per-task=16 --mem=32768M " +
		"--output=/s/job.out --error=/s/job.err '--wrap=[ $(echo a && b) ] && exec x'"
	if out := r.must(line); out != "1001\n" {
		t.Fatalf("sbatch --parsable printed %q, want 1001", out)
	}
	if out := r.must("sbatch --wrap=true"); out != "Submitted batch job 1002\n" {
		t.Errorf("sbatch without --parsable printed %q", out)
	}
	j, ok := r.p.Job(1001)
	if !ok {
		t.Fatal("job 1001 is not registered")
	}
	want := Job{
		ID: 1001, Name: "hb-0123456789ab", User: "example-user", Partition: "example-gpu",
		Account: "example-account", TimeLimit: 30, CPUs: 16, GPUs: 2, MemMiB: 32768,
		State: Pending, Executions: 1, Submit: t0, Output: "/s/job.out", Error: "/s/job.err",
		Wrap: "[ $(echo a && b) ] && exec x",
	}
	if j.Estimate != nil {
		t.Errorf("a new job has an estimate: %v", *j.Estimate)
	}
	j.Estimate = nil
	if j != want {
		t.Errorf("job\n got  %+v\n want %+v", j, want)
	}
	// Defaults of a job given no name, partition or account.
	if j2, _ := r.p.Job(1002); j2.Name != "wrap" || j2.Partition != "batch" || j2.Account != "default" || j2.CPUs != 1 || j2.GPUs != 0 {
		t.Errorf("defaults: %+v", j2)
	}
}

func TestSbatchTimeAndMemoryForms(t *testing.T) {
	r := newRig(t, Config{})
	for _, tc := range []struct {
		args    string
		minutes int
		memMiB  int64
	}{
		{"--time=30 --mem=100M", 30, 100},
		{"--time=01:30:00 --mem=2G", 90, 2048},
		{"--time=1-00:00:00 --mem=512", 1440, 512},
		{"--time=0:30", 1, 0}, // seconds round up
	} {
		id := r.sbatch(tc.args)
		if j, _ := r.p.Job(id); j.TimeLimit != tc.minutes || j.MemMiB != tc.memMiB {
			t.Errorf("sbatch %s: limit %d min, memory %d MiB, want %d and %d", tc.args, j.TimeLimit, j.MemMiB, tc.minutes, tc.memMiB)
		}
	}
	for _, bad := range []string{"--time=abc", "--mem=lots", "--nodes=0", "--cpus-per-task=x"} {
		if _, errOut, code := r.run("sbatch --wrap=true " + bad); code != 1 || !strings.HasPrefix(errOut, "sbatch: error: ") {
			t.Errorf("sbatch %s: exit %d stderr %q, want exit 1 and an sbatch error", bad, code, errOut)
		}
	}
}

// S4: --hold registers a job on a user hold; squeue shows hold true, priority 0
// and the reason JobHeldUser.
func TestSbatchHold(t *testing.T) {
	r := newRig(t, Config{})
	held := r.sbatch("--hold")
	free := r.sbatch()
	if j, _ := r.p.Job(held); !j.Held || j.HoldReason != HeldUser {
		t.Errorf("held job = %+v, want held with JobHeldUser", j)
	}
	if j, _ := r.p.Job(free); j.Held {
		t.Errorf("a job submitted without --hold is held")
	}
	sq := r.squeueJob(held)
	if sq["hold"] != true || sq["state_reason"] != "JobHeldUser" {
		t.Errorf("squeue hold %v reason %v", sq["hold"], sq["state_reason"])
	}
	wantNV(t, "held priority", sq["priority"], true, 0)
	sq = r.squeueJob(free)
	if sq["hold"] != false || sq["state_reason"] != "None" {
		t.Errorf("free job: hold %v reason %v", sq["hold"], sq["state_reason"])
	}
	wantNV(t, "free priority", sq["priority"], true, 1000)
}

// --constraint, which the dialect sends for a partition's tmpDirFeature (issue
// #125), is accepted and recorded; the fake does not model node features.
func TestSbatchRecordsTheConstraint(t *testing.T) {
	r := newRig(t, Config{})
	out, errOut, code := r.run("sbatch --parsable --wrap=true --constraint=localfs")
	if code != 0 {
		t.Fatalf("exit %d stderr %q", code, errOut)
	}
	id, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("job ID %q: %v", out, err)
	}
	if j, ok := r.p.Job(id); !ok || j.Constraint != "localfs" {
		t.Errorf("job %d: %+v, want Constraint localfs", id, j)
	}
}

func TestSbatchRefusesWhatTheDialectNeverSends(t *testing.T) {
	r := newRig(t, Config{})
	for _, line := range []string{
		"sbatch --parsable",               // no --wrap
		"sbatch --wrap=true --exclusive",  // unknown option
		"sbatch --wrap=true script.sh",    // a script file
		"sbatch --wrap=true --time",       // missing value at the end
		"sbatch -t 30 --wrap=true",        // short options are not sent
		"sbatch --wrap=true --exclude=n1", // unknown option
	} {
		if _, errOut, code := r.run(line); code != 1 || !strings.HasPrefix(errOut, "sbatch: error: ") {
			t.Errorf("%q: exit %d stderr %q, want exit 1 and an sbatch error", line, code, errOut)
		}
	}
	// Typed GRES is a definite rejection (Sano refuses it, spike S5).
	_, errOut, code := r.run("sbatch --wrap=true --gres=gpu:a100:1")
	if code != 1 || errOut != submitFailedPrefix+"Invalid generic resource (gres) specification\n" {
		t.Errorf("typed gres: exit %d stderr %q", code, errOut)
	}
	if n := len(r.p.Jobs()); n != 0 {
		t.Errorf("%d jobs registered by refused submissions, want 0", n)
	}
}

func TestSbatchPartitionPolicies(t *testing.T) {
	r := newRig(t, Config{Partitions: []Partition{
		{Name: "single", OneJobPerUser: true},
		{Name: "gpu", GPUOnly: true},
		{Name: "plain"},
	}})
	bob := loginAs(r.p, "bob")

	// A one-job-per-user partition refuses a second live job of the same user.
	a := r.sbatch("--partition=single")
	_, errOut, code := r.run("sbatch --wrap=true --partition=single")
	// Slurm's refusal is two lines, the reason's name first (HBEP-4, QOS limits,
	// A limit refusal).
	const oneJob = "sbatch: error: QOSMaxSubmitJobPerUserLimit\n" +
		submitFailedPrefix + "Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n"
	if code != 1 || errOut != oneJob {
		t.Errorf("second job: exit %d stderr %q, want exit 1 and %q", code, errOut, oneJob)
	}
	r.sbatchAs(bob, "--partition=single") // another user is fine
	r.sbatch("--partition=plain")         // another partition is fine
	if n := len(r.p.Jobs()); n != 3 {
		t.Errorf("%d jobs, want 3: the refused submission registers nothing", n)
	}
	r.start(a)
	if _, _, code := r.run("sbatch --wrap=true --partition=single"); code != 1 {
		t.Errorf("a running job still counts as live: exit %d, want 1", code)
	}
	r.end(a, Completed)
	r.sbatch("--partition=single") // the first job ended: accepted

	// A pending job counts too: bob's first one is still pending.
	if _, _, code := r.runAs(bob, "sbatch --wrap=true --partition=single", nil); code != 1 {
		t.Errorf("bob's second job (bob's first is pending): exit %d, want 1", code)
	}

	// A GPU-only partition refuses a job without --gres=gpu:.
	_, errOut, code = r.run("sbatch --wrap=true --partition=gpu")
	if code != 1 || !strings.HasPrefix(errOut, submitFailedPrefix) {
		t.Errorf("GPU-only without GPU: exit %d stderr %q, want a definite rejection", code, errOut)
	}
	r.sbatch("--partition=gpu --gres=gpu:1")

	// A partition not in the list is refused.
	_, errOut, code = r.run("sbatch --wrap=true --partition=nowhere")
	if code != 1 || errOut != submitFailedPrefix+"Invalid partition name specified\n" {
		t.Errorf("unknown partition: exit %d stderr %q", code, errOut)
	}
	// With no partition option the first listed one is the default.
	if j, _ := r.p.Job(r.sbatchAs(loginAs(r.p, "carol"))); j.Partition != "single" {
		t.Errorf("default partition = %q, want the first listed, single", j.Partition)
	}
}

func TestDefiniteRejectionsAreNeverTransient(t *testing.T) {
	r := newRig(t, Config{Partitions: []Partition{{Name: "single", OneJobPerUser: true}, {Name: "gpu", GPUOnly: true}}})
	r.sbatch("--partition=single")
	for _, line := range []string{
		"sbatch --wrap=true --partition=single", // one job per user
		"sbatch --wrap=true --partition=gpu",    // GPU only
		"sbatch --wrap=true --partition=nowhere",
		"sbatch --wrap=true --partition=gpu --gres=gpu:a100:1",
	} {
		_, errOut, code := r.run(line)
		if code != 1 || !strings.Contains(errOut, submitFailedPrefix) {
			t.Fatalf("%q: exit %d stderr %q, want a definite rejection", line, code, errOut)
		}
		for _, bad := range transientReasons {
			if strings.Contains(errOut, bad) {
				t.Errorf("%q: the rejection %q holds the transient text %q", line, errOut, bad)
			}
		}
	}
}

func TestScriptSteps(t *testing.T) {
	t.Run("Reject", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Reject("Invalid account or account/partition combination specified"))
		out, errOut, code := r.run("sbatch --wrap=true")
		const want = submitFailedPrefix + "Invalid account or account/partition combination specified\n"
		if code != 1 || out != "" || errOut != want {
			t.Errorf("exit %d stdout %q stderr %q, want exit 1 and %q", code, out, errOut, want)
		}
		if len(r.p.Jobs()) != 0 {
			t.Errorf("a rejected submission registered a job")
		}
		if id := r.sbatch(); id != 1001 { // the script is used up: back to accepting
			t.Errorf("next job ID = %d, want 1001 (the rejected call took none)", id)
		}
	})
	t.Run("Transient without registration", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Transient(false))
		_, errOut, code := r.run("sbatch --wrap=true")
		if want := submitFailedPrefix + "Socket timed out on send/recv operation\n"; code != 1 || errOut != want {
			t.Errorf("exit %d stderr %q, want exit 1 and %q", code, errOut, want)
		}
		if len(r.p.Jobs()) != 0 {
			t.Errorf("an unregistered transient failure registered a job")
		}
	})
	t.Run("Transient that registered: the lost reply", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Transient(true))
		out, errOut, code := r.run("sbatch --parsable --hold --wrap=true")
		if want := submitFailedPrefix + "Socket timed out on send/recv operation\n"; code != 1 || out != "" || errOut != want {
			t.Errorf("exit %d stdout %q stderr %q, want exit 1, no stdout, %q", code, out, errOut, want)
		}
		if j, ok := r.p.Job(1001); !ok || !j.Held {
			t.Errorf("the lost-reply job = %+v (present %v), want registered and held as asked", j, ok)
		}
		if r.p.Count("sbatch") != 1 {
			t.Errorf("sbatch count = %d, want 1", r.p.Count("sbatch"))
		}
	})
	t.Run("LostReply", func(t *testing.T) {
		for _, want := range []error{slurm.ErrTimeout, slurm.ErrTransport} {
			r := newRig(t, Config{})
			r.p.Script(LostReply(want))
			res, err := r.s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable --wrap=true"})
			if !errors.Is(err, want) || len(res.Stdout) != 0 || res.ExitCode != 0 {
				t.Errorf("Run = %+v, %v, want the runner error %v and no result", res, err, want)
			}
			if _, ok := r.p.Job(1001); !ok {
				t.Errorf("the job of a lost reply is not registered")
			}
		}
	})
	t.Run("Unparseable", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Unparseable())
		out, _, code := r.run("sbatch --parsable --wrap=true")
		if code != 0 || strings.TrimSpace(out) == "" || strings.Contains(out, "1001") {
			t.Errorf("exit %d stdout %q, want exit 0 and garbage without the job ID", code, out)
		}
		if _, ok := r.p.Job(1001); !ok {
			t.Errorf("the job is not registered")
		}
	})
	t.Run("Rename", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Rename(40))
		tag := "hb-" + strings.Repeat("a", 53) // 56 characters, a full job tag
		id := r.sbatch("--job-name=" + tag)
		if j, _ := r.p.Job(id); j.Name != tag[:40] || len(j.Name) != 40 {
			t.Errorf("stored name = %q (%d), want the first 40 characters", j.Name, len(j.Name))
		}
		if got := r.squeueJob(id)["name"]; got != tag[:40] {
			t.Errorf("squeue name = %v, want the shortened name", got)
		}
		r.p.Script(Rename(100))
		if j, _ := r.p.Job(r.sbatch("--job-name=short")); j.Name != "short" {
			t.Errorf("a name shorter than the cut changed to %q", j.Name)
		}
	})
	t.Run("AsTimeLimit", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(AsTimeLimit(45))
		id := r.sbatch("--time=30")
		if j, _ := r.p.Job(id); j.TimeLimit != 45 {
			t.Errorf("time limit = %d, want 45", j.TimeLimit)
		}
		wantNV(t, "time_limit", r.squeueJob(id)["time_limit"], true, 45)
	})
	t.Run("AdminHold", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(AdminHold())
		id := r.sbatch() // no --hold
		if j, _ := r.p.Job(id); !j.Held || j.HoldReason != HeldAdmin {
			t.Errorf("job = %+v, want an admin hold", j)
		}
		if got := r.squeueJob(id)["state_reason"]; got != "JobHeldAdmin" {
			t.Errorf("state_reason = %v, want JobHeldAdmin", got)
		}
	})
	t.Run("steps are consumed in order, one per call", func(t *testing.T) {
		r := newRig(t, Config{})
		r.p.Script(Reject("first"), Accept(), Transient(true))
		results := []int{}
		for i := 0; i < 4; i++ {
			_, _, code := r.run("sbatch --wrap=true")
			results = append(results, code)
		}
		if want := []int{1, 0, 1, 0}; !equalInts(results, want) {
			t.Errorf("exit codes %v, want %v (reject, accept, transient, default accept)", results, want)
		}
		if n := len(r.p.Jobs()); n != 3 { // accept, transient(registered), default
			t.Errorf("%d jobs, want 3", n)
		}
	})
	t.Run("a policy refusal comes first and still uses up the step", func(t *testing.T) {
		r := newRig(t, Config{Partitions: []Partition{{Name: "single", OneJobPerUser: true}}})
		a := r.sbatch()
		r.p.Script(Transient(true))
		_, errOut, _ := r.run("sbatch --wrap=true")
		if !strings.Contains(errOut, "accounting/QOS policy") {
			t.Fatalf("stderr %q, want the policy refusal", errOut)
		}
		r.end(a, Completed)
		r.sbatch() // fails the test if the transient step were still queued
		if n := len(r.p.Jobs()); n != 2 {
			t.Errorf("%d jobs, want 2", n)
		}
	})
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
