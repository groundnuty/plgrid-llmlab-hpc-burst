package fakeslurm

// New. The QOS model (qos.go). Expected texts and numbers are written by hand
// from HBEP-4 (The Slurm dialect; QOS limits, A limit refusal), never read back
// from the fake's own output.

import (
	"errors"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// twoLines is Slurm's refusal for a policy reason: the reason line first, then
// the accounting-policy line, as HBEP-4 prints them.
func twoLines(reason string) string {
	return "sbatch: error: " + reason + "\n" +
		"sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n"
}

// A helios-shaped cluster: GPU partition with its own QOS plgrid (2000
// submitted jobs, nothing else), and the job QOS now (1 job, 1 GPU).
func heliosShape() Config {
	return Config{
		Partitions: []Partition{{Name: "gh", GPUOnly: true, QOS: "plgrid"}, {Name: "plain"}},
		QOS: []QOS{
			{Name: "now", MaxSubmitPerUser: 1, MaxGPUsPerUser: 1},
			{Name: "plgrid", MaxSubmitPerUser: 2000},
			{Name: "normal"},
		},
	}
}

// --qos is accepted and shown by the argv log and the job; with no --qos the
// log holds neither the flag nor an empty value, and the job has no QOS.
func TestQOSFlagShowsInTheArgvLog(t *testing.T) {
	r := newRig(t, Config{})
	withFlag := r.sbatch("--qos=now")
	without := r.sbatch()
	calls := r.p.Calls()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	hasQOS := func(words []string) (string, bool) {
		for _, w := range words {
			if strings.HasPrefix(w, "--qos") {
				return w, true
			}
		}
		return "", false
	}
	if w, ok := hasQOS(calls[0].Words); !ok || w != "--qos=now" {
		t.Errorf("first call words %q: want the word --qos=now", calls[0].Words)
	}
	if w, ok := hasQOS(calls[1].Words); ok {
		t.Errorf("second call sent no --qos, but its words hold %q: %q", w, calls[1].Words)
	}
	for _, w := range calls[1].Words {
		if w == "" || w == "--qos" {
			t.Errorf("second call holds an empty value or a bare flag: %q", calls[1].Words)
		}
	}
	if j, _ := r.p.Job(withFlag); j.QOS != "now" {
		t.Errorf("job %d QOS = %q, want now", withFlag, j.QOS)
	}
	if j, _ := r.p.Job(without); j.QOS != "" {
		t.Errorf("job %d QOS = %q, want none", without, j.QOS)
	}
}

// A job with no --qos runs under the association's default.
func TestDefaultQOSApplies(t *testing.T) {
	r := newRig(t, Config{QOS: []QOS{{Name: "normal"}, {Name: "now", MaxSubmitPerUser: 1}}, DefaultQOS: "normal"})
	if j, _ := r.p.Job(r.sbatch()); j.QOS != "normal" {
		t.Errorf("QOS = %q, want the default normal", j.QOS)
	}
	// normal sets no limit, so the default does not refuse a second job.
	r.sbatch()
}

// A cluster that lists QOSes refuses a name it does not list.
func TestUnknownQOSIsRefused(t *testing.T) {
	r := newRig(t, heliosShape())
	_, errOut, code := r.run("sbatch --wrap=true --partition=plain --qos=nowhere")
	if code != 1 || errOut != submitFailedPrefix+"Invalid qos specification\n" {
		t.Errorf("exit %d stderr %q", code, errOut)
	}
	if n := len(r.p.Jobs()); n != 0 {
		t.Errorf("%d jobs, want 0", n)
	}
}

// The submit limit counts the user's pending and running jobs under the QOS,
// whatever their name: a job with another tag holds the slot, as the owner's
// other projects would. The refusal is two lines, the reason first.
func TestSubmitLimitCountsEveryJobOfTheUser(t *testing.T) {
	cfg := Config{Partitions: []Partition{{Name: "p"}}, QOS: []QOS{{Name: "now", MaxSubmitPerUser: 1}}}
	r := newRig(t, cfg)
	bob := loginAs(r.p, "bob")

	other := r.p.OutsideJob("example-user", "another-project-job", "p", "now", 1)
	if n := r.p.Count("sbatch"); n != 0 {
		t.Fatalf("OutsideJob is no call, but sbatch was counted %d times", n)
	}
	_, errOut, code := r.run("sbatch --wrap=true --partition=p --qos=now --job-name=hb-example-0123456789ab")
	if code != 1 || errOut != twoLines("QOSMaxSubmitJobPerUserLimit") {
		t.Fatalf("exit %d stderr %q, want exit 1 and the two lines %q", code, errOut, twoLines("QOSMaxSubmitJobPerUserLimit"))
	}
	if n := len(r.p.Jobs()); n != 1 {
		t.Errorf("%d jobs, want 1: a refused submission registers nothing", n)
	}
	r.sbatchAs(bob, "--partition=p", "--qos=now") // another user has a slot of their own
	r.sbatch("--partition=p")                     // no --qos: the default has no limit here

	// A running job counts too, and the slot frees when the job ends.
	r.start(other)
	if _, _, code := r.run("sbatch --wrap=true --partition=p --qos=now"); code != 1 {
		t.Errorf("a running job still holds the slot: exit %d, want 1", code)
	}
	r.end(other, Completed)
	r.sbatch("--partition=p", "--qos=now") // accepted
}

// A held job is a pending job, so it takes the slot (HBEP-4, QOS limits).
func TestAHeldJobTakesTheSlot(t *testing.T) {
	r := newRig(t, Config{QOS: []QOS{{Name: "now", MaxSubmitPerUser: 1}}})
	held := r.sbatch("--qos=now", "--hold")
	if j, _ := r.p.Job(held); !j.Held {
		t.Fatal("the job is not held")
	}
	if _, errOut, code := r.run("sbatch --wrap=true --qos=now"); code != 1 || errOut != twoLines("QOSMaxSubmitJobPerUserLimit") {
		t.Errorf("second submission: exit %d stderr %q, want the refusal", code, errOut)
	}
}

// The partition's QOS comes before the job's, and each limit comes from the
// first QOS that sets it. On a GH200-shaped partition, plgrid's 2000 jobs
// apply, so a second job is accepted under now; now's one GPU per user is the
// one that holds (plgrid sets none), so the second job pends until the first
// ends (HBEP-4, QOS limits, Risks).
func TestPartitionQOSComesFirstAndTheGPUCapHolds(t *testing.T) {
	r := newRig(t, heliosShape())
	a := r.sbatch("--partition=gh", "--qos=now", "--gres=gpu:1", "--hold")
	b := r.sbatch("--partition=gh", "--qos=now", "--gres=gpu:1", "--hold") // accepted: plgrid's 2000 applies
	if j, _ := r.p.Job(b); j.State != Pending {
		t.Fatalf("second job is %s, want PENDING", j.State)
	}
	// Release the holds, as the plugin does.
	r.must("scontrol release " + itoa(a) + "," + itoa(b))
	r.start(a)
	if err := r.p.Start(b); err == nil || !strings.Contains(err.Error(), "QOSMaxGRESPerUser") {
		t.Fatalf("Start(b) = %v, want a refusal naming QOSMaxGRESPerUser: one GPU per user", err)
	}
	if j, _ := r.p.Job(b); j.State != Pending {
		t.Errorf("job b is %s after a refused start, want PENDING", j.State)
	}
	r.end(a, Completed)
	r.start(b) // the GPU is free
	if j, _ := r.p.Job(b); j.State != Running {
		t.Errorf("job b is %s, want RUNNING", j.State)
	}
}

// Another user's GPUs do not count against the user's cap.
func TestGPUCapIsPerUser(t *testing.T) {
	r := newRig(t, heliosShape())
	bob := loginAs(r.p, "bob")
	a := r.sbatch("--partition=gh", "--qos=now", "--gres=gpu:1")
	b := r.sbatchAs(bob, "--partition=gh", "--qos=now", "--gres=gpu:1")
	r.start(a)
	r.start(b)
}

// A job under a QOS with no GPU cap starts beside the user's others.
func TestNoCapNoWait(t *testing.T) {
	r := newRig(t, heliosShape())
	a := r.sbatch("--partition=gh", "--qos=normal", "--gres=gpu:1")
	b := r.sbatch("--partition=gh", "--qos=normal", "--gres=gpu:1")
	r.start(a)
	r.start(b)
}

// RejectPolicy prints the reason line and then the policy line for any reason
// name, exits 1 and registers nothing; Reject prints the one line.
func TestRejectPolicyPrintsBothLines(t *testing.T) {
	names := []string{"QOSMaxSubmitJobPerUserLimit", "AssocMaxSubmitJobLimit", "MaxSubmitJobsPerAccount", "QOSMaxWallDurationPerJobLimit"}
	r := newRig(t, Config{})
	for _, n := range names {
		r.p.Script(RejectPolicy(n))
		_, errOut, code := r.run("sbatch --wrap=true")
		if code != 1 || errOut != twoLines(n) {
			t.Errorf("%s: exit %d stderr %q, want exit 1 and %q", n, code, errOut, twoLines(n))
		}
		if strings.Contains(errOut, "Socket timed out") {
			t.Errorf("%s: a definite refusal holds a transient text", n)
		}
	}
	if n := len(r.p.Jobs()); n != 0 {
		t.Errorf("%d jobs registered by refused submissions, want 0", n)
	}
	// The control: Reject prints the one line, with no reason line.
	r.p.Script(Reject("Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"))
	_, errOut, _ := r.run("sbatch --wrap=true")
	if want := "sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n"; errOut != want {
		t.Errorf("Reject printed %q, want %q", errOut, want)
	}
}

// The fake's two lines agree with the shared fake scheduler's and the
// contract's policy line, which HBEP-4 gives (two independent copies).
func TestLinesAgreeWithTheContract(t *testing.T) {
	for _, n := range []string{"QOSMaxSubmitJobPerUserLimit", "QOSMaxWallDurationPerJobLimit"} {
		if got, want := twoLines(n), fake.PolicyRefusalLines(n); got != want {
			t.Errorf("%s: fake lines %q, pkg/provider/fake lines %q", n, got, want)
		}
		if !strings.HasSuffix(strings.TrimSuffix(twoLines(n), "\n"), provider.PolicyLine) {
			t.Errorf("%s: the second line is not provider.PolicyLine", n)
		}
	}
}

// A lost reply that registers late: the sbatch returns the runner error and
// registers nothing; the job registers, held, when the next sbatch arrives,
// before that sbatch is checked, so the job holds the slot and the next
// attempt is refused for the limit (HBEP-4, QOS limits, A lost reply under a
// one-job limit).
func TestLostReplyLateRegistersBeforeTheNextSubmissionIsChecked(t *testing.T) {
	cfg := Config{Partitions: []Partition{{Name: "p"}}, QOS: []QOS{{Name: "now", MaxSubmitPerUser: 1}}}
	r := newRig(t, cfg)
	r.p.Script(LostReplyLate(slurm.ErrTimeout))
	_, err := r.s.Run(t.Context(), slurm.Command{Line: "sbatch --parsable --job-name=hb-example-0123456789ab --hold --partition=p --qos=now --wrap=true"})
	if !errors.Is(err, slurm.ErrTimeout) {
		t.Fatalf("Run error = %v, want the lost reply (ErrTimeout)", err)
	}
	if n := len(r.p.Jobs()); n != 0 {
		t.Fatalf("%d jobs right after the lost reply, want 0: the job registers late", n)
	}
	if n := r.p.LateJobs(); n != 1 {
		t.Fatalf("%d late jobs, want 1", n)
	}
	_, errOut, code := r.run("sbatch --parsable --job-name=hb-example-0123456789ab --hold --partition=p --qos=now --wrap=true")
	if code != 1 || errOut != twoLines("QOSMaxSubmitJobPerUserLimit") {
		t.Fatalf("exit %d stderr %q, want the limit refusal: the late job holds the slot", code, errOut)
	}
	jobs := r.p.Jobs()
	if len(jobs) != 1 || jobs[0].Name != "hb-example-0123456789ab" || !jobs[0].Held || jobs[0].State != Pending || jobs[0].QOS != "now" {
		t.Fatalf("jobs %+v, want the one late job, held and pending, under now", jobs)
	}
	if n := r.p.LateJobs(); n != 0 {
		t.Errorf("%d late jobs left, want 0", n)
	}
}
