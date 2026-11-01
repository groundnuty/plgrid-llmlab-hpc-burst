package providerctl

// The controller over the real Batch and the fake Slurm with QOS limits
// (HBEP-4, QOS limits, Test plan): a refusal for a full submit limit is
// recorded with its code in the same write as its state, shown as
// Submitting with SubmitLimitReached, stable from one pass to the next, and
// retried after the backoff; a refusal for another limit is an ordinary
// Rejected attempt; a lost reply under a one-job limit is adopted by tag.
// Expected texts and counts are written by hand from the HBEP.

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// withQOS gives the Provider the qos jobQOS and the profile the listed QOSes,
// and the partition the QOS partQOS.
func withQOS(jobQOS, partQOS string, list ...v1alpha1.QOSProfile) realOption {
	return func(e *realEnv) {
		e.profiles.set(func(v *slurm.Validated) {
			v.Settings.QOS = jobQOS
			v.Profile.Spec.QOS = list
			v.Profile.Partition.QOS = partQOS
		})
	}
}

// groupNote is what the error of a group limit adds to Slurm's reason line
// (provider.GroupLimitNote): the limit is shared by the grant's users.
const groupNote = " (group limit, shared by the grant's users)"

func withRecorder(r *recorder) realOption { return func(e *realEnv) { e.opts.Recorder = r } }

func one() *int32 { n := int32(1); return &n }

// heliosLikeSlurm is a cluster with the job QOS now (one job and one GPU per
// user) and the GPU partition's own QOS plgrid (2000 jobs).
func heliosLikeSlurm(clock *fake.Clock) *fakeslurm.Platform {
	return fakeslurm.New(clock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: gpuPart, GPUOnly: true, QOS: "plgrid"}},
		QOS: []fakeslurm.QOS{
			{Name: "now", MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)},
			{Name: "plgrid", MaxSubmitPerUser: fakeslurm.Cap(2000)},
		},
	})
}

// nowOnly is a cluster with a partition without its own QOS, so the job QOS
// now's limit of one job per user is the one that applies.
func nowOnlySlurm(clock *fake.Clock) *fakeslurm.Platform {
	return fakeslurm.New(clock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: gpuPart, GPUOnly: true}},
		QOS:        []fakeslurm.QOS{{Name: "now", MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)}},
	})
}

func nowProfile() []v1alpha1.QOSProfile {
	return []v1alpha1.QOSProfile{
		{Name: "now", MaxSubmitPerUser: one(), MaxGPUsPerUser: one()},
		{Name: "plgrid"},
	}
}

// shown is what an observer of an Allocation sees: the phase and reason, the
// record's code, and the Ready and Synced conditions without their times.
type shown struct {
	Phase, Reason, Code                             string
	ReadyStatus, ReadyReason, ReadyMessage          string
	SyncedStatus, SyncedReason, SyncedMessage       string
	RecordState, RecordCode, RecordReason, MirrCode string
}

func snapshot(a *v1alpha1.Allocation) shown {
	o := a.Status.AtProvider
	s := shown{Phase: string(o.Phase), Reason: o.Reason}
	rd, sy := a.GetCondition(xpv2.TypeReady), a.GetCondition(xpv2.TypeSynced)
	s.ReadyStatus, s.ReadyReason, s.ReadyMessage = string(rd.Status), string(rd.Reason), rd.Message
	s.SyncedStatus, s.SyncedReason, s.SyncedMessage = string(sy.Status), string(sy.Reason), sy.Message
	if rec, err := readRecord(a); err == nil && rec.sub != nil {
		s.RecordState, s.RecordCode, s.RecordReason = string(rec.sub.State), rec.sub.Code, rec.sub.Reason
	}
	if o.Submission != nil {
		s.MirrCode = o.Submission.Code
	}
	return s
}

// Each full-slot reason name, and another policy reason, and a refusal with no
// reason line, run through Create and then several Observe passes. The state is
// recorded at once with its code, and every pass that waits for the backoff
// shows exactly the same phase, reason, conditions and record: nothing flaps
// (the refusal contract, `@hpc-burst-science-agent`, 1 Nov 2026). All three
// kinds are in one table, so the test cannot see only one kind of refusal.
func TestPolicyRefusalSettlesWithoutFlapping(t *testing.T) {
	// What an ordinary Rejected outcome keeps of Slurm's text: the words after "sbatch: error:".
	const policyLine = "Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"
	for _, c := range []struct {
		name   string
		step   fakeslurm.SubmitStep
		code   string // the record's code, and the Allocation's reason
		reason string // the record's Reason: the message
	}{
		{"QOSMaxSubmitJobPerUserLimit", fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), "SubmitLimitReached", "sbatch: error: QOSMaxSubmitJobPerUserLimit"},
		{"AssocMaxSubmitJobLimit", fakeslurm.RejectPolicy("AssocMaxSubmitJobLimit"), "SubmitLimitReached", "sbatch: error: AssocMaxSubmitJobLimit"},
		{"MaxSubmitJobsPerAccount", fakeslurm.RejectPolicy("MaxSubmitJobsPerAccount"), "SubmitLimitReached", "sbatch: error: MaxSubmitJobsPerAccount"},
		// The group limits are shared by the grant's users, and say so (N1).
		{"QOSGrpSubmitJobsLimit", fakeslurm.RejectPolicy("QOSGrpSubmitJobsLimit"), "SubmitLimitReached", "sbatch: error: QOSGrpSubmitJobsLimit" + groupNote},
		{"AssocGrpSubmitJobsLimit", fakeslurm.RejectPolicy("AssocGrpSubmitJobsLimit"), "SubmitLimitReached", "sbatch: error: AssocGrpSubmitJobsLimit" + groupNote},
		{"QOSMaxWallDurationPerJobLimit", fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"), "", "sbatch: error: QOSMaxWallDurationPerJobLimit"},
		{"no reason line", fakeslurm.Reject("Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"), "", policyLine},
	} {
		t.Run(c.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := nowOnlySlurm(clock)
			sl.Script(c.step)
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
			e.newAlloc("alloc-a", 1)

			e.run("alloc-a", 1) // Create: the submission, refused
			first := snapshot(e.get("alloc-a"))
			if first.RecordState != "Rejected" || first.RecordCode != c.code || first.RecordReason != c.reason {
				t.Fatalf("after Create: record state %q code %q reason %q; want Rejected, code %q, reason %q",
					first.RecordState, first.RecordCode, first.RecordReason, c.code, c.reason)
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls after Create, want 1", n)
			}

			// The passes that wait for the backoff, 5 s apart, all inside it.
			var base shown
			for i := 0; i < 8; i++ {
				clock.Advance(5 * time.Second)
				e.run("alloc-a", 1)
				s := snapshot(e.get("alloc-a"))
				if i == 0 {
					base = s
					if s.Phase != "Submitting" || s.MirrCode != c.code {
						t.Fatalf("first Observe: phase %q, submission code %q; want Submitting and code %q", s.Phase, s.MirrCode, c.code)
					}
					if c.code != "" {
						if s.Reason != "SubmitLimitReached" || s.ReadyReason != "Creating" || !strings.Contains(s.ReadyMessage, c.reason) || !strings.Contains(s.ReadyMessage, "SubmitLimitReached") {
							t.Fatalf("limit refusal shown as reason %q, Ready %q %q; want SubmitLimitReached and a message with %q",
								s.Reason, s.ReadyReason, s.ReadyMessage, c.reason)
						}
					} else if s.Reason != c.reason {
						t.Fatalf("plain refusal shown with reason %q, want today's display, the scheduler's reason %q", s.Reason, c.reason)
					}
					continue
				}
				if s != base {
					t.Fatalf("pass %d differs from the first Observe:\n got  %+v\n want %+v", i+1, s, base)
				}
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls while waiting for the backoff, want 1", n)
			}
			// Across the backoff: attempt 2 is refused the same way. The Create
			// pass of every attempt ends in crossplane-runtime's
			// MarkConditions(Creating(), ReconcileSuccess()) (reconciler.go:1474 in
			// v2.4.3), which empties the Ready message; the next pass, requeued at
			// once, writes it again. The test logs what that pass shows, and holds
			// everything else to the waiting passes.
			sl.Script(c.step)
			clock.Advance(time.Minute)
			e.run("alloc-a", 1) // Create of attempt 2
			second := snapshot(e.get("alloc-a"))
			t.Logf("Create pass of attempt 2: Ready %s/%s %q, Synced %s, phase %q, reason %q",
				second.ReadyStatus, second.ReadyReason, second.ReadyMessage, second.SyncedStatus, second.Phase, second.Reason)
			if second.RecordState != "Rejected" || second.RecordCode != c.code {
				t.Fatalf("attempt 2: record state %q code %q, want Rejected and %q", second.RecordState, second.RecordCode, c.code)
			}
			if c.code != "" {
				// Nothing but the Ready message differs on the Create pass: the
				// phase, the reason and Synced are those of the waiting passes.
				if second.Phase != base.Phase || second.Reason != base.Reason || second.SyncedStatus != "True" || second.RecordCode != base.RecordCode {
					t.Errorf("attempt 2's Create pass %+v differs from the waiting passes %+v in more than the Ready message", second, base)
				}
				for i := 0; i < 3; i++ {
					clock.Advance(5 * time.Second)
					e.run("alloc-a", 1)
					if s := snapshot(e.get("alloc-a")); s != base {
						t.Fatalf("pass %d after attempt 2 differs from the waiting passes of attempt 1:\n got  %+v\n want %+v", i+1, s, base)
					}
				}
			}
			if c.code != "" {
				// A limit refusal does not flip Synced between Create and Observe.
				if first.SyncedStatus != "True" || base.SyncedStatus != "True" {
					t.Errorf("Synced is %q after Create and %q while waiting; a limit refusal must leave it True", first.SyncedStatus, base.SyncedStatus)
				}
			}
		})
	}
}

// A full slot held by another job of the user, whatever its tag, refuses the
// submission with both lines; the Allocation waits with SubmitLimitReached and
// fails nothing. Once the other job ends, the next attempt after the backoff
// is accepted (HBEP-4, Test plan, A limit refusal is no failure).
func TestAFullSlotWaitsAndTheNextAttemptAfterTheBackoffIsAccepted(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
	other := sl.OutsideJob("user-a", "another-project-job", gpuPart, "now", 1)
	e.newAlloc("alloc-a", 1)

	e.run("alloc-a", 3)
	a := e.get("alloc-a")
	rec, err := readRecord(a)
	if err != nil || rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached ||
		rec.sub.Reason != "sbatch: error: QOSMaxSubmitJobPerUserLimit" || rec.sub.Attempt != 1 {
		t.Fatalf("record %+v (%v), want attempt 1 Rejected with code SubmitLimitReached and the reason line", rec.sub, err)
	}
	if o := a.Status.AtProvider; o.Phase != v1alpha1.AllocationSubmitting || o.Reason != "SubmitLimitReached" {
		t.Errorf("phase %q reason %q, want Submitting and SubmitLimitReached", o.Phase, o.Reason)
	}
	if rec.jobID != "" {
		t.Errorf("job %q recorded for a refused submission", rec.jobID)
	}
	if n, m := sl.Count("sbatch"), len(sl.Jobs()); n != 1 || m != 1 {
		t.Errorf("%d sbatch calls and %d jobs, want 1 and 1 (the outside job)", n, m)
	}

	// Still full after the backoff: a second attempt, refused again, nothing failed.
	clock.Advance(2 * time.Minute)
	e.run("alloc-a", 2)
	if rec, _ = readRecord(e.get("alloc-a")); rec.sub == nil || rec.sub.Attempt != 2 || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("record %+v, want attempt 2 refused for the limit again", rec.sub)
	}

	// The other job ends; the next attempt after the backoff is accepted.
	if err := sl.End(other, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 2 {
		t.Errorf("%d sbatch calls during the backoff, want 2", n)
	}
	clock.Advance(2 * time.Minute)
	e.run("alloc-a", 3)
	rec, _ = readRecord(e.get("alloc-a"))
	if rec.jobID == "" || rec.sub == nil || rec.sub.Attempt != 3 || rec.sub.State != v1alpha1.SubmissionDone || rec.sub.Code != "" {
		t.Fatalf("record %+v, job %q; want attempt 3 Done with the job recorded and no code", rec.sub, rec.jobID)
	}
	if j := slurmJob(t, sl, rec.jobID); j.QOS != "now" {
		t.Errorf("the job runs under QOS %q, want now", j.QOS)
	}
}

// The same holds for the other two full-slot names, scripted; after each
// refusal the next attempt after the backoff is accepted.
func TestEachFullSlotNameWaitsThenTheNextAttemptIsAccepted(t *testing.T) {
	for _, name := range []string{"QOSMaxSubmitJobPerUserLimit", "AssocMaxSubmitJobLimit", "MaxSubmitJobsPerAccount", "QOSGrpSubmitJobsLimit", "AssocGrpSubmitJobsLimit"} {
		line := "sbatch: error: " + name
		if strings.Contains(name, "Grp") {
			line += groupNote
		}
		t.Run(name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := nowOnlySlurm(clock)
			sl.Script(fakeslurm.RejectPolicy(name))
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			a := e.get("alloc-a")
			if rec, _ := readRecord(a); rec.sub == nil || rec.sub.Code != "SubmitLimitReached" || rec.sub.Reason != line {
				t.Fatalf("record %+v, want code SubmitLimitReached and the reason %q", rec.sub, line)
			}
			if a.Status.AtProvider.Reason != "SubmitLimitReached" || !strings.Contains(a.GetCondition(xpv2.TypeReady).Message, line) {
				t.Errorf("reason %q, Ready %q; want SubmitLimitReached and a message naming %s", a.Status.AtProvider.Reason, a.GetCondition(xpv2.TypeReady).Message, name)
			}
			clock.Advance(2 * time.Minute)
			e.run("alloc-a", 3)
			if rec, _ := readRecord(e.get("alloc-a")); rec.jobID == "" || rec.sub.Attempt != 2 {
				t.Errorf("record %+v, job %q; want attempt 2 accepted", rec.sub, rec.jobID)
			}
		})
	}
}

// A refusal for another limit is an ordinary Rejected attempt with no code,
// retried after the backoff, and shown as before (HBEP-4, QOS limits).
func TestAnotherPolicyReasonIsAnOrdinaryRejectedAttempt(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"), fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"))
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	a := e.get("alloc-a")
	rec, _ := readRecord(a)
	if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != "" || rec.sub.Reason != "sbatch: error: QOSMaxWallDurationPerJobLimit" {
		t.Fatalf("record %+v, want Rejected, no code, the reason line", rec.sub)
	}
	if a.Status.AtProvider.Reason == "SubmitLimitReached" || strings.Contains(a.GetCondition(xpv2.TypeReady).Message, "SubmitLimitReached") {
		t.Errorf("an unrelated policy reason is shown as a full slot: %q / %q", a.Status.AtProvider.Reason, a.GetCondition(xpv2.TypeReady).Message)
	}
	for attempt := int32(2); attempt <= 3; attempt++ {
		clock.Advance(2 * time.Minute)
		e.run("alloc-a", 3)
		if rec, _ = readRecord(e.get("alloc-a")); rec.sub == nil || rec.sub.Attempt != attempt {
			t.Fatalf("record %+v, want attempt %d after the backoff", rec.sub, attempt)
		}
	}
	if rec.jobID == "" || rec.sub.State != v1alpha1.SubmissionDone {
		t.Errorf("record %+v, job %q; want the third attempt accepted", rec.sub, rec.jobID)
	}
}

// A lost reply under a one-job limit (HBEP-4, Test plan). Attempt 1's reply is
// lost and the fake registers its job late, held, after the in-flight window,
// so attempt 1 is dropped. Attempt 2 is refused for the limit, because that
// job holds the slot. The next Observe finds attempt 1's job by its tag and
// adopts it before any new claim; exactly one scontrol release reaches the
// fake and there is no attempt 3. A fixture in which the late job never
// registers proves nothing here, so the test checks that it registered, held,
// before it checks the refusal.
func TestALostReplyUnderAOneJobLimitIsAdoptedByTag(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.LostReplyLate(slurm.ErrTimeout))
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
	a := e.newAlloc("alloc-a", 1)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)

	e.run("alloc-a", 1) // attempt 1: the reply is lost
	if rec, _ := readRecord(e.get("alloc-a")); rec.sub == nil || rec.sub.Attempt != 1 || rec.sub.State != v1alpha1.SubmissionUnknown {
		t.Fatalf("record %+v, want attempt 1 Unknown", rec.sub)
	}
	if n, late := len(sl.Jobs()), sl.LateJobs(); n != 0 || late != 1 {
		t.Fatalf("%d jobs and %d late after the lost reply, want 0 and 1: the job registers late", n, late)
	}

	// The in-flight window (5 min) passes with no job found: attempt 1 is
	// dropped, attempt 2 is claimed, and the late job registers just before
	// attempt 2's sbatch is checked.
	clock.Advance(6 * time.Minute)
	e.run("alloc-a", 1)
	jobs := sl.Jobs()
	if len(jobs) != 1 || jobs[0].Name != tag || !jobs[0].Held || jobs[0].State != fakeslurm.Pending {
		t.Fatalf("jobs %+v, want attempt 1's job registered, held and pending, named by the tag", jobs)
	}
	rec, _ := readRecord(e.get("alloc-a"))
	if rec.sub == nil || rec.sub.Attempt != 2 || rec.sub.State != v1alpha1.SubmissionRejected || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached || rec.jobID != "" {
		t.Fatalf("record %+v, job %q; want attempt 2 refused for the limit, no job recorded yet", rec.sub, rec.jobID)
	}

	// The next Observe finds the late job by its tag and adopts it before any
	// new claim, then lifts its hold once. More passes, with the backoff
	// passing, make no attempt 3.
	for i := 0; i < 4; i++ {
		e.run("alloc-a", 1)
		clock.Advance(2 * time.Minute)
	}
	e.run("alloc-a", 2)
	rec, _ = readRecord(e.get("alloc-a"))
	if rec.jobID != strconv.Itoa(jobs[0].ID) {
		t.Fatalf("recorded job %q, want attempt 1's job %d adopted by tag", rec.jobID, jobs[0].ID)
	}
	if rec.sub == nil || rec.sub.Attempt != 2 {
		t.Errorf("record %+v: attempt %v, want no attempt 3", rec.sub, rec.sub)
	}
	if n := sl.Count("sbatch"); n != 2 {
		t.Errorf("%d sbatch calls, want 2 (attempts 1 and 2)", n)
	}
	releases := 0
	for _, c := range sl.Calls() {
		if c.Words[0] == "scontrol" && len(c.Words) > 1 && c.Words[1] == "release" {
			releases++
		}
	}
	if releases != 1 {
		t.Errorf("%d scontrol release calls, want exactly 1", releases)
	}
	if j := slurmJob(t, sl, rec.jobID); j.Held {
		t.Error("the adopted job is still held")
	}
}

// A per-user GPU cap (HBEP-4, Test plan): a second job that fits alone is
// accepted and pends until the first ends. On a GH200-shaped partition the
// partition's QOS sets 2000 submitted jobs and none for GPUs, so now's cap of
// one GPU is the one that holds.
func TestASecondJobBesideARunningOneIsAcceptedAndPends(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := heliosLikeSlurm(clock)
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "plgrid", nowProfile()...))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.start("alloc-a")
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 2)

	e.newAlloc("alloc-b", 1)
	e.run("alloc-b", 3)
	rec, _ := readRecord(e.get("alloc-b"))
	if rec.jobID == "" || rec.sub == nil || rec.sub.State != v1alpha1.SubmissionDone || rec.sub.Code != "" {
		t.Fatalf("record %+v, job %q; want the second job accepted (plgrid's 2000 applies), not refused", rec.sub, rec.jobID)
	}
	idB, _ := strconv.Atoi(rec.jobID)
	if err := sl.Start(idB); err == nil {
		t.Fatal("the second job started beside the first: now's cap of one GPU did not hold")
	}
	clock.Advance(40 * time.Second)
	e.run("alloc-b", 2)
	if o := e.get("alloc-b").Status.AtProvider; o.Phase != v1alpha1.AllocationPending {
		t.Errorf("phase %q, want Pending while the first job runs", o.Phase)
	}

	idA, _ := strconv.Atoi(e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID])
	if err := sl.End(idA, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.start("alloc-b")
	clock.Advance(40 * time.Second)
	e.run("alloc-b", 2)
	if o := e.get("alloc-b").Status.AtProvider; o.Phase != v1alpha1.AllocationRunning {
		t.Errorf("phase %q, want Running once the first job ended", o.Phase)
	}
}

// What the Allocation shows is derived from the record as it stands, on every
// pass, never carried over from the stored status (HBEP-4, QOS limits: another
// refusal is "never read as a full slot"; PR 151's review, blocker B1). A full
// slot followed by another policy reason shows the plain Rejected display from
// the Create pass on, and one followed by an Unknown outcome shows the wait for
// the tag; neither keeps SubmitLimitReached, with or without a code. The reason
// is asserted at every step, the backoff passes and the Create passes included,
// and the record's code, the submission mirror and the reason must agree.
func TestTheReasonFollowsTheCurrentRecordAtEveryStep(t *testing.T) {
	const (
		limit      = "SubmitLimitReached"
		wallLine   = "sbatch: error: QOSMaxWallDurationPerJobLimit"
		unknownMsg = "submission attempt 3 is Unknown and no job with the tag is found yet; nothing new is submitted"
	)
	type step struct {
		what            string
		advance         time.Duration
		reason, code    string // the display and the record's code after the pass
		phase, recState string
	}
	for _, tc := range []struct {
		name   string
		script []fakeslurm.SubmitStep
		steps  []step
	}{
		{"a full slot, then another policy reason, then a full slot again", []fakeslurm.SubmitStep{
			fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"),
			fakeslurm.RejectPolicy("AssocMaxSubmitJobLimit"), fakeslurm.Accept(),
		}, []step{
			{"Create of attempt 1", 0, limit, limit, "", "Rejected"},
			{"backoff", 5 * time.Second, limit, limit, "Submitting", "Rejected"},
			{"backoff", 5 * time.Second, limit, limit, "Submitting", "Rejected"},
			{"Create of attempt 2, another reason", time.Minute, wallLine, "", "Submitting", "Rejected"},
			{"backoff", 5 * time.Second, wallLine, "", "Submitting", "Rejected"},
			{"backoff", 5 * time.Second, wallLine, "", "Submitting", "Rejected"},
			{"Create of attempt 3, a full slot again", time.Minute, limit, limit, "Submitting", "Rejected"},
			{"backoff", 5 * time.Second, limit, limit, "Submitting", "Rejected"},
			{"Create of attempt 4, accepted", time.Minute, "", "", "Submitting", "Done"},
		}},
		{"a full slot, then an unknown outcome", []fakeslurm.SubmitStep{
			fakeslurm.RejectPolicy("MaxSubmitJobsPerAccount"), fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.Transient(false),
		}, []step{
			{"Create of attempt 1", 0, limit, limit, "", "Rejected"},
			{"backoff", 5 * time.Second, limit, limit, "Submitting", "Rejected"},
			{"Create of attempt 2, a full slot again", time.Minute, limit, limit, "Submitting", "Rejected"},
			{"backoff", 5 * time.Second, limit, limit, "Submitting", "Rejected"},
			{"Create of attempt 3, a transient failure", time.Minute, unknownMsg, "", "SubmissionUnknown", "Unknown"},
			{"Observe, waiting for the tag", 5 * time.Second, unknownMsg, "", "SubmissionUnknown", "Unknown"},
			{"Observe, waiting for the tag", 5 * time.Second, unknownMsg, "", "SubmissionUnknown", "Unknown"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := nowOnlySlurm(clock)
			sl.Script(tc.script...)
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...))
			e.newAlloc("alloc-a", 1)
			for i, st := range tc.steps {
				clock.Advance(st.advance)
				e.run("alloc-a", 1)
				a := e.get("alloc-a")
				got := snapshot(a)
				if got.Reason != st.reason || got.RecordCode != st.code || got.RecordState != st.recState {
					t.Fatalf("step %d (%s): reason %q, record code %q, record state %q; want %q, %q, %q",
						i+1, st.what, got.Reason, got.RecordCode, got.RecordState, st.reason, st.code, st.recState)
				}
				// The reason is the limit's exactly when the record says so, in
				// the record and in its mirror, and in nothing else.
				if (got.Reason == limit) != (got.RecordCode == limit) {
					t.Errorf("step %d (%s): reason %q and record code %q disagree", i+1, st.what, got.Reason, got.RecordCode)
				}
				if st.phase != "" && got.Phase != st.phase {
					t.Errorf("step %d (%s): phase %q, want %q", i+1, st.what, got.Phase, st.phase)
				}
				if m := a.Status.AtProvider.Submission; m != nil && m.Code != got.RecordCode {
					t.Errorf("step %d (%s): the submission mirror has code %q, the record %q", i+1, st.what, m.Code, got.RecordCode)
				}
			}
		})
	}
}

// The event of a full submit limit keeps today's reason, SubmissionRejected
// (SubmitLimitReached is a condition reason only, so HBEP-8's event reasons are
// unchanged), and is a repeating one: emitted when the refusal starts, then at
// most once an hour while it lasts, never on every attempt (HBEP-8, Repeating
// conditions); its note says the submit limit was reached and carries the
// reason line. The gate is in memory, so a restart emits once more, which is
// "when it starts" for that process. An ordinary Rejected attempt keeps its
// event on every attempt.
func TestSubmitLimitEventIsEmittedWhenItStartsThenAtMostOnceAnHour(t *testing.T) {
	count := func(r *recorder, reason string) int {
		n := 0
		for _, x := range r.reasons() {
			if x == reason {
				n++
			}
		}
		return n
	}
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	rec := &recorder{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...), withRecorder(rec))
	sl.OutsideJob("user-a", "another-project-job", gpuPart, "now", 1)
	e.newAlloc("alloc-a", 1)

	// Several backoffs: the refusal repeats, and one event is emitted.
	for i := 0; i < 5; i++ {
		e.run("alloc-a", 2)
		clock.Advance(61 * time.Second)
	}
	if n, m := sl.Count("sbatch"), count(rec, "SubmissionRejected"); n != 5 || m != 1 {
		t.Fatalf("%d attempts and %d SubmissionRejected events over 5 backoffs, want 5 and 1", n, m)
	}
	var note string
	rec.mu.Lock()
	for _, ev := range rec.events {
		if string(ev.Reason) == "SubmissionRejected" {
			note = ev.Message
		}
	}
	rec.mu.Unlock()
	if note != "attempt 1: submit limit reached: sbatch: error: QOSMaxSubmitJobPerUserLimit" {
		t.Errorf("the event's note is %q, want the submit limit and the reason line", note)
	}

	// 70 attempts, one a backoff apart: 70 minutes.
	for i := 5; i < 70; i++ {
		e.run("alloc-a", 2)
		clock.Advance(61 * time.Second)
	}
	attempts := sl.Count("sbatch")
	if attempts < 60 {
		t.Fatalf("%d attempts, want about 70", attempts)
	}
	// Attempts at minutes 0..69: events at 0 and at the first attempt after an hour.
	if n := count(rec, "SubmissionRejected"); n != 2 {
		t.Errorf("%d SubmissionRejected events over %d attempts in 70 minutes, want 2 (at the start, and once after an hour)", n, attempts)
	}
	if n := count(rec, "SubmitLimitReached"); n != 0 {
		t.Errorf("%d events with the reason SubmitLimitReached, want 0: it is a condition reason only", n)
	}
	// crossplane-runtime's own events (reconciler.go:1473, reasonCreated, and
	// :1388, reasonCannotCreate, in v2.4.3): a Create that returns no error
	// would report a creation, once per attempt, although nothing was created;
	// createdFilter drops that event for a recorded full slot. An error would
	// emit a Warning CannotCreateExternalResource per attempt instead; there is
	// none.
	if n, m := count(rec, "CreatedExternalResource"), count(rec, "CannotCreateExternalResource"); n != 0 || m != 0 {
		t.Errorf("crossplane's events over %d refused attempts: %d CreatedExternalResource and %d CannotCreateExternalResource, want 0 and 0", attempts, n, m)
	}
	t.Logf("%d attempts in 70 minutes: %d SubmissionRejected events (the gate), %d CreatedExternalResource events", attempts, count(rec, "SubmissionRejected"), count(rec, "CreatedExternalResource"))

	// The slot clears: the next attempt is accepted, and crossplane's event
	// for that real creation passes through the filter.
	if err := sl.End(1001, fakeslurm.Completed); err != nil { // the outside job, the first job of the cluster
		t.Fatal(err)
	}
	e.run("alloc-a", 3)
	clock.Advance(61 * time.Second)
	e.run("alloc-a", 3)
	if r, _ := readRecord(e.get("alloc-a")); r.jobID == "" {
		t.Fatalf("record %+v: the attempt after the slot cleared was not accepted", r.sub)
	}
	if n := count(rec, "CreatedExternalResource"); n != 1 {
		t.Errorf("%d CreatedExternalResource events after the accepted submission, want 1: the real creation", n)
	}
}

// Plain Rejected attempts keep their event on every attempt, as before.
func TestAPlainRejectionKeepsItsEventOnEveryAttempt(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	rec := &recorder{}
	sl.Script(fakeslurm.Reject("Invalid account or account/partition combination specified"), fakeslurm.Reject("Invalid account or account/partition combination specified"),
		fakeslurm.Reject("Invalid account or account/partition combination specified"))
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...), withRecorder(rec))
	e.newAlloc("alloc-a", 1)
	for i := 0; i < 3; i++ {
		e.run("alloc-a", 2)
		clock.Advance(2 * time.Minute)
	}
	n := 0
	for _, x := range rec.reasons() {
		if x == "SubmissionRejected" {
			n++
		}
	}
	if n != 3 {
		t.Errorf("%d SubmissionRejected events over 3 attempts, want 3", n)
	}
	m := 0
	for _, x := range rec.reasons() {
		if x == "CannotCreateExternalResource" {
			m++
		}
	}
	if m != 3 {
		t.Errorf("%d CannotCreateExternalResource events (crossplane's) over 3 refused attempts, want 3", m)
	}
}

// Create returns no error for a recorded limit refusal, so crossplane-runtime
// marks the creation succeeded (the external-create-succeeded annotation, with
// the pending one before it). That bookkeeping must neither leave the creation
// incomplete, which would stop the reconciler, nor skip the next attempt: with
// crossplane's default creation grace period of 30 s, which counts real time,
// the reconciler waits that long after a succeeded Create before it trusts an
// Observe that says the job is missing, and the Rejected backoff is a minute.
// So the test keeps the default grace, shows the annotations, sets the
// succeeded one an hour ahead (inside the period, whatever the wall clock
// does) and then an hour back, and the next attempt is made.
func TestALimitRefusalLeavesNoCreationIncompleteAndTheNextAttemptIsMade(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"))
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...),
		func(e *realEnv) { e.opts.CreationGracePeriod = 0 }) // 0 keeps crossplane's default, 30 s
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 1)

	a := e.get("alloc-a")
	if meta.ExternalCreateIncomplete(a) {
		t.Fatal("the creation is incomplete after a recorded limit refusal: the reconciler would refuse to go on")
	}
	if meta.GetExternalCreateSucceeded(a).IsZero() {
		t.Fatal("no external-create-succeeded annotation after Create returned no error")
	}
	if rec, _ := readRecord(a); rec.sub == nil || rec.sub.Code != v1alpha1.ReasonSubmitLimitReached || rec.sub.Attempt != 1 {
		t.Fatalf("record %+v, want attempt 1 refused for the limit", rec.sub)
	}

	// Past the Rejected backoff, but inside crossplane's grace period: the
	// annotation is set an hour ahead, so that time.Since is negative whatever
	// the wall clock does, and no timing decides the test. The reconciler waits
	// the period out, and makes no attempt.
	setSucceeded := func(at time.Time) {
		t.Helper()
		a := e.get("alloc-a")
		meta.AddAnnotations(a, map[string]string{meta.AnnotationKeyExternalCreateSucceeded: at.Format(time.RFC3339)})
		if err := e.c.Update(e.ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	setSucceeded(time.Now().Add(time.Hour))
	clock.Advance(2 * time.Minute)
	e.run("alloc-a", 2)
	if n := sl.Count("sbatch"); n != 1 {
		t.Fatalf("%d sbatch calls inside crossplane's grace period, want 1", n)
	}
	// The grace period has passed (an hour of real time ago): the next attempt is made.
	setSucceeded(time.Now().Add(-time.Hour))
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 2 {
		t.Fatalf("%d sbatch calls after the grace period, want 2: a recorded limit refusal blocked the next attempt", n)
	}
	if rec, _ := readRecord(e.get("alloc-a")); rec.jobID == "" || rec.sub.Attempt != 2 {
		t.Errorf("record %+v, job %q; want attempt 2 accepted", rec.sub, rec.jobID)
	}
}

// The literal note the tests expect for a group limit is the contract's.
func TestGroupNoteAgreesWithTheContract(t *testing.T) {
	if want := " (" + provider.GroupLimitNote + ")"; groupNote != want {
		t.Errorf("groupNote = %q, want %q", groupNote, want)
	}
}
