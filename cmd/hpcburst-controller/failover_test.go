package main

// HBEP-11's Lease test, second half (Test plan, "Lease"): "Kill the first
// mid-submit: the second takes lifecycle, then provider-<name>, and finds the
// job by its tag. Whatever it then submits, at most one job with the tag has
// its hold lifted, and any other is cancelled (S4)." The first half, that a
// copy which does not lead starts no Provider manager, is
// TestControllerBinaryProvidersFollowTheLifecycleLeader.
//
// Two real controller processes (child_test.go) run against one envtest API
// server and one fake Slurm. The fake keeps its state in this process, so
// each child reaches it through its own runnerServer (slurmwire_test.go). The
// two copies share ONE state directory, hence one kubeconfig, credentials
// directory and agent builds, and differ in port base and health address: the
// stale second copy of the same launcher that HBEP-11 guards against.
//
// "Mid-submit" is an observable event, not a time window: the server runs the
// first copy's sbatch against the fake, so the job is registered, signals the
// test, and withholds the reply. The test then SIGKILLs the first copy. A
// clean stop (SIGTERM or a cancelled context) would release both Leases and
// never exercise the stale-leader path, so it is not used. After a SIGKILL
// nothing is released: the second copy waits for each Lease to expire.
//
// What is counted and where from:
//   - sbatch calls with the tag come from the fake's call log
//     (fakeslurm.Platform.Calls), never from a return value;
//   - "lifted" is a "scontrol release" in that log, "cancelled" is the fake's
//     job state;
//   - the expected job is the one the first copy's sbatch printed, read from
//     the fake's own reply in the server, not from anything the provider wrote.
//
// TestControllerProcessesFailOverBeforeSubmit is the extra case: the first
// copy dies before its sbatch reaches the fake. It shows that the second
// submits once, not that it resolves by tag, which is the first test's point.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// failoverWait bounds each wait. A killed copy's Leases expire after
// controller-runtime's 15 s lease duration, once the second copy has first
// seen them: lifecycle first, and provider-<name> only after the second copy
// leads lifecycle, so the takeover takes at least 30 s.
const failoverWait = 150 * time.Second

// failoverProfile is the hold-mode example profile with an in-flight window of
// 90 s, not batchProfile's 5 m: it must exceed provider.SubmitTimeout (60 s),
// and a short window lets a lookup by tag that failed show as a second sbatch
// in bounded time instead of after five minutes. A copy that resolves by tag
// never reads it.
func failoverProfile(name string) *v1alpha1.PlatformProfile {
	p := batchProfile(name)
	w := v1alpha1.Duration("90s")
	p.Spec.InflightWindow = &w
	return p
}

// sbatchesFor counts the sbatch calls in the fake's log that name tag.
func sbatchesFor(sl *fakeslurm.Platform, tag string) int {
	n := 0
	for _, c := range sl.Calls() {
		if c.Words[0] != "sbatch" {
			continue
		}
		for _, w := range c.Words {
			if w == "--job-name="+tag {
				n++
			}
		}
	}
	return n
}

// jobsFor returns the fake's jobs named tag.
func jobsFor(sl *fakeslurm.Platform, tag string) []fakeslurm.Job {
	var out []fakeslurm.Job
	for _, j := range sl.Jobs() {
		if j.Name == tag {
			out = append(out, j)
		}
	}
	return out
}

// releasesBy counts the "scontrol release" calls in the fake's log per job ID.
func releasesBy(sl *fakeslurm.Platform) map[int]int {
	out := map[int]int{}
	for _, c := range sl.Calls() {
		if len(c.Words) == 3 && c.Words[0] == "scontrol" && c.Words[1] == "release" {
			if id, err := strconv.Atoi(c.Words[2]); err == nil {
				out[id]++
			}
		}
	}
	return out
}

// sbatchJobID is the job ID on the first line of an sbatch --parsable reply.
func sbatchJobID(res slurm.Result) string {
	first, _, _ := strings.Cut(string(res.Stdout), "\n")
	id, _, _ := strings.Cut(first, ";")
	return strings.TrimSpace(id)
}

// failoverRig is two controller processes on one control plane.
type failoverRig struct {
	t         *testing.T
	api       *apiServer
	sl        *fakeslurm.Platform
	provider  *v1alpha1.Provider
	provLease string
	fa, fb    flags
	a, b      *child
	al        *v1alpha1.Allocation
	tag       string
	// arrived receives the job ID that the first copy's first sbatch got from
	// the fake, once it has; "" when the sbatch was held before it ran.
	arrived chan string
	// lifecycleA and provA are the first copy's Lease holders; killedAt is
	// when it was killed.
	lifecycleA, provA string
	killedAt          time.Time
}

// newFailoverRig starts the control plane, the fake Slurm, the first copy
// (waits until it leads lifecycle and holds provider-<name>), the second copy
// (waits until it runs and does not lead), and creates the Allocation with its
// credential. beforeRun holds the first copy's first sbatch before it runs;
// otherwise it runs, and its reply is withheld.
func newFailoverRig(t *testing.T, beforeRun bool) *failoverRig {
	t.Helper()
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	profile := failoverProfile("failover-slurm")
	prov := batchProvider("failover-batch", profile.Name, partX86, sshCredentials(t))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "failover-allocs"}}
	for _, o := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}, profile, prov, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	r := &failoverRig{t: t, api: api, provider: prov, provLease: v1alpha1.ProviderLeaseName(prov.Name), arrived: make(chan string, 1)}
	var session *fakeslurm.Session
	r.sl, session = newFakeSlurm()

	// The first copy's sbatch, the first only, is held at the server.
	var once sync.Once
	first := func(cmd slurm.Command) bool {
		won := false
		if strings.HasPrefix(cmd.Line, "sbatch ") {
			once.Do(func() { won = true })
		}
		return won
	}
	hooks := runnerHooks{}
	if beforeRun {
		hooks.before = func(cmd slurm.Command) bool {
			if first(cmd) {
				r.arrived <- ""
				return true
			}
			return false
		}
	} else {
		hooks.after = func(cmd slurm.Command, res slurm.Result, _ error) bool {
			if first(cmd) {
				r.arrived <- sbatchJobID(res)
				return true
			}
			return false
		}
	}
	srvA := serveRunner(t, session, hooks)
	srvB := serveRunner(t, session, runnerHooks{})

	var digest string
	r.fa, _, digest = prepareState(t, api, "direct", exampleFront)
	r.a = startChild(t, "A", r.fa, srvA.Addr())
	waitFor(t, "the first copy leads lifecycle and holds "+r.provLease, failoverWait, func() bool {
		return held(ctx, c, v1alpha1.LeaseLifecycle) && held(ctx, c, r.provLease)
	}, r.a)
	r.lifecycleA, _ = holder(ctx, c, v1alpha1.LeaseLifecycle)
	r.provA, _ = holder(ctx, c, r.provLease)

	r.fb = secondCopyFlags(t, r.fa)
	r.b = startChild(t, "B", r.fb, srvB.Addr())
	waitFor(t, "the second copy's health endpoint answers and its agent API started", failoverWait, func() bool {
		return healthy(r.fb.healthAddr) && strings.Contains(r.b.log(), componentLine("component starting", r.fb.host.PortBase, "agent-api"))
	}, r.a, r.b)

	r.al = batchAllocation("alloc-failover", ns.Name, prov.Name, partX86, digest)
	if err := c.Create(ctx, r.al); err != nil {
		t.Fatal(err)
	}
	writeCredential(t, r.fa.host, r.al) // the shared credentials directory
	r.tag = v1alpha1.JobTag(r.al.Namespace, r.al.Name, r.al.UID)
	return r
}

// get re-reads the Allocation from the API server.
func (r *failoverRig) get() *v1alpha1.Allocation {
	r.t.Helper()
	al := &v1alpha1.Allocation{}
	if err := r.api.client.Get(context.Background(), client.ObjectKeyFromObject(r.al), al); err != nil {
		r.t.Fatal(err)
	}
	return al
}

// awaitMidSubmit waits for the first copy's sbatch to reach the server and
// returns the job ID it got ("" when it was held before it ran).
func (r *failoverRig) awaitMidSubmit() string {
	r.t.Helper()
	select {
	case id := <-r.arrived:
		return id
	case <-r.a.exited:
		r.t.Fatalf("the first copy exited (%v) before its sbatch arrived; the end of its stderr:\n%s", r.a.waitErr, r.a.tail(30))
	case <-r.b.exited:
		r.t.Fatalf("the second copy exited (%v) before the first copy's sbatch arrived; the end of its stderr:\n%s", r.b.waitErr, r.b.tail(30))
	case <-time.After(failoverWait):
		r.t.Fatalf("the first copy's sbatch did not reach the fake Slurm within %s", failoverWait)
	}
	return ""
}

// checkMidSubmit checks the state at the moment the first copy is to be
// killed: the job is registered and held (or, when beforeRun, there is none), the
// attempt is claimed and InFlight with no job recorded, the second copy has
// started no Provider supervisor, and the first copy still holds both Leases.
func (r *failoverRig) checkMidSubmit(aJob string, beforeRun bool) {
	t := r.t
	t.Helper()
	jobs := jobsFor(r.sl, r.tag)
	if beforeRun {
		if aJob != "" || len(jobs) != 0 || sbatchesFor(r.sl, r.tag) != 0 {
			t.Errorf("the first copy's sbatch was to be held before it ran, but the fake has %d jobs and %d sbatch calls with the tag", len(jobs), sbatchesFor(r.sl, r.tag))
		}
	} else if len(jobs) != 1 || aJob == "" || strconv.Itoa(jobs[0].ID) != aJob || !jobs[0].Held || sbatchesFor(r.sl, r.tag) != 1 {
		t.Errorf("at the kill the fake has jobs %+v and %d sbatch calls with the tag; want one held job, ID %q, from one sbatch", jobs, sbatchesFor(r.sl, r.tag), aJob)
	}
	al := r.get()
	var sub v1alpha1.SubmissionRecord
	if err := json.Unmarshal([]byte(al.Annotations[v1alpha1.AnnotationSubmission]), &sub); err != nil || sub.State != v1alpha1.SubmissionInFlight || sub.Attempt != 1 {
		t.Errorf("at the kill the submission record is %q (%v), want attempt 1 InFlight", al.Annotations[v1alpha1.AnnotationSubmission], err)
	}
	if id := al.Annotations[v1alpha1.AnnotationJobID]; id != "" {
		t.Errorf("at the kill job %s is already recorded, so the kill is not mid-submit", id)
	}
	for _, name := range []string{"provider-supervisor-batch", "provider-supervisor-managed"} {
		if strings.Contains(r.b.log(), componentLine("component starting", r.fb.host.PortBase, name)) {
			t.Errorf("the second copy started %s while the first copy leads lifecycle", name)
		}
	}
	ctx := context.Background()
	if h, _ := holder(ctx, r.api.client, v1alpha1.LeaseLifecycle); h != r.lifecycleA {
		t.Errorf("lifecycle is held by %q at the kill, not the first copy's %q", h, r.lifecycleA)
	}
	if h, _ := holder(ctx, r.api.client, r.provLease); h != r.provA {
		t.Errorf("%s is held by %q at the kill, not the first copy's %q", r.provLease, h, r.provA)
	}
}

// killFirst kills the first copy with SIGKILL and checks that it is dead:
// reaped, ended by that signal, its PID free.
func (r *failoverRig) killFirst() {
	t := r.t
	t.Helper()
	if err := r.a.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL to the first copy: %v", err)
	}
	r.killedAt = time.Now()
	ws := r.a.kill(t)
	if !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Errorf("the first copy's wait status is %v, want ended by SIGKILL", ws)
	}
	if !r.a.gone() {
		t.Error("the first copy's PID is still there after it was reaped")
	}
	// The first copy is dead and cannot write again: what it recorded is final. A job it had
	// recorded in the gap between checkMidSubmit's read and the kill would let the second copy
	// skip the lookup by tag that the test is about, so the Allocation is read again now.
	if id := r.get().Annotations[v1alpha1.AnnotationJobID]; id != "" {
		t.Errorf("after the first copy was killed and reaped job %s is recorded on the Allocation, so the second copy has no lookup by tag to make", id)
	}
}

// checkTakeover waits for the second copy to take lifecycle and then
// provider-<name> from the dead first copy.
func (r *failoverRig) checkTakeover() {
	t := r.t
	t.Helper()
	ctx, c := context.Background(), r.api.client
	waitFor(t, "the second copy takes lifecycle and "+r.provLease, failoverWait, func() bool {
		l, _ := holder(ctx, c, v1alpha1.LeaseLifecycle)
		p, _ := holder(ctx, c, r.provLease)
		return l != "" && l != r.lifecycleA && p != "" && p != r.provA
	}, r.b)
	_, lifecycleAt := holder(ctx, c, v1alpha1.LeaseLifecycle)
	_, provAt := holder(ctx, c, r.provLease)
	if provAt.Before(lifecycleAt) {
		t.Errorf("the second copy took %s at %s, before lifecycle at %s", r.provLease, provAt, lifecycleAt)
	}
	t.Logf("the second copy took lifecycle %s and %s %s after the kill",
		lifecycleAt.Sub(r.killedAt).Round(time.Second), r.provLease, provAt.Sub(r.killedAt).Round(time.Second))
	for _, name := range []string{"provider-supervisor-batch", "provider-supervisor-managed"} {
		if !strings.Contains(r.b.log(), componentLine("component starting", r.fb.host.PortBase, name)) {
			t.Errorf("the second copy's log has no start of %s", name)
		}
	}
}

// checkOneJob waits for the second copy to record a job and lift its hold, then
// checks HBEP-11's outcome. wantJob is the job the first copy's sbatch
// registered, "" when none was (the first copy died before its sbatch ran).
func (r *failoverRig) checkOneJob(wantJob string) {
	t := r.t
	t.Helper()
	// A second sbatch with the tag ends the first wait, so a failed lookup by
	// tag reports as a duplicate submission and not as a time-out.
	waitFor(t, "a job is recorded on the Allocation, or a second sbatch carries its tag", failoverWait, func() bool {
		return r.get().Annotations[v1alpha1.AnnotationJobID] != "" || sbatchesFor(r.sl, r.tag) > 1
	}, r.b)
	// The hold-lifted marker is written after the lift, and settle cancels the
	// held extras before it lifts.
	waitFor(t, "the Allocation's hold-lifted marker is written", failoverWait, func() bool {
		return r.get().Annotations[v1alpha1.AnnotationHoldLifted] != ""
	}, r.b)
	al := r.get()
	recorded, lifted := al.Annotations[v1alpha1.AnnotationJobID], al.Annotations[v1alpha1.AnnotationHoldLifted]

	// Exactly one sbatch with the tag over the whole test, from the call log.
	if n := sbatchesFor(r.sl, r.tag); n != 1 {
		t.Errorf("%d sbatch calls with the tag over the test, want exactly 1 (the first copy's, or the second's when the first died before its own)", n)
	}
	if n := r.sl.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls in all, want 1", n)
	}
	jobs := jobsFor(r.sl, r.tag)
	if wantJob != "" && recorded != wantJob {
		t.Errorf("the Allocation records job %q, want %s, the job the first copy's sbatch registered", recorded, wantJob)
	}
	if wantJob == "" && len(jobs) == 1 && recorded != strconv.Itoa(jobs[0].ID) {
		t.Errorf("the Allocation records job %q, want %d, the one job with the tag", recorded, jobs[0].ID)
	}
	if lifted != recorded {
		t.Errorf("the hold-lifted marker names job %q, the record names %q", lifted, recorded)
	}

	// HBEP-11: at most one job with the tag has its hold lifted, and any other
	// is cancelled.
	releases := releasesBy(r.sl)
	var states []string
	for _, j := range jobs {
		states = append(states, fmt.Sprintf("%d:%s,held=%t", j.ID, j.State, j.Held))
	}
	t.Logf("jobs with the tag: %v; recorded job %s; scontrol release calls per job %v", states, recorded, releases)
	var liftedJobs []int
	for _, j := range jobs {
		if releases[j.ID] > 0 {
			liftedJobs = append(liftedJobs, j.ID)
		}
	}
	if len(liftedJobs) > 1 {
		t.Errorf("the holds of jobs %v, all with the tag, were lifted; HBEP-11 allows at most one", liftedJobs)
	}
	for _, j := range jobs {
		switch {
		case strconv.Itoa(j.ID) == recorded:
			if j.Held || j.State == fakeslurm.Cancelled {
				t.Errorf("the recorded job %d is held=%t in state %s, want its hold lifted and the job live", j.ID, j.Held, j.State)
			}
			// HBEP-11: "The hold is never lifted again". The recorded job's hold is lifted
			// once: none would leave it held, a second would lift it again.
			if releases[j.ID] != 1 {
				t.Errorf("the log has %d scontrol release calls of the recorded job %d, want exactly 1", releases[j.ID], j.ID)
			}
		case j.State != fakeslurm.Cancelled:
			t.Errorf("job %d carries the tag, is not the recorded job %s and is %s (held=%t), want it cancelled", j.ID, recorded, j.State, j.Held)
		}
	}
}

// finish stops both copies and checks that no controller process is left.
func (r *failoverRig) finish() {
	t := r.t
	t.Helper()
	if ws := r.b.kill(t); !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Errorf("the second copy's wait status is %v, want ended by SIGKILL", ws)
	}
	if !r.a.gone() || !r.b.gone() {
		t.Error("a controller process's PID is still there after it was reaped")
	}
	noProcessesNamed(t, r.fa.host.StateDir)
}

func failover(t *testing.T, beforeRun bool) {
	r := newFailoverRig(t, beforeRun)
	aJob := r.awaitMidSubmit()
	r.checkMidSubmit(aJob, beforeRun)
	r.killFirst()
	r.checkTakeover()
	r.checkOneJob(aJob)
	r.finish()
}

func TestControllerProcessesFailOverMidSubmit(t *testing.T) { failover(t, false) }

func TestControllerProcessesFailOverBeforeSubmit(t *testing.T) { failover(t, true) }
