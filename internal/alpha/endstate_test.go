package alpha

import (
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The wait for the run's jobs to end names what it saw when it gives up (issue #503). Every expectation is a literal typed from
// row A's live failure: one job still running, its Allocation kept under a 30 minute idle limit, its Instance ended.
func TestEndWaitStateNamesWhatTheWaitSawLast(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 50, 0, 0, time.UTC)
	idle := now.Add(-7 * time.Minute)
	ended := now.Add(-time.Minute)
	limit := v1alpha1.Duration("30m")
	until := metav1.NewTime(now.Add(time.Hour))
	alloc := func(name string, created time.Time, mutate func(*v1alpha1.Allocation)) v1alpha1.Allocation {
		a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)}}
		a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
		if mutate != nil {
			mutate(&a)
		}
		return a
	}
	inst := func(name string, created time.Time, phase v1alpha1.InstancePhase, reason string) v1alpha1.Instance {
		in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)}}
		in.Status.Phase, in.Status.EndReason = phase, reason
		return in
	}
	idleAnnotated := func(a *v1alpha1.Allocation) { v1alpha1.SetIdleSince(a, idle) }
	t0 := now.Add(-30 * time.Minute)
	for _, c := range []struct {
		name   string
		jobs   []provider.Job
		err    error
		allocs []v1alpha1.Allocation
		insts  []v1alpha1.Instance
		want   string
	}{
		{
			name: "the live run: a running job, its Allocation idle under a 30 minute limit, its Instance drained",
			jobs: []provider.Job{{ID: "1234567", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning}},
			allocs: []v1alpha1.Allocation{alloc("alloc-x", t0, func(a *v1alpha1.Allocation) {
				idleAnnotated(a)
				a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &limit}
			})},
			insts: []v1alpha1.Instance{inst("inst-y", t0, v1alpha1.InstanceEnded, v1alpha1.ReasonDrained)},
			want: "1 job in the cluster's record, 0 ended: job 1: state RUNNING, phase Running, reason none given, ended no; " +
				"1 Allocation: Allocation 1: phase Running, deleting no, idle for 7m0s, retention ReleaseWhenIdle (idle limit 30m0s); " +
				"1 Instance: Instance 1: phase ended (Drained), deleting no",
		},
		{
			name: "a job ID in the scheduler's reason is replaced, and two jobs are counted, one ended",
			jobs: []provider.Job{
				{ID: "1234567", State: []string{"PENDING"}, Phase: v1alpha1.AllocationPending, Reason: "launch failed for job 1234567 on a node"},
				{ID: "9081726", State: []string{"COMPLETED", "COMPLETING"}, Phase: v1alpha1.AllocationEnded, EndTime: &ended},
			},
			want: "2 jobs in the cluster's record, 1 ended: job 1: state PENDING, phase Pending, reason launch failed for job [job] on a node, ended no; " +
				"job 2: state COMPLETED+COMPLETING, phase Ended, reason none given, ended yes; no Allocation; no Instance",
		},
		{
			name: "no job at all",
			want: "the cluster's record lists no job for the run's tags; no Allocation; no Instance",
		},
		{
			name: "the ledger's read failed: its text, which can hold a host, is not the state",
			err:  errors.New("the jobs of a run's tag could not be read: ssh login.example.org: connection refused"),
			want: "the cluster's record could not be read (the ledger's read failed); no Allocation; no Instance",
		},
		{
			name: "the run made no batch client",
			err:  ErrNoLedger,
			want: "the run made no batch client, so the cluster's record was never read; no Allocation; no Instance",
		},
		{
			name: "an Allocation being deleted with no idle anchor, an unset policy (Keep), and a Keep until a date",
			jobs: []provider.Job{{ID: "1", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning}},
			allocs: []v1alpha1.Allocation{
				alloc("b", t0.Add(time.Minute), func(a *v1alpha1.Allocation) {
					a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &until}
				}),
				alloc("a", t0, func(a *v1alpha1.Allocation) {
					a.DeletionTimestamp = &metav1.Time{Time: now}
					idleAnnotated(a)
				}),
			},
			want: "1 job in the cluster's record, 0 ended: job 1: state RUNNING, phase Running, reason none given, ended no; " +
				// Ordered by creation: "a" is older than "b", whatever the order given.
				"2 Allocations: Allocation 1: phase Running, deleting yes, idle for 7m0s, retention Keep; " +
				"Allocation 2: phase Running, deleting no, not idle (no idle-since), retention Keep (until a date); no Instance",
		},
		{
			name: "a ReleaseWhenIdle with no idle limit, and an Instance still draining",
			jobs: []provider.Job{{ID: "1", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning}},
			allocs: []v1alpha1.Allocation{alloc("a", t0, func(a *v1alpha1.Allocation) {
				a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle}
			})},
			insts: []v1alpha1.Instance{inst("i", t0, v1alpha1.InstanceDraining, "")},
			want: "1 job in the cluster's record, 0 ended: job 1: state RUNNING, phase Running, reason none given, ended no; " +
				"1 Allocation: Allocation 1: phase Running, deleting no, not idle (no idle-since), retention ReleaseWhenIdle (no idle limit); " +
				"1 Instance: Instance 1: phase draining, deleting no",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := EndWaitState(c.jobs, c.err, c.allocs, c.insts, nil, now)
			if got != c.want {
				t.Errorf("EndWaitState =\n%s\nwant\n%s", got, c.want)
			}
		})
	}
}

// The state is for a post, so no name, ID or host of the objects it describes is in it, whatever they are called.
func TestEndWaitStateHoldsNoNameIDOrHost(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 50, 0, 0, time.UTC)
	a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "hb-secret-name-0123456789", Namespace: "private-namespace"}}
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "instance-secret-name", Namespace: "private-namespace"}}
	jobs := []provider.Job{{ID: "7654321", Name: "hb-secret-name-0123456789", Partition: "plgrid-secret", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning,
		Reason: "node secret-node-07 down, job 7654321"}}
	got := EndWaitState(jobs, nil, []v1alpha1.Allocation{a}, []v1alpha1.Instance{in}, nil, now)
	for _, leak := range []string{"hb-secret-name", "0123456789", "private-namespace", "instance-secret-name", "7654321", "plgrid-secret"} {
		if strings.Contains(got, leak) {
			t.Errorf("the state holds %q:\n%s", leak, got)
		}
	}
	// The reason is the scheduler's words, with the job's ID replaced; a node name in it stays, as the reason is the scheduler's own.
	if !strings.Contains(got, "reason node secret-node-07 down, job [job]") {
		t.Errorf("the state does not hold the reason with its ID replaced:\n%s", got)
	}
}

// A state with no Allocation must not be the state of a read that failed: the failure says so, and neither its text nor the objects are
// in the line.
func TestEndWaitStateSaysWhenTheAPIRead(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 50, 0, 0, time.UTC)
	jobs := []provider.Job{{ID: "1", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning}}
	got := EndWaitState(jobs, nil, nil, nil, errors.New("Get https://127.0.0.1:6443/apis/x: connection refused"), now)
	want := "1 job in the cluster's record, 0 ended: job 1: state RUNNING, phase Running, reason none given, ended no; " +
		"the Allocations and Instances could not be read from the API"
	if got != want {
		t.Errorf("EndWaitState =\n%s\nwant\n%s", got, want)
	}
}
