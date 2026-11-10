package scenario_test

// Issue #321: rehearsal part 3's ReplacementLate absence check was
// inconclusive when the one-slot class's start-to-ready time was not yet
// recorded at the first read (CI run 42809336252: "at the first read map[]").
// These tests take the harness's own path, on an API (controller-runtime's
// fake client, with the status subresource) instead of the stack: the
// deployments' status read by startToReadySnapshot, the snapshot baseline
// takes just after the first read, and by armFirstRead's wait; condition 1's
// change from lateChange, as checkForbidden counts it; the verdict from
// eventcount.Absence. The controller's own recording of the time
// (internal/lifecycle, status.go: writeStatus) is tested in internal/lifecycle
// (ready_time_test.go, review_drain_test.go); here a stand-in writes the entry
// through the status subresource, in the form writeStatus writes it, and only
// after the wait has read the status without it.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// firstReadArch is the fixture profile's CPU architecture, as on CI's runner.
const firstReadArch = "x86_64"

// firstReadClass is the class both of part 3's partitions describe
// (stack.BatchProfile), as CI's log names it: {example-gpu x86_64}.
var firstReadClass = v1alpha1.AcceleratorClass{GPUModel: "example-gpu", CPUArch: firstReadArch}

// queuedStartToReady is the queued class's start-to-ready time at CI's first
// read.
const queuedStartToReady = 14 * time.Second

// firstReadFixture is part 3's two served deployments as CI's failing run had
// them at the first read: each primary ready on its own Provider and
// partition, both drain starts at ds, the queued deployment's class measured,
// the one-slot deployment's not yet recorded.
func firstReadFixture(t *testing.T, ds time.Time) (*part3, client.Client) {
	t.Helper()
	creds := filepath.Join(t.TempDir(), "creds")
	objs := []client.Object{part3Profile(firstReadArch), part3Provider(queuedDep, creds), part3Provider(oneSlotDep, creds)}
	p3 := &part3{rig: &rig{t: t}}
	for _, d := range []dep{queuedDep, oneSlotDep} {
		in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: d.Name + "-primary", Namespace: stack.Namespace,
			Labels: map[string]string{v1alpha1.LabelProvider: d.Provider}}}
		objs = append(objs, stack.ServedDeploymentAs(d.Served, rehearsalTiming), in)
		p3.deps = append(p3.deps, &depRun{dep: d, primary: in, ds: ds})
	}
	c := clientfake.NewClientBuilder().WithScheme(stack.APIScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.ModelDeployment{}).Build()
	p3.c = c
	if err := recordStartToReady(c, queuedDep.Name, queuedDep.Name+"-primary", firstReadClass, queuedStartToReady); err != nil {
		t.Fatal(err)
	}
	return p3, c
}

// recordStartToReady stands in for the controller's write: it adds the class's
// start-to-ready entry to the deployment's status through the status
// subresource, in writeStatus's form (internal/lifecycle, status.go:
// measuredStartToReady): the class, the duration as Go prints it, the Instance
// it was measured on and its ready time.
func recordStartToReady(c client.Client, md, instance string, class v1alpha1.AcceleratorClass, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), stack.APICallTimeout)
	defer cancel()
	var obj v1alpha1.ModelDeployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: stack.Namespace, Name: md}, &obj); err != nil {
		return fmt.Errorf("get ModelDeployment %s: %w", md, err)
	}
	obj.Status.StartToReady = append(obj.Status.StartToReady, v1alpha1.ClassStartToReady{
		AcceleratorClass: class, Duration: v1alpha1.Duration(d.String()), Instance: instance,
		MeasuredTime: metav1.NewTime(time.Now().Truncate(time.Second)),
	})
	if err := c.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update ModelDeployment %s status: %w", md, err)
	}
	return nil
}

// earliestLate is checkForbidden's condition-1 change for ReplacementLate:
// the earliest of lateChange over the deployments, from the snapshot atFirst
// and each deployment's status now.
func earliestLate(t *testing.T, p3 *part3, atFirst map[string]map[v1alpha1.AcceleratorClass]time.Duration) time.Time {
	t.Helper()
	var change time.Time
	for _, d := range p3.deps {
		var md v1alpha1.ModelDeployment
		if !p3.get(&md, d.Name, stack.Namespace) {
			t.Fatalf("no ModelDeployment %s", d.Name)
		}
		late, why, ok := p3.lateChange(d, atFirst[d.Name], &md)
		if !ok {
			t.Fatal(why)
		}
		if change.IsZero() || late.Before(change) {
			change = late
		}
	}
	return change
}

// judgeLate is ReplacementLate's verdict with the first read answered at
// first and condition 1's change at change. Every other condition holds: one
// serving process with both collectors at both reads, 0 at both, a reconcile
// pass of the deployment that began at the drain start and was seen to end
// between the reads, and quiet other sources.
func judgeLate(first, change, ds time.Time) (eventcount.Verdict, string) {
	collectors := map[string]bool{}
	for _, c := range eventcount.LifecycleCollectors {
		collectors[c] = true
	}
	read := func(at time.Time) eventcount.Read {
		return eventcount.Read{Sent: at.Add(-10 * time.Millisecond), Answered: at, ProcessStart: 1, HasProcessStart: true,
			Collectors: collectors, Count: 0, HasCount: true}
	}
	a := eventcount.Absence{Key: keyReplacementLate, First: read(first), Second: read(ds.Add(time.Minute)), Change: change,
		Collectors: eventcount.LifecycleCollectors, Quiet: "only the reconcile passes raise it",
		Barriers: []eventcount.Barrier{{What: "a reconcile pass", PassStart: ds, Observed: ds.Add(time.Second), Due: ds}}}
	return a.Judge()
}

// The race of #321, and its fix. With no wait, the first read's snapshot has
// no entry for the one-slot class (CI's "map[]"), so condition 1 counts back
// the 30-minute default from the drain start and the check is inconclusive,
// as in CI. With armFirstRead, the first read is armed only after the
// one-slot class's time is recorded: the stand-in records it at the wait's
// third read without it, so a wait that returns before reading the entry
// leaves the snapshot empty and fails here. The check is then conclusive: not
// raised.
func TestFirstReadWaitsForTheStartToReadyTimes(t *testing.T) {
	// CI's run: both drain starts 4m45s after the primaries were seen ready.
	ds := time.Now().Add(4*time.Minute + 45*time.Second)
	p3, c := firstReadFixture(t, ds)

	beforeAt := time.Now()
	before := p3.startToReadySnapshot("the first read with no wait")
	if got := before[oneSlotDep.Name]; len(got) != 0 {
		t.Fatalf("the fixture's one-slot deployment has start-to-ready %v at the first read, want none (CI's map[])", got)
	}
	if got := before[queuedDep.Name][firstReadClass]; got != queuedStartToReady {
		t.Fatalf("the fixture's queued deployment has start-to-ready %v for %v, want %s", before[queuedDep.Name], firstReadClass, queuedStartToReady)
	}

	const recordAt = 3
	var misses []string
	var wrote error
	p3.s2rMiss = func(m []string) {
		misses = append(misses, strings.Join(m, "; "))
		if len(misses) == recordAt {
			wrote = recordStartToReady(c, oneSlotDep.Name, oneSlotDep.Name+"-primary", firstReadClass, queuedStartToReady)
		}
	}
	if err := p3.armFirstRead(); err != nil {
		t.Fatalf("armFirstRead: %v", err)
	}
	if wrote != nil {
		t.Fatal(wrote)
	}
	if p3.onLed == nil {
		t.Fatal("armFirstRead returned nil without arming the first read")
	}
	firstAt := time.Now() // the first read is taken once the restarted controller serves, after armFirstRead
	atFirst := p3.startToReadySnapshot("the first read's start-to-ready times")
	if len(misses) != recordAt {
		t.Errorf("the wait read the status without the one-slot class's time %d times, want %d (recorded at read %d): %q", len(misses), recordAt, recordAt, misses)
	}
	for i, m := range misses {
		if !strings.Contains(m, oneSlotDep.Name) || strings.Contains(m, queuedDep.Name+":") {
			t.Errorf("miss %d %q: want the one-slot deployment named, the queued one (recorded) not", i+1, m)
		}
	}

	// Each verdict counts the status now, as checkForbidden does after the
	// handovers: the one-slot entry is there now in both, as in CI's "now".
	vBefore, why := judgeLate(beforeAt, earliestLate(t, p3, before), ds)
	t.Logf("with no wait: %s: %s", vBefore, why)
	if vBefore != eventcount.Inconclusive || !strings.Contains(why, "condition 1") {
		t.Errorf("with no wait: %s (%s), want inconclusive on condition 1, as CI's run", vBefore, why)
	}
	vAfter, why := judgeLate(firstAt, earliestLate(t, p3, atFirst), ds)
	t.Logf("with armFirstRead: %s: %s", vAfter, why)
	if vAfter != eventcount.NotRaised {
		t.Errorf("with armFirstRead: %s (%s), want not raised: the first read precedes the drain start counted back by the measured %s", vAfter, why, queuedStartToReady)
	}
}

// A class whose start-to-ready time is never recorded is a FAIL, never a pass
// and never NOT RUN: armFirstRead returns the error, naming the deployment and
// the class, once the bound passes, and arms nothing. The bound is the row's
// own, the drain start minus longStreamLead, here a second away; nothing
// records the time, so the result does not depend on how fast the wait runs.
// The one-slot deployment holds another class's entry, which condition 1 does
// not count by (lateStartToReady), so a wait on any entry fails here too.
func TestFirstReadFailsWhenAClassIsNeverRecorded(t *testing.T) {
	ds := time.Now().Add(longStreamLead + time.Second)
	p3, c := firstReadFixture(t, ds)
	other := v1alpha1.AcceleratorClass{GPUModel: "example-other-gpu", CPUArch: "aarch64"}
	if err := recordStartToReady(c, oneSlotDep.Name, oneSlotDep.Name+"-earlier", other, time.Minute); err != nil {
		t.Fatal(err)
	}
	misses := 0
	p3.s2rMiss = func([]string) { misses++ }
	err := p3.armFirstRead()
	if err == nil {
		t.Fatal("armFirstRead returned nil with the one-slot class's start-to-ready time never recorded, want a FAIL")
	}
	t.Logf("armFirstRead: %v", err)
	msg := err.Error()
	for _, want := range []string{"FAIL", oneSlotDep.Name, oneSlotDep.Name + "-primary", fmt.Sprint(firstReadClass)} {
		if !strings.Contains(msg, want) {
			t.Errorf("the error %q does not name %q", msg, want)
		}
	}
	if strings.Contains(msg, queuedDep.Name+":") {
		t.Errorf("the error %q names the queued deployment, whose class is recorded", msg)
	}
	if p3.onLed != nil {
		t.Error("armFirstRead armed the first read although a class's start-to-ready time is unrecorded")
	}
	if misses == 0 {
		t.Error("armFirstRead failed without reading the status")
	}
}
