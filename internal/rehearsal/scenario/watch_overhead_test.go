package scenario_test

// What Follow's short connections cost the API server (PR #292's delta review: three
// 3 s watches are about 3,600 requests an hour). On the rehearsal's own stack with no
// deployment, a fixed write and read load runs under each of three arms: no watch, the
// three kinds' streams on one long connection each, and the same streams on Follow's
// short ones. An arm's measure is k3s's CPU (one process: API server, kine and the
// rest) over the window, and each verb's latency. Arms take turns, a different one
// first each round, so that drift on this shared host falls on all of them.
//
// Rerun, under launcher.lock, with the seconds each arm's window lasts:
//
//	HPCBURST_WATCH_OVERHEAD=60 REHEARSAL_RUN='^TestWatchOverhead$' hack/rehearsal.sh
//
// For a CPU-constrained host, wrap it in taskset -c 0,1. It asserts that the arms did
// what they say, by their counts of Watch calls; it sets no limit on the cost, which
// is for the reader of its OVERHEAD lines.

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

const (
	overheadRounds = 4
	overheadSettle = 5 * time.Second        // from an arm's start to its window
	overheadCycle  = 200 * time.Millisecond // one create, one LIST and one delete
	userHZ         = 100                    // the unit of /proc/<pid>/stat's times on Linux
)

// overheadArm is one way to watch; seconds is the length of its connections, 0 for no
// watch at all.
type overheadArm struct {
	name    string
	seconds int64
}

// armRun is what one arm's window measured.
type armRun struct {
	cpuPct               float64
	calls, cycles        int
	resets               int
	create, list, delete []time.Duration
}

func TestWatchOverhead(t *testing.T) {
	window, err := strconv.Atoi(os.Getenv("HPCBURST_WATCH_OVERHEAD"))
	if err != nil || window <= 0 {
		t.Skip("the measurement of the short watch connections runs only with HPCBURST_WATCH_OVERHEAD=<seconds per arm> (hack/rehearsal.sh)")
	}
	r := startRigWith(t, rigSetup{mode: "direct", ports: ports146, noWatch: true})
	pid := r.h.StatusPID("k3s")
	if pid == 0 {
		t.Fatal("hpcburst status names no running k3s of this stack")
	}
	t.Logf("OVERHEAD measuring k3s (status of this stack's state directory), %d rounds of %d s an arm", overheadRounds, window)
	arms := []overheadArm{{"none", 0}, {"long", 3600}, {"short", 3}}
	runs := map[string][]armRun{}
	for round := range overheadRounds {
		for i := range arms {
			arm := arms[(i+round)%len(arms)]
			got := measureArm(t, r, pid, arm, time.Duration(window)*time.Second)
			runs[arm.name] = append(runs[arm.name], got)
			load, _ := os.ReadFile("/proc/loadavg")
			t.Logf("OVERHEAD round %d %-5s: k3s CPU %6.2f%% of a core; watch calls %3d; cycles %d; resets %d; load %s", round+1, arm.name, got.cpuPct, got.calls, got.cycles, got.resets, strings.Join(strings.Fields(string(load))[:1], ""))
			switch {
			case arm.seconds == 3600 && got.calls != 0:
				t.Errorf("the long arm made %d Watch calls in its window: it did not hold one connection", got.calls)
			case arm.seconds == 3 && got.calls < window/2:
				t.Errorf("the short arm made %d Watch calls in %d s: it did not reconnect", got.calls, window)
			case arm.seconds == 0 && got.calls != 0:
				t.Errorf("the arm with no watch made %d Watch calls", got.calls)
			}
		}
	}
	reportOverhead(t, arms, runs)
}

// measureArm starts the arm's streams, lets them settle, runs the load over the window
// and stops them.
func measureArm(t *testing.T, r *rig, pid int, arm overheadArm, window time.Duration) armRun {
	t.Helper()
	var calls, resets atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	if arm.seconds > 0 {
		wc := countingWatchClient(t, r, &calls)
		s := stack.Stream{
			Event:   func(watch.EventType, client.Object) {},
			Reached: func(string) {},
			Reset:   func(string) { resets.Add(1) },
		}
		for _, list := range []client.ObjectList{&v1alpha1.InstanceList{}, &v1alpha1.AllocationList{}, &v1alpha1.ModelDeploymentList{}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				stack.FollowEvery(ctx, wc, stack.Namespace, list, s, arm.seconds)
			}()
		}
	}
	time.Sleep(overheadSettle)
	calls.Store(0)
	before, start := processCPU(t, pid), time.Now()
	got := runLoad(t, r, window)
	got.cpuPct = 100 * (processCPU(t, pid) - before).Seconds() / time.Since(start).Seconds()
	got.calls, got.resets = int(calls.Load()), int(resets.Load())
	return got
}

// countingWatchClient is a watching client of the stack that counts its Watch calls.
func countingWatchClient(t *testing.T, r *rig, calls *atomic.Int64) client.WithWatch {
	t.Helper()
	base, err := client.NewWithWatch(stack.RestConfig(t, r.h.State), client.Options{Scheme: stack.APIScheme(t)})
	if err != nil {
		t.Fatalf("a watching client: %v", err)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Watch: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
			calls.Add(1)
			return c.Watch(ctx, list, opts...)
		},
	})
}

// runLoad is the arm's fixed work: on each tick, create a ConfigMap, LIST the
// Allocations as a catch-up does, and delete the ConfigMap, each verb timed. No
// controller acts on these, so the load is the same under each arm.
func runLoad(t *testing.T, r *rig, window time.Duration) armRun {
	t.Helper()
	var got armRun
	timed := func(into *[]time.Duration, what string, call func(context.Context) error) {
		ctx, cancel := stack.CallCtx(t)
		defer cancel()
		start := time.Now()
		if err := call(ctx); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		*into = append(*into, time.Since(start))
	}
	tick := time.NewTicker(overheadCycle)
	defer tick.Stop()
	for end := time.Now().Add(window); time.Now().Before(end); <-tick.C {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("overhead-%d", got.cycles), Namespace: stack.Namespace}}
		timed(&got.create, "create", func(ctx context.Context) error { return r.c.Create(ctx, cm) })
		timed(&got.list, "list", func(ctx context.Context) error {
			return r.c.List(ctx, &v1alpha1.AllocationList{}, client.InNamespace(stack.Namespace))
		})
		timed(&got.delete, "delete", func(ctx context.Context) error { return r.c.Delete(ctx, cm) })
		got.cycles++
	}
	return got
}

// processCPU is the process's CPU time so far, all its threads, in user and system
// mode (proc(5): utime and stime, the 14th and 15th fields of stat).
func processCPU(t *testing.T, pid int) time.Duration {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatalf("the k3s process: %v", err)
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:]) // the second field, the name, can hold anything
	if len(f) < 13 {
		t.Fatalf("the k3s process's stat has %d fields after its name: %q", len(f), s)
	}
	utime, uerr := strconv.ParseInt(f[11], 10, 64)
	stime, serr := strconv.ParseInt(f[12], 10, 64)
	if uerr != nil || serr != nil {
		t.Fatalf("the k3s process's stat: %v %v", uerr, serr)
	}
	return time.Duration(utime+stime) * time.Second / userHZ
}

// reportOverhead logs each arm's median CPU and pooled latencies, and each round's
// difference from the long arm. The spread of the rounds is the noise a difference
// must clear.
func reportOverhead(t *testing.T, arms []overheadArm, runs map[string][]armRun) {
	t.Helper()
	pct := func(d []time.Duration, q float64) time.Duration {
		d = slices.Sorted(slices.Values(d))
		return d[int(float64(len(d)-1)*q)].Round(10 * time.Microsecond)
	}
	for _, arm := range arms {
		var cpu []float64
		var create, list, del []time.Duration
		cycles := 0
		for _, g := range runs[arm.name] {
			cpu = append(cpu, g.cpuPct)
			create, list, del = append(create, g.create...), append(list, g.list...), append(del, g.delete...)
			cycles += g.cycles
		}
		slices.Sort(cpu)
		t.Logf("OVERHEAD %-5s: k3s CPU %% of a core, by round %.2f; %d cycles; create p50 %s p99 %s; list p50 %s p99 %s; delete p50 %s p99 %s",
			arm.name, cpu, cycles, pct(create, .5), pct(create, .99), pct(list, .5), pct(list, .99), pct(del, .5), pct(del, .99))
	}
	for _, other := range []string{"none", "short"} {
		var diffs []string
		for i := range runs[other] {
			diffs = append(diffs, fmt.Sprintf("%+.2f", runs[other][i].cpuPct-runs["long"][i].cpuPct))
		}
		t.Logf("OVERHEAD %s minus long, by round, in points of a core: %s", other, strings.Join(diffs, " "))
	}
}
