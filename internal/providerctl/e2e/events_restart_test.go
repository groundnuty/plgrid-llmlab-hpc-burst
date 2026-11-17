package e2e

import (
	"context"
	"testing"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// startManager starts the Provider's manager as the controller binary runs it:
// NewProviderManager, with its leader Lease and its start observation, and
// Setup, over the rig's Options. A goroutine moves the fake clock on while it
// runs (a second of fake time every 20 ms), since a manager's reconciles come
// from its own timers, and the Batch's cached queue listing ages on the fake
// clock. It returns the stop, which waits for the manager.
func (r *evRig) startManager() (stop func()) {
	r.t.Helper()
	mgr, err := providerctl.NewProviderManager(cfg, scheme(), r.prov, providerctl.ManagerOptions{
		LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond, GracefulShutdownTimeout: 10 * time.Second,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	opts := r.options()
	opts.Counter = r.counter
	opts.PollInterval, opts.BusyInterval = 300*time.Millisecond, 100*time.Millisecond
	if err := providerctl.Setup(mgr, opts); err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(r.ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	ticking := make(chan struct{})
	go func() {
		defer close(ticking)
		for ctx.Err() == nil {
			r.clock.Advance(time.Second)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				r.t.Errorf("the manager stopped with %v", err)
			}
		case <-time.After(30 * time.Second):
			r.t.Fatal("the manager did not stop within 30 s")
		}
		<-ticking
	}
}

// phaseIs reads the stored phase of the Allocation.
func (r *evRig) phaseIs(name string, p v1alpha1.AllocationPhase) bool {
	a, gone := r.get(name)
	return !gone && a.Status.AtProvider.Phase == p
}

// leaseEvents lists the typed events (action set) the API holds about the
// Provider's own Lease, reported by the Provider controller.
func (r *evRig) leaseEvents() []eventsv1.Event {
	r.t.Helper()
	var list eventsv1.EventList
	if err := c.List(r.ctx, &list, client.InNamespace(v1alpha1.ControllerNamespace)); err != nil {
		r.t.Fatal(err)
	}
	var out []eventsv1.Event
	for _, e := range list.Items {
		if e.Regarding.Kind == "Lease" && e.Regarding.Name == v1alpha1.ProviderLeaseName(r.prov) && e.Action != "" && e.ReportingController == v1alpha1.FieldManagerProvider {
			out = append(out, e)
		}
	}
	return out
}

// A restart between transitions: the Provider's manager stops while its job is
// held; the fake Slurm lifts the hold and starts the job; the manager restarts.
// Its first pass raises JobStarted and one ObservationGap regarding its
// provider-<name> Lease, and no HoldLifted: the steps in between are not
// replayed. The absence of HoldLifted is shown by the counter's four conditions.
func TestARestartBetweenTransitions(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	// The lift does not take, so the job stays held by its user while the manager
	// runs and when it stops.
	r.faults.fail("scontrol release", -1, slurm.ErrTransport)
	stop := r.startManager()
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	a := r.newAlloc("a", "3h")
	eventually(t, "the job is submitted, held, and its Allocation shows Held", 60*time.Second, func() bool { return r.phaseIs("a", v1alpha1.AllocationHeld) })
	a, _ = r.get("a")
	id := r.jobID(a)
	if j, ok := r.sl.Job(id); !ok || !j.Held || j.HoldReason != "JobHeldUser" {
		t.Fatalf("the fake Slurm's job is %+v, want held by its user", j)
	}
	if n := r.count(r.key("started", "HoldLifted", "Normal")); n != 0 {
		t.Fatalf("HoldLifted raised %v times while the lift failed", n)
	}
	if got := r.leaseEvents(); len(got) != 0 {
		t.Fatalf("a first start raised an event regarding the Lease: %v", got)
	}
	stop()
	stopped = true

	// First read, before the change: the manager is down, so nothing emits.
	hold := r.key("started", "HoldLifted", "Normal")
	first := r.read(hold)
	change := time.Now()
	// The scheduler lifts the hold and starts the job while the manager is down.
	r.faults.clear()
	if _, err := r.session.Run(r.ctx, slurm.Command{Line: "scontrol release " + a.Annotations[v1alpha1.AnnotationJobID]}); err != nil {
		t.Fatal(err)
	}
	if err := r.sl.Start(id); err != nil {
		t.Fatal(err)
	}
	if j, _ := r.sl.Job(id); j.State != "RUNNING" || j.Held {
		t.Fatalf("the fake Slurm's job is %+v, want running and not held", j)
	}

	restarted := time.Now()
	stop = r.startManager()
	stopped = false
	eventually(t, "the restarted manager's first pass shows Running", 60*time.Second, func() bool { return r.phaseIs("a", v1alpha1.AllocationRunning) })
	observed := time.Now()
	evs := r.waitEvents(a, want{"ready", "JobStarted"})
	r.checkNotes(evs, id)
	deadline := time.Now().Add(30 * time.Second)
	for len(r.leaseEvents()) == 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	gap := r.leaseEvents()
	if len(gap) == 0 || gap[0].Reason != "ObservationGap" || gap[0].Action != "degraded" || gap[0].Regarding.Namespace != v1alpha1.ControllerNamespace {
		t.Fatalf("the Lease's events %+v, want a degraded ObservationGap regarding provider-%s in %s", gap, r.prov, v1alpha1.ControllerNamespace)
	}
	if n := r.count(r.key("degraded", "ObservationGap", "Warning")); n != 1 {
		t.Errorf("ObservationGap handed over %v times, want exactly 1", n)
	}

	// Absence of HoldLifted: the first read came before the change, the second
	// after the pass that saw the job Running, both from this process, and the
	// pass began after the change and after the event would have been due.
	second := r.read(hold)
	abs := eventcount.Absence{Key: hold, First: first, Second: second, Change: change,
		Barriers: []eventcount.Barrier{{What: "the restarted manager's pass over the Allocation, which raised JobStarted", PassStart: restarted, Observed: observed, Due: change}},
		Quiet:    "the Provider has one Allocation, the lift is made only by this controller's pass over it, and the first manager is stopped",
	}
	if err := abs.Require(); err != nil {
		t.Errorf("HoldLifted is not shown absent after the restart: %v", err)
	}
	stop()
	stopped = true
}

// Series at 0: a batch Provider's typed reasons read 0 on the serving copy's
// counter before its first Allocation, from the moment its controller is set up
// (the supervisor runs Setup with the counter), and are deleted when the
// supervisor drops the Provider.
func TestTheTypedSeriesAreCreatedAtZero(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	counter := eventcount.New()
	sup := &providerctl.Supervisor{
		Client: c,
		NewManager: func(p string) (manager.Manager, error) {
			return providerctl.NewProviderManager(cfg, scheme(), p, providerctl.ManagerOptions{
				LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond,
			})
		},
		Setup: func(mgr manager.Manager, p string) error {
			opts := r.options()
			opts.Provider, opts.Counter = p, counter
			return providerctl.Setup(mgr, opts)
		},
		Dropped:  providerctl.ForgetSeries(counter),
		Interval: 200 * time.Millisecond, MinBackoff: 300 * time.Millisecond, MaxBackoff: 2 * time.Second,
	}
	ctx, cancel := context.WithCancel(r.ctx)
	done := make(chan error, 1)
	go func() { done <- sup.Start(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	reg := eventcount.Key{Controller: eventcount.ControllerProvider(r.prov)}
	reasons := map[string][2]string{
		"JobSubmitted": {"started", "Normal"}, "JobAdopted": {"started", "Normal"}, "HoldLifted": {"started", "Normal"},
		"JobStarted": {"ready", "Normal"}, "JobReappeared": {"ready", "Normal"}, "HardEndNear": {"ending-soon", "Normal"},
		"JobEnded": {"ended", "Normal"}, "AllocationEndedEarly": {"failing", "Warning"}, "AllocationFailed": {"failing", "Warning"},
		"Requeued": {"failing", "Warning"}, "SubmittedAsOther": {"failing", "Warning"}, "AllocationRefused": {"failing", "Warning"},
		"JobLost": {"failing", "Warning"}, "AdminHold": {"degraded", "Warning"}, "JobCompleting": {"degraded", "Warning"},
		"ObservationGap": {"degraded", "Warning"},
	}
	readAll := func() (present, nonZero int) {
		for reason, at := range reasons {
			k := reg
			k.Reason, k.Action, k.Type = reason, at[0], at[1]
			rd := readFrom(t, counter, k)
			if rd.HasCount {
				present++
				if rd.Count != 0 {
					nonZero++
				}
			}
		}
		return present, nonZero
	}
	eventually(t, "every typed reason reads 0 on the counter, before any Allocation exists", 60*time.Second, func() bool {
		p, _ := readAll()
		return p == len(reasons)
	})
	if _, nz := readAll(); nz != 0 {
		t.Fatalf("%d typed series are not 0 before the first Allocation", nz)
	}
	// The Provider goes: the supervisor drops it and its series go with it.
	prov := &v1alpha1.Provider{}
	if err := c.Get(r.ctx, client.ObjectKey{Name: r.prov}, prov); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(r.ctx, prov); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the dropped Provider's typed series are deleted", 60*time.Second, func() bool {
		p, _ := readAll()
		return p == 0
	})
}

// readFrom is one read of a counter's series, outside a rig.
func readFrom(t *testing.T, counter *eventcount.Counter, k eventcount.Key) eventcount.Read {
	t.Helper()
	rr := &evRig{t: t, counter: counter, reg: newRegistry(counter)}
	return rr.read(k)
}
