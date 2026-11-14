package launcher

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// orderProbe is a fake runner set for runInStopOrder: each process records when it was told to stop, and
// the front's stop waits on gate, as a front waits out its shutdown grace and cuts its requests.
type orderProbe struct {
	mu    sync.Mutex
	told  []string
	gate  chan struct{}
	front chan struct{} // closed once the front is told to stop
	rest  chan struct{} // closed once every other process is told to stop
	n     int
}

func (o *orderProbe) run(ctx context.Context, sp supervised) {
	<-ctx.Done()
	o.mu.Lock()
	o.told = append(o.told, sp.Name)
	o.mu.Unlock()
	if sp.Name == frontEntryName {
		close(o.front)
		<-o.gate
		return
	}
	o.mu.Lock()
	o.n--
	if o.n == 0 {
		close(o.rest)
	}
	o.mu.Unlock()
}

// On stop the front is told first, and no other process is told while the front's stop runs; once the
// front's runner returns, frps, the bulk frps, the keeper and the controller are told (HBEP-11, #553).
func TestTheSupervisorStopsTheFrontFirst(t *testing.T) {
	specs := []supervised{{Name: controllerEntryName}, {Name: FrpsProcName}, {Name: frontEntryName}, {Name: frpsBulkEntryName}, {Name: keeperEntryName}}
	o := &orderProbe{gate: make(chan struct{}), front: make(chan struct{}), rest: make(chan struct{}), n: len(specs) - 1}
	ctx, cancel := context.WithCancel(context.Background())
	var stages []string
	done := make(chan struct{})
	go func() {
		runInStopOrder(ctx, specs, o.run, func(s string) { stages = append(stages, s) })
		close(done)
	}()
	cancel()
	select {
	case <-o.front:
	case <-time.After(30 * time.Second):
		t.Fatal("the front was never told to stop")
	}
	// While the front's stop runs (the gate is closed) nothing else may be told: in the right order this
	// can never happen, so the wait below only gives an all-at-once stop time to show itself.
	select {
	case <-o.rest:
		t.Fatalf("every other process was told to stop while the front still stopped: told %v", o.told)
	case <-time.After(500 * time.Millisecond):
	}
	o.mu.Lock()
	if !slices.Equal(o.told, []string{frontEntryName}) {
		t.Errorf("told %v while the front stopped, want the front alone", o.told)
	}
	o.mu.Unlock()
	close(o.gate)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runInStopOrder did not return once the front had stopped")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.told) != len(specs) || o.told[0] != frontEntryName {
		t.Errorf("told %v, want the front first, then the other four", o.told)
	}
	if want := []string{"the front first", "the tunnel processes and the controllers"}; !slices.Equal(stages, want) {
		t.Errorf("stages %v, want %v", stages, want)
	}
}

// A host with no front stops every process at once, and its log names one stage.
func TestASupervisorWithNoFrontStopsAllAtOnce(t *testing.T) {
	specs := []supervised{{Name: controllerEntryName}, {Name: FrpsProcName}}
	o := &orderProbe{gate: make(chan struct{}), front: make(chan struct{}), rest: make(chan struct{}), n: len(specs)}
	ctx, cancel := context.WithCancel(context.Background())
	var stages []string
	done := make(chan struct{})
	go func() {
		runInStopOrder(ctx, specs, o.run, func(s string) { stages = append(stages, s) })
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runInStopOrder did not return")
	}
	if len(o.told) != 2 || !slices.Equal(stages, []string{"the tunnel processes and the controllers"}) {
		t.Errorf("told %v, stages %v; want both told, in one stage", o.told, stages)
	}
}

// Until the supervisor is told to stop, no process is: each runner keeps its process running.
func TestNoProcessIsToldToStopBeforeTheSupervisor(t *testing.T) {
	specs := []supervised{{Name: frontEntryName}, {Name: FrpsProcName}}
	started := make(chan string, 2)
	var mu sync.Mutex
	var early []string
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runInStopOrder(ctx, specs, func(c context.Context, sp supervised) {
			started <- sp.Name
			select {
			case <-c.Done():
				if ctx.Err() == nil {
					mu.Lock()
					early = append(early, sp.Name)
					mu.Unlock()
				}
			case <-ctx.Done():
				<-c.Done()
			}
		}, nil)
		close(done)
	}()
	<-started
	<-started
	cancel()
	<-done
	if len(early) != 0 {
		t.Errorf("%v told to stop before the supervisor was", early)
	}
}
