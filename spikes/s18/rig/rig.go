package rig

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// Faults are the run's fault markers. A request carries one in its prompt; the doubles' modes, or the
// cutter, act on it. Each run injects one fault.
const (
	MarkF1     = "S18-F1-D" // + the target's index: only that Instance's double dies under it (die-on)
	MarkF2     = "S18-F2-D" // + the target's index: only that double answers it 500 (fail-on)
	MarkF3     = "S18-F3"   // the cutter: the tunnel closes after its first write, on whichever Instance
	MarkPoison = "S18-F4"   // every double dies under it (die-on on all): the poison request
)

// Config is a run's stand.
type Config struct {
	N      int    // Instances
	Fault  string // "", "F1", "F2", "F3", "F4"
	Policy *Policy
	// D1 and D2 emulate the controller: D1 after a death the Instance leaves routing (not ready), as a
	// probe interval's signal does; D2 after it the double is restarted and, once healthy, ready again.
	// vLLM's restart and verification take minutes; the spike's are short, and every result row records
	// both (README).
	D1, D2 time.Duration
	// Engine is the doubles' command (the test binary).
	Engine EngineCmd
	// TokenDelay, when set, is the doubles' token-delay mode.
	TokenDelay string
	// FrontConfig, when set, edits the front's configuration.
	FrontConfig func(*front.Config)
}

// Rig is the running stand.
type Rig struct {
	Config
	Front   *Front
	Doubles *Doubles
	Trans   *Transports
	Cutter  *Cutter
	Layer   *Layer // nil under arm (a)

	dir   string
	watch []*logWatch

	mu       sync.Mutex
	ready    []bool
	restarts int
	pinned   map[int]string
	closing  bool
	wg       sync.WaitGroup
}

// New starts the stand in dir: the doubles, the front, the transports, and the layer when the run has a
// policy.
func New(dir string, c Config) (*Rig, error) {
	r := &Rig{Config: c, dir: dir, Trans: NewTransports(), ready: make([]bool, c.N), pinned: map[int]string{}}
	r.Cutter = &Cutter{HoldFor: 10 * time.Second}
	if c.Fault == "F3" {
		r.Cutter.Marker = MarkF3
	}
	f, err := NewFront(dir, c.N, r.Trans, c.FrontConfig)
	if err != nil {
		return nil, err
	}
	r.Front = f
	modes, keys := make([]string, c.N), make([]string, c.N)
	for i := 0; i < c.N; i++ {
		m := "early-headers"
		switch c.Fault {
		case "F1":
			m += fmt.Sprintf(",die-on=%s%d", MarkF1, i)
		case "F2":
			m += fmt.Sprintf(",fail-on=%s%d", MarkF2, i)
		case "F4":
			m += ",die-on=" + MarkPoison
		}
		if c.TokenDelay != "" {
			m += ",token-delay=" + c.TokenDelay
		}
		modes[i], keys[i] = m, f.Instances[i].APIKey
	}
	r.Doubles = NewDoubles(dir, c.Engine, modes, keys)
	for i := 0; i < c.N; i++ {
		r.watch = append(r.watch, newLogWatch(r.Doubles.Log(i)))
		if err := r.Doubles.Start(i); err != nil {
			r.Close()
			return nil, err
		}
		r.ready[i] = true
	}
	r.Doubles.onDeath = r.died
	if c.Policy == nil {
		for i := 0; i < c.N; i++ {
			r.Trans.Set(f.Instances[i].UID, r.Cutter.Wrap(DialUnix(r.Doubles.Socket(i)), r.watch[i].Admitted))
		}
	} else if r.Layer, err = newLayer(r, *c.Policy); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// died is the controller emulation's answer to a double that ended on its own.
func (r *Rig) died(i int) {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return
	}
	r.wg.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		t0 := time.Now()
		time.Sleep(r.D1)
		r.setReady(i, false)
		time.Sleep(time.Until(t0.Add(r.D2)))
		r.mu.Lock()
		closing := r.closing
		r.mu.Unlock()
		if closing {
			return
		}
		if err := r.Doubles.Start(i); err != nil {
			return
		}
		r.mu.Lock()
		r.restarts++
		r.mu.Unlock()
		r.setReady(i, true)
	}()
}

func (r *Rig) setReady(i int, ready bool) {
	r.mu.Lock()
	r.ready[i] = ready
	r.mu.Unlock()
	r.Front.SetReady(i, ready)
}

// Ready reports whether Instance i is in routing, as the controller emulation has it.
func (r *Rig) Ready(i int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready[i]
}

// AllReady waits until every Instance is alive and ready again.
func (r *Rig) AllReady(ctx context.Context) error {
	for {
		ok := true
		for i := 0; i < r.N; i++ {
			if !r.Ready(i) || !r.Doubles.Alive(i) {
				ok = false
			}
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not every Instance is ready again: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Restarts is how many deaths the controller emulation has restarted.
func (r *Rig) Restarts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restarts
}

// SessionOn finds a session the front routes to Instance i, read from the answers' Instance header, and
// keeps it. Each probe is an ordinary request of the run, with an ID, so it is accounted for too.
func (r *Rig) SessionOn(ctx context.Context, c *Client, i int) (string, []Result, error) {
	r.mu.Lock()
	s, ok := r.pinned[i]
	r.mu.Unlock()
	if ok {
		return s, nil, nil
	}
	var probes []Result
	for k := 0; k < 256; k++ {
		session := fmt.Sprintf("s18-session-%d", k)
		res := c.Do(ctx, c.NextBase(), 0, session, false, "")
		probes = append(probes, res)
		if res.Status == 200 && res.Instance == string(r.Front.Instances[i].UID) {
			r.mu.Lock()
			r.pinned[i] = session
			r.mu.Unlock()
			return session, probes, nil
		}
	}
	return "", probes, fmt.Errorf("no session of 256 is routed to Instance %d", i)
}

// Close stops the stand: the layer, the front, every double, and the controller emulation's restarts.
func (r *Rig) Close() {
	r.mu.Lock()
	r.closing = true
	r.mu.Unlock()
	r.wg.Wait()
	if r.Layer != nil {
		r.Layer.close()
	}
	if r.Front != nil {
		r.Front.Close()
	}
	if r.Doubles != nil {
		r.Doubles.StopAll()
	}
}
