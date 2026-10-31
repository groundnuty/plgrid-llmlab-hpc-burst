// Package clock is the front's view of time, so its deadlines (2 h total,
// 30 min idle, the lease end, pauses) can be tested at their real defaults
// with a fake clock that a test advances (HBEP-5, Test plan, Deadlines).
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock gives the time and runs functions after a delay.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a stoppable, resettable timer, as time.Timer from AfterFunc.
type Timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

// Real is the wall clock.
type Real struct{}

// Now is time.Now.
func (Real) Now() time.Time { return time.Now() }

// AfterFunc is time.AfterFunc.
func (Real) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// Fake is a manual clock: time moves only through Advance, which runs every
// timer that falls due, in order, on the caller's goroutine.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFake returns a fake clock at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the fake time.
func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules f at Now()+d.
func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, at: c.now.Add(d), f: f, active: true}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock by d and runs the timers that fall due, earliest
// first, each with the clock set to its own time.
func (c *Fake) Advance(d time.Duration) {
	c.mu.Lock()
	end := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.SliceStable(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		var due *fakeTimer
		for _, t := range c.timers {
			if t.active && !t.at.After(end) {
				due = t
				break
			}
		}
		if due == nil {
			c.now = end
			c.mu.Unlock()
			return
		}
		due.active = false
		if due.at.After(c.now) {
			c.now = due.at
		}
		f := due.f
		c.mu.Unlock()
		f()
	}
}

// Pending is the number of active timers.
func (c *Fake) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if t.active {
			n++
		}
	}
	return n
}

type fakeTimer struct {
	c      *Fake
	at     time.Time
	f      func()
	active bool
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.at, t.active = t.c.now.Add(d), true
	return was
}
