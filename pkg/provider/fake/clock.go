package fake

import (
	"sync"
	"time"
)

// Clock is a manual clock. The fakes read it at each call and never sleep, so a
// test moves time with Set and Advance. It is safe for concurrent use. Every
// time it returns is in UTC.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a clock that reads start.
func NewClock(start time.Time) *Clock { return &Clock{now: start.UTC()} }

// Now returns the clock's time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set moves the clock to t.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
