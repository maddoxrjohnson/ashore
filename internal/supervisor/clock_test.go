package supervisor

import (
	"sync"
	"testing"
	"time"
)

// fakeClock hands out timers the test fires by hand. Each After call is
// queued with its duration, so a test can wait for the code to ask, check
// how long it asked for, move the clock, and fire, in that order; that is
// the only order in which a fake timer is not a race.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers chan fakeTimer
}

type fakeTimer struct {
	d  time.Duration
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now:    time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
		timers: make(chan fakeTimer, 16),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	t := fakeTimer{d: d, ch: make(chan time.Time, 1)}
	c.timers <- t
	return t.ch
}

// advance moves the clock without firing anything.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fire waits for the next After, advances the clock by its duration, fires
// it, and returns the duration that was asked for.
func (c *fakeClock) fire(t *testing.T) time.Duration {
	t.Helper()
	select {
	case tm := <-c.timers:
		c.advance(tm.d)
		tm.ch <- c.Now()
		return tm.d
	case <-time.After(5 * time.Second):
		t.Fatal("nothing asked the clock to wait")
		return 0
	}
}

// pending reports whether a timer has been requested and not yet fired,
// without consuming it.
func (c *fakeClock) pending() bool { return len(c.timers) > 0 }
