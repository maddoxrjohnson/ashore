package supervisor

import "time"

// clock is the slice of package time the restart backoff waits on. The
// schedule sums to minutes, so tests substitute a fake and fire the timers
// by hand; everything else in the package uses real time.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
