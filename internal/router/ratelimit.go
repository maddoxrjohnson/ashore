package router

import (
	"sync"
	"time"
)

// limiter is a token bucket per client. Each key (an IP) has a bucket that
// refills at rate tokens per second up to burst; a request takes one token
// or is refused. Idle buckets are swept when the map grows past
// maxBuckets, since a bucket that has been idle long enough to refill is
// indistinguishable from a new one.
type limiter struct {
	rate, burst float64

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

const maxBuckets = 10000

func newLimiter(rps int, burst int) *limiter {
	return &limiter{rate: float64(rps), burst: float64(burst), buckets: make(map[string]*bucket)}
}

// allow takes a token for key at time now and reports whether there was one.
func (l *limiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxBuckets {
			l.sweep(now)
		}
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweep drops buckets that have been idle long enough to be full again.
func (l *limiter) sweep(now time.Time) {
	full := time.Duration(l.burst / l.rate * float64(time.Second))
	for k, b := range l.buckets {
		if now.Sub(b.last) >= full {
			delete(l.buckets, k)
		}
	}
}
