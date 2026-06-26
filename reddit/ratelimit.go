package reddit

import (
	"sync"
	"time"
)

// globalLimiter allows 55 requests per minute (conservative under Reddit's 60/min limit).
// All FetchRaw and FetchSubredditPage calls acquire a token before hitting the API.
var globalLimiter = newRateLimiter(55)

type rateLimiter struct {
	mu       sync.Mutex
	tokens   int
	max      int
	refillAt time.Time
}

func newRateLimiter(perMinute int) *rateLimiter {
	return &rateLimiter{
		tokens:   perMinute,
		max:      perMinute,
		refillAt: time.Now().Add(time.Minute),
	}
}

func (r *rateLimiter) acquire() {
	for {
		r.mu.Lock()
		now := time.Now()
		if now.After(r.refillAt) {
			r.tokens = r.max
			r.refillAt = now.Add(time.Minute)
		}
		if r.tokens > 0 {
			r.tokens--
			r.mu.Unlock()
			return
		}
		waitUntil := r.refillAt
		r.mu.Unlock()
		time.Sleep(time.Until(waitUntil))
	}
}