package auth

import "time"

// Slows down password guessing.
//
// Failed logins are counted per username and per client IP. After a few free
// failures, each further one makes the key wait before the next attempt,
// doubling every time (1 s, 2 s, 4 s, ... up to 15 minutes). While a key
// waits, logins for it get 429 without the password even being checked, so the
// right password doesn't help an attacker who is guessing.
//
//   - Per username: stops guessing one account from many IPs.
//   - Per IP: stops one IP from trying a few passwords on many usernames.
//
// Kept in memory: a restart forgets it, and several server instances would
// each count on their own (production with more than one instance would need
// a shared store). A downside of per-username limits: an attacker can make the
// real user wait too. The cap keeps that to minutes.

type policy struct {
	// freeFailures are allowed before waiting starts.
	freeFailures int
	firstDelay   time.Duration
	maxDelay     time.Duration
	// forgetAfter: a key with no failure for this long starts from zero again.
	forgetAfter time.Duration
}

var perUsername = policy{
	freeFailures: 5,
	firstDelay:   time.Second,
	maxDelay:     15 * time.Minute,
	forgetAfter:  time.Hour,
}

// perIP is looser: several people can share one (mobile networks, NAT).
var perIP = policy{
	freeFailures: 20,
	firstDelay:   perUsername.firstDelay,
	maxDelay:     perUsername.maxDelay,
	forgetAfter:  perUsername.forgetAfter,
}

type entry struct {
	failures     int
	lastFailure  time.Time
	blockedUntil time.Time
}

// limiter is not safe for concurrent use; Service holds a mutex around it.
type limiter struct {
	policy  policy
	entries map[string]*entry
}

// pruneAt: old entries are dropped once the map is this big, so it can't grow forever.
const pruneAt = 10_000

func newLimiter(p policy) *limiter {
	return &limiter{policy: p, entries: map[string]*entry{}}
}

// check returns how long key must wait before the next attempt (0 if it need not).
func (l *limiter) check(key string, now time.Time) time.Duration {
	if e := l.entries[key]; e != nil && e.blockedUntil.After(now) {
		return e.blockedUntil.Sub(now)
	}
	return 0
}

func (l *limiter) failure(key string, now time.Time) {
	if len(l.entries) >= pruneAt {
		l.prune(now)
	}
	e := l.entries[key]
	if e == nil {
		e = &entry{lastFailure: now}
		l.entries[key] = e
	}
	if now.Sub(e.lastFailure) > l.policy.forgetAfter {
		e.failures = 0
	}
	e.failures++
	e.lastFailure = now
	if extra := e.failures - (l.policy.freeFailures + 1); extra >= 0 {
		// The first failure over the limit waits firstDelay, then it doubles.
		delay := l.policy.maxDelay
		if extra < 30 {
			delay = min(l.policy.firstDelay<<extra, l.policy.maxDelay)
		}
		e.blockedUntil = now.Add(delay)
	}
}

func (l *limiter) success(key string) { delete(l.entries, key) }

func (l *limiter) prune(now time.Time) {
	for k, e := range l.entries {
		if now.Sub(e.lastFailure) > l.policy.forgetAfter && !e.blockedUntil.After(now) {
			delete(l.entries, k)
		}
	}
}
