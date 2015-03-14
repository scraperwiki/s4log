package main

import (
	"sync"
	"time"
)

// The Deadliner interface provides an abstraction for ensuring an event happens
// at with the specified frequency.
//
// A Deadliner has a .Deadline() method which is the time by which the deadline
// should be met, and a .Met() function which signals that the deadline was met.
//
// These methods are thread safe.
type Deadliner struct {
	mu     sync.Mutex
	period time.Duration
	next   time.Time
}

func NewDeadliner(period time.Duration) *Deadliner {
	d := &Deadliner{
		period: period,
		next:   time.Now(),
	}
	d.Met()
	return d
}

// Returns the timestamp of the next deadline.
func (d *Deadliner) Deadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.next
}

// When a deadline is met, a new deadline is issued.
func (d *Deadliner) Met() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if -time.Since(d.next) < d.period {
		d.next = d.next.Add(d.period)
	}
}
