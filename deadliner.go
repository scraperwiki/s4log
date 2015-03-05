package main

import (
	"sync"
	"time"
)

type Deadliner struct {
	mu     sync.Mutex
	period time.Duration
	next   time.Time
}

func NewDeadliner(period time.Duration) *Deadliner {
	d := &Deadliner{period: period, next: time.Now()}
	d.Met()
	return d
}

func (d *Deadliner) Deadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.next
}

// When a deadline is met, a new deadline is issued
func (d *Deadliner) Met() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.next = d.next.Add(d.period)
}

// Until returns time until the next deadline. May be negative if unmet.
func (d *Deadliner) Until() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return -time.Since(d.next)
}
