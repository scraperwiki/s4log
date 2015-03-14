package main

import (
	"sync"
	"time"
)

type Deadliner struct {
	c      sync.Cond
	period time.Duration
	next   time.Time
}

func NewDeadliner(period time.Duration) *Deadliner {
	d := &Deadliner{
		period: period,
		next:   time.Now(),
		c:      sync.Cond{L: &sync.Mutex{}},
	}
	d.Met()
	return d
}

func (d *Deadliner) Deadline() time.Time {
	d.c.L.Lock()
	defer d.c.L.Unlock()
	return d.next
}

// When a deadline is met, a new deadline is issued
func (d *Deadliner) Met() {
	d.c.L.Lock()
	defer d.c.L.Unlock()

	d.c.Broadcast()
	if -time.Since(d.next) < d.period {
		d.next = d.next.Add(d.period)
	}
}

// Until returns time until the next deadline. May be negative if unmet.
func (d *Deadliner) Until() time.Duration {
	d.c.L.Lock()
	defer d.c.L.Unlock()

	return -time.Since(d.next)
}

func (d *Deadliner) Passed() bool {
	d.c.L.Lock()
	defer d.c.L.Unlock()
	return d.passed()
}

// Is this passed?
func (d *Deadliner) passed() bool {
	return time.Since(d.next) > 0
}

// Wait until the deadline has been met
func (d *Deadliner) Wait() {
	d.c.L.Lock()
	for d.passed() {
		d.c.Wait()
	}
	d.c.L.Unlock()
}
