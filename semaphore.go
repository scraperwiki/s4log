package main

type Semaphore chan struct{}

func NewSemaphore(size int) Semaphore {
	return make(Semaphore, size)
}

func (s Semaphore) Acquire() {
	s <- struct{}{}
}

func (s Semaphore) Release() {
	<-s
}

// Wait for all of the semaphore to be available.
// Only call this once. Consumes the whole Semaphore.
func (s Semaphore) Wait() {
	for i := 0; i < cap(s); i++ {
		s.Acquire()
	}
}
