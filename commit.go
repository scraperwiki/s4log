package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

type Committer interface {
	// A committer takes an input buffer and returns
	// the number of trailing bytes not processed.
	Commit(buf []byte) int
}

// Allow a func to implement Committer
type CommitterFunc func(buf []byte) int

func (cf CommitterFunc) Commit(buf []byte) int {
	return cf(buf)
}

// Writes the committed buffer to a file with the
// hostname and timestamp in the path.
type FileCommitter struct {
	hostname string
}

func (fc *FileCommitter) Commit(buf []byte) int {
	timestamp := time.Now().Format(time.RFC3339Nano)
	name := fmt.Sprintf("logs/%s-%s.txt", fc.hostname, timestamp)

	log.Printf("Committing %d bytes to %q", len(buf), name)

	fd, err := os.Create(name)
	if err != nil {
		log.Println("failed to os.Create: %v", err)
		return len(buf)
	}
	defer fd.Close()

	nw, err := fd.Write(buf)
	if err != nil || nw != len(buf) {
		log.Printf("Error or short write: (%v leftover): %v", len(buf)-nw, err)
		return nw
	}
	return 0
}

// Wraps a Committer and calls Deadliner.Met() when commit is called.
type DeadlineMetCommitter struct {
	*Deadliner
	Committer
}

func (dc DeadlineMetCommitter) Commit(buf []byte) int {
	dc.Met()
	return dc.Committer.Commit(buf)
}

// Wraps a committer, processing bytes upto the last newline in `buf`.
// If there is data following the final newline, it is moved to the beginning,
// so that it may be processed in a subsequent commit.
type NewlineCommitter struct {
	Committer
}

func (nlc NewlineCommitter) Commit(buf []byte) int {
	idx := bytes.LastIndex(buf, []byte("\n"))

	var toNL []byte
	if idx == -1 {
		// No newline, take everything.
		toNL = buf
	} else {
		// Take up to the last newline, including the newline.
		toNL = buf[:idx+1]
	}

	x := nlc.Committer.Commit(toNL)
	if x != 0 {
		// TODO(pwaller): Not sure what to do with bytes left over here..
		log.Panic("Assertion fail, bytes left over:", x)
	}

	return len(buf) - len(toNL)
}

// Wraps a Committer and calls it asynchronously with a copy of the buffer.
// A semaphore ensures that there are not too many in flight at once.
type AsyncCommitter struct {
	Semaphore
	Committer
}

func (ac AsyncCommitter) Commit(buf []byte) int {
	// Copy the data to a fresh buffer.
	bufCopy := make([]byte, len(buf))
	copy(bufCopy, buf)

	// Commence an asynchronous commit on the copy.
	ac.Acquire()
	log.Printf("Commits in flight: %v", len(ac.Semaphore))
	go func() {
		defer ac.Release()

		x := ac.Committer.Commit(bufCopy)
		if x != 0 {
			// TODO(pwaller): Not sure what to do with bytes left over here..
			log.Panic("Assertion fail, bytes left over:", x)
		}
	}()

	return 0
}

// Only call this once. Consumes the whole Semaphore.
func (ac *AsyncCommitter) Wait() {
	for i := 0; i < cap(ac.Semaphore); i++ {
		ac.Acquire()
	}
}

// A committer which sleeps before committing, for testing AsyncCommitter
type SlowCommitter struct {
	Committer
}

func (sc SlowCommitter) Commit(buf []byte) int {
	time.Sleep(5 * time.Second)
	return sc.Committer.Commit(buf)
}
