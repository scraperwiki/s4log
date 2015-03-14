package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Committer interface {
	// A committer takes an input buffer and returns
	// the number of trailing bytes not processed.
	Commit(buf []byte) int
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
	if len(buf) == 0 {
		// Nothing to do
		return 0
	}
	idx := bytes.LastIndex(buf, []byte("\n"))

	var p []byte
	if idx == -1 {
		// No newline, take everything
		p = buf
	} else {
		// take up to the last newline, including the newline
		p = buf[:idx+1]
	}

	x := nlc.Committer.Commit(buf)
	if x != 0 {
		// TODO(pwaller): Not sure what to do with bytes left over here..
		log.Panic("Assertion fail, bytes left over:", x)
	}

	// Move trailing data to beginning of `buf` and truncate `buf`
	copy(buf, buf[len(p):])
	leftOver := len(buf) - len(p)
	buf = buf[:leftOver]

	return leftOver
}

// Wraps a Committer and calls it asynchronously with a copy of the buffer.
type AsyncCommitter struct {
	sync.WaitGroup
	Committer
}

func (ac *AsyncCommitter) Commit(buf []byte) int {
	// Copy the data to a fresh buffer.
	bufCopy := make([]byte, len(buf))
	copy(bufCopy, buf)

	// Commence an asynchronous commit on the copy.
	ac.Add(1)
	go func() {
		defer ac.Done()

		x := ac.Committer.Commit(bufCopy)
		if x != 0 {
			// TODO(pwaller): Not sure what to do with bytes left over here..
			log.Panic("Assertion fail, bytes left over:", x)
		}
	}()

	return 0
}
