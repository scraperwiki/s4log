package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Committer interface {
	// A committer takes an input buffer and returns the number of trailing
	// bytes not processed.
	Commit(buf []byte) int
}

type FileCommitter struct {
	hostname string
}

func (fc *FileCommitter) Commit(buf []byte) int {
	timestamp := time.Now().Format(time.RFC3339Nano)
	name := fmt.Sprintf("logs/%s-%s.txt", fc.hostname, timestamp)
	log.Printf("Committing %d bytes to %q", len(buf), name)

	err := WriteBuf(name, buf)
	if err != nil {
		log.Printf("Error committing %q: %v", name, err)
		return 0
	}
	return len(buf)
}

func WriteBuf(name string, data []byte) error {
	fd, err := os.Create(name)
	if err != nil {
		return fmt.Errorf("failed to os.Create: %v", err)
	}
	defer fd.Close()

	nw, err := fd.Write(data)
	if err != nil || nw != len(data) {
		log.Printf("Error or short write: (%v leftover): %v", len(data)-nw, err)
		if err != nil {
			err = io.ErrShortWrite
		}
		return err
	}
	return err
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
type NewlineCommitter struct {
	Committer
}

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
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

	nlc.Committer.Commit(buf)

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

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
func (afc *AsyncCommitter) Commit(buf []byte) int {
	// Copy the data to a fresh buffer
	bufCopy := make([]byte, len(buf))
	copy(bufCopy, buf)

	// Commence an asynchronous copy of the buffer to permanent storage.
	afc.Add(1)
	go func() {
		defer afc.Done()

		afc.Committer.Commit(bufCopy)
	}()

	return 0
}
