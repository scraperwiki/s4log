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

type Committer interface {
	Commit(buf []byte) int
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

type AsyncFileCommitter struct {
	sync.WaitGroup
	hostname string
}

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
func (afc *AsyncFileCommitter) Commit(buf []byte) int {
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

	// Copy the data to a fresh buffer
	newbuf := make([]byte, len(p))
	copy(newbuf, p)

	// Commence an asynchronous copy of the buffer to permanent storage.
	afc.Add(1)
	go func() {
		defer afc.Done()

		timestamp := time.Now().Format(time.RFC3339Nano)
		name := fmt.Sprintf("logs/%s-%s.txt", afc.hostname, timestamp)
		log.Printf("Committing %d bytes to %q", len(newbuf), name)

		err := WriteBuf(name, newbuf)
		if err != nil {
			log.Printf("Error committing %q: %v", name, err)
			return
		}
	}()

	// Move trailing data to beginning of `buf` and truncate `buf`
	copy(buf, buf[len(p):])
	leftOver := len(buf) - len(p)
	buf = buf[:leftOver]

	return leftOver
}
