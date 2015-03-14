package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/scraperwiki/s4log/poller"
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

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
func Commit(wg *sync.WaitGroup, hostname string, buf []byte) int {
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
	wg.Add(1)
	go func() {
		defer wg.Done()

		timestamp := time.Now().Format(time.RFC3339Nano)
		name := fmt.Sprintf("logs/%s-%s.txt", hostname, timestamp)
		log.Printf("Committing %d bytes to %q", len(newbuf), name)

		err := WriteBuf(name, newbuf)
		if err != nil {
			log.Printf("Error committing %q: %v", name, err)
			return
		}

		nMu.Lock()
		defer nMu.Unlock()
		n += len(newbuf)
	}()

	// Move trailing data to beginning of `buf` and truncate `buf`
	copy(buf, buf[len(p):])
	leftOver := len(buf) - len(p)
	buf = buf[:leftOver]

	return leftOver
}

var (
	nMu sync.Mutex
	n   int
)

func main() {

	const (
		MiB = 1 << 20
		kiB = 1 << 10
		// Period     = 5 * time.Minute // Maximum time between commits
		Period     = 5 * time.Second // Maximum time between commits
		BufferSize = 10 * kiB        // Size of buffer before flushing
	)

	var (
		deadliner = NewDeadliner(Period)
		done      = make(chan struct{})

		mu  sync.Mutex
		buf = make([]byte, BufferSize)
		p   = buf
	)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to determine hostname:", err)
	}

	defer func() {
		log.Printf("Exiting, total bytes: %v", n)
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	fill := func(in *poller.FD) error {
		mu.Lock()
		defer mu.Unlock()

		// Read deadline allows us to have a large buffer but not wait
		// indefinitely for it to be filled.
		in.SetReadDeadline(deadliner.Deadline())

		n, err := in.Read(p)
		if err != nil {
			return err
		}
		if len(p[n:]) == 0 {
			// Buffer is full!
			n := Commit(&wg, hostname, buf)
			p = buf[n:]
		} else {
			// Advance p
			p = p[n:]
		}
		return nil
	}

	commit := func() {
		mu.Lock()
		defer mu.Unlock()

		deadliner.Met()

		n := Commit(&wg, hostname, buf[:len(buf)-len(p)])
		p = buf[n:]
	}

	go func() {
		defer close(done)
		in := Input(os.Args[1:])
		defer func() {
			err := in.Close()
			if err != nil {
				log.Printf("Failure during Input.Close: %v", err)
			}
		}()

		fd := int(in.(Fder).Fd())
		pollFD, err := poller.NewFD(fd)
		if err != nil {
			log.Fatalf("Problem whilst polling: %v", err)
			return
		}
		for {
			err := fill(pollFD)
			switch err {
			case nil:
				continue
			case poller.ErrTimeout:
				deadliner.Wait()
				continue
			case io.EOF:
				log.Println("EOF")
				return
			default:
				log.Printf("Error during read: %v", err)
				return
			}
		}
	}()

	// Do a final commit
	defer commit()

	for {
		select {
		case <-time.After(deadliner.Until()):
			if !deadliner.Passed() {
				continue
			}
		case <-done:
			return
		}

		log.Println("Deadline commit")
		commit()
	}
}
