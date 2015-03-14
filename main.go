package main

import (
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/scraperwiki/s4log/poller"
)

var (
	nMu sync.Mutex
	n   int
)

type CommitBuffer struct {
	mu     sync.Mutex
	buf, p []byte

	wg        *sync.WaitGroup
	deadliner *Deadliner
	hostname  string
}

func NewCommitBuffer(
	size int,
	wg *sync.WaitGroup,
	d *Deadliner,
	h string,
) *CommitBuffer {
	buf := make([]byte, size)
	return &CommitBuffer{buf: buf, p: buf, wg: wg, deadliner: d, hostname: h}
}

func (buf *CommitBuffer) Fill(in *poller.FD) error {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	// Read deadline allows us to have a large buffer but not wait
	// indefinitely for it to be filled.
	in.SetReadDeadline(buf.deadliner.Deadline())

	n, err := in.Read(buf.p)
	if err != nil {
		return err
	}
	if len(buf.p[n:]) == 0 {
		// Buffer is full!
		n := Commit(buf.wg, buf.hostname, buf.buf)
		buf.p = buf.buf[n:]
	} else {
		// Advance p
		buf.p = buf.p[n:]
	}
	return nil
}

func (buf *CommitBuffer) Commit() {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.deadliner.Met()

	n := Commit(buf.wg, buf.hostname, buf.buf[:len(buf.buf)-len(buf.p)])
	buf.p = buf.buf[n:]
}

func main() {

	const (
		MiB = 1 << 20
		kiB = 1 << 10
		// Period     = 5 * time.Minute // Maximum time between commits
		Period     = 5 * time.Second // Maximum time between commits
		BufferSize = 10 * kiB        // Size of buffer before flushing
	)

	defer func() {
		log.Printf("Exiting, total bytes: %v", n)
	}()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to determine hostname:", err)
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	var (
		deadliner = NewDeadliner(Period)
		done      = make(chan struct{})

		buf = NewCommitBuffer(BufferSize, &wg, deadliner, hostname)
	)

	go func() {
		defer close(done)
		in := Input(os.Args[1:])
		defer func() {
			// Note: blocks until Input() is cleaned up
			//       (e.g, process waited for.)
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
			err := buf.Fill(pollFD)
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
	defer buf.Commit()

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
		buf.Commit()
	}
}
