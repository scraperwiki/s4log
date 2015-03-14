package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/scraperwiki/s4log/poller"
)

func Input(args []string) (in io.Reader, wait func()) {
	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Unable to allocate StdoutPipe: %v", err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatalf("Error starting command: %v", err)
	}
	return stdout, func() {
		err := cmd.Wait()
		if err != nil {
			log.Printf("Command had an error: %v", err)
		}
	}
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

	log.Printf("Commit %d bytes", len(newbuf))
	fmt.Println(string(newbuf))
	// Commence an asynchronous copy of the buffer to permanent storage.
	wg.Add(1)
	go func() {
		defer wg.Done()

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
		Period     = 2 * time.Second // Maximum time between commits
		BufferSize = 1024            // Size of buffer before flushing
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
		in, wait := Input(os.Args[1:])
		defer wait()

		fd := int(in.(*os.File).Fd())
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
