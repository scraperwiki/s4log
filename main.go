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

func Stream(args []string) (in io.Reader, wait func()) {
	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalln("Unable to allocate StdoutPipe:", err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatalln("Error starting command:", err)
	}
	return stdout, func() {
		err := cmd.Wait()
		if err != nil {
			// TODO(pwaller): Better handling
			log.Println("Command had an error:", err)
		}
	}
}

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
// Commit should run very quickly.
func Commit(buf []byte) int {
	idx := bytes.LastIndex(buf, []byte("\n"))

	var p []byte
	if idx == -1 {
		// No newline, take everything
		p = buf
	} else {
		// take up to the last newline
		p = buf[:idx]
	}

	// Copy the data to a fresh buffer
	newbuf := make([]byte, len(p))
	copy(newbuf, p)

	log.Printf("Commit %d bytes", len(newbuf))
	fmt.Println(string(newbuf))
	go func() {
		// Commence a copy to permanent storage on the fresh buffer
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

	fill := func(in *poller.FD) error {
		mu.Lock()
		defer mu.Unlock()

		in.SetReadDeadline(deadliner.Deadline())

		// TODO: Use a read deadline to ensure that the commit deadline has
		// a chance.
		n, err := in.Read(p)
		// log.Println("Read", n, err)
		if err != nil {
			return err
		}
		if len(p[n:]) == 0 {
			// Buffer is full!
			n := Commit(buf)
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

		n := Commit(buf[:len(buf)-len(p)])
		p = buf[n:]
	}

	go func() {
		defer close(done)
		in, wait := Stream(os.Args[1:])
		defer wait()

		fd := int(in.(*os.File).Fd())
		pollFD, err := poller.NewFD(fd)
		if err != nil {
			log.Fatal("Problem:", err)
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
				log.Println("Error during read:", err)
				return
			}
		}
	}()

	defer func() {
		log.Println("Total bytes", n)
	}()

	defer func() {
		log.Println("Final commit")
		commit()
	}()

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
