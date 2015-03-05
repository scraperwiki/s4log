package main

import (
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
		log.Println("Cmd quit")
	}
}

// Commits `buf` to permanent storage, up to the final newline.
// If there is data following the final newline, it is moved to the beginning.
// Commit should run very quickly.
func Commit(buf []byte) {
	// Find the final newline

	// Copy the data to a fresh buffer

	// Move trailing data to beginning of `buf` and truncate `buf`

	// Commence a copy to permanent storage on the fresh buffer
	log.Printf("Commit %d bytes", len(buf))
}

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
		if err != nil {
			return err
		}
		if len(p[n:]) == 0 {
			// Buffer is full!
			Commit(buf)
			p = buf
		} else {
			// Advance p
			p = p[n:]
		}
		return nil
	}

	commit := func() {
		mu.Lock()
		defer mu.Unlock()

		Commit(buf[:len(buf)-len(p)])
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
			case nil, poller.ErrTimeout:
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
		log.Println("Final commit")
		commit()
	}()

	for {
		select {
		case <-time.After(deadliner.Until()):
			log.Println("Deadline")
		case <-done:
			log.Println("Done")
			return
		}

		deadliner.Met()

		log.Println("Commit")
		commit()
	}

}
