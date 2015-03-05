package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

func Stream(args []string) io.Reader {
	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalln("Unable to allocate StdoutPipe:", err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatalln("Error starting command:", err)
	}
	go func() {
		err := cmd.Wait()
		if err != nil {
			// TODO(pwaller): Better handling
			log.Println("Command had an error:", err)
		}
	}()
	return stdout
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
		Frequency  = 2 * time.Second // Maximum time between commits
		BufferSize = 1024            // Size of buffer before flushing
	)

	var (
		mu   sync.Mutex
		done = make(chan struct{})
	)

	buf := make([]byte, BufferSize)
	p := buf

	fill := func(in io.Reader) error {
		mu.Lock()
		defer mu.Unlock()

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
		in := Stream(os.Args[1:])
		for {
			err := fill(in)
			if err != nil {
				close(done)
				return
			}
		}
	}()

	getNextDeadline := func() time.Time { return time.Now().Add(Frequency) }
	until := func(t time.Time) time.Duration { return -time.Since(t) }

	defer func() {
		log.Println("Final commit")
		commit()
	}()

	nextDeadline := getNextDeadline()
	for {
		select {
		case <-time.After(until(nextDeadline)):
		case <-done:
			return
		}
		nextDeadline = getNextDeadline()

		log.Println("Commit")
		commit()
	}

}
