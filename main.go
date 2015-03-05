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
	log.Println("Invoke", args)
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
		Frequency  = 2 * time.Second // Minimum frequency before committing
		BufferSize = 1024            // Size of buffer before flushing
	)

	var (
		mu   sync.Mutex
		done bool
	)
	buf := make([]byte, BufferSize)
	p := buf

	go func() {
		in := Stream(os.Args[1:])

		Fill := func() {
			// TODO: Handle buffer full
			// TODO: Ensure there is a read deadline
			n, err := in.Read(p)
			if err != nil {
				done = true
				return
			}
			if len(p[n:]) == 0 {
				// Buffer is full!
				log.Println("Buffer filled")
				Commit(buf)
				p = buf
			} else {
				// Advance p
				p = p[n:]
			}
		}

		for {
			mu.Lock()
			Fill()
			if done {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
	}()

	getNextDeadline := func() time.Time { return time.Now().Add(Frequency) }
	until := func(t time.Time) time.Duration { return -time.Since(t) }

	nextDeadline := getNextDeadline()
	for {
		time.Sleep(until(nextDeadline))
		nextDeadline = getNextDeadline()

		mu.Lock()
		log.Println("Deadline commit")
		Commit(buf[:len(buf)-len(p)])
		if done {
			mu.Unlock()
			return
		}
		mu.Unlock()
	}
}
