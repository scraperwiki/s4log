package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"
)

func main() {
	const (
		MiB = 1 << 20
		kiB = 1 << 10
		// Period     = 5 * time.Minute // Maximum time between commits
		Period     = 100 * time.Millisecond // Maximum time between commits
		BufferSize = 10 * kiB               // Size of buffer before flushing
	)

	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal("Usage: s4log <command> [args...]")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to determine hostname:", err)
	}

	var committer Committer

	committer = &FileCommitter{hostname}

	committer = &SlowCommitter{committer}

	async := AsyncCommitter{NewSemaphore(4), committer}
	defer async.Wait()
	committer = async

	deadliner := NewDeadliner(Period)
	// During a commit, the Deadline is reset.
	committer = DeadlineMetCommitter{deadliner, committer}
	committer = NewlineCommitter{committer}

	var (
		buf = NewFlushBuffer(BufferSize, committer)

		// Invoke the target command and read its stdout.
		in = Input(flag.Args())
	)

	// Reading from `in` will stop blocking if there is data available or
	// when the deadline passes.
	in, err = NewDeadlineReader(in.(Fder), deadliner)
	if err != nil {
		log.Fatalf("Unable to construct DeadlineReader: %v", err)
	}
	defer func() {
		// Note: blocks until Input() is cleaned up
		//       (e.g, process waited for.)
		err := in.Close()
		if err != nil {
			log.Printf("Failure during Input.Close: %v", err)
		}
	}()

	// Read from `in` until EOF.
	err = buf.ReadFrom(in)
	switch err {
	case io.EOF:
		log.Printf("EOF")
	default:
		log.Printf("error whilst reading: %v", err)
	}
}
