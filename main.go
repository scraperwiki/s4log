package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/scraperwiki/s4log/poller"
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

	async := &AsyncCommitter{Committer: committer}
	defer async.Wait()
	committer = async

	deadliner := NewDeadliner(Period)
	committer = DeadlineMetCommitter{deadliner, committer}
	committer = NewlineCommitter{committer}

	var (
		buf = NewFlushBuffer(BufferSize, committer)

		in = Input(flag.Args())
	)

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

	// Do a final commit
	defer buf.Flush()

	for {
		err := buf.ReadFrom(in)

		switch err {
		case nil: // Everything is fine.
		case poller.ErrTimeout: // The deadline has passed.
			buf.Flush()
		case ErrBufFull: // Buffer is full.
			buf.Flush()
		case io.EOF:
			log.Println("EOF")
			return
		default:
			log.Printf("Error during read: %v", err)
			return
		}
	}
}
