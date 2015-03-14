package main

import (
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/scraperwiki/s4log/poller"
)

type CommitBuffer struct {
	mu     sync.Mutex
	buf, p []byte

	Committer
}

func NewCommitBuffer(
	size int,
	c Committer,
) *CommitBuffer {
	buf := make([]byte, size)
	return &CommitBuffer{buf: buf, p: buf, Committer: c}
}

func (buf *CommitBuffer) Fill(in io.Reader) error {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	n, err := in.Read(buf.p)
	if err != nil {
		return err
	}
	if len(buf.p[n:]) == 0 {
		// Buffer is full!
		n := buf.Committer.Commit(buf.buf)
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

	n := buf.Committer.Commit(buf.buf[:len(buf.buf)-len(buf.p)])
	buf.p = buf.buf[n:]
}

func main() {

	const (
		MiB = 1 << 20
		kiB = 1 << 10
		// Period     = 5 * time.Minute // Maximum time between commits
		Period     = 100 * time.Millisecond // Maximum time between commits
		BufferSize = 10 * kiB               // Size of buffer before flushing
	)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to determine hostname:", err)
	}

	afc := &AsyncCommitter{Committer: &FileCommitter{hostname}}
	defer afc.Wait()

	var (
		deadliner = NewDeadliner(Period)
		committer = DeadlineMetCommitter{deadliner, afc}

		buf = NewCommitBuffer(BufferSize, committer)

		in = Input(os.Args[1:])
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
	defer buf.Commit()

	for {
		err := buf.Fill(in)

		switch err {
		case nil:
			continue
		case poller.ErrTimeout:
			// The deadline has timed out. Issue a commit.
			buf.Commit()
			continue
		case io.EOF:
			log.Println("EOF")
			return
		default:
			log.Printf("Error during read: %v", err)
			return
		}
	}
}
