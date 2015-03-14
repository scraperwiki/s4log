package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/scraperwiki/s4log/poller"
)

var ErrBufFull = errors.New("Buffer full")

type CommitBuffer struct {
	// `buf` is a once-initialized buf, and `cursor` points into it.
	buf, cursor []byte

	Committer
}

func NewCommitBuffer(
	size int,
	c Committer,
) *CommitBuffer {
	buf := make([]byte, size)
	return &CommitBuffer{buf: buf, cursor: buf, Committer: c}
}

func (buf *CommitBuffer) Fill(in io.Reader) error {
	n, err := in.Read(buf.cursor)
	if err != nil {
		return err
	}
	// Advance p
	buf.cursor = buf.cursor[n:]
	if len(buf.cursor) == 0 {
		// Buffer is full!
		return ErrBufFull
	}
	return nil
}

func (buf *CommitBuffer) Size() int {
	return len(buf.buf) - len(buf.cursor)
}

func (buf *CommitBuffer) Commit() {
	if buf.Size() == 0 {
		// No bytes to commit
		return
	}

	ready := buf.buf[:buf.Size()]
	remaining := buf.Committer.Commit(ready)
	buf.MoveTrailer(remaining)
}

// Moven bytes of trailing data to beginning of `buf` and truncate `buf`.
func (buf *CommitBuffer) MoveTrailer(n int) {
	trailer := buf.buf[buf.Size()-n : buf.Size()]

	copy(buf.buf, trailer)
	buf.cursor = buf.buf[n:]
}

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
		buf = NewCommitBuffer(BufferSize, committer)

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
	defer buf.Commit()

	for {
		// buf will read from `in` and issue a `Commit()` if the buffer is full.
		err := buf.Fill(in)

		switch err {
		case nil:
			continue
		case poller.ErrTimeout:
			// The deadline has timed out. Issue a commit.
			buf.Commit()
			continue
		case ErrBufFull:
			// Buffer is full. Issue a commit.
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
