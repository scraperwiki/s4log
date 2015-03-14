package main

import (
	"errors"
	"io"
)

var ErrBufFull = errors.New("Buffer full")

// CommitBuffer is similar to a bytes.Buffer, except it has a bounded size.
// When the buffer is full, a Committer is called to process the data, and
// any remaining trailer is moved to the beginning of the buffer.
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

func (buf *CommitBuffer) ReadFrom(in io.Reader) error {
	n, err := in.Read(buf.cursor)
	if err != nil {
		return err
	}
	// Advance the cursor.
	buf.cursor = buf.cursor[n:]
	if len(buf.cursor) == 0 {
		// No space left in buffer.
		return ErrBufFull
	}
	return nil
}

// Amount of data currently in the buffer waiting to be flushed.
func (buf *CommitBuffer) Len() int {
	return len(buf.buf) - len(buf.cursor)
}

// Called to flush the buffer to the underlying Committer.
func (buf *CommitBuffer) Commit() {
	if buf.Len() == 0 {
		// No bytes to commit.
		return
	}

	ready := buf.buf[:buf.Len()]
	remaining := buf.Committer.Commit(ready)
	buf.MoveTrailer(remaining)
}

// Move n bytes of trailing data to beginning of `buf` and truncate `buf`.
func (buf *CommitBuffer) MoveTrailer(n int) {
	trailer := buf.buf[buf.Len()-n : buf.Len()]

	copy(buf.buf, trailer)
	buf.cursor = buf.buf[n:]
}
