package main

import (
	"errors"
	"io"
)

var ErrBufFull = errors.New("Buffer full")

// FlushBuffer is similar to a bytes.Buffer, except it has a bounded size.
// When the buffer is full, a Committer is called to process the data, and
// any remaining trailer is moved to the beginning of the buffer.
type FlushBuffer struct {
	// `buf` is a once-initialized buf, and `cursor` points into it.
	buf, cursor []byte

	Committer
}

func NewFlushBuffer(
	size int,
	c Committer,
) *FlushBuffer {
	buf := make([]byte, size)
	return &FlushBuffer{buf: buf, cursor: buf, Committer: c}
}

func (fb *FlushBuffer) ReadFrom(in io.Reader) error {
	n, err := in.Read(fb.cursor)
	if err != nil {
		return err
	}
	// Advance the cursor.
	fb.cursor = fb.cursor[n:]
	if len(fb.cursor) == 0 {
		// No space left in buffer.
		return ErrBufFull
	}
	return nil
}

// Amount of data currently in the buffer waiting to be flushed.
func (fb *FlushBuffer) Len() int {
	return len(fb.buf) - len(fb.cursor)
}

// Called to flush the buffer to the underlying Committer.
func (fb *FlushBuffer) Flush() {
	if fb.Len() == 0 {
		// No bytes to commit.
		return
	}

	ready := fb.buf[:fb.Len()]
	remaining := fb.Committer.Commit(ready)
	fb.MoveTrailer(remaining)
}

// Move n bytes of trailing data to beginning of `buf` and truncate `buf`.
func (fb *FlushBuffer) MoveTrailer(n int) {
	trailer := fb.buf[fb.Len()-n : fb.Len()]

	copy(fb.buf, trailer)
	fb.cursor = fb.buf[n:]
}
