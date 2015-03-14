package main

import (
	"errors"
	"io"
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
