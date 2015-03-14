package main

import (
	"io"
	"log"
	"os/exec"

	"github.com/scraperwiki/s4log/poller"
)

type Fder interface {
	Fd() uintptr
}

// Turns a close into a wait. Needed so that Wait() can be called on the *cmd.
type WaitCloser func() error

func (wc WaitCloser) Close() (err error) {
	return wc()
}

func Input(args []string) io.ReadCloser {
	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Unable to allocate StdoutPipe: %v", err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Error starting command: %v", err)
	}

	return struct {
		io.Reader
		io.Closer
		Fder
	}{stdout, WaitCloser(cmd.Wait), stdout.(Fder)}
}

// A DeadlineReader is one where the Read() call will return
// after the deadline has passed.
type DeadlineReader struct {
	*poller.FD
	*Deadliner
	_underlying Fder // Must be kept around to prevent GC from close()'ing it.
}

func NewDeadlineReader(fder Fder, d *Deadliner) (*DeadlineReader, error) {
	fd := int(fder.Fd())
	rd, err := poller.NewFD(fd)
	if err != nil {
		log.Fatalf("Problem whilst polling: %v", err)
		return nil, err
	}
	return &DeadlineReader{rd, d, fder}, nil
}

func (dr *DeadlineReader) Read(buf []byte) (int, error) {
	deadline := dr.Deadline()
	err := dr.FD.SetReadDeadline(deadline)
	if err != nil {
		return 0, err
	}
	return dr.FD.Read(buf)
}
