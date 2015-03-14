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

// Run the sepecified command and return its stdout/stderr as an io.ReadCloser.
// Closing the ReadCloser waits on the process.
func Input(args []string) io.ReadCloser {
	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Unable to allocate StdoutPipe: %v", err)
	}

	cmd.Stderr = cmd.Stdout

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Error starting command: %v", err)
	}

	return struct {
		io.Closer
		io.Reader
		Fder
	}{WaitCloser(cmd.Wait), stdout, stdout.(Fder)}
}

// A DeadlineReader is one where the Read() call will return
// after the deadline has passed.
type DeadlineReader struct {
	io.Closer
	*poller.FD
	*Deadliner
}

type FdCloser interface {
	Fder
	io.Closer
}

func NewDeadlineReader(fder FdCloser, d *Deadliner) (*DeadlineReader, error) {
	fd := int(fder.Fd())
	rd, err := poller.NewFD(fd)
	if err != nil {
		log.Fatalf("Problem whilst polling: %v", err)
		return nil, err
	}
	return &DeadlineReader{fder, rd, d}, nil
}

func (dr *DeadlineReader) Read(buf []byte) (int, error) {
	deadline := dr.Deadline()
	err := dr.FD.SetReadDeadline(deadline)
	if err != nil {
		return 0, err
	}
	n, err := dr.FD.Read(buf)
	if err == poller.ErrTimeout {
		return n, ErrNeedFlush
	}
	return n, err
}

func (dr *DeadlineReader) Close() error {
	err := dr.FD.Close()
	if err != nil {
		log.Printf("Failed to close poller fd: %v", err)
	}
	err = dr.Closer.Close()
	if err != nil {
		return err
	}
	return nil
}
