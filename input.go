package main

import (
	"io"
	"log"
	"os/exec"
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
