all: test build

test: deps
	GOPATH=$$PWD/internal/:$$GOPATH go test -v -race

race: deps
	go build -race

build: deps
	GOPATH=$$PWD/internal/:$$GOPATH go build

deps: update-submodules goimports

goimports: .PHONY
	goimports -w *.go

update-submodules: .PHONY
	git submodule update --init

.PHONY: