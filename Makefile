all: build

race: deps
	go build -race

build: deps
	go build

deps: update-submodules goimports

goimports: .PHONY
	goimports -w *.go

update-submodules: .PHONY
	git submodule update --init

.PHONY: