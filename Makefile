all: build

build: update-submodules
	goimports -w *.go
	go build

update-submodules: .PHONY
	git submodule update --init

.PHONY: