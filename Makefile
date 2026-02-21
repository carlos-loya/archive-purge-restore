MODULE := github.com/carlos-loya/archive-purge-restore
BINARY := apr
VERSION ?= dev
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build test clean lint

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/apr

test:
	go test ./... -v

clean:
	rm -f $(BINARY)

lint:
	go vet ./...
