MODULE := github.com/carlos-loya/archive-purge-restore
BINARY := apr
VERSION ?= dev
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build test clean lint dev-up dev-down dev-reset test-integration test-all

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/apr

test:
	go test ./... -v

clean:
	rm -f $(BINARY)

lint:
	go vet ./...

dev-up:
	docker compose -f dev/docker-compose.yml up -d --wait

dev-down:
	docker compose -f dev/docker-compose.yml down -v

dev-reset: dev-down dev-up

test-integration: dev-up
	go test -tags integration ./integration/... -v

test-all: test test-integration
