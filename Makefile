# Go computes the dependency graph itself; this file only adds flags and names.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build run test lint cover clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/ashored ./cmd/ashored
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/ashore ./cmd/ashore

# Text logs read better in a terminal; production keeps the JSON default.
run: build
	ASHORE_LOG_FORMAT=text ./bin/ashored

test:
	go test -race -count=1 ./...

lint:
	go vet ./...
	golangci-lint run ./...

cover:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

clean:
	rm -rf bin coverage.out
