GO ?= go
GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test race vet lint fmt verify clean

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '-X main.version=$(VERSION)' -o bin/kumaboxd ./cmd/kumaboxd

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	$(GO) fmt ./...

verify: vet test

clean:
	rm -rf bin dist coverage.out
