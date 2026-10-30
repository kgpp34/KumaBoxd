GO ?= go
GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test race vet lint fmt verify clean benchmark e2e

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

benchmark:
	$(GO) test -run '^$$' -bench BenchmarkTemplateStore -benchmem -benchtime=500x -count=5 ./internal/metadata

e2e:
	$(GO) test -tags integration -v ./test/e2e

clean:
	rm -rf bin dist coverage.out
