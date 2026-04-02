GO ?= go
BINARY ?= bin/opencook

.PHONY: build run test fmt vet verify

build:
	$(GO) build -o $(BINARY) ./cmd/opencook

run:
	$(GO) run ./cmd/opencook

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

verify: fmt vet test

