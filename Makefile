GO_BUILD_DIR?=build/

ldflags := -X 'github.com/livepeer/catalyst-api/config.Version=$(shell git rev-parse HEAD)'

.PHONY: all
all: build fmt test lint

.PHONY: build
build:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)catalyst-api" cmd/http-server.go

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: run
run:
	go run cmd/http-server.go

.PHONY: test
test:
	go test ./...
