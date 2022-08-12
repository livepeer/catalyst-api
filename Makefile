GO_BUILD_DIR?=build/

ldflags := -X 'github.com/livepeer/catalyst-api/config.Version=$(shell git rev-parse HEAD)'

.PHONY: all
all: build-server fmt test

.PHONY: build-server
build-server:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)catalyst-api" cmd/http-server/http-server.go

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test
test:
	go test -race ./...
