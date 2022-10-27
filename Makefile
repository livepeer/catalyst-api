GO_BUILD_DIR?=build/

ldflags := -X 'github.com/livepeer/catalyst-api/config.Version=$(shell git rev-parse HEAD)'

.PHONY: all
all: build fmt test lint integration-test

.PHONY: build
build:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)catalyst-api" main.go

.PHONY: build-linux
build-linux:
	# Useful for cross-compiling from Mac for testing on an environment
	env GOOS=linux GOARCH=amd64 go build -o "$(GO_BUILD_DIR)catalyst-api-linux" main.go

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: integration-test
integration-test:
	cd test && godog run --strict --stop-on-failure 2> ./logs/test.log

.PHONY: lint
lint:
	golangci-lint run

.PHONY: run
run:
	go run main.go

.PHONY: test
test:
	go test ./...

.PHONY: test-canary
test-canary:
	cd test && CUCUMBER_ENV=canary godog run --strict --stop-on-failure 2> ./logs/test.log
