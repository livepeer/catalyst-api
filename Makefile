GO_BUILD_DIR?=build/

ldflags := -X 'github.com/livepeer/catalyst-api/config.Version=$(shell git rev-parse HEAD)'

.PHONY: all
all: build fmt test lint integration-test tidy

.PHONY: build
build:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)catalyst-api" main.go
	cp scripts/* "$(GO_BUILD_DIR)"

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
	CATALYST_API_HTTP_ADDR=127.0.0.1:4949 CATALYST_API_HTTP_INTERNAL_ADDR=127.0.0.1:3939 go run main.go

.PHONY: generate
generate:
	go generate ./...

.PHONY: test
test: generate
	go test -race ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: test-canary
test-canary:
	cd test && CUCUMBER_ENV=canary godog run --strict --stop-on-failure 2> ./logs/test.log
