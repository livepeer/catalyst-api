GO_BUILD_DIR?=build/

.PHONY: all
all: build-server

.PHONY: build-server
build-server:
	go build -ldflags="$(GO_LDFLAG_VERSION)" -o "$(GO_BUILD_DIR)catalyst-api" cmd/http-server/http-server.go
