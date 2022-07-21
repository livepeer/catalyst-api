.PHONY: all
all: build-server

.PHONY: build-server
build-server:
	go build -ldflags="$(GO_LDFLAG_VERSION)" -o build/http-server cmd/http-server/http-server.go