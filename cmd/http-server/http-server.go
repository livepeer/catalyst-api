package main

import (
	"log"
	"net/http"

	"github.com/livepeer/dms-api/handlers"
	"github.com/livepeer/dms-api/middleware"
)

func main() {
	server := StartDMSAPIServer("localhost:8080")
	err := server.ListenAndServe()
	log.Fatal(err)
}

func StartDMSAPIServer(addr string) http.Server {
	server := http.Server{Addr: addr}

	mux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()

	mux.Handle("/ok", middleware.IsAuthorized(handlers.DMSAPIHandlers.Ok()))
	mux.Handle("/api/vod", middleware.IsAuthorized(handlers.DMSAPIHandlers.UploadVOD()))

	log.Println("DMS API server listening on", addr)
	server.Handler = mux

	return server
}
