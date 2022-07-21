package main

import (
	"log"
	"net/http"

	"github.com/livepeer/dms-api/handlers"
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

	mux.Handle("/ok", handlers.DMSAPIHandlers.Ok())

	log.Println("DMS API server listening on", addr)
	server.Handler = mux

	return server
}
