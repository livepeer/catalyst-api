package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/dms-api/handlers"
	"github.com/livepeer/dms-api/middleware"
)

func main() {
	port := flag.Int("port", 4949, "Port to listen on (default 4949)")
	flag.Parse()

	listen := fmt.Sprintf("localhost:%d", *port)
	router := StartDMSAPIRouter()

	log.Println("Starting DMS API server listening on", listen)
	err := http.ListenAndServe(listen, router)
	log.Fatal(err)
}

func StartDMSAPIRouter() *httprouter.Router {
	router := httprouter.New()

	router.GET("/ok", middleware.IsAuthorized(handlers.DMSAPIHandlers.Ok()))
	router.POST("/api/vod", middleware.IsAuthorized(handlers.DMSAPIHandlers.UploadVOD()))

	return router
}
