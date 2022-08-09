package main

import (
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/dms-api/handlers"
	"github.com/livepeer/dms-api/middleware"
)

func main() {
	listen := "localhost:8080"
	router := StartDMSAPIRouter()
	log.Println("DMS API server listening on", listen)

	err := http.ListenAndServe(listen, router)
	log.Fatal(err)
}

func StartDMSAPIRouter() *httprouter.Router {
	router := httprouter.New()

	router.GET("/ok", middleware.IsAuthorized(handlers.DMSAPIHandlers.Ok()))
	router.POST("/api/vod", middleware.IsAuthorized(handlers.DMSAPIHandlers.UploadVOD()))
	router.POST("/api/mist/trigger", handlers.MistCallbackHandlers.Trigger())

	return router
}
