package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
)

func main() {
	port := flag.Int("port", 4949, "Port to listen on")
	mistJson := flag.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	flag.Parse()

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	listen := fmt.Sprintf("localhost:%d", *port)
	router := StartCatalystAPIRouter()

	log.Println("Starting Catalyst API version", config.Version, "listening on", listen)
	err := http.ListenAndServe(listen, router)
	log.Fatal(err)

}

func StartCatalystAPIRouter() *httprouter.Router {
	router := httprouter.New()

	router.GET("/ok", middleware.IsAuthorized(handlers.CatalystAPIHandlers.Ok()))
	router.POST("/api/transcode/file", middleware.IsAuthorized(handlers.CatalystAPIHandlers.TranscodeSegment()))
	router.POST("/api/vod", middleware.IsAuthorized(handlers.CatalystAPIHandlers.UploadVOD()))

	return router
}
