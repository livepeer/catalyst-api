package main

import (
	"flag"
	"fmt"
	stdlog "log"
	"net/http"
	"os"

	log "github.com/go-kit/kit/log"

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

	listen := fmt.Sprintf("0.0.0.0:%d", *port)
	router := StartCatalystAPIRouter()

	stdlog.Println("Starting Catalyst API version", config.Version, "listening on", listen)
	err := http.ListenAndServe(listen, router)
	stdlog.Fatal(err)

}

func StartCatalystAPIRouter() *httprouter.Router {
	router := httprouter.New()

	var logger log.Logger
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	withLogging := middleware.LogRequest(logger)

	router.GET("/ok", withLogging(middleware.IsAuthorized(handlers.CatalystAPIHandlers.Ok())))
	router.POST("/api/vod", withLogging(middleware.IsAuthorized(handlers.CatalystAPIHandlers.UploadVOD())))

	return router
}
