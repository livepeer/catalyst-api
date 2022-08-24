package main

import (
	"flag"
	"fmt"
	stdlog "log"
	"net/http"
	"os"

	"github.com/go-kit/kit/log"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
)

func main() {
	mistPort := flag.Int("mist-port", 4242, "Port to listen on")
	port := flag.Int("port", 4949, "Port to listen on")
	mistJson := flag.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	// Transcode endpoint starts MistProcLivepeer as subprocess so we need path to the binary(mistlp) and port of local Broadcaster(b-port)
	bPort := flag.Int("b-port", 8935, "Port of local B node")
	mistProcPath := flag.String("mistlp", "../mistserver/build/MistProcLivepeer", "path to MistProcLivepeer")

	flag.Parse()

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	mc := &handlers.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", *mistPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", *port),
	}

	listen := fmt.Sprintf("0.0.0.0:%d", *port)
	router := StartCatalystAPIRouter(mc, *bPort, *mistProcPath)

	stdlog.Println("Starting Catalyst API version", config.Version, "listening on", listen)
	err := http.ListenAndServe(listen, router)
	stdlog.Fatal(err)

}

func StartCatalystAPIRouter(mc *handlers.MistClient, bPort int, mistProcPath string) *httprouter.Router {
	router := httprouter.New()

	var logger log.Logger
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	withLogging := middleware.LogRequest(logger)

	sc := handlers.NewStreamCache()
	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{MistClient: mc, StreamCache: sc}
	mistCallbackHandlers := &handlers.MistCallbackHandlersCollection{MistClient: mc, StreamCache: sc}

	router.GET("/ok", withLogging(middleware.IsAuthorized(catalystApiHandlers.Ok())))
	router.POST("/api/vod", withLogging(middleware.IsAuthorized(catalystApiHandlers.UploadVOD())))
	router.POST("/api/transcode/file", withLogging(middleware.IsAuthorized(catalystApiHandlers.TranscodeSegment(bPort, mistProcPath))))
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
