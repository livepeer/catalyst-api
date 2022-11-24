package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServe(apiPort, mistPort, mistHttpPort int, apiToken string) error {
	mc := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", mistPort),
		HttpReqUrl:      fmt.Sprintf("http://localhost:%d", mistHttpPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", apiPort),
	}

	// Kick off the callback client, to send job update messages on a regular interval
	statusClient := clients.NewPeriodicCallbackClient(15 * time.Second).Start()
	vodEngine := pipeline.NewCoordinator(mc, statusClient)

	listen := fmt.Sprintf("0.0.0.0:%d", apiPort)
	router := NewCatalystAPIRouter(vodEngine, apiToken)

	log.LogNoRequestID(
		"Starting Catalyst API!",
		"version", config.Version,
		"host", listen,
	)
	return http.ListenAndServe(listen, router)
}

func NewCatalystAPIRouter(vodEngine pipeline.Coordinator, apiToken string) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{VODEngine: vodEngine}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Public Catalyst API
	router.POST("/api/vod", withLogging(withAuth(apiToken, catalystApiHandlers.UploadVOD())))
	router.POST("/api/transcode/file", withLogging(withAuth(apiToken, catalystApiHandlers.TranscodeSegment())))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
