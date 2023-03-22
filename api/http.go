package api

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServe(apiPort int, apiToken string, vodEngine *pipeline.Coordinator) error {
	listen := fmt.Sprintf("0.0.0.0:%d", apiPort)
	router := NewCatalystAPIRouter(vodEngine, apiToken)

	log.LogNoRequestID(
		"Starting Catalyst API!",
		"version", config.Version,
		"host", listen,
	)
	return http.ListenAndServe(listen, router)
}

func NewCatalystAPIRouter(vodEngine *pipeline.Coordinator, apiToken string) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized
	withCapacityChecking := middleware.HasCapacity

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{VODEngine: vodEngine}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Public Catalyst API
	router.POST("/api/vod",
		withLogging(
			withAuth(
				apiToken,
				withCapacityChecking(
					vodEngine,
					catalystApiHandlers.UploadVOD(),
				),
			),
		),
	)

	// Manifest endpoint
	router.GET("/asset/hls/:playbackID/*file",
		withLogging(
			handlers.ManifestHandler(),
		),
	)

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
