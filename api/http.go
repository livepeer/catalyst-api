package api

import (
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func AddRoutes(router *httprouter.Router, vodEngine *pipeline.Coordinator, apiToken string) *httprouter.Router {
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

	router.POST("/api/transcode/file",
		withLogging(
			withAuth(
				apiToken,
				withCapacityChecking(
					vodEngine,
					catalystApiHandlers.TranscodeSegment(),
				),
			),
		),
	)

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
