package api

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/middleware"
)

func ListenAndServe(apiPort, mistPort, mistHttpPort int, apiToken string) error {
	mc := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", mistPort),
		HttpReqUrl:      fmt.Sprintf("http://localhost:%d", mistHttpPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", apiPort),
	}

	listen := fmt.Sprintf("0.0.0.0:%d", apiPort)
	router := NewCatalystAPIRouter(mc, apiToken)

	_ = config.Logger.Log(
		"msg", "Starting Catalyst API",
		"version", config.Version,
		"host", listen,
	)
	return http.ListenAndServe(listen, router)
}

func NewCatalystAPIRouter(mc *clients.MistClient, apiToken string) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{MistClient: mc}
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{MistClient: mc}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Public Catalyst API
	router.POST("/api/vod", withLogging(withAuth(apiToken, catalystApiHandlers.UploadVOD())))
	router.POST("/api/transcode/file", withLogging(withAuth(apiToken, catalystApiHandlers.TranscodeSegment())))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
