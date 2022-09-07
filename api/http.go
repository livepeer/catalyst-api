package api

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/middleware"
)

func ListenAndServe(apiPort, mistPort int) error {
	mc := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", mistPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", apiPort),
	}

	listen := fmt.Sprintf("0.0.0.0:%d", apiPort)
	router := NewCatalystAPIRouter(mc)

	config.Logger.Log(
		"msg", "Starting Catalyst API",
		"version", config.Version,
		"host", listen,
	)
	return http.ListenAndServe(listen, router)
}

func NewCatalystAPIRouter(mc *clients.MistClient) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized

	sc := make(map[string]handlers.StreamInfo)
	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{MistClient: mc, StreamCache: sc}
	mistCallbackHandlers := &handlers.MistCallbackHandlersCollection{MistClient: mc, StreamCache: sc}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Public Catalyst API
	router.POST("/api/vod", withLogging(withAuth(catalystApiHandlers.UploadVOD())))
	router.POST("/api/transcode/file", withLogging(withAuth(catalystApiHandlers.TranscodeSegment())))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
