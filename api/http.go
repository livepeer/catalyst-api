package api

import (
	"context"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServe(ctx context.Context, addr string, apiToken string, vodEngine *pipeline.Coordinator) error {
	router := NewCatalystAPIRouter(vodEngine, apiToken)
	server := http.Server{Addr: addr, Handler: router}
	ctx, cancel := context.WithCancel(ctx)

	log.LogNoRequestID(
		"Starting Catalyst API!",
		"version", config.Version,
		"host", addr,
	)

	var err error
	go func() {
		err = server.ListenAndServe()
		cancel()
	}()

	<-ctx.Done()
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
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
