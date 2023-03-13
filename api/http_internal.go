package api

import (
	"context"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServeInternal(ctx context.Context, cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster) error {
	router := NewCatalystAPIRouterInternal(cli, vodEngine, mapic, bal, c)
	server := http.Server{Addr: cli.HTTPInternalAddress, Handler: router}
	ctx, cancel := context.WithCancel(ctx)

	log.LogNoRequestID(
		"Starting Catalyst Internal API!",
		"version", config.Version,
		"host", cli.HTTPInternalAddress,
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

func NewCatalystAPIRouterInternal(cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized
	withCapacityChecking := middleware.HasCapacity

	geoHandlers := &geolocation.GeolocationHandlersCollection{
		Config:   cli,
		Balancer: bal,
		Cluster:  c,
	}
	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{VODEngine: vodEngine}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/mapic", withLogging(mapic.HandleDefaultStreamTrigger()))

	// Endpoint for handling STREAM_SOURCE requests
	router.POST("/STREAM_SOURCE", withLogging(geoHandlers.StreamSourceHandler()))

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Public Catalyst API
	router.POST("/api/vod",
		withLogging(
			withAuth(
				cli.APIToken,
				withCapacityChecking(
					vodEngine,
					catalystApiHandlers.UploadVOD(),
				),
			),
		),
	)

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	return router
}
