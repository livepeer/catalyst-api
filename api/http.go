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
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServe(ctx context.Context, cli config.Cli, vodEngine *pipeline.Coordinator, bal balancer.Balancer, c cluster.Cluster) error {
	router := NewCatalystAPIRouter(cli, vodEngine, bal, c)
	server := http.Server{Addr: cli.HTTPAddress, Handler: router}
	ctx, cancel := context.WithCancel(ctx)

	log.LogNoRequestID(
		"Starting Catalyst API!",
		"version", config.Version,
		"host", cli.HTTPAddress,
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

func NewCatalystAPIRouter(cli config.Cli, vodEngine *pipeline.Coordinator, bal balancer.Balancer, c cluster.Cluster) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withCORS := middleware.AllowCORS()
	withGatingCheck := middleware.NewGatingHandler(cli).GatingCheck

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	geoHandlers := &geolocation.GeolocationHandlersCollection{
		Balancer: bal,
		Cluster:  c,
		Config:   cli,
	}

	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Playback endpoint
	playback := withLogging(
		withCORS(
			withGatingCheck(
				handlers.PlaybackHandler(),
			),
		),
	)
	router.GET("/asset/hls/:playbackID/*file", playback)
	router.HEAD("/asset/hls/:playbackID/*file", playback)
	router.OPTIONS("/asset/hls/:playbackID/*file", withLogging(withCORS(handlers.PlaybackOptionsHandler())))

	// Handling incoming playback redirection requests
	redirectHandler := withLogging(withCORS(geoHandlers.RedirectHandler()))
	router.NotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectHandler(w, r, httprouter.Params{})
	})

	return router
}
