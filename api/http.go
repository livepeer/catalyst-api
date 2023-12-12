package api

import (
	"context"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
)

func ListenAndServe(ctx context.Context, cli config.Cli, vodEngine *pipeline.Coordinator, bal balancer.Balancer, c cluster.Cluster, mapic mistapiconnector.IMac) error {
	router := NewCatalystAPIRouter(cli, vodEngine, bal, c, mapic)
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

func NewCatalystAPIRouter(cli config.Cli, vodEngine *pipeline.Coordinator, bal balancer.Balancer, c cluster.Cluster, mapic mistapiconnector.IMac) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withCORS := middleware.AllowCORS()
	withGatingCheck := middleware.NewGatingHandler(cli).GatingCheck

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	geoHandlers := &geolocation.GeolocationHandlersCollection{
		Balancer: bal,
		Cluster:  c,
		Config:   cli,
		Mapic:    mapic,
	}

	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	// Playback endpoint
	playback := middleware.LogAndMetrics(metrics.Metrics.PlaybackRequestDurationSec)(
		withCORS(
			withGatingCheck(
				handlers.NewPlaybackHandler(cli.PrivateBucketURLs).Handle,
			),
		),
	)
	router.GET("/asset/hls/:playbackID/*file", playback)
	router.HEAD("/asset/hls/:playbackID/*file", playback)
	for _, path := range [...]string{"/asset/hls/:playbackID/*file", "/webrtc/:playbackID"} {
		router.OPTIONS(path, playback)
	}

	// Handling incoming playback redirection requests
	redirectHandler := withLogging(withCORS(geoHandlers.RedirectHandler()))
	router.NotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectHandler(w, r, httprouter.Params{})
	})

	return router
}
