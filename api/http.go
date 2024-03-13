package api

import (
	"context"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/analytics"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/go-api-client"
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
	withGatingCheck := middleware.NewGatingHandler(cli, mapic).GatingCheck

	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server:      cli.APIServer,
		AccessToken: cli.APIToken,
	})
	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	geoHandlers := &geolocation.GeolocationHandlersCollection{
		Balancer: bal,
		Cluster:  c,
		Config:   cli,
		Lapi:     lapi,
	}

	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))
	if cli.EnableAnalytics == "true" || cli.EnableAnalytics == "enabled" {
		logProcessor, err := analytics.NewLogProcessor(cli.KafkaBootstrapServers, cli.KafkaUser, cli.KafkaPassword, cli.AnalyticsKafkaTopic)
		if err != nil {
			glog.Fatalf("failed to configure analytics log processor, err=%v", err)
		} else {
			analyticsApiHandlers := handlers.NewAnalyticsHandlersCollection(mapic, lapi, logProcessor)
			router.POST("/analytics/log", withCORS(analyticsApiHandlers.Log()))
			// Redirect GET /analytics/log to the specific catalyst node, e.g. "mdw-staging-staging-catalyst-0.livepeer.monster"
			// This is useful for the player, because then it can stick to one node while sending analytics logs
			router.GET("/analytics/log", withLogging(withCORS(geoHandlers.RedirectConstPathHandler())))
		}
	}

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
