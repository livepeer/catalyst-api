package api

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/accesscontrol"
	"github.com/livepeer/catalyst-api/handlers/admin"
	"github.com/livepeer/catalyst-api/handlers/ffmpeg"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/state"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func ListenAndServeInternal(ctx context.Context, cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster, machine *state.Machine) error {
	router := NewCatalystAPIRouterInternal(cli, vodEngine, mapic, bal, c, machine)
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

func NewCatalystAPIRouterInternal(cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster, machine *state.Machine) *httprouter.Router {
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
	ffmpegSegmentingHandlers := &ffmpeg.HandlersCollection{VODEngine: vodEngine}
	accessControlHandlers := accesscontrol.NewAccessControlHandlersCollection(cli)
	encryptionHandlers := accesscontrol.NewEncryptionHandlersCollection(cli)
	adminHandlers := &admin.AdminHandlersCollection{Cluster: c, Machine: machine}
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{}

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	if cli.ShouldMapic() {
		// Endpoint to receive "Triggers" (callbacks) from Mist
		router.POST("/mapic", withLogging(mapic.HandleDefaultStreamTrigger()))

		// Hacky combined metrics handler. To be refactored away with mapic.
		router.GET("/metrics", concatHandlers(promhttp.Handler(), mapic.MetricsHandler()))
	} else {
		router.Handler("GET", "/metrics", promhttp.Handler())
	}

	// Endpoint for handling STREAM_SOURCE requests
	router.POST("/STREAM_SOURCE", withLogging(geoHandlers.StreamSourceHandler()))

	// Endpoint for handling USER_NEW requests
	router.POST("/USER_NEW", withLogging(accessControlHandlers.TriggerHandler()))

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

	// Public GET handler to retrieve the public key for vod encryption
	router.GET("/api/pubkey", withLogging(encryptionHandlers.PublicKeyHandler()))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	// Endpoint to receive segments and manifests that ffmpeg produces
	router.PUT("/api/ffmpeg/:id/:filename", withLogging(ffmpegSegmentingHandlers.NewFile()))

	// Temporary endpoint for admin queries
	router.GET("/admin/members", withLogging(adminHandlers.MembersHandler()))
	router.GET("/admin/state", withLogging(adminHandlers.StateHandler()))

	return router
}

// Hack to combine main metrics and mapic metrics. To be refactored away with mapic.
func concatHandlers(handlers ...http.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var outbuf bytes.Buffer
		writer := bufio.NewWriter(&outbuf)
		for _, handler := range handlers {
			rec := httptest.NewRecorder()
			rec.Body = &bytes.Buffer{}
			handler.ServeHTTP(rec, r)
			for key, val := range rec.Header() {
				w.Header().Set(key, val[0])
			}
			rec.Body.WriteTo(writer) // nolint:errcheck
			writer.WriteString("\n") // nolint:errcheck
		}
		outbuf.WriteTo(w) // nolint:errcheck
	}
}
