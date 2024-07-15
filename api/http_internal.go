package api

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/handlers/accesscontrol"
	"github.com/livepeer/catalyst-api/handlers/admin"
	"github.com/livepeer/catalyst-api/handlers/analytics"
	"github.com/livepeer/catalyst-api/handlers/ffmpeg"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/go-api-client"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func ListenAndServeInternal(ctx context.Context, cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster, broker misttriggers.TriggerBroker, metricsDB *sql.DB) error {
	router := NewCatalystAPIRouterInternal(cli, vodEngine, mapic, bal, c, broker, metricsDB)
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

func NewCatalystAPIRouterInternal(cli config.Cli, vodEngine *pipeline.Coordinator, mapic mistapiconnector.IMac, bal balancer.Balancer, c cluster.Cluster, broker misttriggers.TriggerBroker, metricsDB *sql.DB) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()
	withAuth := middleware.IsAuthorized

	capacityMiddleware := middleware.CapacityMiddleware{}
	withCapacityChecking := capacityMiddleware.HasCapacity

	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server:      cli.APIServer,
		AccessToken: cli.APIToken,
	})
	geoHandlers := geolocation.NewGeolocationHandlersCollection(bal, c, cli, lapi)

	spkiPublicKey, _ := crypto.ConvertToSpki(cli.VodDecryptPublicKey)

	catalystApiHandlers := &handlers.CatalystAPIHandlersCollection{VODEngine: vodEngine}
	eventsHandler := handlers.NewEventsHandlersCollection(c, mapic, bal)
	ffmpegSegmentingHandlers := &ffmpeg.HandlersCollection{VODEngine: vodEngine}
	accessControlHandlers := accesscontrol.NewAccessControlHandlersCollection(cli, mapic)
	analyticsHandlers := analytics.NewAnalyticsHandler(metricsDB)
	encryptionHandlers := accesscontrol.NewEncryptionHandlersCollection(cli, spkiPublicKey)
	adminHandlers := &admin.AdminHandlersCollection{Cluster: c}
	mistCallbackHandlers := misttriggers.NewMistCallbackHandlersCollection(cli, broker)

	// Simple endpoint for healthchecks
	router.GET("/ok", withLogging(catalystApiHandlers.Ok()))

	var metricsHandlers []http.Handler
	if cli.ShouldMapic() {
		metricsHandlers = append(metricsHandlers, mapic.MetricsHandler())
	}
	if cli.MistPrometheus != "" {
		// Enable Mist metrics enrichment
		metricsHandlers = append(metricsHandlers, mapic.MistMetricsHandler())
	}
	metricsHandlers = append(metricsHandlers, promhttp.Handler())
	// Hacky combined metrics handler. To be refactored away with mapic.
	router.GET("/metrics", concatHandlers(metricsHandlers...))

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

	// Public handler to propagate an event to all Catalyst nodes
	router.POST("/api/events", withLogging(eventsHandler.Events()))
	// Handlers to receive and pass serf events
	router.POST("/api/serf/receiveUserEvent", withLogging(eventsHandler.ReceiveUserEvent()))

	// Public GET handler to retrieve the public key for vod encryption
	router.GET("/api/pubkey", withLogging(encryptionHandlers.PublicKeyHandler()))

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/api/mist/trigger", withLogging(mistCallbackHandlers.Trigger()))

	// Handler for STREAM_SOURCE triggers
	broker.OnStreamSource(geoHandlers.HandleStreamSource)

	// Handler for USER_NEW triggers
	broker.OnUserNew(accessControlHandlers.HandleUserNew)

	// Handler for USER_END triggers.
	broker.OnUserEnd(analyticsHandlers.HandleUserEnd)

	// Endpoint to receive segments and manifests that ffmpeg produces
	router.POST("/api/ffmpeg/:id/:filename", withLogging(ffmpegSegmentingHandlers.NewFile()))

	// Temporary endpoint for admin queries
	router.GET("/admin/members", withLogging(adminHandlers.MembersHandler()))

	return router
}

// Hack to combine main metrics and mapic metrics. To be refactored away with mapic.
func concatHandlers(handlers ...http.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		// Response concat is string-based, so we don't accept any encoding
		r.Header.Del("Accept-Encoding")

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
