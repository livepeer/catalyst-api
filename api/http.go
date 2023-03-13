package api

import (
	"context"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/geolocation"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/middleware"
)

func ListenAndServe(ctx context.Context, cli config.Cli, bal balancer.Balancer, c cluster.Cluster) error {
	router := NewCatalystAPIRouter(cli, bal, c)
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

func NewCatalystAPIRouter(cli config.Cli, bal balancer.Balancer, c cluster.Cluster) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()

	geoHandlers := &geolocation.GeolocationHandlersCollection{
		Balancer: bal,
		Cluster:  c,
		Config:   cli,
	}

	// Handling incoming playback redirection requests
	router.GET("/ok", withLogging(geoHandlers.RedirectHandler()))

	return router
}
