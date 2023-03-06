package api

import (
	"context"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
)

func ListenAndServeInternal(ctx context.Context, addr string, mapic mistapiconnector.IMac) error {
	router := NewCatalystAPIRouterInternal(mapic)
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

func NewCatalystAPIRouterInternal(mapic mistapiconnector.IMac) *httprouter.Router {
	router := httprouter.New()
	withLogging := middleware.LogRequest()

	// Endpoint to receive "Triggers" (callbacks) from Mist
	router.POST("/mapic", withLogging(mapic.HandleDefaultStreamTrigger()))

	return router
}
