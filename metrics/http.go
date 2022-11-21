package metrics

import (
	"fmt"
	"net/http"

	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func ListenAndServe(promPort int) error {
	listen := fmt.Sprintf("0.0.0.0:%d", promPort)
	http.Handle("/metrics", promhttp.Handler())

	log.LogNoRequestID(
		"Starting Prometheus metrics",
		"version", config.Version,
		"host", listen,
	)
	return http.ListenAndServe(listen, nil)
}
