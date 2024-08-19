package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/log"
)

type HealthcheckResponse struct {
	Status string `json:"status"`
}

// Returns an HTTP 200 if Catalyst API and related services are running
// Used by the load balancer to determine whether to route to a node
func (d *CatalystAPIHandlersCollection) Healthcheck() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		responseObject := HealthcheckResponse{
			Status: "healthy",
		}

		b, err := json.Marshal(responseObject)
		if err != nil {
			log.LogNoRequestID("Failed to marshal healthcheck status: " + err.Error())
			b = []byte(`{"status": "marshalling status failed"}`)
		}

		if _, err := io.Writer.Write(w, b); err != nil {
			log.LogNoRequestID("Failed to write HTTP response for " + req.URL.RawPath)
		}
	}
}
