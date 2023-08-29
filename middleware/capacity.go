package middleware

import (
	"net/http"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/pipeline"
)

type CapacityMiddleware struct {
	requestsInFlight atomic.Int64
}

func (c *CapacityMiddleware) HasCapacity(vodEngine *pipeline.Coordinator, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		requestsInFlight := c.requestsInFlight.Add(1)
		defer c.requestsInFlight.Add(-1)

		if len(vodEngine.Jobs.GetKeys())+int(requestsInFlight) >= config.MAX_JOBS_IN_FLIGHT {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}

		next(w, r, ps)
	}
}
