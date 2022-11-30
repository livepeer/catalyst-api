package middleware

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/pipeline"
)

const MAX_JOBS_IN_FLIGHT = 10

func HasCapacity(vodEngine *pipeline.Coordinator, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if vodEngine.Jobs.Size() >= MAX_JOBS_IN_FLIGHT {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		next(w, r, ps)
	}
}
