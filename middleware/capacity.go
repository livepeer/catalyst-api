package middleware

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/pipeline"
)

func HasCapacity(vodEngine *pipeline.Coordinator, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if vodEngine.InFlightMistPipelineJobs() >= config.MAX_JOBS_IN_FLIGHT {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		next(w, r, ps)
	}
}
