package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
)

type CapacityMiddleware struct {
	vodRequestsInFlight  atomic.Int64
	clipRequestsInFlight atomic.Int64
}

func (c *CapacityMiddleware) HasCapacity(vodEngine *pipeline.Coordinator, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var vodJobCount, clipJobCount int

		// Keep a gauge of HTTP requests in flight
		metrics.Metrics.HTTPRequestsInFlight.Add(1)
		defer metrics.Metrics.HTTPRequestsInFlight.Add(-1)

		// Get total in-progress VOD jobs (i.e. clipping and regular vod jobs)
		allVodJobs := vodEngine.Jobs.GetJobs()

		// Get total in-progress clipping VOD jobs
		for _, v := range allVodJobs {
			if v.ClipStrategy.Enabled {
				clipJobCount++
			} else {
				vodJobCount++
			}
		}
		fmt.Println("XXX total Clip Jobs:", clipJobCount)

		// Get this current request's job type (i.e. clipping or regular-vod request)
		isClip, err := isClipRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Compare limits for clipping vs regular-vod jobs
		if isClip {
			inFlightClipReqs := c.clipRequestsInFlight.Add(1)
			defer c.clipRequestsInFlight.Add(-1)

			if clipJobCount+int(inFlightClipReqs) >= config.MaxInFlightClipJobs {
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Println("XXX: too many clips in progress")
				return
			}
		} else {
			inFlightVodReqs := c.vodRequestsInFlight.Add(1)
			defer c.vodRequestsInFlight.Add(-1)

			if vodJobCount+int(inFlightVodReqs) >= config.MaxInFlightJobs {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		}

		next(w, r, ps)
	}
}

func isClipRequest(r *http.Request) (bool, error) {

	if r == nil {
		return false, fmt.Errorf("request is empty")
	}
	if r.Body == nil {
		return false, fmt.Errorf("request body is empty")

	}
	var buf bytes.Buffer

	// Read request body to buf so that r.Body can be reset
	tee := io.TeeReader(r.Body, &buf)

	decoder := json.NewDecoder(tee)
	b := handlers.UploadVODRequest{}
	err := decoder.Decode(&b)
	if err != nil {
		return false, err
	}

	// Reset request body so the next http handlers can continue processing the request
	r.Body = io.NopCloser(&buf)

	// Check if current request is a clipping request
	if b.ClipStrategy.PlaybackID != "" {
		fmt.Println("XXX: THIS IS A CLIP")
		return true, nil
	}
	return false, nil
}
