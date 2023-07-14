package handlers

import (
	"github.com/hashicorp/serf/serf"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/errors"
	"io"
	"net/http"
)

type EventsHandlersCollection struct {
	Cluster cluster.Cluster
}

func (d *EventsHandlersCollection) Events() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		// TODO: validate event handler
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot read payload", err)
			return
		}
		err = d.Cluster.BroadcastEvent(serf.UserEvent{
			// TODO: Update event name
			Name:     "event-random",
			Payload:  payload,
			Coalesce: true,
		})
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process event", err)
			return
		}
	}
}
