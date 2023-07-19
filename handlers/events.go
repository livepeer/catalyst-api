package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/serf/serf"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"net/http"
)

type EventsHandlersCollection struct {
	Cluster cluster.Cluster
}

func (d *EventsHandlersCollection) Events() httprouter.Handle {
	type Event struct {
		Resource   string `json:"resource"`
		PlaybackID string `json:"playback_id"`
	}

	schema := inputSchemasCompiled["Event"]
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot validate payload", err)
			return
		}
		if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			return
		}
		var event Event
		if err := json.Unmarshal(payload, &event); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		err = d.Cluster.BroadcastEvent(serf.UserEvent{
			Name:     fmt.Sprintf("%s-%s", event.Resource, event.PlaybackID),
			Payload:  payload,
			Coalesce: true,
		})

		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process event", err)
			return
		}
	}
}
