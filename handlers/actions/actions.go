package actions

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/events"
)

type ActionHandlersCollection struct {
	Signer  events.Signer
	Cluster cluster.Cluster
}

func NewActionsHandlersCollection(cli config.Cli, signer events.Signer, cluster cluster.Cluster) *ActionHandlersCollection {
	return &ActionHandlersCollection{
		Signer:  signer,
		Cluster: cluster,
	}
}

func (act *ActionHandlersCollection) ActionHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var unverified events.UnverifiedEvent
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		err = json.Unmarshal(payload, &unverified)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot handle request body", err)
			return
		}
		signed, err := act.Signer.Verify(unverified)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Could not validate event signature", err)
			return
		}
		// TODO: Add check for allowlisted address here
		err = act.Cluster.BroadcastEvent(signed)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Could not broadcast event", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
