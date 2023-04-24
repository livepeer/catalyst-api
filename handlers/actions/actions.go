package actions

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/events"
)

type ActionHandlersCollection struct{}

type PlaybackAccessControlEntry struct{}

func NewActionsHandlersCollection(cli config.Cli) *ActionHandlersCollection {
	return &ActionHandlersCollection{}
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
		// Verify it's generally semantically valid
		// Verify that it matches some of our action schema
		// Verify that it's correctly signed
		// Pass it to the signer
	}
}
