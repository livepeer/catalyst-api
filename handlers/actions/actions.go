package actions

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
)

type ActionHandlersCollection struct{}

type PlaybackAccessControlEntry struct{}

func NewActionsHandlersCollection(cli config.Cli) *ActionHandlersCollection {
	return &ActionHandlersCollection{}
}

func (act *ActionHandlersCollection) ActionHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	}
}
