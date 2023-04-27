package admin

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/state"
)

// Admin handlers. To be replaced by signed events and GraphQL queries when we get there.
type AdminHandlersCollection struct {
	Cluster cluster.Cluster
	Machine *state.Machine
}

func (c *AdminHandlersCollection) MembersHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		members, err := c.Cluster.MembersFiltered(map[string]string{}, "", "")
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Could not get list of cluster members", err)
			return
		}
		b, err := json.Marshal(members)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Could not marshal list of members", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b) // nolint:errcheck
	}
}

func (c *AdminHandlersCollection) StateHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		state, err := json.Marshal(c.Machine.State)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Could not serialize state to JSON", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(state) // nolint:errcheck
	}
}
