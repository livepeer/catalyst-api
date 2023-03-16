package middleware

import (
	"errors"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers/accesscontrol"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/playback"
	"github.com/livepeer/catalyst-api/requests"
)

type GatingHandler struct {
	AccessControl *accesscontrol.AccessControlHandlersCollection
}

func NewGatingHandler(cli config.Cli) *GatingHandler {
	return &GatingHandler{
		AccessControl: accesscontrol.NewAccessControlHandlersCollection(cli),
	}
}

func (h *GatingHandler) GatingCheck(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		requestID := requests.GetRequestId(req)

		playbackID := params.ByName("playbackID")
		key := req.URL.Query().Get(playback.KeyParam)

		playbackAccessControlAllowed, err := h.AccessControl.IsAuthorized(playbackID, req.URL)
		if err != nil {
			log.LogError(requestID, "unable to get playback access control info", err, "playbackID", playbackID, playback.KeyParam, key)
			deny(w)
			return
		}

		if !playbackAccessControlAllowed {
			log.Log(requestID, "playback access control denied", "playbackID", playbackID, playback.KeyParam, key)
			deny(w)
			return
		}

		next(w, req, params)
	}
}

func deny(w http.ResponseWriter) {
	catErrs.WriteHTTPUnauthorized(w, "denied", errors.New("access control failed"))
}
