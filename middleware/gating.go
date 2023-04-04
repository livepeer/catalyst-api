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
			catErrs.WriteHTTPInternalServerError(w, "error authorizing playback request", nil)
			return
		}

		if !playbackAccessControlAllowed {
			log.Log(requestID, "playback access control denied", "playbackID", playbackID, playback.KeyParam, key)
			deny(params.ByName("file"), w)
			return
		}

		next(w, req, params)
	}
}

func deny(requestFile string, w http.ResponseWriter) {
	if !playback.IsManifest(requestFile) {
		catErrs.WriteHTTPUnauthorized(w, "unauthorised", errors.New("access control denied"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(`#EXTM3U
#EXT-X-ERROR: Shutting down since this session is not allowed to view this stream
#EXT-X-ENDLIST`))
	if err != nil {
		log.LogNoRequestID("error writing HTTP error", "error", err)
	}
}
