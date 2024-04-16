package middleware

import (
	"errors"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers/accesscontrol"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/playback"
	"github.com/livepeer/catalyst-api/requests"
)

type GatingHandler struct {
	AccessControl *accesscontrol.AccessControlHandlersCollection
}

func NewGatingHandler(cli config.Cli, mapic mistapiconnector.IMac) *GatingHandler {
	return &GatingHandler{
		AccessControl: accesscontrol.NewAccessControlHandlersCollection(cli, mapic),
	}
}

func (h *GatingHandler) GatingCheck(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		requestID := requests.GetRequestId(req)

		playbackID := params.ByName("playbackID")
		accessKey := req.URL.Query().Get("accessKey")
		jwt := req.URL.Query().Get("jwt")

		if accessKey == "" {
			accessKey = req.Header.Get("Livepeer-Access-Key")
		}

		if jwt == "" {
			jwt = req.Header.Get("Livepeer-Jwt")
		}

		originIP := req.Header.Get("X-Forwarded-For")
		referer := req.Header.Get("Referer")
		userAgent := req.Header.Get("User-Agent")
		forwardedProto := req.Header.Get("X-Forwarded-Proto")
		host := req.Header.Get("Host")
		origin := req.Header.Get("Origin")

		payload := misttriggers.UserNewPayload{
			URL:            req.URL,
			AccessKey:      accessKey,
			JWT:            jwt,
			OriginIP:       originIP,
			Referer:        referer,
			UserAgent:      userAgent,
			ForwardedProto: forwardedProto,
			Host:           host,
			Origin:         origin,
		}

		playbackAccessControlAllowed, err := h.AccessControl.IsAuthorized(req.Context(), playbackID, &payload)
		if err != nil {
			log.LogError(requestID, "unable to get playback access control info", err, "playbackID", playbackID, "accessKey", accessKey, "jwt", jwt)
			if errors.Is(err, catErrs.InvalidJWT) {
				deny(params.ByName("file"), w)
			} else {
				catErrs.WriteHTTPInternalServerError(w, "error authorizing playback request", nil)
			}
			return
		}

		if !playbackAccessControlAllowed {
			log.Log(requestID, "playback access control denied", "playbackID", playbackID, "accessKey", accessKey, "jwt", jwt)
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
