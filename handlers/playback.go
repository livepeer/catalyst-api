package handlers

import (
	"errors"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/playback"
	"github.com/livepeer/catalyst-api/requests"
)

func PlaybackHandler() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		requestID := requests.GetRequestId(req)

		err := req.ParseForm()
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}

		key := req.URL.Query().Get(playback.KeyParam)
		response, err := playback.Handle(playback.Request{
			RequestID:  requestID,
			PlaybackID: params.ByName("playbackID"),
			File:       params.ByName("file"),
			AccessKey:  key,
		})
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}
		defer response.Body.Close()

		w.Header().Set("content-type", response.ContentType)
		_, err = io.Copy(w, response.Body)
		if err != nil {
			log.LogError(requestID, "failed to write response", err)
		}
	}
}

func handleError(err error, req *http.Request, requestID string, w http.ResponseWriter) {
	log.LogError(requestID, "error in playback handler", err, "url", req.URL)
	switch {
	case errors.Is(err, catErrs.EmptyAccessKeyError):
		catErrs.WriteHTTPBadRequest(w, playback.KeyParam+" param empty", nil)
	case errors.Is(err, catErrs.ObjectNotFoundError):
		catErrs.WriteHTTPNotFound(w, "not found", nil)
	case errors.Is(err, catErrs.UnauthorisedError):
		catErrs.WriteHTTPUnauthorized(w, "denied", nil)
	default:
		catErrs.WriteHTTPInternalServerError(w, "internal server error", nil)
	}
}
