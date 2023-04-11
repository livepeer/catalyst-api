package handlers

import (
	"errors"
	"fmt"
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
			Range:      req.Header.Get("range"),
		})
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}
		defer response.Body.Close()

		w.Header().Set("accept-ranges", "bytes")
		w.Header().Set("content-type", response.ContentType)
		if response.ContentLength != nil {
			w.Header().Set("content-length", fmt.Sprintf("%d", *response.ContentLength))
		}
		w.Header().Set("etag", response.ETag)
		w.WriteHeader(http.StatusOK)

		if req.Method == "HEAD" {
			return
		}
		_, err = io.Copy(w, response.Body)
		if err != nil {
			log.LogError(requestID, "failed to write response", err)
		}
	}
}

func PlaybackOptionsHandler() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		w.Header().Set("allow", "GET, HEAD, OPTIONS")
		w.Header().Set("content-length", "0")
		w.WriteHeader(http.StatusOK)
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
