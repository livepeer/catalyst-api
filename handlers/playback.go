package handlers

import (
	"errors"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/playback"
)

func ManifestHandler() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var requestID = config.RandomTrailer(8)

		err := req.ParseForm()
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}

		key := req.Form.Get(playback.ManifestKeyParam)
		if err := checkKey(key); err != nil {
			handleError(err, req, requestID, w)
			return
		}
		manifest, err := playback.Manifest(playback.PlaybackRequest{
			RequestID:  requestID,
			PlaybackID: params.ByName("playbackID"),
			File:       params.ByName("file"),
			AccessKey:  key,
		})
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}
		w.Header().Set("content-type", "application/x-mpegurl")
		writeResponse(req, requestID, w, manifest)
	}
}

func MediaHandler() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var requestID = config.RandomTrailer(8)
		err := req.ParseForm()
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}

		key := req.Form.Get(playback.ManifestKeyParam)
		if err := checkKey(key); err != nil {
			handleError(err, req, requestID, w)
			return
		}
		media, err := playback.Media(playback.PlaybackRequest{
			RequestID:  requestID,
			PlaybackID: params.ByName("playbackID"),
			File:       params.ByName("file"),
		})
		if err != nil {
			handleError(err, req, requestID, w)
			return
		}
		defer media.Close()

		_, err = io.Copy(w, media)
		if err != nil {
			log.LogError(requestID, "failed to write response", err)
		}
	}
}

// temporary hard coded check until real auth is implemented
func checkKey(key string) error {
	if key != "secretlpkey" {
		return catErrs.UnauthorisedError
	}
	return nil
}

func handleError(err error, req *http.Request, requestID string, w http.ResponseWriter) {
	log.LogError(requestID, "error in playback handler", err, "url", req.URL)
	switch {
	case errors.Is(err, catErrs.EmptyAccessKeyError):
		catErrs.WriteHTTPBadRequest(w, playback.ManifestKeyParam+" param empty", nil)
	case errors.Is(err, catErrs.ObjectNotFoundError):
		catErrs.WriteHTTPNotFound(w, "not found", nil)
	case errors.Is(err, catErrs.UnauthorisedError):
		catErrs.WriteHTTPUnauthorized(w, "denied", nil)
	default:
		catErrs.WriteHTTPInternalServerError(w, "internal server error", nil)
	}
}

func writeResponse(req *http.Request, requestID string, w http.ResponseWriter, b []byte) {
	_, err := w.Write(b)
	if err != nil {
		log.LogError(requestID, "failed to write response", err, "url", req.URL)
	}
}
