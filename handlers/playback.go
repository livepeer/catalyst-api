package handlers

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/playback"
	"github.com/livepeer/catalyst-api/requests"
)

type PlaybackHandler struct {
	PrivateBucketURLs []*url.URL
}

func NewPlaybackHandler(urls []*url.URL) *PlaybackHandler {
	return &PlaybackHandler{
		PrivateBucketURLs: urls,
	}
}

func (p *PlaybackHandler) Handle(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	requestID := requests.GetRequestId(req)

	err := req.ParseForm()
	if err != nil {
		handleError(err, req, requestID, w)
		return
	}

	gatingParamName := "accessKey"
	gatingParam := req.URL.Query().Get(gatingParamName)
	if gatingParam == "" {
		gatingParamName = "jwt"
		gatingParam = req.URL.Query().Get(gatingParamName)
	}

	if gatingParam == "" {
		gatingParam = req.Header.Get("Livepeer-Access-Key")
		if gatingParam == "" {
			gatingParam = req.Header.Get("Livepeer-Jwt")
		}

		if gatingParam != "" {
			gatingParamName = "headerBased"
		}
	}

	playbackReq := playback.Request{
		RequestID:       requestID,
		PlaybackID:      params.ByName("playbackID"),
		File:            params.ByName("file"),
		GatingParam:     gatingParam,
		GatingParamName: gatingParamName,
		Range:           req.Header.Get("range"),
	}
	response, err := playback.Handle(p.PrivateBucketURLs, playbackReq)
	if err != nil {
		handleError(err, req, requestID, w)
		return
	}
	defer response.Body.Close()

	w.Header().Set("accept-ranges", "bytes")
	w.Header().Set("content-type", response.ContentType)
	w.Header().Set("cache-control", "max-age=0")
	if response.ContentLength != nil {
		w.Header().Set("content-length", fmt.Sprintf("%d", *response.ContentLength))
	}
	w.Header().Set("etag", response.ETag)

	if response.ContentRange != "" {
		w.Header().Set("content-range", response.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if req.Method == http.MethodHead {
		return
	}
	_, err = io.Copy(w, response.Body)
	if err != nil {
		log.LogError(requestID, "failed to write response", err)
	}
}

func handleError(err error, req *http.Request, requestID string, w http.ResponseWriter) {
	log.LogError(requestID, "error in playback handler", err, "url", req.URL)
	switch {
	case errors.Is(err, catErrs.EmptyGatingParamError):
		catErrs.WriteHTTPBadRequest(w, "gating param empty", nil)
	case errors.Is(err, catErrs.ObjectNotFoundError):
		catErrs.WriteHTTPNotFound(w, "not found", nil)
	case errors.Is(err, catErrs.UnauthorisedError):
		catErrs.WriteHTTPUnauthorized(w, "denied", nil)
	default:
		catErrs.WriteHTTPInternalServerError(w, "internal server error", nil)
	}
}
