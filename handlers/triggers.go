package handlers

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
)

func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		if t := req.Header.Get("X-Trigger"); t != "PUSH_END" {
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", t))
			return
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
		if len(lines) < 2 {
			errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
			return
		}

		// stream name is the second line in the Mist Trigger payload
		s := lines[1]
		// when uploading is done, remove trigger and stream from Mist
		errT := d.MistClient.DeleteTrigger(s, "PUSH_END")
		errS := d.MistClient.DeleteStream(s)
		if errT != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", s), errT)
			return
		}
		if errS != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove stream '%s'", s), errS)
			return
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(d.StreamCache[s].callbackUrl, clients.TranscodeStatusTranscoding, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		delete(d.StreamCache, s)

		// TODO: add timeout for the stream upload
		// TODO: start transcoding
		stubTranscodingCallbacksForStudio(d.StreamCache[s].callbackUrl, callbackClient)
	}
}
