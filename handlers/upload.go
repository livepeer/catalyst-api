package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schema := inputSchemasCompiled["UploadVOD"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			return
		} else if payload, err := io.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			return
		} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		// find source segment URL
		var tURL string
		for _, o := range uploadVODRequest.OutputLocations {
			if o.Outputs.SourceSegments {
				tURL = o.URL
				break
			}
		}
		if tURL == "" {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("no source segment URL in request"))
			return
		}

		streamName := randomStreamName("catalyst_vod_")
		d.StreamCache[streamName] = StreamInfo{callbackUrl: uploadVODRequest.CallbackUrl}

		// process the request
		if err := d.processUploadVOD(streamName, uploadVODRequest.Url, tURL); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process upload VOD request", err)
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(uploadVODRequest.CallbackUrl, clients.TranscodeStatusPreparing, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		io.WriteString(w, fmt.Sprint(len(uploadVODRequest.OutputLocations)))
	}
}

func (d *CatalystAPIHandlersCollection) processUploadVOD(streamName, sourceURL, targetURL string) error {
	if err := d.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := d.MistClient.AddTrigger(streamName, "PUSH_END"); err != nil {
		return err
	}
	if err := d.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}
