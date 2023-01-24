package handlers

import (
	"encoding/json"
	"github.com/livepeer/catalyst-api/video"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/xeipuuv/gojsonschema"
)

func (d *CatalystAPIHandlersCollection) TranscodeSegment() httprouter.Handle {
	schema := inputSchemasCompiled["TranscodeSegment"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var transcodeRequest transcode.TranscodeSegmentRequest
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read body", err)
			return
		}
		result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "body schema validation failed", err)
			return
		}
		if !result.Valid() {
			errors.WriteHTTPBadBodySchema("TranscodeSegment", w, result.Errors())
			return
		}
		if err := json.Unmarshal(payload, &transcodeRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		// Note: This is no longer used pipeline stage
		// TODO: Revisit later with better mistserver integration
		_, _, err = transcode.RunTranscodeProcess(transcodeRequest, "", video.InputVideo{})
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Error running Transcode process", err)
		}
	}
}
