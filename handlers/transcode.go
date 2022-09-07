package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type EncodedProfile struct {
	Name         string `json:"name"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Bitrate      int    `json:"bitrate"`
	FPS          uint   `json:"fps"`
	FPSDen       uint   `json:"fpsDen"`
	Profile      string `json:"profile"`
	GOP          string `json:"gop"`
	Encoder      string `json:"encoder"`
	ColorDepth   int    `json:"colorDepth"`
	ChromaFormat int    `json:"chromaFormat"`
}

type TranscodeSegmentRequest struct {
	SourceFile           string           `json:"source_location"`
	CallbackUrl          string           `json:"callback_url"`
	ManifestID           string           `json:"manifestID"`
	StreamID             string           `json:"streamID"`
	SessionID            string           `json:"sessionID"`
	StreamKey            string           `json:"streamKey"`
	Presets              []string         `json:"presets"`
	ObjectStore          string           `json:"objectStore"`
	RecordObjectStore    string           `json:"recordObjectStore"`
	RecordObjectStoreURL string           `json:"recordObjectStoreUrl"`
	Profiles             []EncodedProfile `json:"profiles"`
	PreviousSessions     []string         `json:"previousSessions"`
	Detection            struct {
		Freq                uint `json:"freq"`
		SampleRate          uint `json:"sampleRate"`
		SceneClassification []struct {
			Name string `json:"name"`
		} `json:"sceneClassification"`
	} `json:"detection"`
	VerificationFreq uint `json:"verificationFreq"`
}

func (d *CatalystAPIHandlersCollection) TranscodeSegment() httprouter.Handle {
	schema := inputSchemasCompiled["TranscodeSegment"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var transcodeRequest TranscodeSegmentRequest
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

		if err := clients.DefaultCallbackClient.SendTranscodeStatusError(transcodeRequest.CallbackUrl, "NYI - not yet implemented"); err != nil {
			errors.WriteHTTPInternalServerError(w, "error send transcode error", err)
		}
		_, _ = io.WriteString(w, "OK") // TODO later
	}
}
