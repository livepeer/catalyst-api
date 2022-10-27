package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/xeipuuv/gojsonschema"
)

type TranscodeSegmentRequest struct {
	SourceFile      string                   `json:"source_location"`
	CallbackURL     string                   `json:"callback_url"`
	UploadURL       string                   `json:"upload_url"`
	StreamKey       string                   `json:"streamKey"`
	AccessToken     string                   `json:"accessToken"`
	TranscodeAPIUrl string                   `json:"transcodeAPIUrl"`
	Profiles        []clients.EncodedProfile `json:"profiles"`
	Detection       struct {
		Freq                uint `json:"freq"`
		SampleRate          uint `json:"sampleRate"`
		SceneClassification []struct {
			Name string `json:"name"`
		} `json:"sceneClassification"`
	} `json:"detection"`
	SourceStreamInfo clients.MistStreamInfo
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

		if err := transcode.RunTranscodeProcess(transcodeRequest.SourceFile, transcodeRequest.UploadURL); err != nil {
			errors.WriteHTTPInternalServerError(w, "Error running Transcode process", err)
		}
	}
}

type ProcLivepeerConfigProfile struct {
	Name       string `json:"name"`
	Bitrate    int32  `json:"bitrate"`
	Width      *int32 `json:"width,omitempty"`
	Height     *int32 `json:"height,omitempty"`
	Fps        *int32 `json:"fps,omitempty"`
	GOP        string `json:"gop,omitempty"`
	AvcProfile string `json:"profile,omitempty"` // H264High; High; H264Baseline; Baseline; H264Main; Main; H264ConstrainedHigh; High, without b-frames
}

type ProcLivepeerConfig struct {
	AccessToken           string                      `json:"access_token,omitempty"`
	CustomAPIUrl          string                      `json:"custom_url,omitempty"`
	InputStreamName       string                      `json:"source"`
	OutputStreamName      string                      `json:"sink"`
	Leastlive             bool                        `json:"leastlive"`
	HardcodedBroadcasters string                      `json:"hardcoded_broadcasters,omitempty"`
	AudioSelect           string                      `json:"audio_select"`
	Profiles              []ProcLivepeerConfigProfile `json:"target_profiles"`
}
