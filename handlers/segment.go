package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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

var TranscodeSegmentRequestSchemaDefinition string = `{
	"type": "object",
	"properties": {
		"source_location": {"type": "string"},
		"callback_url": {"type": "string"},
		"manifestID": {"type": "string"},
		"streamID": {"type": "string"},
		"sessionID": {"type": "string"},
		"streamKey": {"type": "string"},
		"presets": {
			"items": {"type": "string"},
			"type": "array"
		},
		"objectStore": {"type": "string"},
		"recordObjectStore": {"type": "string"},
		"recordObjectStoreUrl": {"type": "string"},
		"profiles": {
			"items": {
				"properties": {
					"name": {"type": "string"},
					"width": {"type": "integer"},
					"height": {"type": "integer"},
					"bitrate": {"type": "integer"},
					"fps": {"type": "integer"},
					"fpsDen": {"type": "integer"},
					"profile": {"type": "string"},
					"gop": {"type": "string"},
					"encoder": {"type": "string"},
					"colorDepth": {"type": "integer"},
					"chromaFormat": {"type": "integer"}
				},
				"additionalProperties": false,
				"type": "object",
				"required": [
					"name",
					"width",
					"height",
					"bitrate"
				]
			},
			"type": "array"
		},
		"previousSessions": {
			"items": {"type": "string"},
			"type": "array"
		},
		"detection": {
			"properties": {
				"freq": {"type": "integer"},
				"sampleRate": {"type": "integer"},
				"sceneClassification": {
					"items": {
						"properties": {
							"name": {"type": "string"}
						},
						"additionalProperties": false,
						"type": "object",
						"required": [
							"name"
						]
					},
					"type": "array"
				}
			},
			"additionalProperties": false,
			"type": "object",
			"required": [
				"freq",
				"sampleRate"
			]
		},
		"verificationFreq": {"type": "integer"}
	},
	"additionalProperties": false,
	"required": [
		"source_location",
		"callback_url",
		"manifestID",
		"profiles",
		"verificationFreq"
	]
}`

type TranscodeSegmentResult struct {
	SourceFile      string  `json:"source_location"`
	Status          string  `json:"status"`
	CompletionRatio float32 `json:"completion_ratio,omitempty"`
	ErrorMessage    string  `json:"error_message,omitempty"`
}

func invokeTestCallback(transcodeRequest *TranscodeSegmentRequest) {
	time.Sleep(30 * time.Millisecond) // takes some time to transcode
	// invoke callback
	resultEncoded, err := json.Marshal(&TranscodeSegmentResult{
		SourceFile:      transcodeRequest.SourceFile,
		Status:          "error",
		CompletionRatio: 0.0,
		ErrorMessage:    "NYI - not yet implemented",
	})
	if err != nil {
		fmt.Printf("ERROR TranscodeSegmentResult to json SourceFile=%s\n", transcodeRequest.SourceFile)
		return
	}
	req, err := http.NewRequest("POST", transcodeRequest.CallbackUrl, bytes.NewBuffer(resultEncoded))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("ERROR invoking callback %v\n", err)
		return
	}
	resp.Body.Close()
	// What to do with status of callback?
}
