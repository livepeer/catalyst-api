package handlers

import (
	"fmt"
	"strings"
)

const SOURCE_PREFIX = "tr_src_"
const RENDITION_PREFIX = "tr_rend_"

func generateStreamNames() (string, string) {
	suffix := randomTrailer()
	inputStream := SOURCE_PREFIX + suffix
	renditionsStream := RENDITION_PREFIX + suffix
	return inputStream, renditionsStream
}

func extractSuffix(streamName string) (string, error) {
	if strings.HasPrefix(streamName, RENDITION_PREFIX) {
		return streamName[len(RENDITION_PREFIX):], nil
	}
	if strings.HasPrefix(streamName, SOURCE_PREFIX) {
		return streamName[len(SOURCE_PREFIX):], nil
	}
	return "", fmt.Errorf("unknown streamName prefix for %s", streamName)
}

type EncodedProfile struct {
	Name         string `json:"name"`
	Width        int32  `json:"width"`
	Height       int32  `json:"height"`
	Bitrate      int32  `json:"bitrate"`
	FPS          uint   `json:"fps"`
	FPSDen       uint   `json:"fpsDen"`
	Profile      string `json:"profile"`
	GOP          string `json:"gop"`
	Encoder      string `json:"encoder"`
	ColorDepth   int32  `json:"colorDepth"`
	ChromaFormat int32  `json:"chromaFormat"`
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
	InputStreamName       string                      `json:"source"`
	OutputStreamName      string                      `json:"sink"`
	Leastlive             bool                        `json:"leastlive"`
	HardcodedBroadcasters string                      `json:"hardcoded_broadcasters"`
	AudioSelect           string                      `json:"audio_select"`
	Profiles              []ProcLivepeerConfigProfile `json:"target_profiles"`
}

func configForSubprocess(req *TranscodeSegmentRequest, bPort int, inputStreamName, outputStreamName string) *ProcLivepeerConfig {
	conf := &ProcLivepeerConfig{
		InputStreamName:       inputStreamName,
		OutputStreamName:      outputStreamName,
		Leastlive:             true,
		AudioSelect:           "maxbps",
		HardcodedBroadcasters: fmt.Sprintf(`[{"address":"http://127.0.0.1:%d"}]`, bPort),
	}
	// Setup requested rendition profiles
	for i := 0; i < len(req.Profiles); i++ {
		Width := req.Profiles[i].Width
		Height := req.Profiles[i].Height
		Fps := int32(req.Profiles[i].FPS)
		conf.Profiles = append(conf.Profiles, ProcLivepeerConfigProfile{
			Name:       req.Profiles[i].Name,
			Bitrate:    req.Profiles[i].Bitrate,
			Width:      &Width,
			Height:     &Height,
			Fps:        &Fps,
			GOP:        req.Profiles[i].GOP,
			AvcProfile: req.Profiles[i].Profile,
		})
	}
	return conf
}

// Contents of LIVE_TRACK_LIST trigger
type MistTrack struct {
	Id          int32  `json:"trackid"`
	Kfps        int32  `json:"fpks"`
	Height      int32  `json:"height"`
	Width       int32  `json:"width"`
	Index       int32  `json:"idx"`
	Type        string `json:"type"`
	Codec       string `json:"codec"`
	StartTimeMs int32  `json:"firstms"`
	EndTimeMs   int32  `json:"lastms"`
}

type LiveTrackListTriggerJson = map[string]MistTrack

type TranscodeSegmentResult struct {
	SourceFile      string  `json:"source_location"`
	Status          string  `json:"status"`
	CompletionRatio float32 `json:"completion_ratio,omitempty"`
	ErrorMessage    string  `json:"error_message,omitempty"`
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
