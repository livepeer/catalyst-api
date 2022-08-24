package handlers

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const SOURCE_PREFIX = "tr_src_"
const RENDITION_PREFIX = "tr_rend_+"

func generateStreamNames() (string, string) {
	suffix := randomTrailer()
	inputStream := SOURCE_PREFIX + suffix
	renditionsStream := RENDITION_PREFIX + suffix
	return inputStream, renditionsStream
}

func isTranscodeStream(streamName string) (bool, string) {
	if strings.HasPrefix(streamName, RENDITION_PREFIX) {
		return true, streamName[len(RENDITION_PREFIX):]
	}
	// if strings.HasPrefix(streamName, SOURCE_PREFIX) {
	// 	return streamName[len(SOURCE_PREFIX):], nil
	// }
	return false, ""
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

// Transforms request information to MistProcLivepeer config json
// We use .HardcodedBroadcasters assuming we have local B-node.
// The AudioSelect is configured to use single audio track from input.
// Same applies on transcoder side, expect Livepeer to use single best video track as input.
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

func pipeToLog(pipe io.ReadCloser, name string) {
	data := make([]byte, 4096)
	for {
		count, err := pipe.Read(data)
		if err != nil {
			fmt.Printf("ERROR cmd=%s %v\n", name, err)
			return
		}
		fmt.Printf("out [%s] %s\n", name, string(data[0:count]))
	}
}

func commandOutputToLog(cmd *exec.Cmd, name string) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("ERROR: cmd.StdoutPipe() %v\n", err)
		return
	}
	go pipeToLog(stdoutPipe, name)
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("ERROR: cmd.StderrPipe() %v\n", err)
		return
	}
	go pipeToLog(stderrPipe, name)
}
