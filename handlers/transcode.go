package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strings"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
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

// configForSubprocess transforms request information to MistProcLivepeer config json
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
	for _, profile := range req.Profiles {
		Width := profile.Width
		Height := profile.Height
		Fps := int32(profile.FPS)
		conf.Profiles = append(conf.Profiles, ProcLivepeerConfigProfile{
			Name:       profile.Name,
			Bitrate:    profile.Bitrate,
			Width:      &Width,
			Height:     &Height,
			Fps:        &Fps,
			GOP:        profile.GOP,
			AvcProfile: profile.Profile,
		})
	}
	return conf
}

type Transcoding struct {
	httpResp        http.ResponseWriter
	httpReq         *http.Request
	broadcasterPort int
	mistProcPath    string

	request          TranscodeSegmentRequest
	inputUrl         *url.URL
	inputStream      string
	renditionsStream string
}

func (t *Transcoding) ValidateRequest() error {
	payload, err := io.ReadAll(t.httpReq.Body)
	if err != nil {
		errors.WriteHTTPInternalServerError(t.httpResp, "Cannot read body", err)
		return err
	}
	schema := inputSchemasCompiled["TranscodeSegment"]
	result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
	if err != nil {
		errors.WriteHTTPInternalServerError(t.httpResp, "body schema validation failed", err)
		return err
	}
	if !result.Valid() {
		err = fmt.Errorf("%s", result.Errors())
		errors.WriteHTTPBadRequest(t.httpResp, "Invalid request payload", err)
		return err
	}
	if err := json.Unmarshal(payload, &t.request); err != nil {
		errors.WriteHTTPBadRequest(t.httpResp, "Invalid request payload", err)
		return err
	}
	if t.inputUrl, err = url.Parse(t.request.SourceFile); err != nil {
		errors.WriteHTTPBadRequest(t.httpResp, "Invalid request source_location", err)
		return err
	}
	return nil
}

func (t *Transcoding) PrepareStreams(mist MistAPIClient) error {
	t.inputStream, t.renditionsStream = generateStreamNames()
	if err := mist.AddStream(t.inputStream, t.request.SourceFile); err != nil {
		where := fmt.Sprintf("AddStream(%s)", t.inputStream)
		t.errorOut(where, err)
		errors.WriteHTTPInternalServerError(t.httpResp, where, err)
		return err
	}
	return nil
}

// RunTranscodeProcess starts `MistLivepeeerProc` as a subprocess to transcode inputStream into renditionsStream.
// The transcoding happens via local Broadcaster node, that is why we need broadcasterPort.
func (t *Transcoding) RunTranscodeProcess(mist MistAPIClient, cache *StreamCache) {
	configPayload, err := json.Marshal(configForSubprocess(&t.request, t.broadcasterPort, t.inputStream, t.renditionsStream))
	if err != nil {
		t.errorOut("ProcLivepeerConfig json encode", err)
		return
	}
	transcodeCommand := exec.Command(t.mistProcPath, "-")
	stdinPipe, err := transcodeCommand.StdinPipe()
	if err != nil {
		t.errorOut("transcodeCommand.StdinPipe()", err)
		return
	}
	commandOutputToLog(transcodeCommand, "coding", invokeTriggerWorkaround(t))
	sent, err := stdinPipe.Write(configPayload)
	if err != nil {
		t.errorOut("stdinPipe.Write()", err)
		return
	}
	if sent != len(configPayload) {
		t.errorOut("short write on stdinPipe.Write()", err)
		return
	}
	err = stdinPipe.Close()
	if err != nil {
		t.errorOut("stdinPipe.Close()", err)
		return
	}
	err = transcodeCommand.Start()
	if err != nil {
		t.errorOut("start transcodeCommand", err)
		return
	}

	// TODO: remove when Mist code is updated https://github.com/DDVTECH/mistserver/issues/81
	// Starting SOURCE_PREFIX stream because MistProcLivepeer is unable to start it automatically
	if err := mist.PushStart(t.inputStream, "/tmp/mist/alex.ts"); err != nil {
		t.errorOut("PushStart(inputStream)", err)
		return
	}
	currentDir, _ := url.Parse(".")
	uploadDir := t.inputUrl.ResolveReference(currentDir)
	// Cache the stream data, later used in the trigger handlers called by Mist
	cache.Transcoding.Store(t.renditionsStream, SegmentInfo{
		CallbackUrl: t.request.CallbackUrl,
		Source:      t.request.SourceFile,
		Profiles:    t.request.Profiles[:],
		UploadDir:   uploadDir,
	})

	err = transcodeCommand.Wait()
	if exit, ok := err.(*exec.ExitError); ok {
		log.Printf("MistProcLivepeer returned %d", exit.ExitCode())
	} else if err != nil {
		t.errorOut("exec transcodeCommand", err)
		return
	}
}

func (t *Transcoding) errorOut(where string, err error) {
	callback := clients.NewCallbackClient()
	if err := callback.SendSegmentTranscodeError(t.request.CallbackUrl, where, err.Error(), t.request.SourceFile); err != nil {
		log.Printf("send transcode error %v", err)
		return
	}
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

func pipeToLog(pipe io.ReadCloser, name string, trigerProblem func()) {
	data := make([]byte, 4096)
	for {
		count, err := pipe.Read(data)
		if err != nil {
			log.Printf("ERROR cmd=%s %v", name, err)
			return
		}
		txt := string(data[0:count])
		if strings.Contains(txt, "Could not get stream 'tr_rend_' config!") {
			go trigerProblem()
		}
		log.Printf("out [%s] %s", name, txt)
	}
}

func commandOutputToLog(cmd *exec.Cmd, name string, trigerProblem func()) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("ERROR: cmd.StdoutPipe() %v", err)
		return
	}
	go pipeToLog(stdoutPipe, name, trigerProblem)
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ERROR: cmd.StderrPipe() %v", err)
		return
	}
	go pipeToLog(stderrPipe, name, trigerProblem)
}
