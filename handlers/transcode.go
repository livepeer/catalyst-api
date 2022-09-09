package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type TranscodeSegmentRequest struct {
	SourceFile           string                 `json:"source_location"`
	CallbackUrl          string                 `json:"callback_url"`
	ManifestID           string                 `json:"manifestID"`
	StreamID             string                 `json:"streamID"`
	SessionID            string                 `json:"sessionID"`
	StreamKey            string                 `json:"streamKey"`
	Presets              []string               `json:"presets"`
	ObjectStore          string                 `json:"objectStore"`
	RecordObjectStore    string                 `json:"recordObjectStore"`
	RecordObjectStoreURL string                 `json:"recordObjectStoreUrl"`
	Profiles             []cache.EncodedProfile `json:"profiles"`
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

		if err := RunTranscodeProcess(d.MistClient, transcodeRequest); err != nil {
			errors.WriteHTTPInternalServerError(w, "Error running Transcode process", err)
		}
	}
}

// RunTranscodeProcess starts `MistLivepeeerProc` as a subprocess to transcode inputStream into renditionsStream.
func RunTranscodeProcess(mistClient clients.MistAPIClient, request TranscodeSegmentRequest) error {
	inputStream, renditionsStream := config.GenerateStreamNames()
	if err := mistClient.AddStream(inputStream, request.SourceFile); err != nil {
		return fmt.Errorf("error adding stream to Mist: %s", err)
	}

	configPayload, err := json.Marshal(configForSubprocess(request, inputStream, renditionsStream))
	if err != nil {
		return fmt.Errorf("ProcLivepeerConfig json encode: %s", err)
	}

	transcodeCommand := exec.Command(config.PathMistProcLivepeer, "-")
	stdinPipe, err := transcodeCommand.StdinPipe()
	if err != nil {
		return fmt.Errorf("transcodeCommand.StdinPipe: %s", err)
	}
	commandOutputToLog(transcodeCommand, "coding")

	sent, err := stdinPipe.Write(configPayload)
	if err != nil {
		return fmt.Errorf("stdinPipe.Write: %s", err)
	}
	if sent != len(configPayload) {
		return fmt.Errorf("short write on stdinPipe.Write: %s", err)
	}

	if err := stdinPipe.Close(); err != nil {
		return fmt.Errorf("stdinPipe.Close: %s", err)
	}

	// Start the Transcode Command asynchronously - we call Wait() later in this method
	if err := transcodeCommand.Start(); err != nil {
		return fmt.Errorf("start transcodeCommand: %s", err)
	}

	// TODO: remove when Mist code is updated https://github.com/DDVTECH/mistserver/issues/81
	// Starting SOURCE_PREFIX stream because MistProcLivepeer is unable to start it automatically
	file, err := os.CreateTemp("", "tmp-*.ts")
	if err != nil {
		return fmt.Errorf("error creating temporary file for SOURCE_PREFIX stream: %s", err)
	}

	if err := mistClient.PushStart(inputStream, file.Name()); err != nil {
		return fmt.Errorf("PushStart(inputStream): %s", err)
	}

	inputUrl, err := url.Parse(request.SourceFile)
	if err != nil {
		return fmt.Errorf("invalid request source_location: %s", err)
	}

	dir, _ := url.Parse(".")
	uploadDir := inputUrl.ResolveReference(dir)
	// Cache the stream data, later used in the trigger handlers called by Mist
	cache.DefaultStreamCache.Transcoding.Store(renditionsStream, cache.SegmentInfo{
		CallbackUrl: request.CallbackUrl,
		Source:      request.SourceFile,
		Profiles:    request.Profiles[:],
		UploadDir:   uploadDir.String(),
	})

	if err := transcodeCommand.Wait(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			log.Printf("MistProcLivepeer returned %d", exit.ExitCode())
		}
		return fmt.Errorf("exec transcodeCommand: %s", err)
	}

	return nil
}

// configForSubprocess transforms request information to MistProcLivepeer config json
// We use .HardcodedBroadcasters assuming we have local B-node.
// The AudioSelect is configured to use single audio track from input.
// Same applies on transcoder side, expect Livepeer to use single best video track as input.
func configForSubprocess(req TranscodeSegmentRequest, inputStreamName, outputStreamName string) ProcLivepeerConfig {
	conf := ProcLivepeerConfig{
		InputStreamName:       inputStreamName,
		OutputStreamName:      outputStreamName,
		Leastlive:             true,
		AudioSelect:           "maxbps",
		HardcodedBroadcasters: fmt.Sprintf(`[{"address":"http://127.0.0.1:%d"}]`, config.BroadcasterPort),
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

func commandOutputToLog(cmd *exec.Cmd, name string) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("ERROR: cmd.StdoutPipe() %v", err)
		return
	}
	go pipeToLog(stdoutPipe, name)
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ERROR: cmd.StderrPipe() %v", err)
		return
	}
	go pipeToLog(stderrPipe, name)
}

func pipeToLog(pipe io.ReadCloser, name string) {
	data := make([]byte, 4096)
	for {
		count, err := pipe.Read(data)
		if err != nil {
			log.Printf("ERROR cmd=%s %v", name, err)
			return
		}
		log.Printf("out [%s] %s", name, string(data[0:count]))
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
	InputStreamName       string                      `json:"source"`
	OutputStreamName      string                      `json:"sink"`
	Leastlive             bool                        `json:"leastlive"`
	HardcodedBroadcasters string                      `json:"hardcoded_broadcasters"`
	AudioSelect           string                      `json:"audio_select"`
	Profiles              []ProcLivepeerConfigProfile `json:"target_profiles"`
}
