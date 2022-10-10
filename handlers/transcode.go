package handlers

import (
	"bufio"
	"bytes"
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
	"github.com/m7shapan/njson"
	"github.com/xeipuuv/gojsonschema"
)

type TranscodeSegmentRequest struct {
	SourceFile            string                 `json:"source_location"`
	CallbackUrl           string                 `json:"callback_url"`
	ManifestID            string                 `json:"manifestID"`
	StreamID              string                 `json:"streamID"`
	SessionID             string                 `json:"sessionID"`
	StreamKey             string                 `json:"streamKey"`
	AccessToken           string                 `json:"accessToken"`
	TranscodeAPIUrl       string                 `json:"transcodeAPIUrl"`
	HardcodedBroadcasters string                 `json:"hardcodedBroadcasters"`
	Presets               []string               `json:"presets"`
	ObjectStore           string                 `json:"objectStore"`
	RecordObjectStore     string                 `json:"recordObjectStore"`
	RecordObjectStoreURL  string                 `json:"recordObjectStoreUrl"`
	Profiles              []cache.EncodedProfile `json:"profiles"`
	Detection             struct {
		Freq                uint `json:"freq"`
		SampleRate          uint `json:"sampleRate"`
		SceneClassification []struct {
			Name string `json:"name"`
		} `json:"sceneClassification"`
	} `json:"detection"`
	VerificationFreq uint `json:"verificationFreq"`
	SourceStreamInfo string
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

// stream from a source to a destination buffer while also printing
func streamOutput(src io.Reader, dst *bytes.Buffer, out io.Writer) error {
	mw := io.MultiWriter(dst, out)
	s := bufio.NewReader(src)
	for {
		var line []byte
		line, err := s.ReadSlice('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err == io.EOF {
			return fmt.Errorf("Improper termination: %v", line)
		}
		if err != nil {
			return err
		}
		_, err = mw.Write(line)
		if err != nil {
			return err
		}
	}
	return nil
}

// RunTranscodeProcess starts `MistLivepeeerProc` as a subprocess to transcode inputStream into renditionsStream.
func RunTranscodeProcess(mistClient clients.MistAPIClient, request TranscodeSegmentRequest) error {

	inputUrl, err := url.Parse(request.SourceFile)
	if err != nil {
		return fmt.Errorf("invalid request source location: %s, error: %s", request.SourceFile, err)
	}

	inputStream, renditionsStream := config.GenerateStreamNames()
	if err := mistClient.AddStream(inputStream, request.SourceFile); err != nil {
		return fmt.Errorf("error adding stream to Mist: %s", err)
	}

	configPayload, err := json.Marshal(configForSubprocess(request, inputStream, renditionsStream))
	if err != nil {
		return fmt.Errorf("ProcLivepeerConfig json encode: %s", err)
	}
	args := string(configPayload)

	transcodeCommand := exec.Command(config.PathMistProcLivepeer, args, "--debug", "8", "--kickoff")

	var stdout, stderr bytes.Buffer
	stderrPipe, err := transcodeCommand.StderrPipe()
	if err != nil {
		return fmt.Errorf("Failed to open stderr pipe: %s", err)
	}
	stdoutPipe, err := transcodeCommand.StdoutPipe()
	if err != nil {
		return fmt.Errorf("Failed to open stdout pipe: %s", err)
	}

	// Start the Transcode Command asynchronously - we call Wait() later in this method
	fmt.Printf("Starting transcode via: %s\n", transcodeCommand.String())
	err = transcodeCommand.Start()
	if err != nil {
		return fmt.Errorf("Failed to start MistProcLivepeer: %s", err)
	}

	go func() {
		if streamOutput(stdoutPipe, &stdout, os.Stdout) != nil {
			_ = fmt.Errorf("Failed to stream output from stdout")
		}
	}()
	go func() {
		if streamOutput(stderrPipe, &stderr, os.Stderr) != nil {
			_ = fmt.Errorf("Failed to stream output from stderr")
		}
	}()

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

	// If we're here, then transcode completed successfully
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(request.CallbackUrl, clients.TranscodeStatusTranscoding, 1); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
	}

	v := MistSourceVideo{}
	a := MistSourceAudio{}

	err = njson.Unmarshal([]byte(request.SourceStreamInfo), &v)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal source stream info json: %s", err)
	}

	err = njson.Unmarshal([]byte(request.SourceStreamInfo), &a)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal source stream info json: %s", err)
	}

	err = clients.DefaultCallbackClient.SendTranscodeStatusCompleted(
		request.CallbackUrl,
		clients.InputVideo{
			Format:   "unknown",
			Duration: v.Duration,
			Tracks: []clients.InputTrack{
				{
					Type:        "video",
					Codec:       v.Codec,
					DurationSec: v.Duration,
					Bitrate:     v.Bitrate,
					VideoTrack: clients.VideoTrack{
						FPS:         v.FPS,
						Width:       v.Width,
						Height:      v.Height,
						PixelFormat: "unknown",
					},
				},
				{
					Type:        "audio",
					Codec:       a.Codec,
					Bitrate:     a.Bitrate,
					DurationSec: a.Duration,
					AudioTrack: clients.AudioTrack{
						Channels:   a.Channels,
						SampleRate: a.SampleRate,
					},
				},
			},
		},
		[]clients.OutputVideo{
			{
				Type:     "google-s4",
				Manifest: "s4://livepeer-studio-uploads/videos/<video-id>/master.m3u8",
				Videos: []clients.OutputVideoFile{
					{
						Type:      "mp5",
						SizeBytes: 12346,
						Location:  "s4://livepeer-studio-uploads/videos/<video-id>/video-480p.mp4",
					},
				},
			},
		},
	)
	if err != nil {
		_ = config.Logger.Log("msg", "Error sending Transcode Completed in stubTranscodingCallbacksForStudio", "err", err)
	}

	return nil
}

// configForSubprocess transforms request information to cmd line args to MistProcLivepeer (as json string)
// For transcoding, there are two options:
//  1. use HardcodedBroadcasters if a local Broadcaster node is available
//  2. use livepeer.studio nodes via an API key
//
// The AudioSelect is configured to use a single audio track from input.
func configForSubprocess(req TranscodeSegmentRequest, inputStreamName, outputStreamName string) ProcLivepeerConfig {
	// If access-token is provided, use the API url for transcoding.
	// Otherwise, use hardcoded-broadcaster for transcoding.
	var apiUrl, hardcodedBroadcasters string
	if req.AccessToken != "" {
		if req.TranscodeAPIUrl != "" {
			apiUrl = req.TranscodeAPIUrl
		} else {
			apiUrl = config.DefaultCustomAPIUrl
		}
	} else if req.HardcodedBroadcasters != "" {
		hardcodedBroadcasters = req.HardcodedBroadcasters
	} else {
		hardcodedBroadcasters = ""
	}
	conf := ProcLivepeerConfig{
		AccessToken:           req.AccessToken,
		CustomAPIUrl:          apiUrl,
		InputStreamName:       inputStreamName,
		OutputStreamName:      outputStreamName,
		Leastlive:             true,
		AudioSelect:           "maxbps",
		HardcodedBroadcasters: hardcodedBroadcasters,
	}

	// If Profiles are not available (e.g. from /api/vod), then
	// use hardcoded list of profiles for transcoding. Otherwise
	// use the profiles in the request itself (e.g. from /api/transcode/file)
	if len(req.Profiles) == 0 {
		defaultProfiles := []cache.EncodedProfile{
			{
				Name:    "360p",
				Width:   640,
				Height:  360,
				Bitrate: 800000,
				FPS:     24,
			},
			{
				Name:    "720p",
				Width:   1280,
				Height:  720,
				Bitrate: 3000000,
				FPS:     24,
			},
		}
		req.Profiles = append(req.Profiles, defaultProfiles...)
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
	CustomAPIUrl          string                      `json:"custom_api_url,omitempty"`
	InputStreamName       string                      `json:"source"`
	OutputStreamName      string                      `json:"sink"`
	Leastlive             bool                        `json:"leastlive"`
	HardcodedBroadcasters string                      `json:"hardcoded_broadcasters,omitempty"`
	AudioSelect           string                      `json:"audio_select"`
	Profiles              []ProcLivepeerConfigProfile `json:"target_profiles"`
}

type MistSourceVideo struct {
	Duration float64 `njson:"meta.tracks.video*.lastms"`
	Codec    string  `njson:"meta.tracks.video*.codec"`
	Bitrate  int     `njson:"meta.tracks.video*.bps"`
	FPS      int     `njson:"meta.tracks.video*.fpks"`
	Width    int     `njson:"width"`
	Height   int     `njson:"height"`
}

type MistSourceAudio struct {
	Duration   float64 `njson:"meta.tracks.audio*.lastms"`
	Codec      string  `njson:"meta.tracks.audio*.codec"`
	Bitrate    int     `njson:"meta.tracks.audio*.bps"`
	Channels   int     `njson:"meta.tracks.audio*.channels"`
	SampleRate int     `njson:"meta.tracks.audio*.rate"`
}
