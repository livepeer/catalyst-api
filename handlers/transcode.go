package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"path"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/subprocess"
	"github.com/xeipuuv/gojsonschema"
)

type TranscodeSegmentRequest struct {
	SourceFile      string                 `json:"source_location"`
	CallbackURL     string                 `json:"callback_url"`
	UploadURL       string                 `json:"upload_url"`
	StreamKey       string                 `json:"streamKey"`
	AccessToken     string                 `json:"accessToken"`
	TranscodeAPIUrl string                 `json:"transcodeAPIUrl"`
	Profiles        []cache.EncodedProfile `json:"profiles"`
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

		if err := RunTranscodeProcess(d.MistClient, transcodeRequest); err != nil {
			errors.WriteHTTPInternalServerError(w, "Error running Transcode process", err)
		}
	}
}

// RunTranscodeProcess starts `MistLivepeeerProc` as a subprocess to transcode inputStream into renditionsStream.
func RunTranscodeProcess(mistClient clients.MistAPIClient, request TranscodeSegmentRequest) error {

	uploadURL, err := url.Parse(request.UploadURL)
	if err != nil {
		return fmt.Errorf("invalid request source location: %s, error: %s", request.SourceFile, err)
	}

	inputStream, renditionsStream := config.GenerateStreamNames()
	if err := mistClient.AddStream(inputStream, request.UploadURL); err != nil {
		return fmt.Errorf("error adding stream to Mist: %s", err)
	}

	configPayload, err := json.Marshal(configForSubprocess(request, inputStream, renditionsStream))
	if err != nil {
		return fmt.Errorf("ProcLivepeerConfig json encode: %s", err)
	}
	args := string(configPayload)

	transcodeCommand := exec.Command(path.Join(config.PathMistDir, "MistProcLivepeer"), args, "--debug", "8", "--kickoff")
	if err = subprocess.LogOutputs(transcodeCommand); err != nil {
		return err
	}

	// Start the Transcode Command asynchronously - we call Wait() later in this method
	fmt.Printf("Starting transcode via: %s\n", transcodeCommand.String())
	err = transcodeCommand.Start()
	if err != nil {
		return fmt.Errorf("failed to start MistProcLivepeer: %s", err)
	}

	dir, _ := url.Parse("transcoded/")
	uploadDir := uploadURL.ResolveReference(dir)
	// Cache the stream data, later used in the trigger handlers called by Mist
	cache.DefaultStreamCache.Transcoding.Store(renditionsStream, cache.SegmentInfo{
		CallbackUrl: request.CallbackURL,
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
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(request.CallbackURL, clients.TranscodeStatusTranscoding, 1); err != nil {
		_ = config.Logger.Log("msg", "Error in SendTranscodeStatus", "err", err)
	}

	segmentInfo := cache.DefaultStreamCache.Transcoding.Get(renditionsStream)
	if segmentInfo == nil {
		return fmt.Errorf("failed to fetch ID %q from stream cache when building SendTranscodeStatusCompleted message", renditionsStream)
	}

	var tracks []clients.InputTrack
	var videoDurationSecs float64
	for _, track := range request.SourceStreamInfo.Meta.Tracks {
		if track.Type == "video" {
			videoDurationSecs = float64(track.Lastms) / 1000
			tracks = append(tracks, clients.InputTrack{
				Type:        "video",
				Codec:       track.Codec,
				DurationSec: videoDurationSecs,
				Bitrate:     track.Bps,
				VideoTrack: clients.VideoTrack{
					FPS:         track.Fpks,
					Width:       track.Width,
					Height:      track.Height,
					PixelFormat: "unknown",
				},
			})
		} else if track.Type == "audio" {
			tracks = append(tracks, clients.InputTrack{
				Type:        "audio",
				Codec:       track.Codec,
				Bitrate:     track.Bps,
				DurationSec: float64(track.Lastms) / 1000,
				AudioTrack: clients.AudioTrack{
					Channels:   track.Channels,
					SampleRate: track.Rate,
				},
			})
		}
	}

	err = clients.DefaultCallbackClient.SendTranscodeStatusCompleted(
		request.CallbackURL,
		clients.InputVideo{
			Format:   "unknown",
			Duration: videoDurationSecs,
			Tracks:   tracks,
		},
		segmentInfo.Outputs,
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
	} else {
		hardcodedBroadcasters = fmt.Sprintf(`[{"address":"http://127.0.0.1:%d"}]`, config.DefaultBroadcasterPort)
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
