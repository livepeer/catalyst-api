package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/subprocess"
	"github.com/livepeer/go-tools/drivers"
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
			if err := clients.DefaultCallbackClient.SendTranscodeStatusError(transcodeRequest.CallbackURL, fmt.Sprintf("%v", err)); err != nil {
				_ = config.Logger.Log("msg", "Error sending transcode error status", "err", err)
			}
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

	inputStream, uniqueName := config.GenerateStreamNames()
	if err := mistClient.AddStream(inputStream, request.SourceFile); err != nil {
		return fmt.Errorf("error adding stream to Mist: %s", err)
	}

	// Hardcoding "-" as outputStreamName enables convert-mode in MistProcLivepeer
	configPayload, err := json.Marshal(configForSubprocess(request, inputStream, "-"))
	if err != nil {
		return fmt.Errorf("ProcLivepeerConfig json encode: %s", err)
	}
	args := string(configPayload)

	transcodeCommand := exec.Command(path.Join(config.PathMistDir, "MistProcLivepeer"), args, "--debug", "8", "--kickoff")
	if err = subprocess.LogStderr(transcodeCommand); err != nil {
		return err
	}
	transcodedDtscPipe, err := transcodeCommand.StdoutPipe()
	if err != nil {
		return fmt.Errorf("transcodeCommand failed to open stdout pipe: %v", err)
	}

	// Start the Transcode Command asynchronously - we call Wait() later in this method
	fmt.Printf("[dbg] Starting transcode via: %s\n", transcodeCommand.String())
	err = transcodeCommand.Start()
	if err != nil {
		return fmt.Errorf("failed to start MistProcLivepeer: %s", err)
	}
	defer func() {
		// Ensure MistProcLivepeer is terminated if we error out from main pipeline goroutine
		if transcodeCommand.Process != nil {
			fmt.Printf("[dbg][cleanup] kill MistProcLivepeer subprocess\n")
			_ = transcodeCommand.Process.Kill()
		} else {
			fmt.Printf("[dbg][cleanup] MistProcLivepeer subprocess already exited\n")
		}
	}()

	dir, _ := url.Parse("transcoded/")
	uploadDir := uploadURL.ResolveReference(dir)

	go func() {
		if err := transcodeCommand.Wait(); err != nil {
			if exit, ok := err.(*exec.ExitError); ok {
				_ = config.Logger.Log("msg", "MistProcLivepeer returned", "code", exit.ExitCode())
			}
			// transcodedDtscPipe should be closed now. If it isn't we need channel to signal exit
		}
		fmt.Printf("[dbg][cleanup] MistProcLivepeer process exited\n")
	}()

	// size of DTSC + MPEG-TS headers MistAnalyserDTSC required to produce media format json
	const requiredHeaderSize = 4096
	// Optimal size to contain frames of rendition-set in mpeg-ts format produced from single input frame
	const pipeByteSize = 524144
	inputBuffer := make([]byte, pipeByteSize)
	chunkSize, err := readPipe(transcodedDtscPipe, inputBuffer, requiredHeaderSize)
	if err != nil {
		return fmt.Errorf("MistProcLivepeer failed before producing output: %v", err)
	}
	// Read first frame and use it on MistAnalyserDTSC to retrieve media format info
	format, err := readFormat(inputBuffer[:chunkSize])
	if err != nil {
		return fmt.Errorf("MistProcLivepeer failed on readFormat(): %v", err)
	}

	// Now MistController would fire LIVE_TRACK_LIST trigger.
	// We have track information inside format.Tracks, use it to spawn output components.
	// Using MistOutHTTPTS as output component. This will be final sink components of our pipeline.
	outputs := make([]io.WriteCloser, 0, len(format.Tracks))
	var outputsCompleted sync.WaitGroup
	var playlistsProduced []clients.OutputVideo
	defer func() {
		// Ensure output receives EOF and completes uploading of playlist to S3
		for _, output := range outputs {
			_ = output.Close()
		}
	}()

	trackList := []DTSCMistTrack{}
	for _, track := range format.Tracks {
		if track.Type != "video" {
			continue
		}
		// Placing track.Id in dir name handles the case of several renditions having same resolution
		dirPath := fmt.Sprintf("_%s_%dx%d_%d/stream.m3u8", uniqueName, track.Width, track.Height, track.Id)
		fullPathUrl, err := uploadDir.Parse(dirPath)
		if err != nil {
			return fmt.Errorf("RunTranscodeProcess failed on url.JoinPath(%s, %s): %v", uploadDir.String(), dirPath, err)
		}
		// Select tracks via quary params
		urlParams := fullPathUrl.Query()
		// Specific rendition video track
		urlParams.Add("video", strconv.FormatInt(int64(track.Index), 10))
		// Best audio track
		urlParams.Add("audio", "maxbps")
		fullPathUrl.RawQuery = urlParams.Encode()
		track.manifestDestPath = fullPathUrl.String()
		fmt.Printf("[dbg] spawn output for %s\n", track.manifestDestPath)
		output, err := hlsOutput(track.manifestDestPath, &outputsCompleted)
		if err != nil {
			return fmt.Errorf("RunTranscodeProcess failed on hlsOutput(): %v", err)
		}
		outputs = append(outputs, output)
		trackList = append(trackList, track)
	}
	// Generate a sorted list for multivariant playlist (reverse order of bitrate then resolution):
	sort.Sort(sort.Reverse(ByBitrate(trackList)))
	multivariantPlaylist := "#EXTM3U\r\n"
	manifest := createPlaylist(multivariantPlaylist, trackList)
	manifestPath := fmt.Sprintf("%s/%s-master.m3u8", uploadDir.String(), uniqueName)
	fmt.Printf("[dbg] multivariant playlist created %s;\n%s\n", manifestPath, manifest)
	err = uploadPlaylist(manifestPath, manifest) // << this could be in separate goroutine
	if err != nil {
		return fmt.Errorf("RunTranscodeProcess failed on uploadPlaylist(): %v", err)
	}
	playlistsProduced = append(playlistsProduced, clients.OutputVideo{
		Type:     "google-s3", // TODO: Stop hardcoding this once we support other schemes
		Manifest: manifestPath,
	})
	// Duplicate input data to all outputs
	// inputBuffer already contains media data
	for {
		// This logic uses single []byte buffer ot sequentially send data to outputs in order
		// As consequence all outputs are at pace of slowest one.
		//
		// Alternative is to create new []byte buffer for input chunk and send that pointer to
		//   output's buffered channel where dedicated goroutine would read chunk and send to output pipe.
		//   This will take pace of input and buffer all pending data in memory.
		//   Consider this logic as MistProcLivepeer doesn't have robust buffering logic as it relies on
		//   shared-memory buffering of MistController
		fmt.Printf("[dbg] pipe %d bytes to %d outputs\n", chunkSize, len(outputs))
		for i := len(outputs) - 1; i >= 0; i-- { // reverse loop for removing output on error
			err = writeToPipe(outputs[i], inputBuffer[:chunkSize])
			if err != nil {
				// this output failed
				fmt.Printf("[dbg] output %d failed %v\n", i, err)
				_ = outputs[i].Close()
				outputs = append(outputs[:i], outputs[i+1:]...)
				// We can continue or abort pipeline on output error ..
				return fmt.Errorf("hlsOutput failed on writeToPipe: %v", err)
				// Continue only if studio knows how to redo only failed renditions.
				// If studio always repeats entire pipeline, no point in doing extra work.
			}
		}
		// Read more input
		chunkSize, err = transcodedDtscPipe.Read(inputBuffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("RunTranscodeProcess failed reading input: %v", err)
		}
	}

	transcodeEnd := time.Now()
	_ = config.Logger.Log("msg", "transcoding complete, upload in progress")
	outputsCompleted.Wait()
	uploadOverhead := time.Since(transcodeEnd)
	_ = config.Logger.Log("msg", "upload complete", "overhead", uploadOverhead)

	fmt.Printf("[dbg] transcoding pipeline complete\n")
	// Upload of all renditions is successful at this point
	inputTracks, videoDurationSecs := extractTracksFrom(request.SourceStreamInfo)
	err = clients.DefaultCallbackClient.SendTranscodeStatusCompleted(
		request.CallbackURL,
		clients.InputVideo{
			Format:   "unknown",
			Duration: videoDurationSecs,
			Tracks:   inputTracks,
		},
		playlistsProduced,
	)
	if err != nil {
		_ = config.Logger.Log("msg", "Error sending transcode complete status", "err", err)
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

func readPipe(pipe io.ReadCloser, into []byte, requiredSize int) (int, error) {
	pipeSize := 0
	for pipeSize < requiredSize {
		len, err := pipe.Read(into)
		pipeSize += len
		if err != nil {
			return pipeSize, err
		}
	}
	return pipeSize, nil
}

func writeToPipe(pipe io.WriteCloser, data []byte) error {
	for len(data) > 0 {
		count, err := pipe.Write(data)
		if err != nil {
			return err
		}
		data = data[count:]
	}
	return nil
}

func hlsOutput(destination string, signal *sync.WaitGroup) (io.WriteCloser, error) {
	// Without specifying stream name output is in convert-mode taking input DTSC from stdin
	output := exec.Command(path.Join(config.PathMistDir, "MistOutHTTPTS"), destination)
	if err := subprocess.LogStderr(output); err != nil {
		return nil, err
	}
	inputPipe, err := output.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("analyser failed to open stdin pipe: %v", err)
	}
	signal.Add(1)
	go func() {
		defer signal.Done()
		// Wait for subprocess to exit
		if err := output.Wait(); err != nil {
			if _, ok := err.(*exec.ExitError); !ok {
				_ = config.Logger.Log("msg", "hlsOutput process exited", "err", err.Error())
			}
		}
	}()
	return inputPipe, nil
}

func readFormat(inputData []byte) (DTSCHeaderV4, error) {
	format := DTSCHeaderV4{}
	// spawn MistAnalyserDTSC on inputData
	analyser := exec.Command(path.Join(config.PathMistDir, "MistAnalyserDTSC"), "--detail", "-2", "-")
	if err := subprocess.LogStderr(analyser); err != nil {
		return format, err
	}
	jsonOutputPipe, err := analyser.StdoutPipe()
	if err != nil {
		return format, fmt.Errorf("analyser failed to open stdout pipe: %v", err)
	}
	inputPipe, err := analyser.StdinPipe()
	if err != nil {
		return format, fmt.Errorf("analyser failed to open stdin pipe: %v", err)
	}
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	errors := []error{}
	go func() {
		// send input to subprocess
		for len(inputData) > 0 {
			written, err := inputPipe.Write(inputData)
			if err != nil {
				errors = append(errors, fmt.Errorf("analyser failed to write to stdin %v", err))
				// cancel()
			}
			inputData = inputData[written:]
		}
	}()
	go func() {
		// Wait for subprocess to exit
		if err := analyser.Wait(); err != nil {
			if _, ok := err.(*exec.ExitError); !ok {
				errors = append(errors, fmt.Errorf("analyser exited %v", err))
				// cancel()
			}
		}
	}()

	jsonOutput := make([]byte, 4096)
	outputBuffer := make([]byte, 4096)
	for {
		size, err := jsonOutputPipe.Read(outputBuffer)
		jsonOutput = append(jsonOutput, (outputBuffer[:size])...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return format, fmt.Errorf("analyser failed to read json output %v", err)
		}
	}

	if err := json.Unmarshal(jsonOutput, &format); err != nil {
		return format, fmt.Errorf("analyser failed to Unmarshal json output %v", err)
	}
	// store Index for each entry:
	for key, track := range format.Tracks {
		index, err := extractIndexFromTrackKey(key)
		if err != nil {
			errors = append(errors, fmt.Errorf("analyser extractIndexFromTrackKey() %v", err))
			continue
		}
		// update Index
		track.Index = index
		format.Tracks[key] = track
	}
	if len(errors) > 0 {
		// Report errors from goroutines and extractIndexFromTrackKey
		errorStrings := make([]string, len(errors))
		for _, err = range errors {
			errorStrings = append(errorStrings, err.Error())
		}
		return format, fmt.Errorf(strings.Join(errorStrings, "; "))
	}
	return format, nil
}

func extractIndexFromTrackKey(key string) (int32, error) {
	parts := strings.Split(key, "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("extractIndexFromTrackKey unknown format %s", key)
	}
	index, err := strconv.ParseInt(parts[len(parts)-1], 10, 32)
	return int32(index), err
}

func extractTracksFrom(sourceInfo clients.MistStreamInfo) ([]clients.InputTrack, float64) {
	var tracks []clients.InputTrack
	var videoDurationSecs float64
	for _, track := range sourceInfo.Meta.Tracks {
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
	return tracks, videoDurationSecs
}

type DTSCMistTrack struct {
	Id          int32  `json:"trackid"`
	StartTimeMs int32  `json:"firstms"`
	EndTimeMs   int32  `json:"lastms"`
	ByteRate    int32  `json:"bps"`
	MaxByteRate int32  `json:"maxbps"`
	CodecConfig []byte `json:"init"`
	Codec       string `json:"codec"`
	Type        string `json:"type"`
	// Calculated by transcoding process
	Index            int32  `json:"-"` // inferred from track key
	manifestDestPath string `json:"-"`
	// Video:
	Width  int32 `json:"width"`
	Height int32 `json:"height"`
	Fpks   int32 `json:"fpks"`
	// Audio:
	RateHz   int32 `json:"rate"`
	SizeBits int32 `json:"size"`
	Channels int32 `json:"channels"`
}

type DTSCHeaderV4 struct {
	Live     int   `json:"live"`
	Version  int   `json:"version"`
	Unixzero int64 `json:"unixzero"`

	Tracks map[string]DTSCMistTrack `json:"tracks"`
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

type ByBitrate []DTSCMistTrack

func (a ByBitrate) Len() int {
	return len(a)
}

func (a ByBitrate) Less(i, j int) bool {
	if a[i].ByteRate == a[j].ByteRate {
		// if two tracks have the same byterate, then sort by resolution
		return a[i].Width*a[i].Height < a[j].Width*a[j].Height
	} else {
		return a[i].ByteRate < a[j].ByteRate
	}
}

func (a ByBitrate) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func createPlaylist(multivariantPlaylist string, tracks []DTSCMistTrack) string {
	for _, track := range tracks {
		multivariantPlaylist += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\r\n%s\r\n", track.ByteRate*8, track.Width, track.Height, track.manifestDestPath)
	}
	return multivariantPlaylist
}

func uploadPlaylist(uploadPath, manifest string) error {
	storageDriver, err := drivers.ParseOSURL(uploadPath, true)
	if err != nil {
		return fmt.Errorf("error parsing multivariant playlist's upload directory: %s, error: %s", uploadPath, err)
	}
	session := storageDriver.NewSession("")
	ctx := context.Background()
	_, err = session.SaveData(ctx, "", bytes.NewBuffer([]byte(manifest)), nil, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to upload multivariant playlist to: %s, error: %s", uploadPath, err)
	}
	return nil
}
