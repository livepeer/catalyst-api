package pipeline

import (
	"fmt"
	"math"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/transcode"
)

type UploadJobPayload struct {
	SourceFile            string
	CallbackURL           string
	TargetURL             *url.URL
	SegmentingTargetURL   string
	AccessToken           string
	TranscodeAPIUrl       string
	HardcodedBroadcasters string
	RequestID             string
	Profiles              []clients.EncodedProfile
}

type RecordingEndPayload struct {
	StreamName                string
	StreamMediaDurationMillis int64
	WrittenBytes              int
}

type PushEndPayload struct {
	StreamName     string
	PushStatus     string
	Last10LogLines string
}

type StreamInfo struct {
	UploadJobPayload
	StreamName string
}

type Handler interface {
	CreateUploadJob(si StreamInfo, p UploadJobPayload) error
	HandleRecordingEndTrigger(si StreamInfo, p RecordingEndPayload) error
	HandlePushEndTrigger(si StreamInfo, p PushEndPayload) error
}

type Coordinator interface {
	CreateUploadJob(p UploadJobPayload)
	HandleRecordingEndTrigger(p RecordingEndPayload)
	HandlePushEndTrigger(p PushEndPayload)
}

func NewCoordinator(mistClient clients.MistAPIClient) Coordinator {
	return &engine{mistClient, cache.New[StreamInfo]()}
}

func NewStubCoordinator() *engine {
	return &engine{clients.StubMistClient{}, cache.New[StreamInfo]()}
}

type engine struct {
	MistClient clients.MistAPIClient
	Jobs       *cache.Cache[StreamInfo]
}

func (e *engine) CreateUploadJob(p UploadJobPayload) {
	si := StreamInfo{
		UploadJobPayload: p,
		StreamName:       config.SegmentingStreamName(p.RequestID),
	}
	log.AddContext(si.RequestID, "stream_name", si.StreamName)
	e.Jobs.Store(si.StreamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	clients.DefaultCallbackClient.SendTranscodeStatus(si.CallbackURL, si.RequestID, clients.TranscodeStatusPreparing, 0)
	go callbackWrapped(si.CallbackURL, si.RequestID, func() error {
		return e.doCreateUploadJob(si, p)
	})
}

func (e *engine) HandleRecordingEndTrigger(p RecordingEndPayload) {
	si := e.Jobs.Get(p.StreamName)
	if si.CallbackURL == "" {
		log.Log(si.RequestID, "RECORDING_END trigger invoked for unknown stream")
		return
	}
	go callbackWrapped(si.CallbackURL, si.RequestID, func() error {
		// recording end is a terminal event when uploading is done,
		// remove trigger and stream from Mist
		defer e.Jobs.Remove(si.RequestID, p.StreamName)

		return e.doHandleRecordingEndTrigger(si, p)
	})
}

func (e *engine) HandlePushEndTrigger(p PushEndPayload) {
	si := e.Jobs.Get(p.StreamName)
	if si.CallbackURL == "" {
		log.Log(si.RequestID, "PUSH_END trigger invoked for unknown stream", "streamName", p.StreamName)
		return
	}
	go callbackWrapped(si.CallbackURL, si.RequestID, func() error {
		return e.doHandlePushEndTrigger(si, p)
	})
}

func (e *engine) doCreateUploadJob(si StreamInfo, p UploadJobPayload) error {
	// Arweave URLs don't support HTTP Range requests and so Mist can't natively handle them for segmenting
	// This workaround copies the file from Arweave to S3 and then tells Mist to use the S3 URL
	if clients.IsArweaveOrIPFSURL(si.SourceFile) {
		newSourceURL, err := InSameDirectory(p.TargetURL, "source", "arweave-source.mp4")
		if err != nil {
			return fmt.Errorf("cannot create location for arweave source copy: %w", err)
		}
		log.AddContext(si.RequestID, "new_source_url", newSourceURL.String())

		if err := clients.CopyArweaveToS3(si.SourceFile, newSourceURL.String()); err != nil {
			return fmt.Errorf("invalid Arweave URL: %w", err)
		}
		si.SourceFile = newSourceURL.String()
		e.Jobs.Store(si.StreamName, si)
		clients.DefaultCallbackClient.SendTranscodeStatus(si.CallbackURL, si.RequestID, clients.TranscodeStatusPreparing, 0.1)
	}

	// Attempt an out-of-band call to generate the dtsh headers using MistIn*
	var dtshStartTime = time.Now()
	dstDir, _ := filepath.Split(si.SegmentingTargetURL)
	dtshFileName := filepath.Base(si.SourceFile)
	if err := e.MistClient.CreateDTSH(si.RequestID, si.SourceFile, dstDir+dtshFileName); err != nil {
		log.LogError(si.RequestID, "Failed to create DTSH", err, "destination", si.SourceFile)
	} else {
		log.Log(si.RequestID, "Generated DTSH File", "dtsh_generation_duration", time.Since(dtshStartTime).String())
	}

	clients.DefaultCallbackClient.SendTranscodeStatus(si.CallbackURL, si.RequestID, clients.TranscodeStatusPreparing, 0.2)

	log.Log(si.RequestID, "Beginning segmenting")
	// Tell Mist to do the segmenting. Upon completion / error, Mist will call Triggers to notify us.
	if err := e.processUploadVOD(si.StreamName, si.SourceFile, si.SegmentingTargetURL); err != nil {
		log.LogError(si.RequestID, "Cannot process upload VOD request", err)
		return fmt.Errorf("cannot process upload VOD request: %w", err)
	}

	clients.DefaultCallbackClient.SendTranscodeStatus(si.CallbackURL, si.RequestID, clients.TranscodeStatusPreparing, 0.3)
	return nil
}

func (e *engine) processUploadVOD(streamName, sourceURL, targetURL string) error {
	sourceURL = "mp4:" + sourceURL
	if err := e.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := e.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}

func (e *engine) doHandleRecordingEndTrigger(si StreamInfo, p RecordingEndPayload) error {
	// Grab the Request ID to enable us to log properly
	requestID, callbackUrl := si.RequestID, si.CallbackURL

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	defer func() {
		if err := e.MistClient.DeleteStream(p.StreamName); err != nil {
			log.LogError(requestID, "Failed to delete stream in triggerRecordingEndSegmenting", err)
		}
	}()

	// Let Studio know that we've almost finished the Segmenting phase
	clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, requestID, clients.TranscodeStatusPreparing, 0.9)

	// HACK: Wait a little bit to give the segmenting time to finish uploading.
	// Proper fix comes with a new Mist trigger to notify us that uploads are also complete
	time.Sleep(5 * time.Second)

	// Let Studio know that we've finished the Segmenting phase
	clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, requestID, clients.TranscodeStatusPreparingCompleted, 1)

	// Get the source stream's detailed track info before kicking off transcode
	// Mist currently returns the "booting" error even after successfully segmenting MOV files
	streamInfo, err := e.MistClient.GetStreamInfo(p.StreamName)
	if err != nil {
		log.LogError(requestID, "Failed to get stream info", err)
		return fmt.Errorf("failed to get stream info: %w", err)
	}

	// Compare duration of source stream to the segmented stream to ensure the input file was completely segmented before attempting to transcode
	var inputVideoLengthMillis int64
	for track, trackInfo := range streamInfo.Meta.Tracks {
		if strings.Contains(track, "video") {
			inputVideoLengthMillis = trackInfo.Lastms
		}
	}
	if math.Abs(float64(inputVideoLengthMillis-p.StreamMediaDurationMillis)) > 500 {
		log.Log(requestID, "Input video duration does not match segmented video duration", "input_duration_ms", inputVideoLengthMillis, "segmented_duration_ms", p.StreamMediaDurationMillis)
		return fmt.Errorf("input video duration (%dms) does not match segmented video duration (%dms)", inputVideoLengthMillis, p.StreamMediaDurationMillis)
	}

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        si.SourceFile,
		CallbackURL:       si.CallbackURL,
		AccessToken:       si.AccessToken,
		TranscodeAPIUrl:   si.TranscodeAPIUrl,
		SourceStreamInfo:  streamInfo,
		Profiles:          si.Profiles,
		SourceManifestURL: si.SegmentingTargetURL,
		RequestID:         requestID,
	}

	inputInfo := clients.InputVideo{
		Format:    "mp4", // hardcoded as mist stream is in dtsc format.
		Duration:  float64(p.StreamMediaDurationMillis) / 1000.0,
		SizeBytes: p.WrittenBytes,
	}
	for _, track := range streamInfo.Meta.Tracks {
		inputInfo.Tracks = append(inputInfo.Tracks, clients.InputTrack{
			Type:         track.Type,
			Codec:        track.Codec,
			Bitrate:      int64(track.Bps * 8),
			DurationSec:  float64(track.Lastms-track.Firstms) / 1000.0,
			StartTimeSec: float64(track.Firstms) / 1000.0,
			VideoTrack: clients.VideoTrack{
				Width:  int64(track.Width),
				Height: int64(track.Height),
				FPS:    float64(track.Fpks) / 1000.0,
			},
			AudioTrack: clients.AudioTrack{
				Channels:   track.Channels,
				SampleRate: track.Rate,
				SampleBits: track.Size,
			},
		})
	}

	outputs, err := transcode.RunTranscodeProcess(transcodeRequest, p.StreamName, inputInfo)
	if err != nil {
		log.LogError(requestID, "RunTranscodeProcess returned an error", err)
		return fmt.Errorf("transcoding failed: %w", err)
	}

	// defer func() {
	// Send the success callback after the DTSH creation logic
	clients.DefaultCallbackClient.SendTranscodeStatusCompleted(transcodeRequest.CallbackURL, requestID, inputInfo, outputs)
	// }()
	// TODO: CreateDTSH is hardcoded to call MistInMP4 - the call below requires a call to MistInHLS instead.
	//	 Update this logic later as it's required for Mist playback.
	/*
		// prepare .dtsh headers for all rendition playlists
		for _, output := range outputs {
			if err := d.MistClient.CreateDTSH(output.Manifest); err != nil {
				// should not block the ingestion flow or make it fail on error.
				log.LogError(requestID, "CreateDTSH() for rendition failed", err, "destination", output.Manifest)
			}
		}
	*/
	return nil
}

func (e *engine) doHandlePushEndTrigger(si StreamInfo, p PushEndPayload) error {
	// TODO: Find a better way to determine if the push status failed or not (i.e. segmenting step was successful)
	if strings.Contains(p.Last10LogLines, "FAIL") {
		log.Log(si.RequestID, "Segmenting Failed. PUSH_END trigger for stream "+p.StreamName+" was "+p.PushStatus)
		return fmt.Errorf("segmenting failed: %s", p.PushStatus)
	}
	return nil
}

func callbackWrapped(callbackURL, requestID string, f func() error) {
	err := recovered(f)
	if err != nil {
		clients.DefaultCallbackClient.SendTranscodeStatusError(callbackURL, requestID, err.Error())
		metrics.Metrics.UploadVODPipelineFailureCount.Inc()
	}
}

func recovered(f func() error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogNoRequestID("panic in callback goroutine, recovering", "err", rec)
			err = fmt.Errorf("panic in callback goroutine: %v", rec)
		}
	}()
	return f()
}

func InSameDirectory(base *url.URL, paths ...string) (*url.URL, error) {
	baseDir := path.Dir(base.Path)
	paths = append([]string{baseDir}, paths...)
	fullPath := path.Join(paths...)
	pathUrl, err := url.Parse(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse same directory path: %w", err)
	}
	return base.ResolveReference(pathUrl), nil
}
