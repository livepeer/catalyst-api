package pipeline

import (
	"fmt"
	"math"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/transcode"
)

type mist struct {
	MistClient clients.MistAPIClient
}

func (m *mist) Name() string {
	return "mist"
}

func (m *mist) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	targetManifestFilename := path.Base(job.TargetURL.Path)
	targetExtension := path.Ext(targetManifestFilename)
	if targetExtension != ".m3u8" {
		return nil, fmt.Errorf("target output file should have .m3u8 extension, found %q", targetExtension)
	}

	segmentingTargetURL, err := inSameDirectory(job.TargetURL, "source", targetManifestFilename)
	if err != nil {
		return nil, fmt.Errorf("cannot create targetSegmentedOutputURL: %w", err)
	}
	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)

	// Arweave URLs don't support HTTP Range requests and so Mist can't natively handle them for segmenting
	// This workaround copies the file from Arweave to S3 and then tells Mist to use the S3 URL
	if clients.IsArweaveOrIPFSURL(job.SourceFile) {
		newSourceURL, err := inSameDirectory(job.TargetURL, "source", "arweave-source.mp4")
		if err != nil {
			return nil, fmt.Errorf("cannot create location for arweave source copy: %w", err)
		}
		log.AddContext(job.RequestID, "new_source_url", newSourceURL.String())

		if err := clients.CopyArweaveToS3(job.SourceFile, newSourceURL.String()); err != nil {
			return nil, fmt.Errorf("invalid Arweave URL: %w", err)
		}
		job.SourceFile = newSourceURL.String()
		job.ReportProgress(clients.TranscodeStatusPreparing, 0.1)
	}

	// Attempt an out-of-band call to generate the dtsh headers using MistIn*
	var dtshStartTime = time.Now()
	dstDir, _ := filepath.Split(job.SegmentingTargetURL)
	dtshFileName := filepath.Base(job.SourceFile)
	if err := m.MistClient.CreateDTSH(job.RequestID, job.SourceFile, dstDir+dtshFileName); err != nil {
		log.LogError(job.RequestID, "Failed to create DTSH", err, "destination", job.SourceFile)
	} else {
		log.Log(job.RequestID, "Generated DTSH File", "dtsh_generation_duration", time.Since(dtshStartTime).String())
	}

	job.ReportProgress(clients.TranscodeStatusPreparing, 0.2)

	log.Log(job.RequestID, "Beginning segmenting")
	// Tell Mist to do the segmenting. Upon completion / error, Mist will call Triggers to notify us.
	if err := m.processUploadVOD(job.StreamName, job.SourceFile, job.SegmentingTargetURL); err != nil {
		log.LogError(job.RequestID, "Cannot process upload VOD request", err)
		return nil, fmt.Errorf("cannot process upload VOD request: %w", err)
	}

	job.ReportProgress(clients.TranscodeStatusPreparing, 0.3)
	return ContinuePipeline, nil
}

func (m *mist) processUploadVOD(streamName, sourceURL, targetURL string) error {
	sourceURL = "mp4:" + sourceURL
	if err := m.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := m.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}

func (m *mist) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	// Grab the Request ID to enable us to log properly
	requestID := job.RequestID

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	defer func() {
		if err := m.MistClient.DeleteStream(p.StreamName); err != nil {
			log.LogError(requestID, "Failed to delete stream in triggerRecordingEndSegmenting", err)
		}
	}()

	// Let Studio know that we've almost finished the Segmenting phase
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.9)

	// HACK: Wait a little bit to give the segmenting time to finish uploading.
	// Proper fix comes with a new Mist trigger to notify us that uploads are also complete
	time.Sleep(5 * time.Second)

	// Let Studio know that we've finished the Segmenting phase
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)

	// Get the source stream's detailed track info before kicking off transcode
	// Mist currently returns the "booting" error even after successfully segmenting MOV files
	streamInfo, err := m.MistClient.GetStreamInfo(p.StreamName)
	if err != nil {
		log.LogError(requestID, "Failed to get stream info", err)
		return nil, fmt.Errorf("failed to get stream info: %w", err)
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
		return nil, fmt.Errorf("input video duration (%dms) does not match segmented video duration (%dms)", inputVideoLengthMillis, p.StreamMediaDurationMillis)
	}

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        job.SourceFile,
		CallbackURL:       job.CallbackURL,
		AccessToken:       job.AccessToken,
		TranscodeAPIUrl:   job.TranscodeAPIUrl,
		SourceStreamInfo:  streamInfo,
		Profiles:          job.Profiles,
		SourceManifestURL: job.SegmentingTargetURL,
		RequestID:         requestID,
		ReportProgress:    job.ReportProgress,
	}

	var audioCodec = ""
	var videoCodec = ""

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

		if track.Type == "video" {
			if videoCodec != "" {
				videoCodec = "multiple"
			} else {
				videoCodec = track.Codec
			}
		} else if track.Type == "audio" {
			if audioCodec != "" {
				audioCodec = "multiple"
			} else {
				audioCodec = track.Codec
			}
		}
	}

	job.sourceCodecVideo = videoCodec
	job.sourceCodecVideo = audioCodec

	job.state = "transcoding"
	job.sourceBytes = int64(p.WrittenBytes)
	job.sourceDurationMs = p.StreamMediaDurationMillis

	sourceManifest, err := transcode.DownloadRenditionManifest(transcodeRequest.SourceManifestURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading source manifest: %s", err)
	}

	job.sourceSegments = len(sourceManifest.Segments)

	outputs, transcodedSegments, err := transcode.RunTranscodeProcess(transcodeRequest, p.StreamName, inputInfo)
	if err != nil {
		log.LogError(requestID, "RunTranscodeProcess returned an error", err)
		return nil, fmt.Errorf("transcoding failed: %w", err)
	}

	job.transcodedSegments = transcodedSegments

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
	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

func (m *mist) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	// TODO: Find a better way to determine if the push status failed or not (i.e. segmenting step was successful)
	if strings.Contains(p.Last10LogLines, "FAIL") {
		log.Log(job.RequestID, "Segmenting Failed. PUSH_END trigger for stream "+p.StreamName+" was "+p.PushStatus)
		return nil, fmt.Errorf("segmenting failed: %s", p.PushStatus)
	}
	return ContinuePipeline, nil
}

func inSameDirectory(base *url.URL, paths ...string) (*url.URL, error) {
	baseDir := path.Dir(base.Path)
	paths = append([]string{baseDir}, paths...)
	fullPath := path.Join(paths...)
	pathUrl, err := url.Parse(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse same directory path: %w", err)
	}
	return base.ResolveReference(pathUrl), nil
}
