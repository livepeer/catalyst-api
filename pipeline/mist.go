package pipeline

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
)

const MIST_SEGMENTING_SUBDIR = "source"
const MIST_SEGMENTING_TARGET_MANIFEST = "index.m3u8"

type mist struct {
	MistClient      clients.MistAPIClient
	SourceOutputUrl string
}

func (m *mist) Name() string {
	return "mist"
}

func (m *mist) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	var sourceOutputUrl *url.URL
	perRequestPath, err := url.JoinPath(m.SourceOutputUrl, job.RequestID)
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	perRequestMistPath, err := url.JoinPath(perRequestPath, "index.m3u8")
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	if sourceOutputUrl, err = url.Parse(perRequestMistPath); err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}

	segmentingTargetURL, err := inSameDirectory(*sourceOutputUrl, MIST_SEGMENTING_SUBDIR, MIST_SEGMENTING_TARGET_MANIFEST)
	if err != nil {
		return nil, fmt.Errorf("cannot create targetSegmentedOutputURL: %w", err)
	}
	job.SegmentingTargetURL = segmentingTargetURL.String()
	job.SourceOutputURL = perRequestPath

	mistTargetURL, err := targetURLToMistTargetURL(*sourceOutputUrl, job.TargetSegmentSizeSecs)
	if err != nil {
		return nil, fmt.Errorf("cannot create mistTargetURL: %w", err)
	}
	log.AddContext(job.RequestID, "mist_target_url", mistTargetURL)
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)

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
	if err := m.processUploadVOD(job.StreamName, job.SourceFile, mistTargetURL); err != nil {
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
	if err := CheckSegmentedDurationWithinBounds(streamInfo, p.StreamMediaDurationMillis); err != nil {
		log.Log(requestID, "Segmented Video duration is not within acceptable bounds vs. source duration: %s", err)
		return nil, fmt.Errorf("segmented Video duration is not within acceptable bounds vs. source duration: %s", err)
	}

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        job.SourceFile,
		CallbackURL:       job.CallbackURL,
		AccessToken:       job.AccessToken,
		TranscodeAPIUrl:   job.TranscodeAPIUrl,
		SourceStreamInfo:  streamInfo,
		Profiles:          job.Profiles,
		SourceManifestURL: job.SegmentingTargetURL,
		SourceOutputURL:   job.SourceOutputURL,
		HlsTargetURL:      toStr(job.HlsTargetURL),
		Mp4TargetUrl:      toStr(job.Mp4TargetURL),
		RequestID:         requestID,
		ReportProgress:    job.ReportProgress,
		GenerateMP4:       job.GenerateMP4,
	}

	var audioCodec = ""
	var videoCodec = ""

	inputInfo := video.InputVideo{
		Format:    "mp4", // hardcoded as mist stream is in dtsc format.
		Duration:  float64(p.StreamMediaDurationMillis) / 1000.0,
		SizeBytes: int64(p.WrittenBytes),
	}
	for _, track := range streamInfo.Meta.Tracks {
		inputInfo.Tracks = append(inputInfo.Tracks, video.InputTrack{
			Type:         track.Type,
			Codec:        track.Codec,
			Bitrate:      int64(track.Bps * 8),
			DurationSec:  float64(track.Lastms-track.Firstms) / 1000.0,
			StartTimeSec: float64(track.Firstms) / 1000.0,
			VideoTrack: video.VideoTrack{
				Width:  int64(track.Width),
				Height: int64(track.Height),
				FPS:    float64(track.Fpks) / 1000.0,
			},
			AudioTrack: video.AudioTrack{
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

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

func toStr(URL *url.URL) string {
	if URL != nil {
		return URL.String()
	}
	return ""
}

func (m *mist) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	// TODO: Find a better way to determine if the push status failed or not (i.e. segmenting step was successful)
	if strings.Contains(p.Last10LogLines, "FAIL") {
		log.Log(job.RequestID, "Segmenting Failed. PUSH_END trigger for stream "+p.StreamName+" was "+p.PushStatus)
		return nil, fmt.Errorf("segmenting failed: %s", p.PushStatus)
	}
	return ContinuePipeline, nil
}

func inSameDirectory(base url.URL, paths ...string) (*url.URL, error) {
	baseDir := path.Dir(base.Path)
	paths = append([]string{baseDir}, paths...)
	fullPath := path.Join(paths...)
	pathUrl, err := url.Parse(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse same directory path: %w", err)
	}
	return base.ResolveReference(pathUrl), nil
}

// We receive something in the form s3+https://xyz:xyz@storage.googleapis.com/a/b/c/index.m3u8
// and give it to Mist in the form:
//
//	s3+https://xyz:xyz@storage.googleapis.com/a/b/c/seg_$currentMediaTime.ts?m3u8=index.m3u8&split=5
func targetURLToMistTargetURL(targetURL url.URL, targetSegmentLengthSecs int64) (string, error) {
	targetManifestFilename := path.Base(targetURL.Path)
	segmentingTargetURL, err := inSameDirectory(targetURL, MIST_SEGMENTING_SUBDIR, "$currentMediaTime.ts")
	if err != nil {
		return "", fmt.Errorf("cannot create segmentingTargetURL: %w", err)
	}

	queryValues := segmentingTargetURL.Query()
	queryValues.Add("m3u8", targetManifestFilename)
	queryValues.Add("split", strconv.FormatInt(targetSegmentLengthSecs, 10))
	segmentingTargetURL.RawQuery = queryValues.Encode()

	return segmentingTargetURL.String(), nil
}

func CheckSegmentedDurationWithinBounds(streamInfo clients.MistStreamInfo, segmentedDurationMillis int64) error {
	var inputVideoLengthMillis int64
	for track, trackInfo := range streamInfo.Meta.Tracks {
		if strings.Contains(track, "video") {
			inputVideoLengthMillis = trackInfo.Lastms
		}
	}

	// We're more lenient (2x video length) if the segmented video is longer than the input
	// If the output is shorter then it's more likely to be representative of an issue
	if float64(inputVideoLengthMillis-segmentedDurationMillis) > 500 ||
		float64(inputVideoLengthMillis-segmentedDurationMillis) < -float64(inputVideoLengthMillis) {
		return fmt.Errorf("input video duration (%dms) does not match segmented video duration (%dms)", inputVideoLengthMillis, segmentedDurationMillis)
	}

	return nil
}
