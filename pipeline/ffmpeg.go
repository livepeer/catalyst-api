package pipeline

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
)

type ffmpeg struct {
	// The base of where to output source segments to
	SourceOutputUrl string
}

func (f *ffmpeg) Name() string {
	return "catalyst_ffmpeg"
}

func (f *ffmpeg) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	log.Log(job.RequestID, "Handling job via FFMPEG/Livepeer pipeline")

	sourceOutputBaseURL, err := url.Parse(f.SourceOutputUrl)
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	log.Log(job.RequestID, "XXX:", "sourceOutputBaseURL", sourceOutputBaseURL.String())
	sourceOutputURL := sourceOutputBaseURL.JoinPath(job.RequestID)
	job.SourceOutputURL = sourceOutputURL.String()
	log.Log(job.RequestID, "XXX:", "job.SourceOutputURL", sourceOutputURL.String())

	segmentingTargetURL := sourceOutputURL.JoinPath(SEGMENTING_SUBDIR, SEGMENTING_TARGET_MANIFEST)
	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.Log(job.RequestID, "XXX:", "job.SegmentingTargetURL", segmentingTargetURL.String())

	// Begin Segmenting for non-hls input files
	if job.InputFileInfo.Format != "hls" {

		log.Log(job.RequestID, "Beginning segmenting via FFMPEG/Livepeer pipeline")
		job.ReportProgress(clients.TranscodeStatusPreparing, 0.5)

		// FFMPEG fails when presented with a raw IP + Path type URL, so we prepend "http://" to it
		internalAddress := config.HTTPInternalAddress
		if !strings.HasPrefix(internalAddress, "http") {
			internalAddress = "http://" + internalAddress
		}

		destinationURL := fmt.Sprintf("%s/api/ffmpeg/%s/index.m3u8", internalAddress, job.StreamName)
		if err := video.Segment(job.SignedSourceURL, destinationURL, job.TargetSegmentSizeSecs); err != nil {
			return nil, err
		}
	} else {
		// If hls input is detected, overwrite use the SegmentingTargetURL as the SourceManifestURL
		job.SegmentingTargetURL = job.SourceFile
		log.Log(job.RequestID, "YYY", "job.SegmentingTargetURL", job.SegmentingTargetURL) 
	}
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)

	// Segmenting Finished
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)

	// Transcode Beginning
	log.Log(job.RequestID, "Beginning transcoding via FFMPEG/Livepeer pipeline")
	job.ReportProgress(clients.TranscodeStatusTranscoding, 0.1)

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        job.SourceFile,
		CallbackURL:       job.CallbackURL,
		AccessToken:       job.AccessToken,
		TranscodeAPIUrl:   job.TranscodeAPIUrl,
		Profiles:          job.Profiles,
		SourceManifestURL: job.SegmentingTargetURL,
		SourceOutputURL:   job.SourceOutputURL,
		HlsTargetURL:      toStr(job.HlsTargetURL),
		Mp4TargetUrl:      toStr(job.Mp4TargetURL),
		RequestID:         job.RequestID,
		ReportProgress:    job.ReportProgress,
		GenerateMP4:       job.GenerateMP4,
	}

	inputInfo := video.InputVideo{
		Format:    job.InputFileInfo.Format,
		Duration:  job.InputFileInfo.Duration,
		SizeBytes: int64(job.sourceBytes),
		Tracks: []video.InputTrack{
			// Video Track
			{
				Type:         "video",
				Codec:        job.sourceCodecVideo,
				Bitrate:      job.sourceBitrateVideo,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: 0,
				VideoTrack: video.VideoTrack{
					Width:  job.sourceWidth,
					Height: job.sourceHeight,
					FPS:    job.sourceFPS,
				},
			},
			// Audio Track
			{
				Type:         "audio",
				Codec:        job.sourceCodecAudio,
				Bitrate:      job.sourceBitrateAudio,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: 0,
				AudioTrack: video.AudioTrack{
					Channels:   job.sourceChannels,
					SampleRate: job.sourceSampleRate,
					SampleBits: job.sourceSampleBits,
				},
			},
		},
	}

	job.state = "transcoding"

	sourceManifest, err := transcode.DownloadRenditionManifest(transcodeRequest.SourceManifestURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading source manifest: %s", err)
	}
	segs := sourceManifest.GetAllSegments()
	job.sourceSegments = len(segs)
	fmt.Printf(job.RequestID, "XXX: %+v", segs)

	outputs, transcodedSegments, err := transcode.RunTranscodeProcess(transcodeRequest, job.StreamName, inputInfo)
	if err != nil {
		log.LogError(job.RequestID, "RunTranscodeProcess returned an error", err)
		return nil, fmt.Errorf("transcoding failed: %w", err)
	}

	job.transcodedSegments = transcodedSegments

	// Transcoding Finished
	job.ReportProgress(clients.TranscodeStatusCompleted, 1)

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

// Boilerplate to implement the Handler interface

func (f *ffmpeg) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on ffmpeg/livepeer pipeline")
}

func (f *ffmpeg) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on ffmpeg/livepeer pipeline")
}
