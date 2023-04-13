package pipeline

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
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
	sourceOutputURL := sourceOutputBaseURL.JoinPath(job.RequestID)
	segmentingTargetURL := sourceOutputURL.JoinPath(SEGMENTING_SUBDIR, SEGMENTING_TARGET_MANIFEST)

	job.SourceOutputURL = sourceOutputURL.String()
	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)

	// Begin Segmenting
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

	// Segmenting Finished
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)

	// TODO: Transcode
	log.Log(job.RequestID, "Beginning transcoding via FFMPEG/Livepeer pipeline")
	job.ReportProgress(clients.TranscodeStatusTranscoding, 0.5)

	// TODO: Transcoding Finished
	job.ReportProgress(clients.TranscodeStatusCompleted, 1)

	return ContinuePipeline, nil
}

// Boilerplate to implement the Handler interface

func (f *ffmpeg) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on ffmpeg/livepeer pipeline")
}

func (f *ffmpeg) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on ffmpeg/livepeer pipeline")
}
