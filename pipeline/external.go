package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/video"
)

type external struct {
	transcoder clients.TranscodeProvider
}

func (m *external) Name() string {
	return "external"
}

func (e *external) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	job.sourceBytes = job.InputFileInfo.SizeBytes
	job.sourceDurationMs = int64(math.Round(job.InputFileInfo.Duration) * 1000)
	sourceFileUrl, err := url.Parse(job.SignedSourceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid source file URL: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	outputVideos, err := e.transcoder.Transcode(ctx, clients.TranscodeJobArgs{
		RequestID:         job.RequestID,
		SegmentSizeSecs:   job.targetSegmentSizeSecs,
		InputFile:         sourceFileUrl,
		HLSOutputLocation: job.HlsTargetURL,
		MP4OutputLocation: job.Mp4TargetURL,
		Profiles:          job.Profiles,
		GenerateMP4:       job.GenerateMP4,
		ReportProgress: func(progress float64) {
			job.ReportProgress(clients.TranscodeStatusTranscoding, progress)
		},
		CollectSourceSize: func(size int64) {
			job.sourceBytes = size
		},
		CollectTranscodedSegment: func() {
			job.transcodedSegments++
		},
		InputFileInfo: job.InputFileInfo,
	})
	if err != nil {
		return nil, fmt.Errorf("external transcoder error: %w", err)
	}

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: video.InputVideo{
				// TODO: Figure out what to do here. Studio doesn't use these anyway.
			},
			Outputs: outputVideos,
		},
	}, nil
}

// Boilerlplate to implement the Handler interface

func (e *external) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on external transcode provider pipeline")
}

func (e *external) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on external transcode provider pipeline")
}
