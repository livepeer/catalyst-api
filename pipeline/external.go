package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/livepeer/catalyst-api/transcode"
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
	sourceFileUrl, err := url.Parse(job.SourceFile)
	if err != nil {
		return nil, fmt.Errorf("invalid source file URL: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	outputVideos, err := e.transcoder.Transcode(ctx, clients.TranscodeJobArgs{
		RequestID:     job.RequestID,
		InputFile:     sourceFileUrl,
		HLSOutputFile: job.TargetURL,
		Profiles:      job.Profiles,
		ReportProgress: func(progress float64) {
			job.ReportProgress(clients.TranscodeStatusTranscoding, progress)
		},
		CollectSourceSize: func(size int64) {
			job.sourceBytes = size
		},
		CollectTranscodedSegment: func() {
			job.transcodedSegments++
		},
	})
	if err != nil {
		return nil, fmt.Errorf("external transcoder error: %w", err)
	}

	playbackURL, err := transcode.PublishDriverSession(job.TargetURL.String(), job.TargetURL.Path)
	if err != nil {
		return nil, err
	}

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: video.InputVideo{
				// TODO: Figure out what to do here. Studio doesn't use these anyway.
			},
      // TODO: Resolve 
			Outputs: []clients.OutputVideo{
				{
					Type:     "object_store",
					Manifest: playbackURL,
					Videos:   []clients.OutputVideoFile{
						// TODO: Figure out what to do here. Studio doesn't use these anyway.
					},
				},
			},
			// TODO add mp4 outs?
      // TODO: Resolve conflicts
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
