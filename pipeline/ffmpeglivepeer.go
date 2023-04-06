package pipeline

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
)

type ffmpeglivepeer struct {
	SourceOutputUrl string
}

func (f *ffmpeglivepeer) Name() string {
	return "ffmpeglivepeer"
}

func (f *ffmpeglivepeer) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
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

	// TODO: Segment
	log.Log(job.RequestID, "Beginning segmenting via FFMPEG/Livepeer pipeline")
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.5)

	// TODO: Segmenting Finished
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)
	log.Log(job.RequestID, "Beginning transcoding via FFMPEG/Livepeer pipeline")

	// TODO: Transcode
	job.ReportProgress(clients.TranscodeStatusTranscoding, 0.5)

	// TODO: Transcoding Finished
	job.ReportProgress(clients.TranscodeStatusCompleted, 1)

	return ContinuePipeline, nil
}

// Boilerplate to implement the Handler interface

func (f *ffmpeglivepeer) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on ffmpeg/livepeer pipeline")
}

func (f *ffmpeglivepeer) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on ffmpeg/livepeer pipeline")
}
