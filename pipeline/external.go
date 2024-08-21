package pipeline

import (
	"context"
	"fmt"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/thumbnails"
	"net/url"
	"time"

	"github.com/livepeer/catalyst-api/clients"
)

type external struct {
	transcoder clients.TranscodeProvider
}

func (m *external) Name() string {
	return "external"
}

func (e *external) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	if e == nil || e.transcoder == nil {
		return nil, fmt.Errorf("no external transcoder configured")
	}

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
	job.TranscodingDone = time.Now()

	generateThumbs(job)

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: job.InputFileInfo,
			Outputs:    outputVideos,
		},
	}, nil
}

func generateThumbs(job *JobInfo) {
	if job.ThumbnailsTargetURL == nil {
		return
	}

	manifestUrl, err := clients.GetFirstRenditionURL(job.RequestID, job.HlsTargetURL.JoinPath("index.m3u8"))
	if err != nil {
		log.LogError(job.RequestID, "failed to get rendition URL for mediaconvert thumbs", err)
		return
	}

	log.Log(job.RequestID, "generating thumbs for mediaconvert", "manifest", manifestUrl.Redacted())
	manifest := manifestUrl.String()
	err = thumbnails.GenerateThumbsAndVTT(job.RequestID, manifest, job.ThumbnailsTargetURL)
	if err != nil {
		log.LogError(job.RequestID, "mediaconvert thumbs failed", err, "in", manifest, "out", job.ThumbnailsTargetURL)
		return
	}
}
