package clients

import "context"

type TranscodeJobInput struct {
	// For now you can assume these are valid s3:// URLs for MediaConvert
	InputFile     string
	HLSOutputFile string
	// Call this function on the polling loop for the status of the job
	ReportProgress func(completionRatio float64)
}

type TranscodeProvider interface {
	Transcode(ctx context.Context, input TranscodeJobInput) error
}
