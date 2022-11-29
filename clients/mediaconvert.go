package clients

import "context"

type MediaConvert struct {
	// TODO
}

func NewMediaConvertClient( /*add any static config you need like endpoint, role, queue etc*/ ) TranscodeProvider {
	return &MediaConvert{
		// TODO
	}
}

// This should do the whole transcode job, including the polling loop for the
// job status until it is completed.
//
// It should call the input.ReportProgress function to report the progress of
// the job during the polling loop.
func (mc *MediaConvert) Transcode(ctx context.Context, input TranscodeJobInput) error {
	// TODO
	panic("not implemented")
}
