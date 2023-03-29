package clients

import (
	"context"
	"fmt"
	"net/url"

	"github.com/livepeer/catalyst-api/video"
)

type TranscodeJobArgs struct {
	// Input and output URLs for the job. Input can be any HTTP or OS URL, while
	// output must be a OS URL.
	InputFile, HLSOutputLocation, MP4OutputLocation *url.URL
	// Target segment size
	SegmentSizeSecs int64
	// Just for logging purposes.
	RequestID string
	// Function that should be called every so often with the progress of the job.
	ReportProgress func(completionRatio float64)
	// Input File info used to by transcoder provider(s) to set transcode options
	InputFileInfo video.InputVideo
	Profiles      []video.EncodedProfile
	GenerateMP4   bool

	// Collect size of an asset
	CollectSourceSize        func(size int64)
	CollectTranscodedSegment func()
}

// TranscodProviders is the interface to an external video processing service
// that can be used instead of the Mist+Livepeer Network pipeline. It's used for
// several reason, including reliability (e.g. fallback on error, use to
// transcode unsupported files, etc) and quality assurance (compare result of
// external vs mist pipelines).
type TranscodeProvider interface {
	Transcode(ctx context.Context, args TranscodeJobArgs) ([]video.OutputVideo, error)
}

// Used only for mocking the client constructor on tests
var newMediaConvertClientFunc = NewMediaConvertClient

// ParseTranscodeProviderURL returns the correct provider for a given URL
func ParseTranscodeProviderURL(input string) (TranscodeProvider, error) {
	u, err := url.Parse(input)
	if err != nil {
		return nil, err
	}
	// mediaconvert://<key id>:<key secret>@<endpoint host>?region=<aws region>&role=<arn for role>&s3_aux_bucket=<s3 bucket url>
	if u.Scheme == "mediaconvert" {
		endpoint := fmt.Sprintf("https://%s", u.Host)
		if u.Host == "" {
			return nil, fmt.Errorf("missing endpoint in url: %s", u.String())
		}

		accessKeyId := u.User.Username()
		accessKeySecret, ok := u.User.Password()
		if !ok || accessKeyId == "" || accessKeySecret == "" {
			return nil, fmt.Errorf("missing credentials in url: %s", u.String())
		}

		region := u.Query().Get("region")
		if region == "" {
			return nil, fmt.Errorf("missing region querystring: %s", u.String())
		}
		role := u.Query().Get("role")
		if role == "" {
			return nil, fmt.Errorf("missing role querystring: %s", u.String())
		}

		s3AuxBucketStr := u.Query().Get("s3_aux_bucket")
		s3AuxBucket, err := url.Parse(s3AuxBucketStr)
		if err != nil || s3AuxBucket.Scheme != "s3" {
			return nil, fmt.Errorf("invalid s3_aux_bucket %s: %w", s3AuxBucketStr, err)
		}

		return newMediaConvertClientFunc(MediaConvertOptions{
			Endpoint:         endpoint,
			Region:           region,
			Role:             role,
			AccessKeyID:      accessKeyId,
			AccessKeySecret:  accessKeySecret,
			S3TransferBucket: s3AuxBucket,
		})
	}
	return nil, fmt.Errorf("unrecognized OS scheme: %s", u.Scheme)
}
