package clients

import (
	"context"
	"fmt"
	"net/url"
)

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

// ParseTranscodeProviderURL returns the correct provider for a given URL
func ParseTranscodeProviderURL(input string) (TranscodeProvider, error) {
	u, err := url.Parse(input)
	if err != nil {
		return nil, err
	}
	// mediaconvert://<key id>:<key secret>@<endpoint host>?region=<aws region>&role=<arn for role>&s3_aux_bucket=<s3 bucket url>
	if u.Scheme == "mediaconvert" {
		endpoint := fmt.Sprintf("https://%s", u.Host)

		accessKeyId := u.User.Username()
		accessKeySecret, ok := u.User.Password()
		if !ok {
			return nil, fmt.Errorf("missing password in url: %s", u.String())
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
		s3AuxBucket.User = u.User

		return NewMediaConvertClient(MediaConvertOptions{
			Endpoint:        endpoint,
			Region:          region,
			Role:            role,
			AccessKeyID:     accessKeyId,
			AccessKeySecret: accessKeySecret,
			S3AuxBucket:     s3AuxBucket,
		}), nil
	}
	return nil, fmt.Errorf("unrecognized OS scheme: %s", u.Scheme)
}
