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
	// mediaconvert://<key id>:<key secret>@<endpoint host>?region=<aws region>&role=<arn for role>
	if u.Scheme == "mediaconvert" {
		endpoint := fmt.Sprintf("https://%s", u.Host)
		region := u.Query().Get("region")
		role := u.Query().Get("role")
		accessKeyId := u.User.Username()
		accessKeySecret, ok := u.User.Password()
		if !ok {
			return nil, fmt.Errorf("password is required with mediaconvert:// transcode provider")
		}
		return NewMediaConvertClient(MediaConvertOptions{
			Endpoint:        endpoint,
			Region:          region,
			Role:            role,
			AccessKeyID:     accessKeyId,
			AccessKeySecret: accessKeySecret,
		}), nil
	}
	return nil, fmt.Errorf("unrecognized OS scheme: %s", u.Scheme)
}
