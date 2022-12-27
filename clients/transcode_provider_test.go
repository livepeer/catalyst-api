package clients

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURLRequiresMandatoryFields(t *testing.T) {
	testCases := []struct {
		url string
		err string
	}{
		{"https://wrong.scheme", "unrecognized OS scheme"},
		{"mediaconvert://", "missing endpoint"},
		{"mediaconvert://test.com", "missing credentials"},
		{"mediaconvert://user@test.com", "missing credentials"},
		{"mediaconvert://user:pwd@test.com", "missing region"},
		{"mediaconvert://user:pwd@test.com?region=reg", "missing role"},
		{"mediaconvert://user:pwd@test.com?region=reg&role=me", "invalid s3_aux_bucket"},
		{"mediaconvert://user:pwd@test.com?region=reg&role=me&s3_aux_bucket=not_an_s3_url", "invalid s3_aux_bucket"},
	}

	for _, tc := range testCases {
		t.Run(tc.err, func(t *testing.T) {
			_, err := ParseTranscodeProviderURL(tc.url)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestParseURLSendsAllTheRightOptionsToClient(t *testing.T) {
	require := require.New(t)
	oldMediaConvertClientFunc := newMediaConvertClientFunc
	defer func() { newMediaConvertClientFunc = oldMediaConvertClientFunc }()

	callCount := 0
	newMediaConvertClientFunc = func(opts MediaConvertOptions) (TranscodeProvider, error) {
		callCount++
		require.Equal("https://test.com", opts.Endpoint)
		require.Equal("reg", opts.Region)
		require.Equal("me", opts.Role)
		require.Equal("user", opts.AccessKeyID)
		require.Equal("pwd", opts.AccessKeySecret)
		require.Equal("s3://bucket", opts.S3TransferBucket.String())
		return nil, nil
	}

	_, err := ParseTranscodeProviderURL("mediaconvert://user:pwd@test.com/?region=reg&role=me&s3_aux_bucket=s3://bucket")
	require.NoError(err)
	require.Equal(1, callCount)
}
