package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTargetOutputs(t *testing.T) {
	tests := []struct {
		name                 string
		req                  UploadVODRequest
		expectedHlsURL       string
		expectedMp4URL       string
		expectedFragMp4URL   string
		expectedMp4ShortOnly bool
	}{
		{
			name: "single output location with HLS",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					HLS: "enabled",
				}}}},
			expectedHlsURL:     "s3+https://user:pass@bucket",
			expectedMp4URL:     "",
			expectedFragMp4URL: "",
		},
		{
			name: "no location with HLS",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					MP4: "enabled",
				}}}},
			expectedHlsURL:     "",
			expectedMp4URL:     "s3+https://user:pass@bucket",
			expectedFragMp4URL: "",
		},
		{
			name: "multiple output locations, one with source segments",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{
				{
					URL: "s3+https://first:first@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						HLS: "enabled",
					},
				},
				{
					URL: "s3+https://second:second@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						MP4: "only_short",
					},
				},
				{
					URL: "s3+https://third:third@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						FragmentedMP4: "enabled",
					},
				},
			}},
			expectedHlsURL:       "s3+https://first:first@bucket",
			expectedMp4URL:       "s3+https://second:second@bucket",
			expectedFragMp4URL:   "s3+https://third:third@bucket",
			expectedMp4ShortOnly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHls := tt.req.getTargetHlsOutput()
			require.Equal(t, tt.expectedHlsURL, gotHls.URL)
			gotMp4, gotShortOnly := tt.req.getTargetMp4Output()
			gotFragMp4 := tt.req.getTargetFragMp4Output()
			require.Equal(t, tt.expectedMp4URL, gotMp4.URL)
			require.Equal(t, tt.expectedMp4ShortOnly, gotShortOnly)
			require.Equal(t, tt.expectedFragMp4URL, gotFragMp4.URL)
		})
	}
}

func TestItRejectsLocalDomain(t *testing.T) {
	err := CheckSourceURLValid("http://ipfs.libraries.svc.cluster.local:8080/ipfs/asdasd")
	require.EqualError(t, err, ".local domains are not valid")
}

func TestItRejectsEmptyURL(t *testing.T) {
	err := CheckSourceURLValid("")
	require.EqualError(t, err, "empty source URL")
}

func TestItAcceptsValidSourceURLs(t *testing.T) {
	require.NoError(t, CheckSourceURLValid("http://www.google.com"))
	require.NoError(t, CheckSourceURLValid("http://www.google.com:8080/123/asdsdf"))
	require.NoError(t, CheckSourceURLValid("ipfs://sfsdf234fdsdfsd"))
	require.NoError(t, CheckSourceURLValid("ar://123456"))
}
