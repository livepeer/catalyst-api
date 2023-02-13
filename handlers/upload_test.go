package handlers

import (
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestGetSourceOutputURL(t *testing.T) {
	tests := []struct {
		name        string
		req         UploadVODRequest
		expectedURL *url.URL
	}{
		{
			name: "single output location with source segments",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					SourceSegments:     true,
					TranscodedSegments: true,
				}}}},
			expectedURL: toUrl("s3+https://user:pass@bucket"),
		},
		{
			name: "no location with source segments",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					TranscodedSegments: true,
				}}}},
			expectedURL: nil,
		},
		{
			name: "multiple output locations, one with source segments",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{
				{
					URL: "s3+https://first:first@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						TranscodedSegments: true,
					},
				},
				{
					URL: "s3+https://second:second@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						TranscodedSegments: true,
						SourceSegments:     true,
					},
				},
			}},
			expectedURL: toUrl("s3+https://second:second@bucket"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.req.getSourceOutputURL()
			require.Equal(t, tt.expectedURL, got)
			require.NoError(t, err)
		})
	}
}

func TestGetTargetURL(t *testing.T) {
	tests := []struct {
		name        string
		req         UploadVODRequest
		expectedURL string
		isErr       bool
	}{
		{
			name: "single output location",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					SourceSegments:     true,
					TranscodedSegments: true,
				}}}},
			expectedURL: "s3+https://user:pass@bucket",
		},
		{
			name: "multiple output locations",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{
				{
					URL: "s3+https://first:first@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						TranscodedSegments: true,
					},
				},
				{
					URL: "s3+https://second:second@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						TranscodedSegments: true,
						SourceSegments:     true,
					},
				},
			}},
			expectedURL: "s3+https://first:first@bucket",
		},
		{
			name:        "empty output locations",
			req:         UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{}},
			expectedURL: "",
			isErr:       true,
		},
		{
			name:        "nil output locations",
			req:         UploadVODRequest{},
			expectedURL: "",
			isErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.req.getTargetOutput()
			require.Equal(t, tt.expectedURL, got.URL)
			if tt.isErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func toUrl(URL string) *url.URL {
	res, _ := url.Parse(URL)
	return res
}
