package clients

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_isHLSInput(t *testing.T) {
	tests := []struct {
		name      string
		inputFile string
		want      bool
	}{
		{
			name:      "valid manifest",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4/index.m3u8",
			want:      true,
		},
		{
			name:      "invalid manifest",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/2697c12g97x2sxn4",
			want:      false,
		},
		{
			name:      "invalid manifest",
			inputFile: "https://lp-us-vod-com.storage.HELLO.com/2697c12g97x2sxn4/video.mp4",
			want:      false,
		},
		{
			name:      "invalid manifest",
			inputFile: "s3+https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4/output.m3u",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputURL, err := url.Parse(tt.inputFile)
			require.NoError(t, err)
			require.Equal(t, tt.want, IsHLSInput(inputURL))
		})
	}
}
