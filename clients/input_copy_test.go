package clients

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_isDirectUpload(t *testing.T) {
	tests := []struct {
		name      string
		inputFile string
		want      bool
	}{
		{
			name:      "direct upload",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4",
			want:      true,
		},
		{
			name:      "not direct upload",
			inputFile: "s3+https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputURL, err := url.Parse(tt.inputFile)
			require.NoError(t, err)
			require.Equal(t, tt.want, isDirectUpload(inputURL))
		})
	}
}
