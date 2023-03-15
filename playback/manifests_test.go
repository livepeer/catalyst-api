package playback

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	tests := []struct {
		name        string
		req         PlaybackRequest
		expected    string
		expectedErr string
	}{
		{
			name: "master playlist",
			req: PlaybackRequest{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "index.m3u8",
				AccessKey:  "secretlpkey",
			},
			expected: "hls/dbe3q3g6q2kia036/index.m3u8",
		},
		{
			name: "rendition playlist",
			req: PlaybackRequest{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "720p0/index.m3u8",
				AccessKey:  "secretlpkey",
			},
			expected: "hls/dbe3q3g6q2kia036/720p0/index.m3u8",
		},
		{
			name: "file not found",
			req: PlaybackRequest{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "doesntexist",
				AccessKey:  "secretlpkey",
			},
			expectedErr: "failed to get master manifest",
		},
		{
			name: "empty access key",
			req: PlaybackRequest{
				PlaybackID: "dbe3q3g6q2kia036",
			},
			expectedErr: errors.EmptyAccessKeyError.Error(),
		},
		{
			name: "invalid m3u8",
			req: PlaybackRequest{
				File:      "not_m3u8.txt",
				AccessKey: "secretlpkey",
			},
			expectedErr: "failed to read manifest contents",
		},
	}
	wd, err := os.Getwd()
	require.NoError(t, err)
	privateBucket, err := url.Parse("file://" + path.Join(wd, "../test/fixtures/playback-bucket"))
	require.NoError(t, err)
	config.PrivateBucketURL = privateBucket
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Manifest(tt.req)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				fmt.Println(got)
				expectedFile, err := os.ReadFile(path.Join(wd, "../test/fixtures/responses", tt.expected))
				require.NoError(t, err)
				gotBytes, err := io.ReadAll(got)
				require.NoError(t, err)

				require.Equal(t, strings.TrimSpace(string(expectedFile)), strings.TrimSpace(string(gotBytes)))
			}
		})
	}
}
