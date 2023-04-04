package playback

import (
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
		req         Request
		expected    string
		expectedErr string
	}{
		{
			name: "master playlist",
			req: Request{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "index.m3u8",
				AccessKey:  "secretlpkey",
			},
			expected: "hls/dbe3q3g6q2kia036/index.m3u8",
		},
		{
			name: "rendition playlist",
			req: Request{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "720p0/index.m3u8",
				AccessKey:  "secretlpkey",
			},
			expected: "hls/dbe3q3g6q2kia036/720p0/index.m3u8",
		},
		{
			name: "file not found",
			req: Request{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "doesntexist",
				AccessKey:  "secretlpkey",
			},
			expectedErr: "failed to get file for playback",
		},
		{
			name: "empty access key",
			req: Request{
				PlaybackID: "dbe3q3g6q2kia036",
				File:       "index.m3u8",
			},
			expectedErr: errors.EmptyAccessKeyError.Error(),
		},
		{
			name: "invalid m3u8",
			req: Request{
				File:      "not_m3u8.m3u8",
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
			got, err := Handle(tt.req)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				expectedFile, err := os.ReadFile(path.Join(wd, "../test/fixtures/responses", tt.expected))
				require.NoError(t, err)
				defer got.Body.Close()
				gotBytes, err := io.ReadAll(got.Body)
				require.NoError(t, err)

				require.Equal(t, strings.TrimSpace(string(expectedFile)), strings.TrimSpace(string(gotBytes)))
			}
		})
	}
}
