package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	privateBucket, err := url.Parse("file://" + path.Join(wd, "../test/fixtures/playback-bucket"))
	require.NoError(t, err)
	emptyBucket, err := url.Parse("file://" + path.Join(wd, "../test/fixtures/"))
	require.NoError(t, err)
	tests := []struct {
		name              string
		reqURL            string
		playbackID        string
		file              string
		expected          string
		expectedStatus    int
		privateBucketURLs []*url.URL
	}{
		{
			name:           "master playlist",
			reqURL:         "/index.m3u8?accessKey=secretlpkey",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "index.m3u8",
			expected:       "hls/dbe3q3g6q2kia036/index.m3u8",
			expectedStatus: http.StatusOK,
		},
		{
			name:              "master playlist first bucket",
			reqURL:            "/index.m3u8?accessKey=secretlpkey",
			playbackID:        "dbe3q3g6q2kia036",
			file:              "index.m3u8",
			privateBucketURLs: []*url.URL{privateBucket, emptyBucket},
			expected:          "hls/dbe3q3g6q2kia036/index.m3u8",
			expectedStatus:    http.StatusOK,
		},
		{
			name:              "master playlist second bucket",
			reqURL:            "/index.m3u8?accessKey=secretlpkey",
			playbackID:        "dbe3q3g6q2kia036",
			file:              "index.m3u8",
			privateBucketURLs: []*url.URL{emptyBucket, privateBucket},
			expected:          "hls/dbe3q3g6q2kia036/index.m3u8",
			expectedStatus:    http.StatusOK,
		},
		{
			name:              "not found multi buckets",
			reqURL:            "/doesntexist?accessKey=secretlpkey",
			playbackID:        "dbe3q3g6q2kia036",
			file:              "doesntexist",
			privateBucketURLs: []*url.URL{privateBucket, emptyBucket},
			expectedStatus:    http.StatusNotFound,
		},
		{
			name:           "rendition playlist",
			reqURL:         "/720p0/index.m3u8?accessKey=secretlpkey",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "720p0/index.m3u8",
			expected:       "hls/dbe3q3g6q2kia036/720p0/index.m3u8",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "master playlist jwt",
			reqURL:         "/index_jwt.m3u8?jwt=secretlpkey",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "index_jwt.m3u8",
			expected:       "hls/dbe3q3g6q2kia036/index_jwt.m3u8",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "rendition playlist jwt",
			reqURL:         "/720p0/index_jwt.m3u8?jwt=secretlpkey",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "720p0/index_jwt.m3u8",
			expected:       "hls/dbe3q3g6q2kia036/720p0/index_jwt.m3u8",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "file not found",
			reqURL:         "/doesntexist?accessKey=secretlpkey",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "doesntexist",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "empty access key",
			playbackID:     "dbe3q3g6q2kia036",
			file:           "index.m3u8",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid m3u8",
			reqURL:         "/not_m3u8.m3u8?accessKey=secretlpkey",
			file:           "not_m3u8.m3u8",
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PlaybackHandler{
				PrivateBucketURLs: []*url.URL{privateBucket},
			}
			if len(tt.privateBucketURLs) > 0 {
				p.PrivateBucketURLs = tt.privateBucketURLs
			}
			writer := httptest.NewRecorder()
			req, err := http.NewRequest("GET", tt.reqURL, strings.NewReader(""))
			require.NoError(t, err)
			p.Handle(writer, req, []httprouter.Param{
				{
					Key:   "playbackID",
					Value: tt.playbackID,
				},
				{
					Key:   "file",
					Value: tt.file,
				},
			})

			require.Equal(t, tt.expectedStatus, writer.Code)
			if tt.expected != "" {
				require.NoError(t, err)
				expectedFile, err := os.ReadFile(path.Join(wd, "../test/fixtures/responses", tt.expected))
				require.NoError(t, err)
				body, err := io.ReadAll(writer.Body)
				require.NoError(t, err)

				require.Equal(t, strings.TrimSpace(string(expectedFile)), strings.TrimSpace(string(body)))
			}
		})
	}
}
