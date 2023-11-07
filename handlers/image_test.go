package handlers

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
)

func TestImageHandler_Handle(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	handler := &ImageHandler{
		PublicBucketURLs: []*url.URL{{Scheme: "file", Path: wd + "/../test"}},
	}
	tests := []struct {
		name           string
		time           string
		playbackID     string
		expectedStatus int
	}{
		{
			name: "first segment",
			time: "5",
		},
		{
			name: "second segment",
			time: "21",
		},
		{
			name: "final segment",
			time: "29",
		},
		{
			name:           "out of bounds",
			time:           "30",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "invalid time",
			time:           "",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "playbackID not found",
			time:           "29",
			playbackID:     "foo",
			expectedStatus: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, err := http.NewRequest(http.MethodGet, "?time="+tt.time, nil)
			require.NoError(t, err)

			if tt.playbackID == "" {
				tt.playbackID = "fixtures" // just use the fixtures directory for testing
			}
			handler.Handle(w, req, []httprouter.Param{{
				Key:   "playbackID",
				Value: tt.playbackID,
			}})
			resp := w.Result()
			if tt.expectedStatus == 0 {
				tt.expectedStatus = 200
			}
			require.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus != 200 {
				return
			}
			respBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			outfile, err := os.CreateTemp(os.TempDir(), "out*.jpg")
			require.NoError(t, err)
			defer os.Remove(outfile.Name())
			_, err = outfile.Write(respBytes)
			require.NoError(t, err)
			log.Println(outfile.Name())
			probeData, err := ffprobe.ProbeURL(context.Background(), outfile.Name())
			require.NoError(t, err)
			require.Equal(t, "image2", probeData.Format.FormatName)
			require.Len(t, probeData.Streams, 1)
			require.Greater(t, probeData.Streams[0].Width, 0)
			require.Greater(t, probeData.Streams[0].Height, 0)
		})
	}
}
