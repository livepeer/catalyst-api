package ffmpeg

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grafov/m3u8"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/stretchr/testify/require"
)

func TestItReturnsAnErrorWhenJobDoesntExist(t *testing.T) {
	h := HandlersCollection{
		VODEngine: pipeline.NewStubCoordinator(),
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPut, "/api/ffmpeg/exampleStreamName/index.m3u8", strings.NewReader("example manifest contents"))

	h.NewFile()(
		w,
		r,
		[]httprouter.Param{
			{
				Key:   "id",
				Value: "THIS-DOES-NOT-EXIST",
			},
			{
				Key:   "filename",
				Value: "index.m3u8",
			},
		},
	)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestItWritesAReceivedFileToStorage(t *testing.T) {
	tempDir, err := os.MkdirTemp(os.TempDir(), "TestItWritesAReceivedFileToStorage*")
	require.NoError(t, err)
	segmentingTarget := filepath.Join(tempDir, "something.m3u8")

	h := HandlersCollection{
		VODEngine: pipeline.NewStubCoordinator(),
	}

	h.VODEngine.Jobs.Store("exampleStreamName", &pipeline.JobInfo{
		StreamName:          "exampleStreamName",
		SegmentingTargetURL: "file://" + segmentingTarget,
	})

	masterPl := `#EXTM3U
#EXT-X-VERSION:0
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=0
uri
`
	tests := []struct {
		name             string
		manifest         string
		expectedManifest string
	}{
		{
			name:             "just text",
			manifest:         "example manifest contents",
			expectedManifest: "example manifest contents",
		},
		{
			name: "media manifest",
			manifest: (&m3u8.MediaPlaylist{
				TargetDuration: 5,
				Closed:         true,
			}).Encode().String(),
			expectedManifest: (&m3u8.MediaPlaylist{
				TargetDuration: 5,
				Closed:         true,
				MediaType:      m3u8.VOD,
			}).Encode().String(), // media playlists should have the VOD type added
		},
		{
			name:             "master manifest",
			manifest:         masterPl,
			expectedManifest: masterPl, // master playlists should be unchanged
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPut, "/api/ffmpeg/exampleStreamName/index.m3u8", strings.NewReader(tt.manifest))

			h.NewFile()(
				w,
				r,
				[]httprouter.Param{
					{
						Key:   "id",
						Value: "exampleStreamName",
					},
					{
						Key:   "filename",
						Value: "index.m3u8",
					},
				},
			)
			require.Equal(t, w.Code, http.StatusOK)

			// Check the file got written to Object Storage
			targetFileContents, err := os.ReadFile(filepath.Join(tempDir, "index.m3u8"))
			require.NoError(t, err)
			require.Equal(t, tt.expectedManifest, string(targetFileContents))
		})
	}
}
