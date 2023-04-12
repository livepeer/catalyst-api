package ffmpeg

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	segmentingTarget := filepath.Join(tempDir, "index.m3u8")

	h := HandlersCollection{
		VODEngine: pipeline.NewStubCoordinator(),
	}

	h.VODEngine.Jobs.Store("exampleStreamName", &pipeline.JobInfo{
		StreamName:          "exampleStreamName",
		SegmentingTargetURL: "file://" + segmentingTarget,
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPut, "/api/ffmpeg/exampleStreamName/index.m3u8", strings.NewReader("example manifest contents"))

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
	targetFileContents, err := os.ReadFile(segmentingTarget)
	require.NoError(t, err)
	require.Equal(t, "example manifest contents", string(targetFileContents))
}
