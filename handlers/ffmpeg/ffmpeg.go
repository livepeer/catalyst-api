package ffmpeg

import (
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/pipeline"
)

type HandlersCollection struct {
	VODEngine *pipeline.Coordinator
}

// FFMPEG is called with something like the following:
//
//	ffmpeg -re -i SomeFile.mp4 -f hls -method PUT http://localhost:1234/<request id>/out.m3u8
//
// This HTTP handler is responsible for accepting that file and writing it out
// to an external storage location
func (h *HandlersCollection) NewFile() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		id := params.ByName("id")
		filename := params.ByName("filename")

		job := h.VODEngine.Jobs.Get(id)
		if job == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// job.SegmentingTargetURL comes in the format the Mist wants, looking like:
		//   protocol://abc@123:s3.com/a/b/c/index.m3u8
		// but since this endpoint receives both .ts segments and m3u8 updates, we strip off the filename
		// and pass the one ffmpeg gives us to UploadToOSURL instead
		targetURLBase := strings.TrimSuffix(job.SegmentingTargetURL, "index.m3u8")

		if err := clients.UploadToOSURL(targetURLBase, filename, req.Body, config.SEGMENT_WRITE_TIMEOUT); err != nil {
			errors.WriteHTTPInternalServerError(w, "Error uploading segment", err)
			return
		}
	}
}
