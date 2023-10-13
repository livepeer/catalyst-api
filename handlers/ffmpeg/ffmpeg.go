package ffmpeg

import (
	"bytes"
	"io"
	"net/http"
	"regexp"

	"github.com/grafov/m3u8"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
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

		var body io.Reader = req.Body
		reg := regexp.MustCompile(`[^/]+.m3u8$`)
		if reg.MatchString(filename) {
			// ensure that playlist type in the manifest is set to vod
			buf := bytes.Buffer{}
			_, err := buf.ReadFrom(req.Body)
			if err != nil {
				errors.WriteHTTPInternalServerError(w, "Error reading body", err)
				return
			}

			playlist, playlistType, err := m3u8.Decode(buf, true)
			if err != nil {
				log.LogError(job.RequestID, "failed to parse segmented manifest", err)
				body = &buf
			} else if playlistType == m3u8.MEDIA {
				mediaPl := playlist.(*m3u8.MediaPlaylist)
				if !mediaPl.Closed {
					// we don't want to upload an unfinished playlist, otherwise there's a race condition
					// where potentially a playlist before the final one is written last and we're missing segments
					return
				}

				mediaPl.MediaType = m3u8.VOD
				body = mediaPl.Encode()
			} else {
				// should never happen but useful to at least see a log line if it ever did
				log.Log(job.RequestID, "media playlist not found")
				body = playlist.Encode()
			}
		}
		// job.SegmentingTargetURL comes in the format the Mist wants, looking like:
		//   protocol://abc@123:s3.com/a/b/c/<something>.m3u8
		// but since this endpoint receives both .ts segments and m3u8 updates, we strip off the filename
		// and pass the one ffmpeg gives us to UploadToOSURL instead
		targetURLBase := reg.ReplaceAllString(job.SegmentingTargetURL, "")

		if err := clients.UploadToOSURL(targetURLBase, filename, body, config.SEGMENT_WRITE_TIMEOUT); err != nil {
			errors.WriteHTTPInternalServerError(w, "Error uploading segment", err)
			return
		}
	}
}
