package playback

import (
	"io"
)

func Media(req PlaybackRequest) (io.ReadCloser, error) {
	return osFetch(req.PlaybackID, req.File)
}
