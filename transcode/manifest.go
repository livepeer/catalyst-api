package transcode

import (
	"fmt"
	"io"
	"strings"

	"github.com/grafov/m3u8"
)

func GetSourceSegmentURLs(sourceManifestURL string, manifest io.Reader) ([]string, error) {
	// Parse the source manifest
	playlist, playlistType, err := m3u8.DecodeFrom(manifest, true)
	if err != nil {
		return nil, fmt.Errorf("error decoding manifest: %s", err)
	}

	// We shouldn't ever receive Master playlists from the previous section
	if playlistType != m3u8.MEDIA {
		return nil, fmt.Errorf("received non-Media manifest, but currently only Media playlists are supported")
	}

	// The check above means we should be able to cast to the correct type
	mediaPlaylist, ok := playlist.(*m3u8.MediaPlaylist)
	if !ok {
		return nil, fmt.Errorf("failed to parse playlist as MediaPlaylist: %s", err)
	}

	// Loop over each segment and convert it from a relative to a full ObjectStore-compatible URL
	var urls []string
	for _, segment := range mediaPlaylist.Segments {
		// The segments list is a ring buffer - see https://github.com/grafov/m3u8/issues/140
		// and so we only know we've hit the end of the list when we find a nil element
		if segment == nil {
			break
		}

		urls = append(urls, manifestURLToSegmentURL(sourceManifestURL, segment.URI))
	}

	return urls, nil
}

func manifestURLToSegmentURL(manifestURL, segmentFilename string) string {
	i := strings.LastIndex(manifestURL, "/")
	return manifestURL[:i] + "/" + segmentFilename
}
