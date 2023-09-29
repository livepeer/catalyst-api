package video

import (
	"fmt"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

// Split a source video URL into segments
//
// FFMPEG can use remote files, but depending on the layout of the file can get bogged
// down and end up making multiple range requests per segment.
// Because of this, we download first and then clean up at the end.
func Segment(sourceFilename string, outputManifestURL string, targetSegmentSize int64) error {
	// Do the segmenting, using the local file as source
	err := ffmpeg.Input(sourceFilename).
		Output(
			outputManifestURL,
			ffmpeg.KwArgs{
				"c:a":               "aac",
				"c:v":               "copy",
				"f":                 "hls",
				"hls_segment_type":  "mpegts",
				"hls_playlist_type": "vod",
				"hls_list_size":     "0",
				"hls_time":          targetSegmentSize,
				"method":            "PUT",
			},
		).OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("failed to segment source file (%s): %s", sourceFilename, err)
	}
	return nil
}
