package video

import (
	"fmt"
	"strings"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

// Segment splits a source video URL into segments
//
// FFMPEG can use remote files, but depending on the layout of the file can get bogged
// down and end up making multiple range requests per segment.
// Because of this, we download first and then clean up at the end.
func Segment(sourceFilename string, outputManifestURL string, targetSegmentSize int64) error {
	// Do the segmenting, using the local file as source
	err := ffmpeg.Input(sourceFilename).
		Output(
			strings.Replace(outputManifestURL, ".m3u8", "", 1)+"%d.ts",
			ffmpeg.KwArgs{
				"c:a":               "copy",
				"c:v":               "copy",
				"f":                 "segment",
				"segment_list":      outputManifestURL,
				"segment_list_type": "m3u8",
				"segment_format":    "mpegts",
				"segment_time":      targetSegmentSize,
				"min_seg_duration":  "2",
			},
		).OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("failed to segment source file (%s): %s", sourceFilename, err)
	}
	return nil
}
