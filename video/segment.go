package video

import (
	"fmt"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

func Segment(requestID string, sourceURL string, outputManifestURL string, targetSegmentSize int64) error {
	err := ffmpeg.Input(sourceURL).
		Output(
			outputManifestURL,
			ffmpeg.KwArgs{
				"c:a":               "copy",
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
		return fmt.Errorf("failed to segment source file (%s): %s", sourceURL, err)
	}
	return nil
}
