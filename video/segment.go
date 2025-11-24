package video

import (
	"bytes"
	"fmt"
	"strings"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

// Segment splits a source video URL into segments.
//
// FFMPEG can use remote files, but depending on the layout of the file can get bogged
// down and end up making multiple range requests per segment.
// Because of this, we download first and then clean up at the end.
//
// The reencode parameter allows callers to force a re-encoding pass that inserts
// keyframes, which can be used as a fallback if simple "copy" based segmenting
// produces segments that don't start on a keyframe.
func Segment(sourceFilename string, outputManifestURL string, targetSegmentSize int64, reencode bool) error {
	// Do the segmenting, using the local file as source
	ffmpegErr := bytes.Buffer{}

	var outputArgs ffmpeg.KwArgs
	if reencode {
		// More expensive path that forces keyframes on a fixed cadence and
		// resets timestamps; used as a fallback for problematic inputs.
		outputArgs = ffmpeg.KwArgs{
			"c:v":               "libx264",
			"preset":            "veryfast",
			"sc_threshold":      "0",
			"force_key_frames":  "expr:gte(t,n_forced*3)",
			"c:a":               "aac",
			"f":                 "segment",
			"segment_list":      outputManifestURL,
			"segment_list_type": "m3u8",
			"segment_format":    "mpegts",
			"segment_time":      targetSegmentSize,
			"min_seg_duration":  "2",
			"reset_timestamps":  "1",
		}
	} else {
		// Faster path that keeps the original encoding and simply remuxes to TS.
		outputArgs = ffmpeg.KwArgs{
			"c:a":               "aac",
			"c:v":               "copy",
			"f":                 "segment",
			"segment_list":      outputManifestURL,
			"segment_list_type": "m3u8",
			"segment_format":    "mpegts",
			"segment_time":      targetSegmentSize,
			"min_seg_duration":  "2",
		}
	}

	err := ffmpeg.Input(sourceFilename).
		Output(
			strings.Replace(outputManifestURL, ".m3u8", "", 1)+"%d.ts",
			outputArgs,
		).OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()

	if err != nil {
		return fmt.Errorf("failed to segment source file (%s) [%s]: %s", sourceFilename, ffmpegErr.String(), err)
	}
	return nil
}
