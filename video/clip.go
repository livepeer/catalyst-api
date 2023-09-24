package video

import (
	"fmt"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/log"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"time"
)

type ClipStrategy struct {
	Enabled    bool
	StartTime  int64  `json:"start_time,omitempty"`
	EndTime    int64  `json:"end_time,omitempty"`
	PlaybackID string `json:"playback_id,omitempty"` // playback-id of asset to clip
}

// format time in secs to be copatible with ffmpeg's expected time syntax
func formatTime(timeSeconds float64) string {
	timeMillis := int64(timeSeconds * 1000)
	duration := time.Duration(timeMillis) * time.Millisecond
	formattedTime := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add(duration)
	return formattedTime.Format("15:04:05.000")
}

func GetTotalDurationAndSegments(manifest *m3u8.MediaPlaylist) (float64, uint64) {
	if manifest == nil {
		return 0.0, 0
	}

	var totalDuration float64
	allSegments := manifest.GetAllSegments()
	for _, segment := range allSegments {
		totalDuration += segment.Duration
	}
	return totalDuration, uint64(len(allSegments))
}

// Finds the segment in an HLS manifest that contains the timestamp (aka playhead) specified.
func getRelevantSegment(allSegments []*m3u8.MediaSegment, playHeadTime float64, startingIdx uint64) (uint64, error) {
	playHeadDiff := 0.0

	for _, segment := range allSegments[startingIdx:] {
		// Break if we reach the end of the MediaSegment slice that contains all segments in the manifest
		if segment == nil {
			break
		}
		// Skip any segments where duration is 0s
		if segment.Duration <= 0.0 {
			return 0, fmt.Errorf("error clipping: found 0s duration segments")
		}
		// Check if the playhead is within the current segment and skip to
		// the next segment if it's not. Also update the play head by referencing
		// the starting time of the next segment.
		playHeadDiff = playHeadTime - segment.Duration
		if playHeadDiff > 0.0 {
			playHeadTime = playHeadDiff
			continue
		}
		// If we reach here, then we've found the relevant segment that falls within the playHeadTime
		return segment.SeqId, nil
	}
	return 0, fmt.Errorf("error clipping: did not find a segment that falls within %v seconds", playHeadTime)
}

func ConvertUnixMillisToSeconds(requestID string, firstSegment *m3u8.MediaSegment, startTimeUnixMillis, endTimeUnixMillis int64) (float64, float64, error) {
	firstSegProgramDateTimeUTC := firstSegment.ProgramDateTime
	if firstSegProgramDateTimeUTC.IsZero() {
		return 0.0, 0.0, fmt.Errorf("error clipping: PROGRAM-DATE-TIME of first segment is not set")
	}
	firstSegUnixMillis := firstSegProgramDateTimeUTC.UnixNano() / int64(time.Millisecond)

	startTimeSeconds := float64(startTimeUnixMillis-firstSegUnixMillis) / 1000.0
	endTimeSeconds := float64(endTimeUnixMillis-firstSegUnixMillis) / 1000.0

	log.Log(requestID, "Clipping timestamps",
		"start-PROGRAM-DATE-TIME-UTC", firstSegProgramDateTimeUTC,
		"UNIX-time-milliseconds", firstSegUnixMillis,
		"start-offset-seconds", startTimeSeconds,
		"end-offset-seconds", endTimeSeconds)

	return startTimeSeconds, endTimeSeconds, nil
}

// Function to find relevant segments that span from the clipping start and end times
func ClipManifest(requestID string, manifest *m3u8.MediaPlaylist, startTime, endTime float64) ([]*m3u8.MediaSegment, error) {
	var startSegIdx, endSegIdx uint64
	var err error

	manifestDuration, manifestSegments := GetTotalDurationAndSegments(manifest)

	// Find the segment index that correlates with the specified startTime
	// but error out it exceeds the  manifest's duration.
	if startTime > manifestDuration {
		return nil, fmt.Errorf("error clipping: start time specified exceeds duration of manifest")
	} else {
		startSegIdx, err = getRelevantSegment(manifest.Segments, startTime, 0)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to get a starting index")
		}
	}

	// Find the segment index that correlates with the specified endTime.
	if endTime > manifestDuration {
		endSegIdx = manifestSegments - 1
	} else {
		endSegIdx, err = getRelevantSegment(manifest.Segments, endTime, startSegIdx)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to get an ending index")
		}
	}

	// Generate a slice of all segments that overlap with startTime to endTime
	relevantSegments := manifest.Segments[startSegIdx : endSegIdx+1]
	totalRelSegs := len(relevantSegments)
	if totalRelSegs == 0 {
		return nil, fmt.Errorf("error clipping: no relevant segments found in the specified time range")
	}

	log.Log(requestID, "Clipping segments", "from", startSegIdx, "to", endSegIdx)

	return relevantSegments, nil
}

// Clips a segment by re-encoding so that I-frames are placed at the right intervals
// Currently using these settings for the encode step:
//
//	"c:v": "libx264": Specifies H.264 video codec.
//	"r": "24": Sets video frame rate to 24 FPS.
//	"g": "48": Inserts keyframe every 48 frames (GOP size).
//	"keyint_min": "48": Minimum keyframe interval.
//	"sc_threshold": 50: Detects scene changes with threshold 50.
//	"bf": "0": Disables B-frames for bidirectional prediction.
//	"c:a": "copy": Copies audio stream without re-encoding.
func ClipSegment(tsInputFile, tsOutputFile string, startTime, endTime float64) error {
	start := formatTime(startTime)
	end := formatTime(endTime)
	err := ffmpeg.Input(tsInputFile).
		Output(tsOutputFile, ffmpeg.KwArgs{"ss": start, "to": end,
			"c:v":          "libx264",
			"r":            "24",
			"g":            "48",
			"keyint_min":   "48",
			"sc_threshold": 50,
			"bf":           "0",
			"c:a":          "copy"}).
		OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("failed to clip segments from %s: %w", tsInputFile, err)
	}
	return nil
}
