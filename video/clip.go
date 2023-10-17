package video

import (
	"bytes"
	"context"
	"fmt"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/log"
	"os/exec"
	"time"
)

type ClipStrategy struct {
	Enabled    bool
	StartTime  int64  `json:"start_time,omitempty"`
	EndTime    int64  `json:"end_time,omitempty"`
	PlaybackID string `json:"playback_id,omitempty"` // playback-id of asset to clip
}

type ClipSegmentInfo struct {
	SequenceID     uint64
	Duration       float64
	ClipOffsetSecs float64
}

// format time in secs to be compatible with ffmpeg's expected time syntax
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
func getRelevantSegment(allSegments []*m3u8.MediaSegment, playHeadTime float64, startingIdx uint64) (ClipSegmentInfo, error) {
	playHeadDiff := 0.0

	for _, segment := range allSegments[startingIdx:] {
		// Break if we reach the end of the MediaSegment slice that contains all segments in the manifest
		if segment == nil {
			break
		}
		// Skip any segments where duration is 0s
		if segment.Duration <= 0.0 {
			return ClipSegmentInfo{}, fmt.Errorf("error clipping: found 0s duration segments")
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
		return ClipSegmentInfo{SequenceID: segment.SeqId,
			Duration:       segment.Duration,
			ClipOffsetSecs: playHeadTime}, nil
	}
	return ClipSegmentInfo{}, fmt.Errorf("error clipping: did not find a segment that falls within %v seconds", playHeadTime)
}

func ConvertUnixMillisToSeconds(requestID string, firstSegment *m3u8.MediaSegment, startTimeUnixMillis, endTimeUnixMillis int64) (float64, float64, error) {
	firstSegProgramDateTimeUTC := firstSegment.ProgramDateTime
	if firstSegProgramDateTimeUTC.IsZero() {
		return 0.0, 0.0, fmt.Errorf("error clipping: PROGRAM-DATE-TIME of first segment is not set")
	}
	// explicity use GMT+0(UTC) timezone so that local timezone is not used
	firstSegProgramDateTimeUTC = firstSegProgramDateTimeUTC.In(time.UTC)
	// convert first segment's program-date-time tag in manifest to unix time in milliseconds
	firstSegUnixMillis := firstSegProgramDateTimeUTC.UnixNano() / int64(time.Millisecond)

	// calculate offsets in seconds of start/end times from first segment's program-date-time
	startTimeSeconds := float64(startTimeUnixMillis-firstSegUnixMillis) / 1000.0
	endTimeSeconds := float64(endTimeUnixMillis-firstSegUnixMillis) / 1000.0

	// set start-time to 0 if the clipping start time preceeds the first segment's program-date-time
	if startTimeSeconds < 0 {
		startTimeSeconds = 0
	}

	log.Log(requestID, "clipping timestamps",
		"start-PROGRAM-DATE-TIME-UTC", firstSegProgramDateTimeUTC,
		"UNIX-time-milliseconds", firstSegUnixMillis,
		"start-time-unix-milliseconds", startTimeUnixMillis,
		"start-offset-seconds", startTimeSeconds,
		"end-time-unix-milliseconds", endTimeUnixMillis,
		"end-offset-seconds", endTimeSeconds)

	return startTimeSeconds, endTimeSeconds, nil
}

// Function to find relevant segments that span from the clipping start and end times
func ClipManifest(requestID string, manifest *m3u8.MediaPlaylist, startTime, endTime float64) ([]*m3u8.MediaSegment, []ClipSegmentInfo, error) {
	var clipStartSegmentInfo, clipEndSegmentInfo ClipSegmentInfo
	var err error

	manifestDuration, manifestSegments := GetTotalDurationAndSegments(manifest)

	// Find the segment index that correlates with the specified startTime
	// but error out it exceeds the  manifest's duration.
	if startTime > manifestDuration {
		return nil, []ClipSegmentInfo{}, fmt.Errorf("error clipping: start time specified exceeds duration of manifest")
	} else {
		clipStartSegmentInfo, err = getRelevantSegment(manifest.Segments, startTime, 0)
		if err != nil {
			return nil, []ClipSegmentInfo{}, fmt.Errorf("error clipping: failed to get a starting index: %w", err)
		}
	}

	// Find the segment index that correlates with the specified endTime.
	if endTime > manifestDuration {
		log.Log(requestID, "clipping end segment past manifest duration", "end-time", endTime, "total-manifest-duration", manifestDuration)
		lastSegmentIdx := manifestSegments - 1
		lastSegmentDuration := manifest.Segments[lastSegmentIdx].Duration
		clipEndSegmentInfo = ClipSegmentInfo{SequenceID: lastSegmentIdx, Duration: lastSegmentDuration, ClipOffsetSecs: lastSegmentDuration}
	} else {
		clipEndSegmentInfo, err = getRelevantSegment(manifest.Segments, endTime, 0)
		if err != nil {
			return nil, []ClipSegmentInfo{}, fmt.Errorf("error clipping: failed to get an ending index")
		}
	}

	firstSegmentToClip := clipStartSegmentInfo.SequenceID
	lastSegmentToClip := clipEndSegmentInfo.SequenceID
	// Generate a slice of all segments that overlap with startTime to endTime
	relevantSegments := manifest.Segments[firstSegmentToClip : lastSegmentToClip+1]
	totalRelSegs := len(relevantSegments)
	if totalRelSegs == 0 {
		return nil, []ClipSegmentInfo{}, fmt.Errorf("error clipping: no relevant segments found in the specified time range")
	}

	log.Log(requestID, "clipping segments", "from", firstSegmentToClip, "to", lastSegmentToClip)
	log.Log(requestID, "clipping segments",
		"start-segment-duration", clipStartSegmentInfo.Duration, "start-segment-offset", clipStartSegmentInfo.ClipOffsetSecs,
		"end-segment-duration", clipEndSegmentInfo.Duration, "end-segment-offset", clipEndSegmentInfo.ClipOffsetSecs)

	// If the clip start/end times fall within the same segment, then
	// save only the single segment's info
	var cs []ClipSegmentInfo
	if firstSegmentToClip == lastSegmentToClip {
		cs = []ClipSegmentInfo{clipStartSegmentInfo}
	} else {
		cs = []ClipSegmentInfo{clipStartSegmentInfo, clipEndSegmentInfo}
	}
	return relevantSegments, cs, nil
}

// Clips a segment from the specified start/end times. In some cases, the segment
// will be re-encoded so that I-frames are placed at the right intervals.
// This allows for frame-accurate clipping.
// Currently using a combination of these settings for the encode step:
//
//		"c:v": "libx264": Specifies H.264 video codec.
//		"g": "48": Inserts keyframe every 48 frames (GOP size).
//		"keyint_min": "48": Minimum keyframe interval.
//		"sc_threshold": 50: Detects scene changes with threshold 50.
//		"bf": "0": Disables B-frames for bidirectional prediction.
//		"c:a": "aac": re-encode audio and clip.
//	     "map 0:a map 0:v": so that audio track is always first which matches recording segments
func ClipSegment(requestID, tsInputFile, tsOutputFile string, startTime, endTime float64) error {

	var baseArgs []string
	mapArgs := []string{"-map", "0:a", "-map", "0:v"}

	// append input file
	baseArgs = append(baseArgs,
		"-i", tsInputFile)

	// append args that will apply re-encoding
	baseArgs = append(baseArgs,
		"-bf", "0",
		"-c:a", "aac",
		"-c:v", "libx264",
		"-g", "48",
		"-keyint_min", "48",
		"-sc_threshold", "50")

	if endTime < 0 {
		// Clip from specified start time to the end of the segment
		// (when clipping starting segment)
		start := formatTime(startTime)
		baseArgs = append(baseArgs, "-ss", start)
	} else if startTime < 0 {
		// Clip from beginning of segment to specified end time
		// (when clipping ending segment)
		end := formatTime(endTime)
		baseArgs = append(baseArgs, "-ss", "00:00:00.000", "-to", end)
	} else {
		// Clip from specified start/end times
		// (when start/end falls within same segment)
		start := formatTime(startTime)
		end := formatTime(endTime)
		baseArgs = append(baseArgs, "-ss", start, "-to", end)
	}

	// append map parameters so that audio track is always first and video track second
	baseArgs = append(baseArgs, mapArgs...)

	// append output file
	baseArgs = append(baseArgs, tsOutputFile, "-y")

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", baseArgs...)

	log.Log(requestID, "clipping", "compiled-command", fmt.Sprintf("ffmpeg %s", baseArgs))

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to clip segments from %s [%s] [%s]: %w", tsInputFile, outputBuf.String(), stdErr.String(), err)
	}

	return nil
}
