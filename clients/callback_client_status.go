package clients

import (
	"fmt"

	"github.com/livepeer/catalyst-api/config"
)

// An enum of potential statuses a Transcode job can have

type TranscodeStatus int

const (
	TranscodeStatusPreparing TranscodeStatus = iota
	TranscodeStatusPreparingCompleted
	TranscodeStatusTranscoding
	TranscodeStatusCompleted
	TranscodeStatusError
)

func (ts TranscodeStatus) String() string {
	switch ts {
	case TranscodeStatusPreparing:
		return "preparing"
	case TranscodeStatusPreparingCompleted:
		return "preparing-completed"
	case TranscodeStatusTranscoding:
		return "transcoding"
	case TranscodeStatusCompleted:
		return "success"
	case TranscodeStatusError:
		return "error"
	}
	return "unknown"
}

// The various status messages we can send

type RecordingEvent struct {
	Event       string `json:"event"`
	StreamName  string `json:"stream_name"`
	RecordingId string `json:"recording_id"`
	Hostname    string `json:"host_name"`
	Timestamp   int64  `json:"timestamp"`
	Success     *bool  `json:"success,omitempty"`
}

type TranscodeStatusMessage struct {
	// Internal fields, not included in the message we send
	RequestID string `json:"-"`
	URL       string `json:"-"`

	// Fields included in all status messages
	CompletionRatio float64 `json:"completion_ratio"` // No omitempty or we lose this for 0% completion case
	Status          string  `json:"status"`
	Timestamp       int64   `json:"timestamp"`

	// Only used for the "Error" status message
	Error       string `json:"error,omitempty"`
	Unretriable bool   `json:"unretriable,omitempty"`

	// Only used for the "Completed" status message
	Type       string        `json:"type,omitempty"`
	InputVideo InputVideo    `json:"video_spec,omitempty"`
	Outputs    []OutputVideo `json:"outputs,omitempty"`
}

type VideoTrack struct {
	Width       int64   `json:"width,omitempty"`
	Height      int64   `json:"height,omitempty"`
	PixelFormat string  `json:"pixel_format,omitempty"`
	FPS         float64 `json:"fps,omitempty"`
}

type AudioTrack struct {
	Channels   int `json:"channels,omitempty"`
	SampleRate int `json:"sample_rate,omitempty"`
	SampleBits int `json:"sample_bits,omitempty"`
}

type InputTrack struct {
	Type         string  `json:"type"`
	Codec        string  `json:"codec"`
	Bitrate      int64   `json:"bitrate"`
	DurationSec  float64 `json:"duration"`
	SizeBytes    int64   `json:"size"`
	StartTimeSec float64 `json:"start_time"`

	// Fields only used if this is a Video Track
	VideoTrack

	// Fields only used if this is an Audio Track
	AudioTrack
}

type InputVideo struct {
	Format    string       `json:"format,omitempty"`
	Tracks    []InputTrack `json:"tracks,omitempty"`
	Duration  float64      `json:"duration,omitempty"`
	SizeBytes int          `json:"size,omitempty"`
}

type OutputVideoFile struct {
	Type      string `json:"type"`
	SizeBytes int    `json:"size"`
	Location  string `json:"location"`
}

type OutputVideo struct {
	Type     string            `json:"type"`
	Manifest string            `json:"manifest"`
	Videos   []OutputVideoFile `json:"videos"`
}

// This method will accept the completion ratio of the current stage and will translate that into the overall ratio
func NewTranscodeStatusProgress(url, requestID string, status TranscodeStatus, currentStageCompletionRatio float64) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:             url,
		RequestID:       requestID,
		CompletionRatio: OverallCompletionRatio(status, currentStageCompletionRatio),
		Status:          status.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
	}
}

func NewTranscodeStatusError(url, requestID, errorMsg string, unretriable bool) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:         url,
		RequestID:   requestID,
		Error:       errorMsg,
		Unretriable: unretriable,
		Status:      TranscodeStatusError.String(),
		Timestamp:   config.Clock.GetTimestampUTC(),
	}
}

// Separate method as this requires a much richer message than the other status callbacks
func NewTranscodeStatusCompleted(url, requestID string, iv InputVideo, ov []OutputVideo) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:             url,
		CompletionRatio: OverallCompletionRatio(TranscodeStatusCompleted, 1),
		RequestID:       requestID,
		Status:          TranscodeStatusCompleted.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
		Type:            "video", // Assume everything is a video for now
		InputVideo:      iv,
		Outputs:         ov,
	}
}

// IsTerminal returns whether the given status message is a terminal state,
// meaning no other updates will be sent for this request.
func (tsm TranscodeStatusMessage) IsTerminal() bool {
	return tsm.Status == TranscodeStatusError.String() ||
		tsm.Status == TranscodeStatusCompleted.String()
}

// Finds the video track from the list of input video tracks
// If multiple video tracks present, returns the first one
// If no video tracks present, returns an error
func (i InputVideo) GetVideoTrack() (InputTrack, error) {
	for _, t := range i.Tracks {
		if t.Type == "video" {
			return t, nil
		}
	}
	return InputTrack{}, fmt.Errorf("no video tracks found")
}

// Calculate the overall completion ratio based on the completion ratio of the current stage.
// The weighting will need to be tweaked as we understand better the relative time spent in the
// segmenting vs. transcoding stages.
func OverallCompletionRatio(status TranscodeStatus, currentStageCompletionRatio float64) float64 {
	// Sanity check the inputs are within the 0-1 bounds
	if currentStageCompletionRatio < 0 {
		currentStageCompletionRatio = 0
	}
	if currentStageCompletionRatio > 1 {
		currentStageCompletionRatio = 1
	}

	// These are at the end of stages, so should always be 100% complete
	if status == TranscodeStatusPreparingCompleted || status == TranscodeStatusCompleted {
		currentStageCompletionRatio = 1
	}

	switch status {
	case TranscodeStatusPreparing, TranscodeStatusPreparingCompleted:
		return scaleProgress(currentStageCompletionRatio, 0, 0.4)
	case TranscodeStatusTranscoding:
		return scaleProgress(currentStageCompletionRatio, 0.4, 0.9)
	case TranscodeStatusCompleted:
		return scaleProgress(currentStageCompletionRatio, 0.9, 1)
	}

	// Either unhandled or an error
	return -1
}

func scaleProgress(progress, start, end float64) float64 {
	return start + progress*(end-start)
}
