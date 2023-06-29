package clients

import (
	"encoding/json"
	"fmt"

	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
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

func (ts TranscodeStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.String())
}

func (ts *TranscodeStatus) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "\"preparing\"":
		*ts = TranscodeStatusPreparing
	case "\"preparing-completed\"":
		*ts = TranscodeStatusPreparingCompleted
	case "\"transcoding\"":
		*ts = TranscodeStatusTranscoding
	case "\"success\"":
		*ts = TranscodeStatusCompleted
	case "\"error\"":
		*ts = TranscodeStatusError
	default:
		return fmt.Errorf("invalid transcode status %q", string(b))
	}
	return nil
}

// The various status messages we can send

type TranscodeStatusMessage struct {
	// Internal fields, not included in the message we send
	URL string `json:"-"`

	// Fields included in all status messages
	RequestID       string          `json:"request_id"`
	CompletionRatio float64         `json:"completion_ratio"` // No omitempty or we lose this for 0% completion case
	Status          TranscodeStatus `json:"status"`
	Timestamp       int64           `json:"timestamp"`

	// Only used for the "Error" status message
	Error       string `json:"error,omitempty"`
	Unretriable bool   `json:"unretriable,omitempty"`

	// Only used for the "Completed" status message
	Type       string              `json:"type,omitempty"`
	InputVideo video.InputVideo    `json:"video_spec,omitempty"`
	Outputs    []video.OutputVideo `json:"outputs,omitempty"`

	SourcePlayback *video.OutputVideo `json:"source_playback,omitempty"`
}

// This method will accept the completion ratio of the current stage and will translate that into the overall ratio
func NewTranscodeStatusProgress(url, requestID string, status TranscodeStatus, currentStageCompletionRatio float64) TranscodeStatusMessage {
	return NewTranscodeStatusSourcePlayback(url, requestID, status, currentStageCompletionRatio, nil)
}

func NewTranscodeStatusSourcePlayback(url, requestID string, status TranscodeStatus, currentStageCompletionRatio float64, sourcePlayback *video.OutputVideo) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:             url,
		RequestID:       requestID,
		CompletionRatio: OverallCompletionRatio(status, currentStageCompletionRatio),
		Status:          status,
		Timestamp:       config.Clock.GetTimestampUTC(),
		SourcePlayback:  sourcePlayback,
	}
}

func NewTranscodeStatusError(url, requestID, errorMsg string, unretriable bool) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:         url,
		RequestID:   requestID,
		Error:       errorMsg,
		Unretriable: unretriable,
		Status:      TranscodeStatusError,
		Timestamp:   config.Clock.GetTimestampUTC(),
	}
}

// Separate method as this requires a much richer message than the other status callbacks
func NewTranscodeStatusCompleted(url, requestID string, iv video.InputVideo, ov []video.OutputVideo) TranscodeStatusMessage {
	return TranscodeStatusMessage{
		URL:             url,
		CompletionRatio: OverallCompletionRatio(TranscodeStatusCompleted, 1),
		RequestID:       requestID,
		Status:          TranscodeStatusCompleted,
		Timestamp:       config.Clock.GetTimestampUTC(),
		Type:            "video", // Assume everything is a video for now
		InputVideo:      iv,
		Outputs:         ov,
	}
}

// IsTerminal returns whether the given status message is a terminal state,
// meaning no other updates will be sent for this request.
func (tsm TranscodeStatusMessage) IsTerminal() bool {
	return tsm.Status == TranscodeStatusError ||
		tsm.Status == TranscodeStatusCompleted
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
