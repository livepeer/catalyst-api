package clients

import (
	"fmt"
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
