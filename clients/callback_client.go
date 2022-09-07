package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
)

type CallbackClient struct {
	httpClient *http.Client
}

var DefaultCallbackClient = NewCallbackClient()

func NewCallbackClient() CallbackClient {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		Timeout: 5 * time.Second, // Give up on requests that take more than this long
	}

	return CallbackClient{
		httpClient: client.StandardClient(),
	}
}

func (c CallbackClient) DoWithRetries(r *http.Request) error {
	// TODO: Replace with a proper shared Secret, probably coming from the initial request
	r.Header.Set("Authorization", "Bearer IAmAuthorized")

	resp, err := c.httpClient.Do(r)
	if err != nil {
		return fmt.Errorf("failed to send callback to %q. Error: %s", r.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("failed to send callback to %q. HTTP Code: %d", r.URL.String(), resp.StatusCode)
	}

	return nil
}

// Sends a Transcode Status message to the Client (initially just Studio)
// The status strings will be useful for debugging where in the workflow we got to, but everything
// in Studio will be driven off the overall "Completion Ratio".
// This method will accept the completion ratio of the current stage and will translate that into the overall ratio
func (c CallbackClient) SendTranscodeStatus(url string, status TranscodeStatus, currentStageCompletionRatio float64) error {
	tsm := TranscodeStatusMessage{
		CompletionRatio: overallCompletionRatio(status, currentStageCompletionRatio),
		Status:          status.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
	}

	j, err := json.Marshal(tsm)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(j))
	if err != nil {
		return err
	}

	return c.DoWithRetries(r)
}

func (c CallbackClient) SendTranscodeStatusError(callbackURL, errorMsg string) error {
	tsm := TranscodeStatusMessage{
		Error:     errorMsg,
		Status:    TranscodeStatusError.String(),
		Timestamp: config.Clock.GetTimestampUTC(),
	}

	j, err := json.Marshal(tsm)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, callbackURL, bytes.NewReader(j))
	if err != nil {
		return err
	}

	return c.DoWithRetries(r)
}

// Separate method as this requires a much richer message than the other status callbacks
func (c CallbackClient) SendTranscodeStatusCompleted(url string, iv InputVideo, ov []OutputVideo) error {
	tsm := TranscodeStatusCompletedMessage{
		TranscodeStatusMessage: TranscodeStatusMessage{
			CompletionRatio: overallCompletionRatio(TranscodeStatusCompleted, 1),
			Status:          TranscodeStatusCompleted.String(),
			Timestamp:       config.Clock.GetTimestampUTC(),
		},
		Type:       "video", // Assume everything is a video for now
		InputVideo: iv,
		Outputs:    ov,
	}

	j, err := json.Marshal(tsm)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(j))
	if err != nil {
		return err
	}

	return c.DoWithRetries(r)

}

// Calculate the overall completion ratio based on the completion ratio of the current stage.
// The weighting will need to be tweaked as we understand better the relative time spent in the
// segmenting vs. transcoding stages.
func overallCompletionRatio(status TranscodeStatus, currentStageCompletionRatio float64) float64 {
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
	case TranscodeStatusTranscoding, TranscodeStatusCompleted:
		return scaleProgress(currentStageCompletionRatio, 0.4, 1)
	}

	// Either unhandled or an error
	return -1
}

func scaleProgress(progress, start, end float64) float64 {
	return start + progress*(end-start)
}

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

type TranscodeStatusMessage struct {
	CompletionRatio float64 `json:"completion_ratio"` // No omitempty or we lose this for 0% completion case
	Error           string  `json:"error,omitempty"`
	Unretriable     bool    `json:"unretriable,omitempty"`
	Status          string  `json:"status"`
	Timestamp       int64   `json:"timestamp"`
}

type VideoTrack struct {
	Width       int    `json:"width,omitempty"`
	Height      int    `json:"height,omitempty"`
	PixelFormat string `json:"pixel_format,omitempty"`
	FPS         int    `json:"fps,omitempty"`
}

type AudioTrack struct {
	Channels   int `json:"channels,omitempty"`
	SampleRate int `json:"sample_rate,omitempty"`
}

type InputTrack struct {
	Type         string  `json:"type"`
	Codec        string  `json:"codec"`
	Bitrate      int     `json:"bitrate"`
	DurationSec  float64 `json:"duration"`
	SizeBytes    int     `json:"size"`
	StartTimeSec float64 `json:"start_time"`

	// Fields only used if this is a Video Track
	VideoTrack

	// Fields only used if this is an Audio Track
	AudioTrack
}

type InputVideo struct {
	Format    string       `json:"format"`
	Tracks    []InputTrack `json:"tracks"`
	Duration  float64      `json:"duration"`
	SizeBytes int          `json:"size"`
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

type TranscodeStatusCompletedMessage struct {
	TranscodeStatusMessage
	Type       string        `json:"type"`
	InputVideo InputVideo    `json:"video_spec"`
	Outputs    []OutputVideo `json:"outputs"`
}
