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

	// Define the "base" numbers - e.g the overall ratio we start each stage at
	var TranscodeStatusPreparingBase float64 = 0.0
	var TranscodeStatusPreparingTranscodingBase float64 = 0.4
	var TranscodeStatusCompletedBase float64 = 1

	switch status {
	case TranscodeStatusPreparing:
		return TranscodeStatusPreparingBase + (currentStageCompletionRatio * (TranscodeStatusPreparingTranscodingBase - TranscodeStatusPreparingBase))
	case TranscodeStatusPreparingCompleted:
		return TranscodeStatusPreparingTranscodingBase
	case TranscodeStatusTranscoding:
		return TranscodeStatusPreparingTranscodingBase + (currentStageCompletionRatio * (TranscodeStatusCompletedBase - TranscodeStatusPreparingTranscodingBase))
	case TranscodeStatusCompleted:
		return TranscodeStatusCompletedBase
	default:
		// Either unhandled or an error
		return -1
	}
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

type TranscodeStatusMessage struct {
	CompletionRatio float64 `json:"completion_ratio"` // No omitempty or we lose this for 0% completion case
	Error           string  `json:"error,omitempty"`
	Retriable       *bool   `json:"retriable,omitempty"` // Has to be a pointer or we can't differentiate omission from 'false'
	Status          string  `json:"status,omitempty"`
	Timestamp       int64   `json:"timestamp"`
}

func (ts TranscodeStatus) String() string {
	switch ts {
	case TranscodeStatusPreparing:
		return "preparing"
	case TranscodeStatusPreparingCompleted:
		return "preparing-completed"
	case TranscodeStatusTranscoding:
		return "transcoding"
	case TranscodeStatusCompleted:
		return "transcoding-completed"
	case TranscodeStatusError:
		return "error"
	}
	return "unknown"
}
