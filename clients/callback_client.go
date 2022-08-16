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

func (c CallbackClient) SendTranscodeStatus(url string, status TranscodeStatus, completionRatio float32) error {
	tsm := TranscodeStatusMessage{
		CompletionRatio: completionRatio,
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

// An enum of potential statuses a Transcode job can have

type TranscodeStatus int

const (
	TranscodeStatusPreparing TranscodeStatus = iota
	TranscodeStatusTranscoding
	TranscodeStatusCompleted
	TranscodeStatusError
)

type TranscodeStatusMessage struct {
	CompletionRatio float32 `json:"completion_ratio,omitempty"`
	Error           string  `json:"error,omitempty"`
	Retriable       bool    `json:"retriable,omitempty"`
	Status          string  `json:"status,omitempty"`
	Timestamp       int64   `json:"timestamp"`
}

func (ts TranscodeStatus) String() string {
	switch ts {
	case TranscodeStatusPreparing:
		return "preparing"
	case TranscodeStatusTranscoding:
		return "transcoding"
	case TranscodeStatusCompleted:
		return "completed"
	case TranscodeStatusError:
		return "error"
	}
	return "unknown"
}
