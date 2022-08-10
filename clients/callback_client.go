package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/livepeer/catalyst-api/config"
)

const max_retries = 3
const backoff_millis = 200

type CallbackClient struct {
	httpClient *http.Client
}

func NewCallbackClient() CallbackClient {
	return CallbackClient{
		httpClient: &http.Client{},
	}
}

func (c CallbackClient) DoWithRetries(r *http.Request) error {
	var resp *http.Response
	var err error
	for x := 0; x < max_retries; x++ {
		resp, err = c.httpClient.Do(r)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}

		// Back off to give us more chance of succeeding during a network blip
		time.Sleep(backoff_millis * time.Millisecond)
	}

	if err != nil {
		return fmt.Errorf("failed to send callback to %q. Error: %q", r.URL.String(), err)
	}
	return fmt.Errorf("failed to send callback to %q. Response Status Code: %d", r.URL.String(), resp.StatusCode)
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
