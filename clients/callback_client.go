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
	httpClient *retryablehttp.Client
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
		httpClient: client,
	}
}

func (c CallbackClient) DoWithRetries(r *retryablehttp.Request) error {
	resp, err := c.httpClient.Do(r)
	if err != nil {
		fmt.Printf(">> failed to send callback to %q. Error: %s", r.URL.String(), err)
		return fmt.Errorf("failed to send callback to %q. Error: %s", r.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		fmt.Printf(">> failed to send callback to %q. HTTP Code: %d", r.URL.String(), resp.StatusCode)
		return fmt.Errorf("failed to send callback to %q. HTTP Code: %d", r.URL.String(), resp.StatusCode)
	}

	return nil
}

func (c CallbackClient) SendTranscodeStatus(url string, status TranscodeStatus, completionRatio float32) error {
	return c.sendTSM(url, TranscodeStatusMessage{
		CompletionRatio: completionRatio,
		Status:          status.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
	})
}

func (c CallbackClient) SendTranscodeStatusError(callbackURL, errorMsg string) error {
	return c.sendTSM(callbackURL, TranscodeStatusMessage{
		Error:     errorMsg,
		Status:    TranscodeStatusError.String(),
		Timestamp: config.Clock.GetTimestampUTC(),
	})
}

// CatalystAPIHandlersCollection::TranscodeSegment invokes this on failed upload
func (c CallbackClient) SendRenditionUploadError(callbackURL, sourceLocation, destination, err string) error {
	return c.sendTSM(callbackURL, TranscodeStatusMessage{
		Error:                err,
		Status:               TranscodeStatusRenditionUpload.String(),
		Timestamp:            config.Clock.GetTimestampUTC(),
		SourceLocations:      []string{sourceLocation},
		DestinationLocations: []string{destination},
	})
}

// CatalystAPIHandlersCollection::TranscodeSegment invokes this on successful upload
func (c CallbackClient) SendRenditionUpload(callbackURL, sourceLocation, destination string) error {
	return c.sendTSM(callbackURL, TranscodeStatusMessage{
		Status:               TranscodeStatusRenditionUpload.String(),
		Timestamp:            config.Clock.GetTimestampUTC(),
		SourceLocations:      []string{sourceLocation},
		DestinationLocations: []string{destination},
		CompletionRatio:      100,
	})
}

// CatalystAPIHandlersCollection::TranscodeSegment invokes this on success
func (c CallbackClient) SendSegmentTranscodeStatus(callbackURL, sourceLocation string) error {
	return c.sendTSM(callbackURL, TranscodeStatusMessage{
		Status:          TranscodeStatusSegmentTranscoding.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
		SourceLocations: []string{sourceLocation},
		CompletionRatio: 100,
	})
}

// CatalystAPIHandlersCollection::TranscodeSegment invokes this on error
func (c CallbackClient) SendSegmentTranscodeError(callbackURL, where, errorMsg, sourceLocation string) error {
	return c.sendTSM(callbackURL, TranscodeStatusMessage{
		Error:           fmt.Sprintf("%s; %s", where, errorMsg),
		Status:          TranscodeStatusSegmentTranscoding.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
		SourceLocations: []string{sourceLocation},
	})
}

func (c CallbackClient) sendTSM(callbackURL string, tsm TranscodeStatusMessage) error {
	j, err := json.Marshal(tsm)
	if err != nil {
		return err
	}

	r, err := retryablehttp.NewRequest(http.MethodPost, callbackURL, bytes.NewReader(j))
	if err != nil {
		return err
	}

	// Caller may be blocking trigger. Run in background, otherwise we introduce latency in current operation.
	go c.DoWithRetries(r)

	return nil
}

// An enum of potential statuses a Transcode job can have

type TranscodeStatus int

const (
	TranscodeStatusPreparing TranscodeStatus = iota
	TranscodeStatusTranscoding
	TranscodeStatusCompleted
	TranscodeStatusError
	TranscodeStatusSegmentTranscoding
	TranscodeStatusRenditionUpload
)

type TranscodeStatusMessage struct {
	CompletionRatio      float32  `json:"completion_ratio,omitempty"`
	Error                string   `json:"error,omitempty"`
	Retriable            bool     `json:"retriable,omitempty"`
	Status               string   `json:"status,omitempty"`
	Timestamp            int64    `json:"timestamp"`
	SourceLocations      []string `json:"source_locations,omitempty"`
	DestinationLocations []string `json:"destination_locations,omitempty"`
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
	case TranscodeStatusSegmentTranscoding:
		return "segment-transcode"
	case TranscodeStatusRenditionUpload:
		return "segment-rendition-upload"
	}
	return "unknown"
}
