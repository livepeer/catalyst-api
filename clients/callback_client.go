package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

const MAX_TIME_WITHOUT_UPDATE = 30 * time.Minute

var DefaultCallbackClient = NewPeriodicCallbackClient(15 * time.Second)

type PeriodicCallbackClient struct {
	requestIDToLatestMessage map[string]TranscodeStatusMessage
	mapLock                  sync.RWMutex
	httpClient               *http.Client
	callbackInterval         time.Duration
}

func NewPeriodicCallbackClient(callbackInterval time.Duration) *PeriodicCallbackClient {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.CheckRetry = metrics.HttpRetryHook
	client.HTTPClient = &http.Client{
		Timeout: 5 * time.Second, // Give up on requests that take more than this long
	}

	return &PeriodicCallbackClient{
		httpClient:               client.StandardClient(),
		callbackInterval:         callbackInterval,
		requestIDToLatestMessage: map[string]TranscodeStatusMessage{},
		mapLock:                  sync.RWMutex{},
	}
}

// Start looping through all active jobs, sending a callback for the latest status of each
// and then pausing for a set amount of time
func (pcc *PeriodicCallbackClient) Start() *PeriodicCallbackClient {
	go func() {
		for {
			recoverer(pcc.SendCallbacks)
			time.Sleep(pcc.callbackInterval)
		}
	}()
	return pcc
}

func recoverer(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.LogNoRequestID("panic in callback goroutine, recovering", "err", err)
			go recoverer(f)
		}
	}()
	f()
}

// Sends a Transcode Status message to the Client (initially just Studio)
// The status strings will be useful for debugging where in the workflow we got to, but everything
// in Studio will be driven off the overall "Completion Ratio".
// This method will accept the completion ratio of the current stage and will translate that into the overall ratio
func (pcc *PeriodicCallbackClient) SendTranscodeStatus(url, requestID string, status TranscodeStatus, currentStageCompletionRatio float64) {
	tsm := TranscodeStatusMessage{
		URL:             url,
		RequestID:       requestID,
		CompletionRatio: OverallCompletionRatio(status, currentStageCompletionRatio),
		Status:          status.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
	}

	pcc.statusCallback(tsm)
}

func (pcc *PeriodicCallbackClient) SendRecordingEvent(event *RecordingEvent) {
	go func() {
		j, err := json.Marshal(event)
		if err != nil {
			log.LogNoRequestID("failed to marshal recording event callback JSON", "err", err)
			return
		}

		r, err := http.NewRequest(http.MethodPost, config.RecordingCallback, bytes.NewReader(j))
		if err != nil {
			log.LogNoRequestID("failed to create recording event callback request", "err", err)
			return
		}

		err = pcc.doWithRetries(r)
		if err != nil {
			log.LogNoRequestID("failed to send recording event callback", "err", err)
			return
		}
	}()
}

func (pcc *PeriodicCallbackClient) SendTranscodeStatusError(url, requestID, errorMsg string) {
	tsm := TranscodeStatusMessage{
		URL:       url,
		RequestID: requestID,
		Error:     errorMsg,
		Status:    TranscodeStatusError.String(),
		Timestamp: config.Clock.GetTimestampUTC(),
	}

	pcc.statusCallback(tsm)
}

// Separate method as this requires a much richer message than the other status callbacks
func (pcc *PeriodicCallbackClient) SendTranscodeStatusCompleted(url, requestID string, iv InputVideo, ov []OutputVideo) {
	tsm := TranscodeStatusMessage{
		URL:             url,
		CompletionRatio: OverallCompletionRatio(TranscodeStatusCompleted, 1),
		RequestID:       requestID,
		Status:          TranscodeStatusCompleted.String(),
		Timestamp:       config.Clock.GetTimestampUTC(),
		Type:            "video", // Assume everything is a video for now
		InputVideo:      iv,
		Outputs:         ov,
	}

	pcc.statusCallback(tsm)
}

// Update with a status message
func (pcc *PeriodicCallbackClient) statusCallback(tsm TranscodeStatusMessage) {
	pcc.mapLock.Lock()
	defer pcc.mapLock.Unlock()

	pcc.requestIDToLatestMessage[tsm.RequestID] = tsm
}

// Loop over all active jobs, sending a (non-blocking) HTTP callback for each
func (pcc *PeriodicCallbackClient) SendCallbacks() {
	pcc.mapLock.Lock()
	defer pcc.mapLock.Unlock()

	log.LogNoRequestID(fmt.Sprintf("Sending %d callbacks", len(pcc.requestIDToLatestMessage)))
	for _, tsm := range pcc.requestIDToLatestMessage {
		// Check timestamp and give up on the job if we haven't received an update for a long time
		cutoff := int64(config.Clock.GetTimestampUTC() - MAX_TIME_WITHOUT_UPDATE.Milliseconds())
		if tsm.Timestamp < cutoff {
			delete(pcc.requestIDToLatestMessage, tsm.RequestID)
			log.Log(
				tsm.RequestID,
				"timed out waiting for callback updates",
				"last_timestamp", tsm.Timestamp,
				"cutoff_timestamp", cutoff)
			continue
		}

		// Do the JSON marshalling and HTTP call in a goroutine to avoid blocking the loop
		go func(tsm TranscodeStatusMessage) {
			j, err := json.Marshal(tsm)
			if err != nil {
				log.LogError(tsm.RequestID, "failed to marshal callback JSON", err)
				return
			}

			r, err := http.NewRequest(http.MethodPost, tsm.URL, bytes.NewReader(j))
			if err != nil {
				log.LogError(tsm.RequestID, "failed to create callback HTTP request", err)
				return
			}

			err = pcc.doWithRetries(r)
			if err != nil {
				log.LogError(tsm.RequestID, "failed to send callback", err)
				return
			}
		}(tsm)

		// Error is a terminal state, so remove the job from the list after sending the callback
		if tsm.Status == TranscodeStatusError.String() || tsm.Status == TranscodeStatusCompleted.String() {
			log.Log(tsm.RequestID, "Removing job from active list")
			delete(pcc.requestIDToLatestMessage, tsm.RequestID)
		}
	}
}

func (pcc *PeriodicCallbackClient) doWithRetries(r *http.Request) error {
	// TODO: Replace with a proper shared Secret, probably coming from the initial request
	r.Header.Set("Authorization", "Bearer IAmAuthorized")

	resp, err := metrics.MonitorRequest(metrics.Metrics.TranscodingStatusUpdate, pcc.httpClient, r)
	if err != nil {
		return fmt.Errorf("failed to send callback to %q. Error: %s", r.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("failed to send callback to %q. HTTP Code: %d", r.URL.String(), resp.StatusCode)
	}

	return nil
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
