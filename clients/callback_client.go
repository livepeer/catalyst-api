package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

const MAX_TIME_WITHOUT_UPDATE = 30 * time.Minute

// The default client is only used for the recording event. This is to avoid
// misusing the singleton client to send transcode status updates, which should
// be sent through the JobInfo.ReportStatus function instead.
var recordingCallbackClient = NewPeriodicCallbackClient(15*time.Second, map[string]string{})

func SendRecordingEventCallback(event *RecordingEvent) {
	recordingCallbackClient.SendRecordingEvent(event)
}

type TranscodeStatusClient interface {
	SendTranscodeStatus(tsm TranscodeStatusMessage)
}

type TranscodeStatusFunc func(tsm TranscodeStatusMessage)

func (f TranscodeStatusFunc) SendTranscodeStatus(tsm TranscodeStatusMessage) {
	f(tsm)
}

type PeriodicCallbackClient struct {
	requestIDToLatestMessage map[string]TranscodeStatusMessage
	mapLock                  sync.RWMutex
	httpClient               *http.Client
	callbackInterval         time.Duration
	headers                  map[string]string
}

func NewPeriodicCallbackClient(callbackInterval time.Duration, headers map[string]string) *PeriodicCallbackClient {
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
		headers:                  headers,
	}
}

// Start looping through all active jobs, sending a callback for the latest status of each
// and then pausing for a set amount of time
func (pcc *PeriodicCallbackClient) Start() *PeriodicCallbackClient {
	go func() {
		for {
			recoverer(func() {
				time.Sleep(pcc.callbackInterval)
				pcc.SendCallbacks()
			})
		}
	}()
	return pcc
}

func recoverer(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.LogNoRequestID("panic in callback goroutine, recovering", "err", err, "trace", debug.Stack())
		}
	}()
	f()
}

// Sends a Transcode Status message to the Client (initially just Studio)
// The status strings will be useful for debugging where in the workflow we got to, but everything
// in Studio will be driven off the overall "Completion Ratio".
func (pcc *PeriodicCallbackClient) SendTranscodeStatus(tsm TranscodeStatusMessage) {
	if tsm.URL != "" {
		pcc.mapLock.Lock()
		defer pcc.mapLock.Unlock()

		previousMessage, ok := pcc.requestIDToLatestMessage[tsm.RequestID]
		previousCompletion := OverallCompletionRatio(previousMessage.Status, previousMessage.CompletionRatio)
		newCompletion := OverallCompletionRatio(tsm.Status, tsm.CompletionRatio)

		// Don't update the current message with one that represents an earlier stage
		if !ok || tsm.IsTerminal() || newCompletion >= previousCompletion {
			pcc.requestIDToLatestMessage[tsm.RequestID] = tsm
		}
	}

	log.Log(tsm.RequestID, "Updated transcode status",
		"timestamp", tsm.Timestamp, "status", tsm.Status, "completion_ratio", tsm.CompletionRatio,
		"error", tsm.Error)
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

		// Error is a terminal state so remove the job from the list after sending the callback
		if tsm.IsTerminal() {
			log.Log(tsm.RequestID, "Removing job from active list")
			delete(pcc.requestIDToLatestMessage, tsm.RequestID)
		}
	}
}

func (pcc *PeriodicCallbackClient) doWithRetries(r *http.Request) error {
	for k, v := range pcc.headers {
		r.Header.Set(k, v)
	}

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
