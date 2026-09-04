package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

const MAX_TIME_WITHOUT_UPDATE = 30 * time.Minute

type TranscodeStatusClient interface {
	SendTranscodeStatus(tsm TranscodeStatusMessage) error
}

type TranscodeStatusFunc func(tsm TranscodeStatusMessage) error

func (f TranscodeStatusFunc) SendTranscodeStatus(tsm TranscodeStatusMessage) error {
	return f(tsm)
}

type PeriodicCallbackClient struct {
	requestIDToLatestMessage map[string]TranscodeStatusMessage
	mapLock                  sync.RWMutex
	httpClient               *http.Client
	callbackInterval         time.Duration
	headers                  map[string]string
	apiServer                string
	done                     chan struct{}
	stopOnce                 sync.Once
}

func NewPeriodicCallbackClient(callbackInterval time.Duration, headers map[string]string, apiServer string) *PeriodicCallbackClient {
	return newPeriodicCallbackClient(
		callbackInterval,
		headers,
		apiServer,
		newPublicHTTPClientForSurface(5*time.Second, "callback", false, false),
	)
}

// newPeriodicCallbackClient permits unit tests to inject an isolated client. Production
// callers must use NewPeriodicCallbackClient so callback URLs are checked at delivery time.
func newPeriodicCallbackClient(callbackInterval time.Duration, headers map[string]string, apiServer string, httpClient *http.Client) *PeriodicCallbackClient {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.CheckRetry = metrics.HttpRetryHook
	client.HTTPClient = httpClient
	client.Logger = log.NewRetryableHTTPLogger()

	standardClient := client.StandardClient()
	standardClient.CheckRedirect = httpClient.CheckRedirect
	standardClient.Jar = httpClient.Jar
	standardClient.Timeout = httpClient.Timeout

	return &PeriodicCallbackClient{
		httpClient:               standardClient,
		callbackInterval:         callbackInterval,
		requestIDToLatestMessage: map[string]TranscodeStatusMessage{},
		mapLock:                  sync.RWMutex{},
		headers:                  headers,
		apiServer:                apiServer,
		done:                     make(chan struct{}),
	}
}

// Start looping through all active jobs, sending a callback for the latest status of each
// and then pausing for a set amount of time
func (pcc *PeriodicCallbackClient) Start() *PeriodicCallbackClient {
	go func() {
		ticker := time.NewTicker(pcc.callbackInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				recoverer(pcc.SendCallbacks)
			case <-pcc.done:
				return
			}
		}
	}()
	return pcc
}

func (pcc *PeriodicCallbackClient) Stop() {
	pcc.stopOnce.Do(func() { close(pcc.done) })
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
func (pcc *PeriodicCallbackClient) SendTranscodeStatus(tsm TranscodeStatusMessage) error {
	if tsm.URL == "" {
		return nil
	}
	pcc.updateTranscodeStatus(tsm)

	// Terminal callbacks are sent here in a sync manner
	// Non-terminal callbacks are sent periodically, in an async manner
	if tsm.IsTerminal() || tsm.SourcePlayback != nil {
		return pcc.sendCallback(tsm)
	}
	return nil
}

func (pcc *PeriodicCallbackClient) updateTranscodeStatus(tsm TranscodeStatusMessage) {
	pcc.mapLock.Lock()
	defer pcc.mapLock.Unlock()

	previousMessage, ok := pcc.requestIDToLatestMessage[tsm.RequestID]
	previousCompletion := OverallCompletionRatio(previousMessage.Status, previousMessage.CompletionRatio)
	newCompletion := OverallCompletionRatio(tsm.Status, tsm.CompletionRatio)

	// Don't update the current message with one that represents an earlier stage
	if !ok || tsm.IsTerminal() || newCompletion >= previousCompletion {
		pcc.requestIDToLatestMessage[tsm.RequestID] = tsm
	}

	log.Log(tsm.RequestID, "Updated transcode status",
		"timestamp", tsm.Timestamp, "status", tsm.Status, "completion_ratio", tsm.CompletionRatio,
		"error", tsm.Error)

	// Error is a terminal state so remove the job from the list after sending the callback
	if tsm.IsTerminal() {
		log.Log(tsm.RequestID, "Removing job from active list")
		delete(pcc.requestIDToLatestMessage, tsm.RequestID)
	}
}

// Loop over all active jobs, sending a (non-blocking) HTTP callback for each
func (pcc *PeriodicCallbackClient) SendCallbacks() {
	pcc.mapLock.Lock()
	defer pcc.mapLock.Unlock()

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

		// Send non-terminal callbacks here in an async manner
		// Terminal callbacks are sent when the job is finished in the sync manner
		if !tsm.IsTerminal() {
			go func(tsm TranscodeStatusMessage) {
				// Ignore errors during async callback sending
				_ = pcc.sendCallback(tsm)
			}(tsm)
		}
	}
}

func (pcc *PeriodicCallbackClient) sendCallback(tsm TranscodeStatusMessage) error {
	if err := ValidateCallbackURL(tsm.URL, pcc.apiServer); err != nil {
		return err
	}

	j, err := json.Marshal(tsm)
	if err != nil {
		log.LogError(tsm.RequestID, "failed to marshal callback JSON", err)
		return err
	}

	r, err := http.NewRequest(http.MethodPost, tsm.URL, bytes.NewReader(j))
	if err != nil {
		log.LogError(tsm.RequestID, "failed to create callback HTTP request", err)
		return err
	}

	err = pcc.doWithRetries(r)
	if err != nil {
		log.LogError(tsm.RequestID, "failed to send callback", err)
		return err
	}
	return nil
}

func ValidateCallbackURL(rawURL, apiServer string) error {
	callback, err := url.Parse(rawURL)
	if err != nil {
		return rejectDestination("callback", "callback URL is invalid")
	}
	if err := ValidatePublicURL(callback, "callback", false, "https"); err != nil {
		return err
	}
	server, err := url.Parse(apiServer)
	if err != nil || !strings.EqualFold(server.Scheme, "https") || server.Hostname() == "" {
		return rejectDestination("callback", "callback policy requires an HTTPS API server")
	}
	port := func(u *url.URL) string {
		if u.Port() != "" {
			return u.Port()
		}
		return "443"
	}
	callbackPath := path.Clean("/" + strings.TrimPrefix(callback.Path, "/"))
	if !strings.EqualFold(callback.Hostname(), server.Hostname()) || port(callback) != port(server) ||
		(callbackPath != "/task-runner" && !strings.HasPrefix(callbackPath, "/task-runner/")) {
		return rejectDestination("callback", "callback URL is not a Task Runner endpoint for the API server")
	}
	if callback.RawPath != "" || callback.Fragment != "" {
		return rejectDestination("callback", "encoded paths and fragments are not allowed")
	}
	return nil
}

func (pcc *PeriodicCallbackClient) doWithRetries(r *http.Request) error {
	for k, v := range pcc.headers {
		r.Header.Set(k, v)
	}

	resp, err := metrics.MonitorRequest(metrics.Metrics.TranscodingStatusUpdate, pcc.httpClient, r)
	if err != nil {
		return fmt.Errorf("failed to send callback to %q. Error: %s", log.RedactURL(r.URL.String()), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("failed to send callback to %q. HTTP Code: %d", log.RedactURL(r.URL.String()), resp.StatusCode)
	}

	return nil
}
