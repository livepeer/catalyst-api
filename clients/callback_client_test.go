package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestItRetriesOnFailedCallbacks(t *testing.T) {
	// Counter for the number of retries we've done
	var tries int64

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		// Check we got a valid callback message of the type we'd expect
		var actualMsg TranscodeStatusMessage
		require.NoError(t, json.Unmarshal(body, &actualMsg))
		require.Equal(t, TranscodeStatusCompleted, actualMsg.Status)

		// Return HTTP error codes the first two times
		atomic.AddInt64(&tries, 1)
		if atomic.LoadInt64(&tries) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Return a successful response the third time
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Create a client that sends heartbeats very irregularly, to let us assert things about a single iteration of the callback
	client := NewPeriodicCallbackClient(100 * time.Hour)

	// Send the status in, but it shouldn't get sent yet because we haven't started the client
	client.SendTranscodeStatus(NewTranscodeStatusProgress(svr.URL, "example-request-id", TranscodeStatusCompleted, 1))

	// Trigger the callback client to send any pending callbacks
	client.SendCallbacks()
	time.Sleep(1 * time.Second)

	require.Equal(t, int64(3), atomic.LoadInt64(&tries), "Expected the client to retry on failed callbacks")
}

func TestItSendsPeriodicHeartbeats(t *testing.T) {
	// Counter for the number of retries we've done
	var tries int64

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		// Check we got a valid callback message of the type we'd expect
		var actualMsg TranscodeStatusMessage
		require.NoError(t, json.Unmarshal(body, &actualMsg))
		require.Equal(t, TranscodeStatusCompleted, actualMsg.Status)

		atomic.AddInt64(&tries, 1)

		// Return an error code
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewPeriodicCallbackClient(100 * time.Millisecond).Start()
	client.SendTranscodeStatus(NewTranscodeStatusProgress(svr.URL, "example-request-id", TranscodeStatusCompleted, 1))

	time.Sleep(400 * time.Millisecond)

	require.Equal(t, int64(1), atomic.LoadInt64(&tries), "Expected the client to have sent 1 status within the timeframe")
}

func TestTranscodeStatusErrorNotifcation(t *testing.T) {
	// Set up a dummy server to receive the callbacks
	var requestCount int64
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		// Check we got a valid callback message of the type we'd expect
		var actualMsg TranscodeStatusMessage
		require.NoError(t, json.Unmarshal(body, &actualMsg))
		require.Equal(t, TranscodeStatusError, actualMsg.Status)
		require.Equal(t, "something went wrong", actualMsg.Error)

		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewPeriodicCallbackClient(100 * time.Millisecond).Start()
	client.SendTranscodeStatus(NewTranscodeStatusError(svr.URL, "example-request-id", "something went wrong", false))

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, int64(1), atomic.LoadInt64(&requestCount))
}

// Updates might still theoretically arrive at the callback endpoint out of order,
// but this test ensures that we don't send one progress callback of X% completion
// and then repeatedly send one with < X% completion
// (i.e that our internal state never regresses)
func TestItDoesntSendOutOfOrderUpdates(t *testing.T) {
	// Counter for the number of retries we've done
	var tries int64
	var latestCompletion float64
	var latestCompletionMutex sync.Mutex

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		// Check we got a valid callback message of the type we'd expect
		var actualMsg TranscodeStatusMessage
		require.NoError(t, json.Unmarshal(body, &actualMsg))

		atomic.AddInt64(&tries, 1)
		latestCompletionMutex.Lock()
		latestCompletion = actualMsg.CompletionRatio
		latestCompletionMutex.Unlock()

		// Return an error code
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewPeriodicCallbackClient(100 * time.Millisecond).Start()
	client.SendTranscodeStatus(NewTranscodeStatusProgress(svr.URL, "example-request-id", TranscodeStatusTranscoding, 1))
	client.SendTranscodeStatus(NewTranscodeStatusProgress(svr.URL, "example-request-id", TranscodeStatusPreparing, 1))
	time.Sleep(400 * time.Millisecond)

	// Sanity check that the client has sent multiple callbacks in this timeframe
	require.Greater(t, atomic.LoadInt64(&tries), int64(2), "Expected the client to have sent multiple status updates within the timeframe")

	// Check that we're being sent the latest message in terms of progress, rather than order of being sent
	latestCompletionMutex.Lock()
	defer latestCompletionMutex.Unlock()
	require.Equal(t, 0.9, latestCompletion)
}

func TestItCalculatesTheOverallCompletionRatioCorrectly(t *testing.T) {
	testCases := []struct {
		status                         TranscodeStatus
		completionRatio                float64
		expectedOverallCompletionRatio float64
	}{
		{TranscodeStatusPreparing, 0.5, 0.2},           // Half complete in the Preparing stage (i.e half way between 0 and 0.4)
		{TranscodeStatusPreparingCompleted, 1234, 0.4}, // Preparing Completed should always == 0.4 for now, regardless of what's reported as the stage ratio
		{TranscodeStatusTranscoding, 0.5, 0.65},        // Half complete in the Transcoding stage (i.e half way between 0.4 and 0.9)
		{TranscodeStatusCompleted, 5678, 1},            // Completed should always == 1, regardless of what's reported as the stage ratio
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%f in %s", tc.completionRatio, tc.status), func(t *testing.T) {
			require.Equal(t, tc.expectedOverallCompletionRatio, OverallCompletionRatio(tc.status, tc.completionRatio))
		})
	}
}
