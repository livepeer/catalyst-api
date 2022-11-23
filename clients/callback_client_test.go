package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
		require.Equal(t, "success", actualMsg.Status)

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
	client.SendTranscodeStatus(svr.URL, "example-request-id", TranscodeStatusCompleted, 1)

	// Start the client and wait for an iteration of the loop
	client.Start()
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
		require.Equal(t, "success", actualMsg.Status)

		atomic.AddInt64(&tries, 1)

		// Return an error code
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewPeriodicCallbackClient(100 * time.Millisecond).Start()
	client.SendTranscodeStatus(svr.URL, "example-request-id", TranscodeStatusCompleted, 1)

	time.Sleep(400 * time.Millisecond)

	require.Less(t, int64(1), atomic.LoadInt64(&tries), "Expected the client to have sent at least 2 statuses within the timeframe")
	require.Greater(t, int64(6), atomic.LoadInt64(&tries), "Expected the client to have backed off between heartbeats")
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
		require.Equal(t, "error", actualMsg.Status)
		require.Equal(t, "something went wrong", actualMsg.Error)

		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewPeriodicCallbackClient(100 * time.Millisecond).Start()
	client.SendTranscodeStatusError(svr.URL, "example-request-id", "something went wrong")

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, int64(1), atomic.LoadInt64(&requestCount))
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
