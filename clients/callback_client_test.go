package clients

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

func TestItRetriesOnFailedCallbacks(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Counter for the number of retries we've done
	var tries int

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"completion_ratio":1, "status":"completed", "timestamp": 123456789}`, string(body))

		// Return HTTP error codes the first two times
		tries += 1
		if tries <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Return a successful response the third time
		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewCallbackClient()
	require.NoError(t, client.SendTranscodeStatus(svr.URL, TranscodeStatusCompleted, 1))
	require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
}

func TestItEventuallyStopsRetrying(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Counter for the number of retries we've done
	var tries int

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"completion_ratio":1, "status":"completed", "timestamp": 123456789}`, string(body))

		tries += 1

		// Return an error code
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewCallbackClient()
	err := client.SendTranscodeStatus(svr.URL, TranscodeStatusCompleted, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to send callback")
	require.Contains(t, err.Error(), "giving up after 3 attempt(s)")
	require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
}

func TestTranscodeStatusErrorNotifcation(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"error": "something went wrong", "status":"error", "timestamp": 123456789}`, string(body))

		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewCallbackClient()
	require.NoError(t, client.SendTranscodeStatusError(svr.URL, "something went wrong"))
}

func TestItCalculatesTheOverallCompletionRatioCorrectly(t *testing.T) {
	testCases := []struct {
		status                         TranscodeStatus
		completionRatio                float64
		expectedOverallCompletionRatio float64
	}{
		{TranscodeStatusPreparing, 0.5, 0.2},           // Half complete in the Preparing stage (i.e half way between 0 and 0.4)
		{TranscodeStatusPreparingCompleted, 1234, 0.4}, // Preparing Completed should always == 0.4 for now, regardless of what's reported as the stage ratio
		{TranscodeStatusTranscoding, 0.5, 0.7},         // Half complete in the Transcoding stage (i.e half way between 0.4 and 1)
		{TranscodeStatusCompleted, 5678, 1},            // Completed should always == 1, regardless of what's reported as the stage ratio
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%f in %s", tc.completionRatio, tc.status), func(t *testing.T) {
			require.Equal(t, tc.expectedOverallCompletionRatio, overallCompletionRatio(tc.status, tc.completionRatio))
		})
	}
}
