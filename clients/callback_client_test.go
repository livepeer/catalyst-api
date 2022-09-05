package clients

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

func TestItRetriesOnFailedCallbacks(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Counter for the number of retries we've done
	var tries int
	callbacks := make(chan struct{}, 3)

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { callbacks <- struct{}{} }()
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
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
	waitForCallbacks(t, callbacks, 3, 5*time.Second)
	require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
}

func TestItEventuallyStopsRetrying(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Counter for the number of retries we've done
	var tries int
	callbacks := make(chan struct{}, 3)

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { callbacks <- struct{}{} }()
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"completion_ratio":1, "status":"completed", "timestamp": 123456789}`, string(body))

		tries += 1

		// Return an error code
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewCallbackClient()
	require.NoError(t, client.SendTranscodeStatus(svr.URL, TranscodeStatusCompleted, 1))
	waitForCallbacks(t, callbacks, 3, 5*time.Second)
	require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
	// The case for more than 3 retries is not tested
}

func TestTranscodeStatusErrorNotifcation(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	callbacks := make(chan struct{}, 3)
	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { callbacks <- struct{}{} }()
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"error": "something went wrong", "status":"error", "timestamp": 123456789}`, string(body))

		w.WriteHeader(http.StatusOK)
	}))
	defer svr.Close()

	// Send the callback and confirm the number of times we retried
	client := NewCallbackClient()
	require.NoError(t, client.SendTranscodeStatusError(svr.URL, "something went wrong"))
	waitForCallbacks(t, callbacks, 1, 5*time.Second)
}

func waitForCallbacks(t *testing.T, c chan struct{}, count int, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	happened := 0
	for happened < count {
		select {
		case <-c:
			happened += 1
		case <-ctx.Done():
			require.FailNow(t, "Expected async result within this timeout")
		}
	}
}
