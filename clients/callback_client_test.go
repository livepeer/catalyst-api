package clients

import (
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

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	select {
	case err := <-client.Errors:
		require.NoError(t, err)
		require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Expected async result within this timeout")
	}
}

func TestItEventuallyStopsRetrying(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Counter for the number of retries we've done
	var tries int

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	err := client.SendTranscodeStatus(svr.URL, TranscodeStatusCompleted, 1)
	require.NoError(t, err)
	select {
	case err = <-client.Errors:
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send callback")
		require.Contains(t, err.Error(), "giving up after 3 attempt(s)")
		require.Equal(t, 3, tries, "Expected the client to retry on failed callbacks")
	case <-time.After(time.Second * 3):
		require.FailNow(t, "Expected async result within this timeout")
	}
}

func TestTranscodeStatusErrorNotifcation(t *testing.T) {
	config.Clock = config.FixedTimestampGenerator{Timestamp: 123456789}
	defer func() { config.Clock = config.RealTimestampGenerator{} }()

	// Set up a dummy server to receive the callbacks
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	select {
	case err := <-client.Errors:
		require.NoError(t, err, "something went wrong")
	case <-time.After(time.Second * 3):
		require.FailNow(t, "Expected async result within this timeout")
	}
}
