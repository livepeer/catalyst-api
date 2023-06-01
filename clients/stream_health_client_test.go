package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostStreamHealthPayloadFailsWithInvalidURL(t *testing.T) {
	c := streamHealthClient{
		apiToken: "apiToken",
		hookURL:  "http://invalid.url",
		client:   &http.Client{},
	}
	streamHealthPayload := StreamHealthPayload{
		StreamName: "stream1",
		SessionID:  "session1",
		IsActive:   true,
		IsHealthy:  true,
		Tracks:     nil,
		Issues:     "",
	}

	err := c.PostStreamHealthPayload(streamHealthPayload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error pushing stream health to hook")
}

func TestPostStreamHealthPayloadWithTestServer(t *testing.T) {
	// Start an HTTP test server to simulate the webhook endpoint
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	streamHealthPayload := StreamHealthPayload{
		StreamName: "stream1",
		SessionID:  "session1",
		IsActive:   true,
		IsHealthy:  true,
		Tracks:     nil,
		Issues:     "No issues",
	}

	c := streamHealthClient{
		apiToken: "apiToken",
		hookURL:  server.URL,
		client:   &http.Client{},
	}

	err := c.PostStreamHealthPayload(streamHealthPayload)
	require.NoError(t, err)
	require.Equal(t, 1, callCount)
}
