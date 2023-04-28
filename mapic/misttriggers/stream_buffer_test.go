package misttriggers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

const (
	streamBufferPayloadFull    = "stream1\nFULL\n{\"track1\":{\"codec\":\"h264\",\"kbits\":1000,\"keys\":{\"B\":\"1\"},\"fpks\":30,\"height\":720,\"width\":1280}}"
	streamBufferPayloadIssues  = "stream1\nRECOVER\n{\"track1\":{\"codec\":\"h264\",\"kbits\":1000,\"keys\":{\"B\":\"1\"},\"fpks\":30,\"height\":720,\"width\":1280},\"issues\":\"Stream is feeling under the weather\"}"
	streamBufferPayloadEmpty   = "stream1\nEMPTY\n"
	streamBufferPayloadInvalid = "stream1\nFULL\n{\"track1\":{\"codec\":\"h264\",\"kbits\":1000,\"keys\":{\"B\":\"1\"},\"fpks\":30,\"height\":720,\"width\":1280},\"issues\":false}"
)

func TestItCanParseAValidStreamBufferPayload(t *testing.T) {
	lines := strings.Split(streamBufferPayloadFull, "\n")
	p, err := ParseStreamBufferPayload(lines)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "FULL")
	require.NotNil(t, p.Details)
	require.Equal(t, p.Details.Issues, "")
	require.Len(t, p.Details.Tracks, 1)
	require.Contains(t, p.Details.Tracks, "track1")
}

func TestItCanParseAStreamBufferPayloadWithStreamIssues(t *testing.T) {
	lines := strings.Split(streamBufferPayloadIssues, "\n")
	p, err := ParseStreamBufferPayload(lines)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "RECOVER")
	require.NotNil(t, p.Details)
	require.Equal(t, p.Details.Issues, "Stream is feeling under the weather")
	require.Len(t, p.Details.Tracks, 1)
	require.Contains(t, p.Details.Tracks, "track1")
}

func TestItCanParseAValidStreamBufferPayloadWithEmptyState(t *testing.T) {
	lines := strings.Split(streamBufferPayloadEmpty, "\n")
	p, err := ParseStreamBufferPayload(lines)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "EMPTY")
	require.Nil(t, p.Details)
}

func TestItFailsToParseAnInvalidStreamBufferPayload(t *testing.T) {
	lines := strings.Split(streamBufferPayloadInvalid, "\n")
	_, err := ParseStreamBufferPayload(lines)
	require.Error(t, err)
	require.Contains(t, err.Error(), "issues field is not a string")
}

func TestPostStreamHealthPayloadFailsWithInvalidURL(t *testing.T) {
	streamHealthPayload := StreamHealthPayload{
		StreamName: "stream1",
		SessionID:  "session1",
		State:      "FULL",
		Tracks:     nil,
		Issues:     "",
	}

	err := PostStreamHealthPayload("http://invalid.url", "apiToken", streamHealthPayload)
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
		State:      "FULL",
		Tracks:     nil,
		Issues:     "No issues",
	}

	err := PostStreamHealthPayload(server.URL, "apiToken", streamHealthPayload)
	require.NoError(t, err)
	require.Equal(t, 1, callCount)
}

func TestTriggerStreamBufferE2E(t *testing.T) {
	// Start an HTTP test server to simulate the webhook endpoint
	var receivedPayload StreamHealthPayload
	var receivedAuthHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")

		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&receivedPayload)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("error unmarshalling payload"))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	// Prepare the request and payload
	req, err := http.NewRequest("GET", "http://example.com", strings.NewReader(streamBufferPayloadFull))
	require.NoError(t, err)
	req.Header.Set("X-UUID", "session1")

	// Call the TriggerStreamBuffer function
	cli := &config.Cli{
		StreamHealthHookURL: server.URL,
		APIToken:            "apiToken",
	}
	err = TriggerStreamBuffer(cli, req, strings.Split(streamBufferPayloadFull, "\n"))
	require.NoError(t, err)

	// Check the payload received by the test server
	require.Equal(t, receivedAuthHeader, "Bearer apiToken")
	require.Equal(t, receivedPayload.StreamName, "stream1")
	require.Equal(t, receivedPayload.SessionID, "session1")
	require.Equal(t, receivedPayload.State, "FULL")
	require.Len(t, receivedPayload.Tracks, 1)
	require.Contains(t, receivedPayload.Tracks, "track1")
	require.Equal(t, receivedPayload.Issues, "")
}
