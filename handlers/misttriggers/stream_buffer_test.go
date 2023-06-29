package misttriggers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

var streamBufferPayloadFull = []byte(`stream1
FULL
{"track1":{"codec":"h264","kbits":1000,"keys":{"B":"1"},"fpks":30,"height":720,"width":1280},"jitter":420}`)

var streamBufferPayloadIssues = []byte(`stream1
RECOVER
{"track1":{"codec":"h264","kbits":1000,"keys":{"B":"1"},"fpks":30,"height":720,"width":1280},"issues":"The aqueous linear entity, in a manner pertaining to its metaphorical state of existence, appears to be experiencing an ostensibly suboptimal condition that is reminiscent of an individual's disposition when subjected to an unfavorable meteorological phenomenon","human_issues":["Stream is feeling under the weather"]}`)

var streamBufferPayloadInvalid = []byte(`stream1
FULL
{"track1":{},"notatrack":{"codec":2}}`)

var streamBufferPayloadEmpty = []byte(`stream1
EMPTY`)

func TestItCanParseAValidStreamBufferPayload(t *testing.T) {
	p, err := ParseStreamBufferPayload(streamBufferPayloadFull)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "FULL")
	require.NotNil(t, p.Details)
	require.Equal(t, p.Details.Issues, "")
	require.Len(t, p.Details.Tracks, 1)
	require.Contains(t, p.Details.Tracks, "track1")
	require.Equal(t, p.Details.Extra["jitter"], float64(420))
}

func TestItCanParseAStreamBufferPayloadWithStreamIssues(t *testing.T) {
	p, err := ParseStreamBufferPayload(streamBufferPayloadIssues)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "RECOVER")
	require.NotNil(t, p.Details)
	require.Equal(t, p.Details.HumanIssues, []string{"Stream is feeling under the weather"})
	require.Contains(t, p.Details.Issues, "unfavorable meteorological phenomenon")
	require.Len(t, p.Details.Tracks, 1)
	require.Contains(t, p.Details.Tracks, "track1")
}

func TestItCanParseAValidStreamBufferPayloadWithEmptyState(t *testing.T) {
	p, err := ParseStreamBufferPayload(streamBufferPayloadEmpty)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "stream1")
	require.Equal(t, p.State, "EMPTY")
	require.Nil(t, p.Details)
}

func TestItFailsToParseAnInvalidStreamBufferPayload(t *testing.T) {
	_, err := ParseStreamBufferPayload(streamBufferPayloadInvalid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot unmarshal number into Go struct field TrackDetails.codec of type string")
}

func TestPostStreamHealthPayloadFailsWithInvalidURL(t *testing.T) {
	streamHealthPayload := StreamHealthPayload{
		StreamName: "stream1",
		SessionID:  "session1",
		IsActive:   true,
		IsHealthy:  true,
		Tracks:     nil,
		Issues:     "",
	}

	d := MistCallbackHandlersCollection{cli: &config.Cli{
		APIToken:            "apiToken",
		StreamHealthHookURL: "http://invalid.url",
	}}
	err := d.PostStreamHealthPayload(streamHealthPayload)
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

	d := MistCallbackHandlersCollection{cli: &config.Cli{
		APIToken:            "apiToken",
		StreamHealthHookURL: server.URL,
	}}

	err := d.PostStreamHealthPayload(streamHealthPayload)
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
			_, err := w.Write([]byte("error unmarshalling payload"))
			require.NoError(t, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	// Prepare the request and payload
	payload := bytes.NewReader(streamBufferPayloadIssues)
	req, err := http.NewRequest("GET", "http://example.com", payload)
	require.NoError(t, err)
	req.Header.Set("X-UUID", "session1")

	// Call the TriggerStreamBuffer function
	d := MistCallbackHandlersCollection{
		cli: &config.Cli{
			StreamHealthHookURL: server.URL,
			APIToken:            "apiToken",
		},
		broker: NewBroker(),
	}
	rr := httptest.NewRecorder()
	d.TriggerStreamBuffer(context.Background(), rr, req, streamBufferPayloadIssues)

	require.Equal(t, rr.Code, 200)
	require.Len(t, rr.Body.Bytes(), 0)

	// Check the payload received by the test server
	require.Equal(t, receivedAuthHeader, "Bearer apiToken")
	require.Equal(t, receivedPayload.StreamName, "stream1")
	require.Equal(t, receivedPayload.SessionID, "session1")
	require.Equal(t, receivedPayload.IsActive, true)
	require.Equal(t, receivedPayload.IsHealthy, false)
	require.Len(t, receivedPayload.Tracks, 1)
	require.Contains(t, receivedPayload.Tracks, "track1")
	require.Equal(t, receivedPayload.HumanIssues, []string{"Stream is feeling under the weather"})
}
