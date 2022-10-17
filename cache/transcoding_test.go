package cache

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

func TestStoreAndRetrieveTranscoding(t *testing.T) {
	c := NewStreamCache()
	c.Transcoding.Store("some-stream-name", SegmentInfo{
		CallbackUrl: "some-callback-url",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})

	si := c.Transcoding.Get("some-stream-name")
	require.NotNil(t, si)
	require.Equal(t, "some-callback-url", si.CallbackUrl)
	require.Equal(t, "s3://source", si.Source)
	require.Equal(t, "upload-dir", si.UploadDir)
	require.Equal(t, []string{"s3://destination-1", "s3://destination-2"}, si.Destinations)
}

func TestStoreAndRemoveTranscoding(t *testing.T) {
	c := NewStreamCache()
	c.Transcoding.Store("some-stream-name", SegmentInfo{
		CallbackUrl: "some-callback-url",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})
	require.NotNil(t, c.Transcoding.Get("some-stream-name"))

	c.Transcoding.Remove("some-stream-name")
	require.Nil(t, c.Transcoding.Get("some-stream-name"))
}

func TestHeartbeatsAreFiredWithInterval(t *testing.T) {
	// Create a stub server to receive the callbacks and a variable to track how many we get
	var requests = map[string]int{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check the message is a valid TranscodeStatusMessage
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var tsm clients.TranscodeStatusMessage
		require.NoError(t, json.Unmarshal(body, &tsm))

		// Increment our counter for the stream ID, which comes on the final part of our URL
		parts := strings.Split(r.URL.Path, "/")
		require.NotZero(t, len(parts), 0, "Expected "+r.URL.Path+" to have some slashes in")
		id := parts[len(parts)-1]
		requests[id] += 1
	}))
	defer ts.Close()

	// Add 2 jobs into the stream cache with different names
	c := NewStreamCache()
	c.Transcoding.Store("some-stream-name", SegmentInfo{
		CallbackUrl: ts.URL + "/some-stream-name",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})
	c.Transcoding.Store("some-stream-name-2", SegmentInfo{
		CallbackUrl: ts.URL + "/some-stream-name-2",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})

	// Start the callback loop
	heartbeatStop := make(chan bool)
	go c.Transcoding.SendTranscodingHeartbeats(200*time.Millisecond, heartbeatStop)
	defer func() { heartbeatStop <- true }()

	// Wait for a few iterations
	time.Sleep(time.Second)

	// Check that we got roughly the amount of callbacks we'd expect
	require.GreaterOrEqual(t, requests["some-stream-name"], 3)
	require.LessOrEqual(t, requests["some-stream-name"], 10)

	require.GreaterOrEqual(t, requests["some-stream-name-2"], 3)
	require.LessOrEqual(t, requests["some-stream-name-2"], 10)
}
