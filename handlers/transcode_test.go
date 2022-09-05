//go:build externaldeps
// +build externaldeps

/* Depends:
	 - Mist binaries located at ../../mistserver/build/
   - ../mist_transcoding.json is merged to Mist config
	 - Mist running on port 4242
	 - checkedout go-livepeer branch mock_transcoder && start `go run cmd/offchain_transcoder/transcoder.go`
	 cd handlers
	 export GCP_SECRETS="some-s3-id:some-s3-secret"
	 go test . -v -count 1 -tags externaldeps -run TestSegmentTranscode
*/

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

const port int = 4949
const mistPort int = 4242

// const bPort int = 8935

func TestSegmentTranscode(t *testing.T) {
	mc := &MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", mistPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", port),
	}

	// Setup our HTTP endpoints:
	router := httprouter.New()
	sc := NewStreamCache()
	catalystApiHandlers := &CatalystAPIHandlersCollection{MistClient: mc, StreamCache: sc}
	mistCallbackHandlers := &MistCallbackHandlersCollection{MistClient: mc, StreamCache: sc}
	router.POST("/api/transcode/file", catalystApiHandlers.TranscodeSegment(mistProcPath))
	router.POST("/api/mist/trigger", mistCallbackHandlers.Trigger())
	// Setup callback capture server
	callbacks := make(chan string, 10)
	callbackServer := newStudioMock(callbacks)
	defer callbackServer.Close()
	// Prepare transcode request json payload
	jsonData := strings.ReplaceAll(transcodeJsonData, "CALLBACK_URL", callbackServer.URL)
	credentials := os.Getenv("GCP_SECRETS")
	require.NotEqual(t, "", credentials, "set environment variable GCP_SECRETS to id:secret. Use urlencoded strings")
	jsonData = strings.ReplaceAll(jsonData, "GCP_SECRETS", credentials)
	// Start our API server to be tested
	stopApi := serveAPI(port, router)
	defer stopApi()
	// Send HTTP request
	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rr, req)
	// Check for response code
	require.Equal(t, 200, rr.Result().StatusCode)
	require.Equal(t, "Transcode done; Upload in progress", rr.Body.String())
	// Wait for callbacks. 200 response code indicates transcoding is complete, we are still waiting for renditions to be stored into s3 destination.
	jsonMessages := readItems(t, callbacks, 3, 7*time.Second)
	// Must find 2x segment_rendition_upload and 1x segment_transcode
	for _, message := range jsonMessages {
		require.Equal(t, "", message.Error, "%s", message)
		require.Truef(t, "segment_transcode" == message.Status || "segment_rendition_upload" == message.Status, "%s", message)
	}
}

// Helper func to gather callback json data or time out
func readItems(t *testing.T, queue chan string, count int, deadline time.Duration) []*clients.TranscodeStatusMessage {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(deadline))
	defer cancel()
	items := make([]string, 0, count)
	for len(items) < count {
		select {
		case item := <-queue:
			items = append(items, item)
		case <-ctx.Done():
			require.FailNow(t, "Transcoding taking too long", "Got %d of %d callbacks", len(items), count)
		}
	}
	messages := make([]*clients.TranscodeStatusMessage, 0, len(items))
	for _, item := range items {
		msg := clients.TranscodeStatusMessage{}
		err := json.Unmarshal([]byte(item), &msg)
		require.NoError(t, err)
		messages = append(messages, &msg)
	}
	return messages
}

// Helper func to capture callbacks for later inspection
func newStudioMock(callbacks chan string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("newStudioMock error reading req body")
			w.WriteHeader(451)
			return
		}
		w.WriteHeader(200)
		body := string(payload)
		log.Printf("[studio callback] %s", body)
		callbacks <- body
	}))
}

// Helper func to run API server in background
func serveAPI(port int, router *httprouter.Router) func() {
	server := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", port), Handler: router}
	go func() {
		// start API server
		if err := server.ListenAndServe(); err != nil {
			log.Printf("server.ListenAndServe %v", err)
		}
	}()
	return func() {
		// stop API server
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("server.Shutdown %v", err)
		}
	}
}

// HTTP request template we want to test
var transcodeJsonData = `{
	"source_location": "/home/alex/livepeer/vod/avsample.mp4",
		"callback_url": "CALLBACK_URL/",
		"manifestID": "somestream",
		"profiles": [
			{
					"name": "720p",
						"width": 1280,
						"height": 720,
						"bitrate": 700000,
						"fps": 24
					}, {
					"name": "360p",
						"width": 640,
						"height": 360,
						"bitrate": 200000,
						"fps": 24
					}
			],
		"verificationFreq": 1
	}`
