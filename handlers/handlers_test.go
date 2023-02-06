package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/mokeypatching"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
	"github.com/stretchr/testify/require"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{}
	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	router.GET("/ok", catalystApiHandlers.Ok())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}

func TestSegmentBodyFormat(t *testing.T) {
	require := require.New(t)

	badRequests := [][]byte{
		// missing source_location
		[]byte(`{
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"name": "t","width": 1280,"height": 720,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing callback_url
		[]byte(`{
			"source_location": "http://localhost/input",
			"manifestID": "somestream",
			"profiles": [{"name": "t","width": 1280,"height": 720,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing manifestID
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"profiles": [{"name": "t","width": 1280,"height": 720,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing profiles
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"verificationFreq": 1
		}`),
		// missing name
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"width": 1280,"height": 720,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing width
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"name": "t","height": 720,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing height
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"name": "t","width": 1280,"bitrate": 70000,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing bitrate
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"name": "t","width": 1280,"height": 720,"fps": 30}],
			"verificationFreq": 1
		}`),
		// missing verificationFreq
		[]byte(`{
			"source_location": "http://localhost/input",
			"callback_url": "http://localhost:8080/callback",
			"manifestID": "somestream",
			"profiles": [{"name": "t","width": 1280,"height": 720,"bitrate": 70000,"fps": 30}]
		}`),
	}

	catalystApiHandlers := CatalystAPIHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	router := httprouter.New()

	router.POST("/api/transcode/file", catalystApiHandlers.TranscodeSegment())
	for _, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(400, rr.Result().StatusCode, string(payload))
	}
}

func TestVODHandlerProfiles(t *testing.T) {
	// Create temporary manifest + segment files on the local filesystem
	inputUrl, outputUrl := createTempManifests(t)

	// Define profiles
	profiles := []video.EncodedProfile{
		{Name: "p360", Width: 640, Height: 360, Bitrate: 200000, FPS: 24},
		{Name: "p240", Width: 427, Height: 240, Bitrate: 100000, FPS: 24},
	}

	// Mock Broadcaster node returning valid response
	var seq int
	videoData, err := os.ReadFile("lpt.ts")
	require.NoError(t, err)
	broadcasterMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		seq += 1
		// discard payload
		_, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		// return multipart response
		boundary := config.RandomTrailer(10)
		accept := req.Header.Get("Accept")
		if accept != "multipart/mixed" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		contentType := "multipart/mixed; boundary=" + boundary
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		multipart := multipart.NewWriter(w)
		defer multipart.Close()
		err = multipart.SetBoundary(boundary)
		require.NoError(t, err)
		for i := 0; i < len(profiles); i++ {
			fileName := fmt.Sprintf(`"%s_%d%s"`, profiles[i].Name, seq, ".ts")
			hdrs := textproto.MIMEHeader{
				"Content-Type":        {"video/mp2t" + "; name=" + fileName},
				"Content-Length":      {strconv.Itoa(len(videoData))},
				"Content-Disposition": {"attachment; filename=" + fileName},
				"Rendition-Name":      {profiles[i].Name},
			}
			part, err := multipart.CreatePart(hdrs)
			require.NoError(t, err)
			_, err = io.Copy(part, bytes.NewReader(videoData))
			require.NoError(t, err)
		}
	}))
	defer broadcasterMock.Close()
	patch_cleanup := changeDefaultBroadcasterUrl(t, broadcasterMock.URL)
	defer patch_cleanup()

	// Start callback server to record reported events
	callbacks := make(chan *clients.TranscodeStatusMessage, 100)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		message := clients.TranscodeStatusMessage{}
		err = json.Unmarshal(payload, &message)
		require.NoError(t, err)
		callbacks <- &message
	}))
	defer callbackServer.Close()

	// Set up out own callback client so that we can ensure it just fires once
	statusClient := clients.NewPeriodicCallbackClient(100 * time.Minute)
	// Workflow engine
	vodEngine := pipeline.NewStubCoordinatorOpts("", statusClient, nil, nil)
	internalState := vodEngine.Jobs.UnittestIntrospection()

	// Setup handlers to test
	mistCallbackHandlers := &misttriggers.MistCallbackHandlersCollection{VODEngine: vodEngine}
	catalystApiHandlers := CatalystAPIHandlersCollection{VODEngine: vodEngine}
	var jsonData = fmt.Sprintf(`{
		"url": "%s",
		"callback_url": "%s",
		"profiles": [
			{"name": "p360", "width": 640, "height": 360, "bitrate": 200000, "fps": 24},
			{"name": "p240", "width": 427, "height": 240, "bitrate": 100000, "fps": 24}
		],
		"output_locations": [{
			"type": "object_store",
			"url": "%s",
			"outputs": {"source_segments": true,"transcoded_segments": true}
		}]
	}`, inputUrl, callbackServer.URL, outputUrl)
	router := httprouter.New()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.POST("/api/mist/trigger", mistCallbackHandlers.Trigger())

	// First request goes to /api/vod
	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Result().StatusCode)

	// Wait for the request to run its course, then fire callbacks
	time.Sleep(time.Second)
	statusClient.SendCallbacks()

	// Waiting for SendTranscodeStatus(TranscodeStatusPreparing, 0.2)
	segmentingDeadline, segmentingCancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer segmentingCancelCtx()
waitsegmenting:
	for {
		select {
		case message := <-callbacks:
			// 0.2 is final progress reported for TranscodeStatusPreparing from UploadVOD()
			if message.Status == clients.TranscodeStatusPreparing && message.CompletionRatio == clients.OverallCompletionRatio(clients.TranscodeStatusPreparing, 0.3) {
				break waitsegmenting
			}
			// Fail on any error in callbacks
			require.Equal(t, "", message.Error, "received error callback")
		case <-segmentingDeadline.Done():
			require.FailNow(t, "expected segmenting callback, never received")
		}
	}
	// Request to /api/vod should store profiles in server state. Check for stored record in internal state
	var streamName string
	require.True(t, func() bool {
		for name, info := range *internalState {
			correctCount := len(info.Profiles) == 2
			correctNames := info.Profiles[0].Name == "p360" && info.Profiles[1].Name == "p240"
			if correctCount && correctNames {
				streamName = name
				return true
			}
		}
		return false
	}())

	// Second request invokes trigger
	triggerPayload := fmt.Sprintf("%s\n%s\nhls\n1234\n3\n1667489141941\n1667489144941\n10000\n0\n10000", streamName, outputUrl)
	req, _ = http.NewRequest("POST", "/api/mist/trigger", bytes.NewBuffer([]byte(triggerPayload)))
	req.Header.Set("X-Trigger", "RECORDING_END")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Result().StatusCode, "trigger handler failed")

	// Check we received proper callback events
	time.Sleep(10 * time.Second)
	statusClient.SendCallbacks()
	deadline, cancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCtx()
	for {
		select {
		case message := <-callbacks:
			if message.Status == clients.TranscodeStatusCompleted {
				return
			}
			require.Equal(t, "", message.Error, "received error callback")
		case <-deadline.Done():
			require.FailNow(t, "expected success callback, never received")
		}
	}
}

func TestSuccessfulVODUploadHandler(t *testing.T) {
	require := require.New(t)

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer callbackServer.Close()

	catalystApiHandlers := CatalystAPIHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	var jsonData = `{
		"url": "http://localhost/input",
		"callback_url": "CALLBACK_URL",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://localhost/output.m3u8",
 				"outputs": {
					"source_segments": true,
					"transcoded_segments": true
				}
			},
			{
				"type": "pinata",
				"pinata_access_key": "abc",
 				"outputs": {
					"transcoded_segments": true
				}
			}
		]
	}`
	jsonData = strings.ReplaceAll(jsonData, "CALLBACK_URL", callbackServer.URL)

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)
	require.Equal(http.StatusOK, rr.Result().StatusCode)

	var uvr UploadVODResponse
	require.NoError(json.Unmarshal(rr.Body.Bytes(), &uvr))
	require.Greater(len(uvr.RequestID), 1) // Check that we got some value for Request ID
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	badRequests := [][]byte{
		// missing url
		[]byte(`{
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// missing callback_url
		[]byte(`{
			"url": "http://localhost/input",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// missing output_locations
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback"
		}`),
		// invalid url
		[]byte(`{
			"url": "x://}]:&7@localhost/",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// invalid callback_url
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "x://}]:&7@localhost/",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// invalid output_location's object_store url
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "x://}]:&7@localhost/" } ]
		}`),
		// invalid output_location type
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "foo", "url": "http://localhost/" } ]
		}`),
		// invalid output_location's pinata params
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "pinata", "pinata_access_key": "" } ]
		}`),
	}

	router := httprouter.New()

	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	for _, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(rr.Result().StatusCode, 400)
	}
}

func TestWrongContentTypeVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			{
				"type": "object_store",
				"url": "http://localhost/"
			}
		]
	}`)

	router := httprouter.New()
	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(415, rr.Result().StatusCode)
	require.JSONEq(rr.Body.String(), `{"error": "Requires application/json content type", "error_detail":""}`)
}

func storeTestFile(t *testing.T, path, name, data string) {
	// Store to dir
	storage, err := drivers.ParseOSURL("file://"+path, true)
	require.NoError(t, err)
	_, err = storage.NewSession("").SaveData(context.Background(), name, bytes.NewReader([]byte(data)), nil, 1*time.Second)
	require.NoError(t, err)
}

func createTempManifests(t *testing.T) (string, string) {
	drivers.Testing = true
	tmpDir := os.TempDir()
	dir := path.Join(tmpDir, "/live/peer/test")
	sourceDir := path.Join(tmpDir, "/live/peer/test/source")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(sourceDir, os.ModePerm)
	require.NoError(t, err)

	storeTestFile(t, dir, "manifest.m3u8", exampleMediaManifest)
	storeTestFile(t, sourceDir, "manifest.m3u8", exampleMediaManifest)
	storeTestFile(t, dir, "0.ts", "segment data")
	storeTestFile(t, sourceDir, "0.ts", "segment data")
	storeTestFile(t, dir, "5000.ts", "lots of segment data")
	storeTestFile(t, sourceDir, "5000.ts", "lots of segment data")
	storedManifest := fmt.Sprintf("file://%s", path.Join(dir, "manifest.m3u8"))
	// Just in tests we use same manifest for segmenting request and same for transcoding
	return storedManifest, storedManifest
}

func changeDefaultBroadcasterUrl(t *testing.T, testingServerURL string) func() {
	var err error
	mokeypatching.MonkeypatchingMutex.Lock()
	// remember default value
	originalUrl := config.DefaultBroadcasterURL
	originalClient := transcode.LocalBroadcasterClient
	originalMistDir := config.PathMistDir
	// Modify configuration
	config.DefaultBroadcasterURL = testingServerURL
	transcode.LocalBroadcasterClient, err = clients.NewLocalBroadcasterClient(testingServerURL)
	// Modify PathMistDir configuration
	require.NoError(t, err)
	tempDir := t.TempDir()
	script := path.Join(tempDir, "MistInHLS")
	err = os.WriteFile(script, []byte("#!/bin/sh\necho livepeer\n"), 0744)
	require.NoError(t, err)
	config.PathMistDir = tempDir

	return func() {
		// Restore original values
		config.DefaultBroadcasterURL = originalUrl
		transcode.LocalBroadcasterClient = originalClient
		config.PathMistDir = originalMistDir
		mokeypatching.MonkeypatchingMutex.Unlock()
	}
}

const exampleMediaManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`
