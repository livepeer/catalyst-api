package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/mokeypatching"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/transcode"
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
					"hls": "enabled"
				}
			},
			{
				"type": "pinata",
				"pinata_access_key": "abc",
 				"outputs": {
					"hls": "enabled"
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
		// none of outputs enabled: hls or mp4
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output.m3u8", "outputs": {} } ]
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

func createTempManifest(t *testing.T, dir string) string {
	drivers.Testing = true

	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	storeTestFile(t, dir, "index.m3u8", exampleMediaManifest)
	storeTestFile(t, dir, "0.ts", "segment data")
	storeTestFile(t, dir, "5000.ts", "lots of segment data")
	storedManifest := fmt.Sprintf("file://%s", path.Join(dir, "index.m3u8"))
	return storedManifest
}

func changeDefaultBroadcasterUrl(t *testing.T, testingServerURL string) func() {
	var err error
	mokeypatching.MonkeypatchingMutex.Lock()
	// remember default value
	originalUrl := config.DefaultBroadcasterURL
	originalClient := transcode.LocalBroadcasterClient
	// Modify configuration
	config.DefaultBroadcasterURL = testingServerURL
	transcode.LocalBroadcasterClient, err = clients.NewLocalBroadcasterClient(testingServerURL)
	// Modify PathMistDir configuration
	require.NoError(t, err)
	tempDir := t.TempDir()
	script := path.Join(tempDir, "MistInHLS")
	err = os.WriteFile(script, []byte("#!/bin/sh\necho livepeer\n"), 0744)
	require.NoError(t, err)

	return func() {
		// Restore original values
		config.DefaultBroadcasterURL = originalUrl
		transcode.LocalBroadcasterClient = originalClient
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
