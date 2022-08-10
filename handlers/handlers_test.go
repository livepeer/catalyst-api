package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	router.GET("/ok", CatalystAPIHandlers.Ok())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}

func TestSegmentCallback(t *testing.T) {
	callbacks := NewWebhookReceiver(8080)
	defer callbacks.Stop()

	var jsonData = []byte(`{
		"source_location": "http://localhost/input",
		"callback_url": "http://localhost:8080/callback",
		"manifestID": "somestream",
		"profiles": [
			{
				"name": "720p",
				"width": 1280,
				"height": 720,
				"bitrate": 700000,
				"fps": 30
			}, {
				"name": "360p",
				"width": 640,
				"height": 360,
				"bitrate": 200000,
				"fps": 30
			}
		],
		"verificationFreq": 1
	}`)

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/transcode/file", CatalystAPIHandlers.TranscodeSegment())
	router.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Result().StatusCode)
	require.Equal(t, "OK", rr.Body.String())

	// Wait for callback
	event := string(callbacks.WaitForCallback(t, 300*time.Millisecond))
	require.Equal(t, `{"source_location":"http://localhost/input","status":"error","error_message":"NYI - not yet implemented"}`, event)
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

	router := httprouter.New()

	router.POST("/api/transcode/file", CatalystAPIHandlers.TranscodeSegment())
	for _, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(400, rr.Result().StatusCode, string(payload))
	}
}

func TestSuccessfulVODUploadHandler(t *testing.T) {
	require := require.New(t)

	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://localhost/output"
			},
			{
				"type": "pinata",
				"pinata_access_key": "abc"
			}
		]
	}`)

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", CatalystAPIHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 200)
	require.Equal(rr.Body.String(), "2")
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)

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
		// missing output_locatoins
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

	router.POST("/api/vod", CatalystAPIHandlers.UploadVOD())
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
	router.POST("/api/vod", CatalystAPIHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 415)
	require.JSONEq(rr.Body.String(), `{"error": "Requires application/json content type"}`)
}
