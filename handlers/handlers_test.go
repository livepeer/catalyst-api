package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

const mistProcPath = "../../mistserver/build/MistProcLivepeer"

func TestSegmentTranscode(t *testing.T) {
	port := 4949
	mistPort := 4242
	bPort := 8935
	mc := &MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", mistPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", port),
	}
	err := mc.RemoveAllStreams()
	require.NoError(t, err)
	err = mc.DeleteAllTriggers()
	require.NoError(t, err)

	router := httprouter.New()
	sc := make(map[string]StreamInfo)
	catalystApiHandlers := &CatalystAPIHandlersCollection{MistClient: mc, StreamCache: sc}
	mistCallbackHandlers := &MistCallbackHandlersCollection{MistClient: mc, StreamCache: sc}
	router.POST("/api/transcode/file", catalystApiHandlers.TranscodeSegment(bPort, mistProcPath))
	router.POST("/api/mist/trigger", mistCallbackHandlers.Trigger())

	callbacks := make(chan string, 10)
	callbackServer := newStudioMock(callbacks)
	defer callbackServer.Close()
	jsonData := strings.ReplaceAll(transcodeJsonData, "CALLBACK_URL", callbackServer.URL)

	stopApi := serveAPI(port, router)
	defer stopApi()

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Result().StatusCode)
	require.Equal(t, "OK", rr.Body.String())

	// Wait for callback
	// select {
	// case data := <-callbacks:
	// 	message := &clients.TranscodeStatusMessage{}
	// 	err := json.Unmarshal([]byte(data), message)
	// 	require.NoErrorf(t, err, "json unmarshal failed, src=%s", data)
	// 	// require.Equal(t, "error", message.Status)
	// case <-time.After(3300 * time.Millisecond):
	// 	require.FailNow(t, "Callback not fired by handler")
	// }
}

var transcodeJsonData = `{
	"source_location": "s3+https://redacted:redacted@storage.googleapis.com/alexk-dms-upload-test/avsample.mp4",
		"callback_url": "CALLBACK_URL",
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

func TestSegmentCallback(t *testing.T) {
	var jsonData = `{
		"source_location": "http://localhost/input",
		"callback_url": "CALLBACK_URL",
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
	}`
	bPort := 8935
	callbacks := make(chan string, 10)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("WebhookReceiver error reading req body\n")
			w.WriteHeader(451)
			return
		}
		w.WriteHeader(200)
		callbacks <- string(payload)
	}))
	defer callbackServer.Close()
	jsonData = strings.ReplaceAll(jsonData, "CALLBACK_URL", callbackServer.URL)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/transcode/file", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/transcode/file", catalystApiHandlers.TranscodeSegment(bPort, mistProcPath))
	router.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Result().StatusCode)
	require.Equal(t, "OK", rr.Body.String())

	// Wait for callback
	select {
	case data := <-callbacks:
		message := &clients.TranscodeStatusMessage{}
		err := json.Unmarshal([]byte(data), message)
		require.NoErrorf(t, err, "json unmarshal failed, src=%s", data)
		require.Equal(t, "error", message.Status)
		require.Equal(t, "NYI - not yet implemented", message.Error)
	case <-time.After(300 * time.Millisecond):
		require.FailNow(t, "Callback not fired by handler")
	}
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
	bPort := 8935
	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
	router := httprouter.New()

	router.POST("/api/transcode/file", catalystApiHandlers.TranscodeSegment(bPort, mistProcPath))
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

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer callbackServer.Close()

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}, StreamCache: make(map[string]StreamInfo)}
	var jsonData = `{
		"url": "http://localhost/input",
		"callback_url": "CALLBACK_URL",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://localhost/output",
 				"outputs": {
					"source_segments": true
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
	require.Equal("2", rr.Body.String())
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
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

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
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

	require.Equal(rr.Result().StatusCode, 415)
	require.JSONEq(rr.Body.String(), `{"error": "Requires application/json content type"}`)
}

func newStudioMock(callbacks chan string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("newStudioMock error reading req body\n")
			w.WriteHeader(451)
			return
		}
		w.WriteHeader(200)
		body := string(payload)
		fmt.Printf("[studio callback] %s\n", body)
		callbacks <- body
	}))
}

func serveAPI(port int, router *httprouter.Router) func() {
	server := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", port), Handler: router}
	go func() {
		// start API server
		if err := server.ListenAndServe(); err != nil {
			fmt.Printf("server.ListenAndServe %v\n", err)
		}
	}()
	return func() {
		// stop API server
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			fmt.Printf("server.Shutdown %v\n", err)
		}
	}
}
